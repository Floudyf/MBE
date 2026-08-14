package execution

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestBatchSICompactWireReconstructsCanonicalRichPlan(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"k2"}, []string{"k1", "k4"}),
		batchSITestTx("T2", []string{"k4"}, []string{"k1", "k3"}),
		batchSITestTx("T3", []string{"k3"}, []string{"k2"}),
		batchSITestTx("T4", []string{"k1"}, []string{"k4"}),
		batchSITestTx("T5", []string{"k7"}, []string{"k1", "k2"}),
		batchSITestTx("T6", []string{"k2"}, []string{"k3", "k4"}),
		batchSITestTx("T7", []string{"k1"}, []string{"k2"}),
		batchSITestTx("T8", []string{"k4"}, []string{"k3"}),
	}
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, `"wire_version":"`+BatchSIConsensusWireVersion+`"`) {
		t.Fatalf("compact wire version missing: %s", text)
	}
	if strings.Contains(text, `"order_evidence"`) {
		t.Fatalf("derivable order evidence leaked into compact consensus wire: %s", text)
	}
	if strings.Contains(text, `"transaction_ordinals"`) {
		t.Fatalf("canonical transaction ordinals leaked into compact consensus wire: %s", text)
	}
	parsed, err := ParseBatchSIPlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed, planned.Plan) {
		t.Fatalf("compact wire did not reconstruct the exact rich plan\ngot:  %#v\nwant: %#v", parsed, planned.Plan)
	}
	accepted := batchSITestBlock(planned.Ordered...)
	accepted.Height = planned.Plan.BlockHeight
	if err := VerifyBatchSIPlan(accepted, parsed, DefaultBatchSIConfig()); err != nil {
		t.Fatalf("compact wire must retain full AWRT/WRBP/OFAS verification: %v", err)
	}
}

func TestBatchSICompactWirePreservesNonCanonicalExplicitOrdinals(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T3", []string{"a"}, []string{"b"}),
		batchSITestTx("T1", []string{"c"}, []string{"a"}),
		batchSITestTx("T2", nil, []string{"c"}),
	}
	ordinals := map[string]int{"T3": 30, "T1": 10, "T2": 20}
	planned, err := BuildBatchSIPlanWithOrdinals(batchSITestBlock(items...), DefaultBatchSIConfig(), ordinals)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"transaction_ordinals"`) {
		t.Fatal("non-canonical explicit paper T.id mapping must remain consensus-bound")
	}
	parsed, err := ParseBatchSIPlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed.TransactionOrdinals, ordinals) || parsed.PlanDigest != planned.Plan.PlanDigest {
		t.Fatalf("explicit ordinal mapping drifted: got=%#v want=%#v", parsed.TransactionOrdinals, ordinals)
	}
}

func TestBatchSICompactWireReadsLegacyRichDurablePlan(t *testing.T) {
	planned, err := BuildBatchSIPlan(batchSITestBlock(
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", []string{"a"}, []string{"b"}),
	), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	legacyRaw, err := json.Marshal(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacyRaw), `"wire_version"`) {
		t.Fatal("legacy fixture unexpectedly contains compact wire marker")
	}
	parsed, err := ParseBatchSIPlan(legacyRaw)
	if err != nil {
		t.Fatalf("pre-closure durable rich plan must remain replayable: %v", err)
	}
	if !reflect.DeepEqual(parsed, planned.Plan) {
		t.Fatalf("legacy rich plan changed during compatibility parse")
	}
}

func TestBatchSICompactWireRejectsTamperAndNonDerivableEvidence(t *testing.T) {
	planned, err := BuildBatchSIPlan(batchSITestBlock(
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
		batchSITestTx("T3", []string{"a"}, []string{"c"}),
	), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}

	tamperedPlan := planned.Plan
	tamperedPlan.OrderEvidence = append([]BatchSIOrderEvidence(nil), planned.Plan.OrderEvidence...)
	tamperedPlan.OrderEvidence[0].BatchNumber++
	if _, err := MarshalBatchSIPlan(tamperedPlan); err == nil {
		t.Fatal("marshal must reject rich evidence that no longer matches the bound plan digest")
	}

	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	candidate := wire["candidate_transaction_ids"].([]any)
	candidate[0], candidate[1] = candidate[1], candidate[0]
	tamperedRaw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseBatchSIPlan(tamperedRaw); err == nil {
		t.Fatal("candidate-order tamper must fail the rich plan digest binding")
	}
}

func TestBatchSICompactWireActuallyReducesConsensusPayload(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 128)
	for i := 0; i < 128; i++ {
		id := fmt.Sprintf("T%03d", i+1)
		items = append(items, batchSITestTx(id, []string{fmt.Sprintf("r%d", i%17)}, []string{fmt.Sprintf("w%d", i%31)}))
	}
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	compact, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	rich, err := json.Marshal(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(compact) >= len(rich) {
		t.Fatalf("compact consensus wire did not reduce payload: compact=%d rich=%d", len(compact), len(rich))
	}
	if float64(len(compact))/float64(len(rich)) > 0.70 {
		t.Fatalf("compact consensus wire regression: compact=%d rich=%d ratio=%.3f", len(compact), len(rich), float64(len(compact))/float64(len(rich)))
	}
}
