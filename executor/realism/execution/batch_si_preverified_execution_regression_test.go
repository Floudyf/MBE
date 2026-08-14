package execution

import (
	"context"
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBatchSIPreverifiedConsensusPlanMatchesFullVerification(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
		batchSITestTx("T3", []string{"a"}, []string{"c"}),
		batchSITestTx("T4", []string{"b"}, []string{"d"}),
	}
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	b.Height = planned.Plan.BlockHeight
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{
		AlgorithmID:   BatchSIPlanAlgorithmID,
		PayloadDigest: batchSITextDigest(string(raw)),
		PlanDigest:    planned.Plan.PlanDigest,
		Payload:       raw,
	}

	fullExecutor := NewBatchSIExecutor(config)
	full, err := fullExecutor.ExecuteBlockWithCommitment(context.Background(), b, map[string]string{}, nil)
	if err != nil {
		t.Fatalf("full verification path failed: %v", err)
	}
	parsed, err := ParseBatchSIPlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	fastExecutor := NewBatchSIExecutor(config)
	fast, err := fastExecutor.ExecuteConsensusPlanWithCommitment(context.Background(), b, map[string]string{}, nil, parsed, true)
	if err != nil {
		t.Fatalf("preverified path failed: %v", err)
	}

	if full.StateRootAfter != fast.StateRootAfter || full.ReceiptRoot != fast.ReceiptRoot || full.PlanDigest != fast.PlanDigest {
		t.Fatalf("preverified path changed execution result: full=%s/%s/%s fast=%s/%s/%s", full.StateRootAfter, full.ReceiptRoot, full.PlanDigest, fast.StateRootAfter, fast.ReceiptRoot, fast.PlanDigest)
	}
	if !reflect.DeepEqual(full.Receipts, fast.Receipts) || !reflect.DeepEqual(full.TxDeltas, fast.TxDeltas) {
		t.Fatal("preverified path changed receipts or transaction deltas")
	}
	if fullExecutor.Metrics.PlanVerificationMode != "full_recompute" {
		t.Fatalf("unexpected full verification mode: %q", fullExecutor.Metrics.PlanVerificationMode)
	}
	if fastExecutor.Metrics.PlanVerificationMode != "preverified_projection" {
		t.Fatalf("unexpected preverified mode: %q", fastExecutor.Metrics.PlanVerificationMode)
	}
}

func TestBatchSIPreverifiedProjectionRejectsAcceptedOrderTamper(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
	}
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Ordered) != 2 {
		t.Fatalf("fixture should accept both transactions, got %d", len(planned.Ordered))
	}
	b := batchSITestBlock(planned.Ordered[1], planned.Ordered[0])
	b.Height = planned.Plan.BlockHeight
	if err := ValidateBatchSIPlanProjection(b, planned.Plan, config); err == nil {
		t.Fatal("preverified projection must reject a block whose accepted order differs from the consensus plan")
	}
}
