package execution

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func v14bComparableDeltas(input []TxDelta) []TxDelta {
	out := append([]TxDelta(nil), input...)
	for index := range out {
		out[index].Receipt.StateRootAfterTx = ""
	}
	return out
}

func v14bDirectBlock(t *testing.T) (map[string]string, []tx.SignedTransaction) {
	t.Helper()
	first := universalDirectAccessTransaction()
	first.TxID = "v14b-direct-1"
	first.Timestamp = 11
	second := universalDirectAccessTransaction()
	second.TxID = "v14b-direct-2"
	second.Timestamp = 12
	base := map[string]string{
		"s0::world/read":  "seed-read",
		"s0::world/rmw":   "seed-rmw",
		"s0::world/write": "seed-write",
	}
	return base, []tx.SignedTransaction{first, second}
}

func TestV14BSerialDeltaOnlyMatchesFullTransactionEvidence(t *testing.T) {
	base, items := v14bDirectBlock(t)
	b := blockForExecutionTest(items)

	full := NewSerialExecutor().ExecuteBlock(b, base)
	deltaOnly := NewSerialExecutor().ExecuteBlockDeltas(b, base)

	if deltaOnly.StateRootBefore != "" || deltaOnly.StateRootAfter != "" || deltaOnly.ReceiptRoot != "" ||
		len(deltaOnly.StateUpdates) != 0 || len(deltaOnly.StateDelta) != 0 {
		t.Fatalf("delta-only serial wave performed durable materialization: %+v", deltaOnly)
	}
	if full.SuccessfulTxs != deltaOnly.SuccessfulTxs || full.FailedTxs != deltaOnly.FailedTxs {
		t.Fatalf("serial success accounting changed: full=%+v delta=%+v", full, deltaOnly)
	}
	if !reflect.DeepEqual(v14bComparableDeltas(full.TxDeltas), v14bComparableDeltas(deltaOnly.TxDeltas)) {
		t.Fatalf("serial delta-only transaction evidence changed\nfull=%#v\ndelta=%#v", full.TxDeltas, deltaOnly.TxDeltas)
	}
}

func TestV14BBlockSTMDeltaOnlyMatchesValidatedTransactionEvidence(t *testing.T) {
	base, items := v14bDirectBlock(t)
	b := blockForExecutionTest(items)

	fullExecutor := NewBlockSTMExecutor(2)
	fullExecutor.ExecutionMode = "performance"
	fullExecutor.OracleMode = "off"
	full, err := fullExecutor.ExecuteBlock(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}

	deltaExecutor := NewBlockSTMExecutor(2)
	deltaExecutor.ExecutionMode = "performance"
	deltaExecutor.OracleMode = "off"
	deltaOnly, err := deltaExecutor.ExecuteBlockDeltas(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}

	if deltaOnly.StateRootBefore != "" || deltaOnly.StateRootAfter != "" || deltaOnly.ReceiptRoot != "" ||
		len(deltaOnly.StateUpdates) != 0 || len(deltaOnly.StateDelta) != 0 {
		t.Fatalf("delta-only Block-STM wave performed durable materialization: %+v", deltaOnly)
	}
	if !reflect.DeepEqual(v14bComparableDeltas(full.TxDeltas), v14bComparableDeltas(deltaOnly.TxDeltas)) {
		t.Fatalf("Block-STM delta-only transaction evidence changed\nfull=%#v\ndelta=%#v", full.TxDeltas, deltaOnly.TxDeltas)
	}
	if deltaExecutor.Metrics.ExecutionTaskCount == 0 || deltaExecutor.Metrics.ValidationTaskCount == 0 {
		t.Fatalf("delta-only path bypassed Block-STM execution/validation: %+v", deltaExecutor.Metrics)
	}
	if deltaExecutor.Metrics.CommittedTransactionCount != len(items) {
		t.Fatalf("delta-only Block-STM committed count mismatch: got=%d want=%d", deltaExecutor.Metrics.CommittedTransactionCount, len(items))
	}
	if deltaExecutor.Metrics.MaterializationMS != 0 || deltaExecutor.Metrics.StateCommitmentMS != 0 {
		t.Fatalf("temporary wave still performed durable Block-STM materialization: %+v", deltaExecutor.Metrics)
	}
}

func TestV14BBlockSTMCorrectnessModeFallsBackToFullMaterialization(t *testing.T) {
	base, items := v14bDirectBlock(t)
	b := blockForExecutionTest(items[:1])
	executor := NewBlockSTMExecutor(1)
	executor.ExecutionMode = "correctness"
	executor.OracleMode = "full"
	got, err := executor.ExecuteBlockDeltas(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}
	if got.StateRootAfter == "" || got.ReceiptRoot == "" {
		t.Fatalf("correctness-mode delta entry must fall back to full execution: %+v", got)
	}
}
