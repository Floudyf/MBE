package execution

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBatchSIWorkerScalingBoundaryAllows32Rejects33(t *testing.T) {
	cfg := DefaultBatchSIConfig()
	cfg.WorkerCount = 32
	if err := cfg.Validate(); err != nil {
		t.Fatalf("worker_count=32 must be accepted for the formal scaling experiment: %v", err)
	}
	cfg.WorkerCount = 33
	if err := cfg.Validate(); err == nil {
		t.Fatal("worker_count=33 must remain outside the formal scaling bound")
	}
}

func TestBatchSIWorkerScaling32PreservesDeterministicResult(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
		batchSITestTx("T3", []string{"a"}, []string{"c"}),
		batchSITestTx("T4", []string{"b"}, []string{"d"}),
	}
	planCfg := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), planCfg)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
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

	roots := map[string]bool{}
	receipts := map[string]bool{}
	for _, workers := range []int{2, 4, 8, 16, 32} {
		cfg := planCfg
		cfg.WorkerCount = workers
		result, err := NewBatchSIExecutor(cfg).ExecuteBlock(context.Background(), b, map[string]string{})
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		roots[result.StateRootAfter] = true
		receipts[result.ReceiptRoot] = true
		if result.WorkerCount != workers {
			t.Fatalf("workers=%d reported worker_count=%d", workers, result.WorkerCount)
		}
	}
	if len(roots) != 1 || len(receipts) != 1 {
		t.Fatalf("worker scaling changed deterministic output: roots=%v receipts=%v", roots, receipts)
	}
}
