package execution

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func fairnessBatchSITx(id, key string) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID:             id,
		AccessListSchema: "batch_si_worker_pool_fixture_v1",
		AccessListSource: "fixture",
		AccessList: []tx.AccessItem{{
			Key:             key,
			Mode:            tx.AccessWrite,
			UpdateSemantics: "set",
		}},
	}
}

func TestBatchSIUsesOneWorkerPoolAcrossBatchBarriers(t *testing.T) {
	cfg := DefaultBatchSIConfig()
	cfg.WorkerCount = 2
	candidate := block.Block{ShardID: "s0", Height: 1, TxList: []tx.SignedTransaction{
		fairnessBatchSITx("T1", "a"),
		fairnessBatchSITx("T2", "a"),
		fairnessBatchSITx("T3", "b"),
		fairnessBatchSITx("T4", "b"),
	}}
	planned, err := BuildBatchSIPlan(candidate, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Plan.Batches) < 2 {
		t.Fatalf("fixture did not create multiple batches: %d", len(planned.Plan.Batches))
	}
	candidate.TxList = append([]tx.SignedTransaction(nil), planned.Ordered...)
	candidate.TxIDs = make([]string, 0, len(candidate.TxList))
	for _, item := range candidate.TxList {
		candidate.TxIDs = append(candidate.TxIDs, item.TxID)
	}
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	candidate.ExecutionPlan = &block.ExecutionPlanEnvelope{
		AlgorithmID:   BatchSIPlanAlgorithmID,
		PayloadDigest: batchSITextDigest(string(raw)),
		PlanDigest:    planned.Plan.PlanDigest,
		Payload:       raw,
	}
	executor := NewBatchSIExecutor(cfg)
	if _, err := executor.ExecuteConsensusPlanWithCommitment(context.Background(), candidate, map[string]string{}, nil, planned.Plan, true); err != nil {
		t.Fatal(err)
	}
	if executor.Metrics.WorkerPoolCreateCount != 1 {
		t.Fatalf("worker pool create count=%d want=1", executor.Metrics.WorkerPoolCreateCount)
	}
	if executor.Metrics.BatchBarrierCount != len(planned.Plan.Batches) {
		t.Fatalf("barrier count=%d want=%d", executor.Metrics.BatchBarrierCount, len(planned.Plan.Batches))
	}
	if executor.Metrics.ExecutionTaskCount != len(candidate.TxList) {
		t.Fatalf("execution task count=%d want=%d", executor.Metrics.ExecutionTaskCount, len(candidate.TxList))
	}
}
