package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBatchSIBlockExecutorUsesRuntimePreverifiedPlan(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("T1", nil, []string{"a"}),
		batchSIV5TestTx("T2", nil, []string{"b"}),
		batchSIV5TestTx("T3", []string{"a"}, []string{"c"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(4))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	plugin := batchSIBlockExecutor{makeBasic("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(4))}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:                 planned.Block,
		BaseStateSnapshot:     map[string]string{},
		NodeID:                "n0",
		ShardID:               "s0",
		WorkerCount:           4,
		ExecutionPlanVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.ActualMetrics["batch_si_executor_plan_verify_mode"]; got != "preverified_projection" {
		t.Fatalf("unexpected plan verification mode: %#v", got)
	}
	if got := result.ActualMetrics["batch_si_executor_full_verify_count"]; got != 0 {
		t.Fatalf("normal consensus execution must skip duplicate full re-plan verification, got %#v", got)
	}
	if got := result.ActualMetrics["batch_si_executor_full_verify_skip_count"]; got != 1 {
		t.Fatalf("missing duplicate verification skip evidence: %#v", got)
	}
	if got, ok := result.ActualMetrics["batch_si_plan_payload_bytes"].(int); !ok || got <= 0 {
		t.Fatalf("missing Batch-SI plan payload size evidence: %#v", result.ActualMetrics["batch_si_plan_payload_bytes"])
	}
}

func TestBatchSIBlockExecutorDirectPathRetainsFullVerification(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("T1", nil, []string{"a"}),
		batchSIV5TestTx("T2", nil, []string{"b"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(2))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	plugin := batchSIBlockExecutor{makeBasic("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(2))}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             planned.Block,
		BaseStateSnapshot: map[string]string{},
		NodeID:            "n0",
		ShardID:           "s0",
		WorkerCount:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.ActualMetrics["batch_si_executor_plan_verify_mode"]; got != "full_recompute" {
		t.Fatalf("direct/recovery-style execution must retain full verification, got %#v", got)
	}
	if got := result.ActualMetrics["batch_si_executor_full_verify_count"]; got != 1 {
		t.Fatalf("missing full verification evidence: %#v", got)
	}
}

func TestBatchSIVerifiedExecutionPlanCacheRequiresExactBlockAndPlanIdentity(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("T1", nil, []string{"a"}),
		batchSIV5TestTx("T2", []string{"a"}, []string{"b"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(2))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	runtime := &NodeRuntime{}
	if runtime.hasVerifiedExecutionPlan(planned.Block) {
		t.Fatal("unverified plan must not be treated as preverified")
	}
	runtime.rememberVerifiedExecutionPlan(planned.Block)
	if !runtime.hasVerifiedExecutionPlan(planned.Block) {
		t.Fatal("exact verified block/plan identity was not remembered")
	}

	tampered := planned.Block
	tampered.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   planned.Block.ExecutionPlan.AlgorithmID,
		PayloadDigest: planned.Block.ExecutionPlan.PayloadDigest,
		PlanDigest:    planned.Block.ExecutionPlan.PlanDigest + "-tampered",
		Payload:       append([]byte(nil), planned.Block.ExecutionPlan.Payload...),
	}
	if runtime.hasVerifiedExecutionPlan(tampered) {
		t.Fatal("plan-digest drift must invalidate the preverification cache")
	}

	tampered = planned.Block
	tampered.BlockHash += "-tampered"
	if runtime.hasVerifiedExecutionPlan(tampered) {
		t.Fatal("block-hash drift must invalidate the preverification cache")
	}
}
