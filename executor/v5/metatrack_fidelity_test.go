package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

func TestMetaTrackRuntimeUsesPerWorkerDualLanesAndDeterministicFinalMerge(t *testing.T) {
	txs := independentTransferTxs(12)
	// Preserve a real signed transaction envelope while marking one transaction
	// as a hard conservative boundary for scheduler-lane coverage. Signature
	// verification occurs before block execution in the real runtime.
	txs[len(txs)-1].SourceKind = "cross_shard_relay"
	b := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	realblock.AssignHash(&b)
	plugin := metaTrackBlockExecutor{makeBasic("block_executor", metaTrackBlockExecutorID, map[string]any{"worker_count": 4})}
	input := BlockExecutionInput{
		Block: b, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: 4,
		Execution: dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)},
		Scheduler: builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)},
	}
	result, err := plugin.ExecuteBlock(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.ActualMetrics["worker_queue_model"] != "per_worker_dual_lane" {
		t.Fatalf("missing per-worker dual-lane evidence: %#v", result.ActualMetrics)
	}
	if result.ActualMetrics["steal_policy"] != "same_shard_same_track_only" || intMetric(t, result.ActualMetrics, "cross_track_steal_count") != 0 {
		t.Fatalf("cross-track stealing must remain disabled: %#v", result.ActualMetrics)
	}
	if result.ActualMetrics["completion_processing_policy"] != "dependency_ready_completion_order_deterministic_final_merge" {
		t.Fatalf("completion processing policy regressed: %#v", result.ActualMetrics)
	}
	if result.ActualMetrics["metatrack_scheduler_evidence_scope"] != "actual_dual_track_runtime" {
		t.Fatalf("unexpected scheduler evidence scope: %#v", result.ActualMetrics)
	}
	if intMetric(t, result.ActualMetrics, "metatrack_classification_transaction_count") != len(txs) {
		t.Fatalf("classification count drifted: %#v", result.ActualMetrics)
	}
	repeat, err := plugin.ExecuteBlock(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExecutionResult.StateRootAfter != repeat.ExecutionResult.StateRootAfter || result.ExecutionResult.ReceiptRoot != repeat.ExecutionResult.ReceiptRoot {
		t.Fatalf("completion timing changed deterministic materialization: first=%s/%s repeat=%s/%s", result.ExecutionResult.StateRootAfter, result.ExecutionResult.ReceiptRoot, repeat.ExecutionResult.StateRootAfter, repeat.ExecutionResult.ReceiptRoot)
	}
}

func TestMetaTrackBlockSTMExportsClassificationScopeWithoutPretendingToUseDualTrackRuntime(t *testing.T) {
	txs := independentTransferTxs(8)
	b := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	realblock.AssignHash(&b)
	plugin := blockSTMBlockExecutor{makeBasic("block_executor", execution.BlockSTMExecutorID, map[string]any{
		"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off", "maximum_incarnations": 0, "incarnation_limit_action": "fail",
	})}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block: b, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: 4,
		Execution: dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ActualMetrics["metatrack_scheduler_evidence_scope"] != "classification_plan_plus_blockstm_runtime" {
		t.Fatalf("Block-STM evidence scope must be explicit: %#v", result.ActualMetrics)
	}
	if value, ok := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); !ok || value {
		t.Fatalf("Block-STM backend must not advertise actual dual-track worker runtime: %#v", result.ActualMetrics)
	}
	if intMetric(t, result.ActualMetrics, "metatrack_classification_transaction_count") != len(txs) {
		t.Fatalf("classification count drifted: %#v", result.ActualMetrics)
	}
}
