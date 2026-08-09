package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBatchSIBlockExecutorPersistsAcceptedAndDeferredTransactionEvidence(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("E1", []string{"k1"}, []string{"k3"}),
		batchSIV5TestTx("E2", []string{"k2"}, []string{"k1"}),
		batchSIV5TestTx("E3", []string{"k3"}, []string{"k2"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(4))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Deferred) == 0 {
		t.Fatal("fixture did not produce first-pass OFAS deferral evidence")
	}
	realblock.AssignHash(&planned.Block)
	plugin := batchSIBlockExecutor{makeBasic("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(4))}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	accepted, ok := result.ActualMetrics["batch_si_accepted_tx_ids"].([]string)
	if !ok {
		t.Fatalf("accepted transaction identity evidence missing: %#v", result.ActualMetrics["batch_si_accepted_tx_ids"])
	}
	deferred, ok := result.ActualMetrics["batch_si_deferred_tx_ids"].([]string)
	if !ok {
		t.Fatalf("deferred transaction identity evidence missing: %#v", result.ActualMetrics["batch_si_deferred_tx_ids"])
	}
	if !sameStringList(accepted, transactionIDs(planned.Block.TxList)) {
		t.Fatalf("accepted transaction identity evidence mismatch: got=%v want=%v", accepted, transactionIDs(planned.Block.TxList))
	}
	if !sameStringList(deferred, transactionIDs(planned.Deferred)) {
		t.Fatalf("deferred transaction identity evidence mismatch: got=%v want=%v", deferred, transactionIDs(planned.Deferred))
	}
}
