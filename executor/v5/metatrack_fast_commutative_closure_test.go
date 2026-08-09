package v5

import (
	"context"
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func metaTrackCommutativeTestTx(id string, delta int64) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID:       id,
		Sender:     "sender_" + id,
		Receiver:   "receiver_" + id,
		AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: delta}},
	}
}

func TestMetaTrackCommutativeFastTransactionsDoNotFormWAWChain(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 32)
	for index := 0; index < 32; index++ {
		items = append(items, metaTrackCommutativeTestTx(fmt.Sprintf("T%02d", index), 1))
	}
	plugin := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})}
	classification := plugin.ClassifyBatch(BatchClassificationInput{Transactions: items})
	for _, item := range items {
		if got := classification.Decisions[item.TxID].Track; got != "fast" {
			t.Fatalf("commutative tx %s classified %s", item.TxID, got)
		}
		if deps := classification.Dependencies[item.TxID]; len(deps) != 0 {
			t.Fatalf("commutative peers must not form ordinary WAW dependencies: %s -> %#v", item.TxID, deps)
		}
	}
	if classification.CommutativeDependencySuppressedCount != 496 {
		t.Fatalf("unexpected suppressed WAW evidence: got=%d want=496", classification.CommutativeDependencySuppressedCount)
	}
}

func TestMetaTrackCommutativeGroupMaterializesDeterministicallyAndFolds(t *testing.T) {
	items := []tx.SignedTransaction{
		metaTrackCommutativeTestTx("T1", 1),
		metaTrackCommutativeTestTx("T2", 1),
		metaTrackCommutativeTestTx("T3", 1),
		metaTrackCommutativeTestTx("T4", 1),
	}
	block := realblock.Block{Height: 1, ShardID: "s0", TxList: items}
	for _, item := range items {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	executor := metaTrackBlockExecutor{makeBasic("block_executor", metaTrackBlockExecutorID, map[string]any{"worker_count": 4})}
	result, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             block,
		BaseStateSnapshot: map[string]string{"s0::coaccess:hot-update": "0"},
		ShardID:           "s0",
		WorkerCount:       4,
		Execution:         dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})},
		Scheduler:         builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.ExecutionResult.StateUpdates["s0::coaccess:hot-update"]; got != "4" {
		t.Fatalf("commutative final value mismatch: got=%q want=4", got)
	}
	decision := aggregationCommit{makeBasic("commit", "commutative_hot_update_aggregation", nil)}.DecideCommit(CommitInput{
		ShardID: "s0", Height: 1, Transactions: items,
		TxDeltas:          result.ExecutionResult.TxDeltas,
		StateDelta:        result.StateDelta,
		BaseStateSnapshot: map[string]string{"s0::coaccess:hot-update": "0"},
	})
	if !decision.Applied || decision.AggregatedKeyCount != 1 || decision.AggregatedLogicalDeltaCount != 4 {
		t.Fatalf("native MetaTrack folding evidence missing: %#v", decision)
	}
	if decision.PostAggregationPhysicalOps >= decision.PreAggregationPhysicalOps {
		t.Fatalf("folding did not reduce physical operations: %#v", decision)
	}
}

func TestMetaTrackCommutativeBarrierPreservesOrdinaryReadOrder(t *testing.T) {
	c1 := metaTrackCommutativeTestTx("C1", 1)
	c2 := metaTrackCommutativeTestTx("C2", 1)
	reader := tx.SignedTransaction{TxID: "R", Sender: "reader", Receiver: "reader2", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessRead, UpdateSemantics: "read"}}}
	plugin := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})}
	classification := plugin.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{c1, c2, reader}})
	deps := classification.Dependencies[reader.TxID]
	if len(deps) != 2 {
		t.Fatalf("ordinary read must wait for complete preceding commutative prefix: %#v", deps)
	}
}
