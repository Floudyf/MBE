package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
)

func TestV13IncrementalCommitmentMatchesFullSnapshotAfterEveryTransaction(t *testing.T) {
	snapshot := map[string]string{
		"s0::alpha": "1",
		"s0::beta":  "2",
		"s1::gamma": "3",
	}
	working := copyRegistryStringMap(snapshot)
	commitment := state.NewCommitment(working)
	if got, want := commitment.Root(), state.RootOfSnapshot(working); got != want {
		t.Fatalf("initial root mismatch: got %s want %s", got, want)
	}
	writeSets := []map[string]string{
		{"alpha": "10"},
		{"beta": "20", "delta": "4"},
		{"alpha": "11", "epsilon": "5"},
	}
	for index, writes := range writeSets {
		for key, value := range writes {
			working[qualifyStateKey("s0", key)] = value
		}
		commitment.Apply(writes, func(key string) string {
			return qualifyStateKey("s0", key)
		})
		if got, want := commitment.Root(), state.RootOfSnapshot(working); got != want {
			t.Fatalf("tx %d incremental root mismatch: got %s want %s", index, got, want)
		}
	}
}

func TestV13MetaTrackIncrementalMaterializationPreservesReceiptRoots(t *testing.T) {
	txs := independentTransferTxs(4)
	got := runMetaTrackExecutorTestBlock(t, 2, txs, nil).ExecutionResult

	block := realblock.Block{
		ShardID:      "s0",
		Height:       1,
		PreviousHash: "genesis",
		ProposerID:   "n0",
		Timestamp:    1,
		TxList:       txs,
	}
	for _, item := range txs {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	want := execution.NewSerialExecutor().ExecuteBlock(block, map[string]string{})

	if got.StateRootBefore != want.StateRootBefore ||
		got.StateRootAfter != want.StateRootAfter ||
		got.ReceiptRoot != want.ReceiptRoot {
		t.Fatalf("incremental MetaTrack materialization changed roots")
	}
	if len(got.Receipts) != len(want.Receipts) {
		t.Fatalf("receipt count mismatch: got %d want %d", len(got.Receipts), len(want.Receipts))
	}
	for index := range got.Receipts {
		if got.Receipts[index].StateRootAfterTx != want.Receipts[index].StateRootAfterTx {
			t.Fatalf("receipt %d state root changed: got %s want %s",
				index, got.Receipts[index].StateRootAfterTx, want.Receipts[index].StateRootAfterTx)
		}
	}
}

func TestV13MetaTrackUsesProvidedBaseCommitmentWithoutChangingSemantics(t *testing.T) {
	txs := independentTransferTxs(2)
	block := realblock.Block{
		ShardID:      "s0",
		Height:       1,
		PreviousHash: "genesis",
		ProposerID:   "n0",
		Timestamp:    1,
		TxList:       txs,
	}
	for _, item := range txs {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	base := map[string]string{"s0::seed": "7"}
	plugin := metaTrackBlockExecutor{makeBasic("block_executor", metaTrackBlockExecutorID, map[string]any{"worker_count": 1})}
	common := BlockExecutionInput{
		Block:             block,
		BaseStateSnapshot: base,
		NodeID:            "n0",
		ShardID:           "s0",
		WorkerCount:       1,
		Execution:         dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)},
		Scheduler:         builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)},
	}
	rebuilt, err := plugin.ExecuteBlock(context.Background(), common)
	if err != nil {
		t.Fatal(err)
	}
	withCommitment := common
	withCommitment.BaseStateCommitment = state.NewCommitment(base)
	cloned, err := plugin.ExecuteBlock(context.Background(), withCommitment)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.ExecutionResult.StateRootAfter != cloned.ExecutionResult.StateRootAfter ||
		rebuilt.ExecutionResult.ReceiptRoot != cloned.ExecutionResult.ReceiptRoot {
		t.Fatalf("CloneOrBuild path changed semantics")
	}
}
