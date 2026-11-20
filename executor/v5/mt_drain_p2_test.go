package v5

import (
	"context"
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMTDrainP2ResultEqualsOriginalEmptyExecution(t *testing.T) {
	// Distinct roots, non-trivial committed system state and an intentionally
	// larger view exercise original plan digests and StateUpdates ownership.
	for _, size := range []int{1, 64, 1024} {
		before := make(map[string]string, size+1)
		before["s0::counter"] = "3"
		for i := 0; i < size; i++ {
			before["s0::key:"+string(rune(0x4e00+i))] = "value"
		}
		next, root, err := applyStateDeltaToSnapshotWithCommitment(before, state.NewCommitment(before), []state.StateKV{{Key: "counter", Value: "4"}}, "s0", 2)
		if err != nil {
			t.Fatal(err)
		}
		b := realblock.Block{ShardID: "s0", Height: 2, BlockHash: "empty-system-only", SystemStateDeltas: []realblock.SystemStateDelta{{DeltaID: "d", Key: "counter", Value: "4"}}}
		serial := execution.NewSerialExecutor().ExecuteBlockWithCommitment(b, next, root)
		fast, ok := execution.MTDrainP2EmptyResult(b, next, root, execution.SerialBlockExecutorID, 1, "", "")
		if !ok {
			t.Fatal("serial fast path unavailable")
		}
		assertMTDrainP2Same(t, serial, fast)
		stm := execution.NewBlockSTMExecutor(4)
		stm.ExecutionMode = "performance"
		stm.OracleMode = "off"
		oldSTM, err := stm.ExecuteBlockWithCommitment(context.Background(), b, next, root)
		if err != nil {
			t.Fatal(err)
		}
		newer, ok := execution.MTDrainP2EmptyResult(b, next, root, execution.BlockSTMExecutorID, 4, stm.SchedulerMode, stm.DependencyWaitMode)
		if !ok {
			t.Fatal("stm fast path unavailable")
		}
		assertMTDrainP2Same(t, oldSTM, newer)
		fast.StateUpdates["s0::counter"] = "bad"
		if next["s0::counter"] != "4" {
			t.Fatal("StateUpdates must not alias execution view")
		}
		if got, want := root.Root(), state.RootOfSnapshot(next); got != want {
			t.Fatalf("commitment mismatch %s %s", got, want)
		}
	}
}

func assertMTDrainP2Same(t *testing.T, want, got execution.Result) {
	t.Helper()
	if want.BlockHash != got.BlockHash || want.Height != got.Height || want.StateRootBefore != got.StateRootBefore || want.StateRootAfter != got.StateRootAfter || want.ReceiptRoot != got.ReceiptRoot || want.PlanDigest != got.PlanDigest || !reflect.DeepEqual(want.Plan, got.Plan) || !reflect.DeepEqual(want.Receipts, got.Receipts) || !reflect.DeepEqual(want.StateUpdates, got.StateUpdates) || !reflect.DeepEqual(want.StateDelta, got.StateDelta) || !reflect.DeepEqual(want.TxDeltas, got.TxDeltas) || want.SuccessfulTxs != got.SuccessfulTxs || want.FailedTxs != got.FailedTxs || want.BlockExecutorID != got.BlockExecutorID || want.ExecutorVersion != got.ExecutorVersion || want.WorkerCount != got.WorkerCount || want.StateRootVersion != got.StateRootVersion || want.SerialEquivalent != got.SerialEquivalent {
		t.Fatalf("non-timing execution contract diverged\nold: %+v\nnew: %+v", want, got)
	}
}

func TestMTDrainP2NeverHandlesUserTxOrMissingCommitment(t *testing.T) {
	b := realblock.Block{ShardID: "s0", Height: 2, SystemStateDeltas: []realblock.SystemStateDelta{{DeltaID: "d", Key: "k", Value: "x"}}}
	base := map[string]string{"s0::k": "x"}
	root := state.NewCommitment(base)
	for _, tc := range []struct {
		name  string
		block realblock.Block
		root  *state.Commitment
		id    string
	}{
		{"no_certificate", b, nil, execution.BlockSTMExecutorID},
		{"unknown_engine", b, root, "other"},
		{"not_system_only", realblock.Block{TxList: []tx.SignedTransaction{{TxID: "tx"}}, SystemStateDeltas: b.SystemStateDeltas}, root, execution.BlockSTMExecutorID},
		{"no_system_delta", realblock.Block{ShardID: "s0"}, root, execution.BlockSTMExecutorID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := execution.MTDrainP2EmptyResult(tc.block, base, tc.root, tc.id, 4, "legacy_scan_v1", "abort_new_incarnation_v1"); ok {
				t.Fatal("unsafe fast path accepted")
			}
		})
	}
}
