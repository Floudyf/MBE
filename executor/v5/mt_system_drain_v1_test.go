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

// The exception is permitted only for zero-user-tx system-delta blocks on
// Serial/Block-STM engines. It does not alter normal prefetch or paper execution.
func TestMTSystemDrainV1Guard(t *testing.T) {
	root := state.NewCommitment(map[string]string{"s0::asset": "1"})
	system := realblock.Block{ShardID: "s0", Height: 2, SystemStateDeltas: []realblock.SystemStateDelta{{DeltaID: "d1", Key: "asset", Value: "2"}}}
	cases := []struct {
		name       string
		block      realblock.Block
		executor   string
		historical bool
		root       *state.Commitment
		want       bool
	}{
		{"blockstm_system_only", system, execution.BlockSTMExecutorID, false, root, true},
		{"serial_system_only", system, execution.SerialBlockExecutorID, false, root, true},
		{"ordinary_empty", realblock.Block{ShardID: "s0"}, execution.BlockSTMExecutorID, false, root, false},
		{"user_tx_even_with_system_delta", realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{{TxID: "tx1"}}, SystemStateDeltas: system.SystemStateDeltas}, execution.BlockSTMExecutorID, false, root, false},
		{"historical_projection", system, execution.BlockSTMExecutorID, true, root, false},
		{"no_authenticated_commitment", system, execution.BlockSTMExecutorID, false, nil, false},
		{"metatrack_frozen", system, metaTrackBlockExecutorID, false, root, false},
		{"porygon_frozen", system, porygonBlockExecutorID, false, root, false},
		{"unknown_executor", system, "unknown", false, root, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mtSystemDrainCertifiedCommitmentV1(tc.block, tc.executor, tc.historical, tc.root); got != tc.want {
				t.Fatalf("reuse=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestMTSystemDrainV1AuthenticatedDeltaRootEqualsFullBuild(t *testing.T) {
	before := map[string]string{"s0::balance:alice": "10", "s0::balance:bob": "20", "s0::asset": "1"}
	certified := state.NewCommitment(before)
	updates := []state.StateKV{{Key: "asset", Value: "2"}}
	after, authenticated, err := applyStateDeltaToSnapshotWithCommitment(before, certified, updates, "s0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := authenticated.Root(), state.RootOfSnapshot(after); got != want {
		t.Fatalf("system delta root mismatch: %s != %s", got, want)
	}
	block := realblock.Block{ShardID: "s0", Height: 2, SystemStateDeltas: []realblock.SystemStateDelta{{DeltaID: "d1", Key: "asset", Value: "2"}}}
	if !mtSystemDrainCertifiedCommitmentV1(block, execution.BlockSTMExecutorID, false, authenticated) {
		t.Fatal("certified system-only drain not eligible")
	}
	oldSTM, err := execution.NewBlockSTMExecutor(2).ExecuteBlockWithCommitment(context.Background(), block, after, nil)
	if err != nil {
		t.Fatal(err)
	}
	newSTM, err := execution.NewBlockSTMExecutor(2).ExecuteBlockWithCommitment(context.Background(), block, after, authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if oldSTM.StateRootBefore != newSTM.StateRootBefore || oldSTM.StateRootAfter != newSTM.StateRootAfter || oldSTM.ReceiptRoot != newSTM.ReceiptRoot || !reflect.DeepEqual(oldSTM.StateDelta, newSTM.StateDelta) || !reflect.DeepEqual(oldSTM.StateUpdates, newSTM.StateUpdates) || !reflect.DeepEqual(oldSTM.Receipts, newSTM.Receipts) {
		t.Fatal("Block-STM state/receipt/delta changed when using certified system-only root")
	}
	oldSerial := execution.NewSerialExecutor().ExecuteBlockWithCommitment(block, after, nil)
	newSerial := execution.NewSerialExecutor().ExecuteBlockWithCommitment(block, after, authenticated)
	if oldSerial.StateRootBefore != newSerial.StateRootBefore || oldSerial.StateRootAfter != newSerial.StateRootAfter || oldSerial.ReceiptRoot != newSerial.ReceiptRoot || !reflect.DeepEqual(oldSerial.StateDelta, newSerial.StateDelta) || !reflect.DeepEqual(oldSerial.Receipts, newSerial.Receipts) {
		t.Fatal("Serial state/receipt/delta changed when using certified system-only root")
	}
	if certified.Root() == authenticated.Root() {
		t.Fatal("system update did not change root")
	}
}
