package v5

import (
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestTxAlloIdentityPrefetchCommitmentV1Guard(t *testing.T) {
	base := state.NewCommitment(map[string]string{"s0::asset": "7"})
	cases := []struct {
		name                                   string
		stateful, genericStateless, historical bool
		base                                   *state.Commitment
		want                                   bool
	}{
		{"stateful_exact_identity", true, false, false, base, true},
		{"historical_read_projection", true, false, true, base, false},
		{"stateless_remote_projection", false, true, false, base, false},
		{"not_stateful", false, false, false, base, false},
		{"missing_certified_root", true, false, false, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := txalloIdentityPrefetchCanReuseCommitmentV1(tc.stateful, tc.genericStateless, tc.historical, tc.base)
			if got != tc.want {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestTxAlloCertifiedIdentityCommitmentV1SerialRootsAndReceipts(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 4, Sender: "alice", Receiver: "bob", Value: 1, Seed: "txallo-root-reuse"})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: items}
	for _, item := range items {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	realblock.AssignHash(&block)
	db := state.NewDB(t.TempDir(), "s0")
	db.Set("balance:alice", "1000000")
	db.Set("balance:bob", "1000000")
	snapshot, certified := db.SnapshotWithCommitment()
	old := execution.NewSerialExecutor().ExecuteBlockWithCommitment(block, snapshot, nil)
	new := execution.NewSerialExecutor().ExecuteBlockWithCommitment(block, snapshot, certified)
	if old.StateRootBefore != new.StateRootBefore || old.StateRootAfter != new.StateRootAfter || old.ReceiptRoot != new.ReceiptRoot {
		t.Fatalf("authenticated roots changed old=(%s,%s,%s) new=(%s,%s,%s)", old.StateRootBefore, old.StateRootAfter, old.ReceiptRoot, new.StateRootBefore, new.StateRootAfter, new.ReceiptRoot)
	}
	if !reflect.DeepEqual(old.Receipts, new.Receipts) || !reflect.DeepEqual(old.StateDelta, new.StateDelta) || !reflect.DeepEqual(old.StateUpdates, new.StateUpdates) || !reflect.DeepEqual(old.TxDeltas, new.TxDeltas) {
		t.Fatal("serial replay changed receipts, state or delta")
	}
	if got, want := db.Root(), state.Root(snapshot); got != want {
		t.Fatalf("physical DB root diverged %s != %s", got, want)
	}
}
