package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

// This is a paper-fidelity regression, not an optimization: only true signed
// read/write conflicts may cause cross-ESC OC abandonment on a shared key.
func TestPorygonReadConflictV1SignedAccessSemantics(t *testing.T) {
	mk := func(id string, items ...tx.AccessItem) tx.SignedTransaction {
		return tx.SignedTransaction{TxID: id, AccessList: items}
	}
	read := tx.AccessItem{Key: "category:wearable", Mode: tx.AccessRead}
	readEmote := tx.AccessItem{Key: "category:emote", Mode: tx.AccessRead}
	write := tx.AccessItem{Key: "category:wearable", Mode: tx.AccessWrite}
	rw := tx.AccessItem{Key: "category:wearable", Mode: tx.AccessReadWrite}
	delta := tx.AccessItem{Key: "category:wearable", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "market_sale_counter", Delta: 1}
	unknown := tx.AccessItem{Key: "category:wearable", Mode: tx.AccessUnknown}
	left := porygonTxAssignment{ExecutionShard: 0, CrossShard: true, LockedKeys: []string{"category:wearable"}}
	right := porygonTxAssignment{ExecutionShard: 1, CrossShard: true, LockedKeys: []string{"category:wearable"}}
	cases := []struct {
		name        string
		left, right tx.SignedTransaction
		want        bool
	}{
		{"shared-read-only", mk("a", read), mk("b", read), false},
		{"read-and-write", mk("a", read), mk("b", write), true},
		{"write-and-read", mk("a", write), mk("b", read), true},
		{"readwrite-and-read", mk("a", rw), mk("b", read), true},
		{"write-and-write", mk("a", write), mk("b", write), true},
		// Do not introduce a Porygon commutative exemption without paper evidence.
		{"commutative-delta-remains-conflict", mk("a", delta), mk("b", delta), true},
		{"unknown-mode-is-conservative", mk("a", unknown), mk("b", read), true},
		{"disjoint-keys", mk("a", read), mk("b", readEmote), false},
		{"read-shared-but-writes-disjoint", mk("a", read, tx.AccessItem{Key: "balance:a", Mode: tx.AccessWrite}), mk("b", read, tx.AccessItem{Key: "balance:b", Mode: tx.AccessWrite}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := porygonAssignmentsConflict(left, right, tc.left, tc.right)
			if got != tc.want {
				t.Fatalf("conflict=%t want %t", got, tc.want)
			}
		})
	}
	// Shared ReadOnly locked key still cannot force a CTx/ITx abort.
	itx := right
	itx.CrossShard = false
	if porygonAssignmentsConflict(left, itx, mk("a", read), mk("b", read)) {
		t.Fatal("pure reads incorrectly abandoned in post-execution CTx/ITx")
	}
	// A single ESC remains sequential even for disjoint signed accesses.
	same := right
	same.ExecutionShard = left.ExecutionShard
	if !porygonAssignmentsConflict(left, same, mk("a", read), mk("b", readEmote)) {
		t.Fatal("same-ESC sequential lane changed")
	}
	// LockedKeys may contain a projected key absent from signed AccessLists;
	// this is not sufficient to manufacture a write conflict.
	if porygonAssignmentsConflict(left, right, mk("a", readEmote), mk("b", readEmote)) {
		t.Fatal("LockedKeys improperly overrode signed read-only truth")
	}
}
