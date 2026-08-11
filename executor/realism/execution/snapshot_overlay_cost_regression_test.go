package execution

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestSpeculativeOverlaySharesImmutableBaseAndKeepsPrivateWrites(t *testing.T) {
	base := map[string]string{"s0::k": "before", "s0::other": "x"}
	overlay := newTxOverlay("s0", base)

	// Aliasing is intentional: the caller promises that a speculative snapshot
	// is immutable while workers use it. This probes that no full-state copy is
	// performed by the per-transaction overlay constructor.
	base["s0::k"] = "after"
	if got := overlay.get("k"); got != "after" {
		t.Fatalf("speculative overlay unexpectedly copied base: got=%q", got)
	}

	overlay.set("k", "local")
	if got := overlay.get("k"); got != "local" {
		t.Fatalf("private write was not read back: got=%q", got)
	}
	if got := base["s0::k"]; got != "after" {
		t.Fatalf("private write mutated shared base: got=%q", got)
	}
	snapshot := overlay.snapshot()
	if snapshot["s0::k"] != "local" || snapshot["s0::other"] != "x" {
		t.Fatalf("materialized overlay snapshot mismatch: %#v", snapshot)
	}
}

func TestBatchSIOverlayUsesOneSharedBatchSnapshotWithPrivateWrites(t *testing.T) {
	batchSnapshot := map[string]string{"s0::k": "before", "s0::other": "x"}
	overlay := newBatchSIOverlay("s0", batchSnapshot)

	batchSnapshot["s0::k"] = "after"
	if got := overlay.get("k"); got != "after" {
		t.Fatalf("Batch-SI transaction overlay unexpectedly copied BS_k: got=%q", got)
	}
	overlay.set("k", "private")
	if got := overlay.get("k"); got != "private" {
		t.Fatalf("Batch-SI private write was not read back: got=%q", got)
	}
	if got := batchSnapshot["s0::k"]; got != "after" {
		t.Fatalf("Batch-SI private write mutated shared BS_k: got=%q", got)
	}
}

func TestAriaTentativeEpochDoesNotHashDurableStateRoot(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:             "aria-t1",
		AccessListSchema: "direct_v1",
		AccessListSource: "regression",
		AccessList:       []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
	}
	b := block.Block{ShardID: "s0", Height: 1, BlockHash: "aria-overlay-regression", TxList: []tx.SignedTransaction{item}}
	executor := NewAriaExecutor(1)
	attempts, _, err := executor.executeEpoch(context.Background(), b, map[string]string{"s0::k": "seed"}, []int{0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempt count=%d want=1", len(attempts))
	}
	if attempts[0].Delta.Receipt.StateRootAfterTx != "" {
		t.Fatalf("tentative Aria receipt unexpectedly hashed a durable state root: %q", attempts[0].Delta.Receipt.StateRootAfterTx)
	}
	if len(attempts[0].Delta.WriteSet) == 0 {
		t.Fatal("tentative Aria attempt lost its write-set evidence")
	}
}

func TestGroundhogReservationTableSharesImmutableBaseWithoutMutation(t *testing.T) {
	base := map[string]string{"s0::groundhog:set": `{"limit":64,"entries":[]}`}
	table := newGroundhogReservationTable("s0", base, GroundhogOrderedSetInitialLimit)

	base["s0::groundhog:set"] = `{"limit":32,"entries":[]}`
	if got := table.Base["s0::groundhog:set"]; got != base["s0::groundhog:set"] {
		t.Fatalf("Groundhog reservation table unexpectedly copied the immutable base: got=%q", got)
	}

	_, err := table.reserveTransaction([]groundhogModification{{
		Key:  "groundhog:set",
		Kind: groundhogOrderedSetInsert,
		Tag:  1,
		Hash: "tx-hash",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := base["s0::groundhog:set"]; got != `{"limit":32,"entries":[]}` {
		t.Fatalf("Groundhog reservation mutated caller-owned base: got=%q", got)
	}
}
