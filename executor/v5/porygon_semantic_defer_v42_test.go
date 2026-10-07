package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
)

func porygonV42Deferred(view, height uint64, hash string) deferredPrePrepare {
	return deferredPrePrepare{
		FromNode: "n0", View: view, Sequence: height,
		Block: realblock.Block{Height: height, BlockHash: hash, ShardID: "porygon-global"}, Signature: "sig",
	}
}

func TestPorygonV42SemanticDeferredSameDigestIsIdempotentAndConflictFailsClosed(t *testing.T) {
	state := &porygonSemanticDeferredPrePrepareState{byHeight: map[uint64]deferredPrePrepare{}}
	stored, err := porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(0, 3, "b3"))
	if err != nil || !stored {
		t.Fatalf("first semantic defer failed: stored=%v err=%v", stored, err)
	}
	stored, err = porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(0, 3, "b3"))
	if err != nil || stored {
		t.Fatalf("same-digest retransmission was not idempotent: stored=%v err=%v", stored, err)
	}
	if _, err := porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(0, 3, "conflict")); err == nil {
		t.Fatal("same-view conflicting semantic-deferred proposal was accepted")
	}
}

func TestPorygonV42SemanticDeferredNeverReplaysOldView(t *testing.T) {
	state := &porygonSemanticDeferredPrePrepareState{byHeight: map[uint64]deferredPrePrepare{}}
	if _, err := porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(0, 3, "b3-v0")); err != nil {
		t.Fatal(err)
	}
	if pending, ok, stale := porygonTakeSemanticDeferredPrePrepare(state, 3, 1); ok || !stale || pending.Block.BlockHash != "" {
		t.Fatalf("old-view proposal was replayable: ok=%v stale=%v pending=%+v", ok, stale, pending)
	}
	if _, err := porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(1, 3, "b3-v1")); err != nil {
		t.Fatal(err)
	}
	pending, ok, stale := porygonTakeSemanticDeferredPrePrepare(state, 3, 1)
	if !ok || stale || pending.View != 1 || pending.Block.BlockHash != "b3-v1" {
		t.Fatalf("current-view proposal did not replay: ok=%v stale=%v pending=%+v", ok, stale, pending)
	}
}

func TestPorygonV42SemanticDeferredDropsPastHeight(t *testing.T) {
	state := &porygonSemanticDeferredPrePrepareState{byHeight: map[uint64]deferredPrePrepare{}}
	if _, err := porygonRememberSemanticDeferredPrePrepare(state, porygonV42Deferred(1, 3, "b3")); err != nil {
		t.Fatal(err)
	}
	if pending, ok, stale := porygonTakeSemanticDeferredPrePrepare(state, 4, 1); ok || stale || pending.Block.BlockHash != "" {
		t.Fatalf("past-height proposal survived cursor advance: ok=%v stale=%v pending=%+v", ok, stale, pending)
	}
	if len(state.byHeight) != 0 {
		t.Fatalf("past-height semantic-deferred entry was not pruned: %+v", state.byHeight)
	}
}
