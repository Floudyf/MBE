package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
)

func TestMTPoryNewViewV1SelectedLeaderDeferredRecordIsExactAndViewBound(t *testing.T) {
	r := &NodeRuntime{}
	defer porygonV51SelectedPrepareStates.Delete(r)
	selected := realblock.Block{Height: 35, BlockHash: "certified-B35"}
	nv := pbft.NewView{View: 1, Height: 35, LeaderID: "n1"}
	if err := r.porygonV51RememberSelectedPrepare(nv, selected); err != nil {
		t.Fatal(err)
	}
	// A delayed E33/E34 completion must not force loss of this selected identity.
	if _, ok, stale := r.porygonV51TakeSelectedPrepare(1, 34); ok || stale {
		t.Fatal("future-height selected evidence was dropped")
	}
	got, ok, stale := r.porygonV51TakeSelectedPrepare(1, 35)
	if !ok || stale || got.Block.BlockHash != selected.BlockHash || got.LeaderID != "n1" || got.View != 1 {
		t.Fatalf("selected proposal identity changed: %+v ok=%v stale=%v", got, ok, stale)
	}
	if err := r.porygonV51RememberSelectedPrepare(nv, selected); err != nil {
		t.Fatal(err)
	}
	_, ok, stale = r.porygonV51TakeSelectedPrepare(2, 35)
	if ok || !stale {
		t.Fatal("old-view proposal not discarded")
	}
}

func TestMTPoryNewViewV1LeaderReadinessRejectsMissingCertifiedPredecessor(t *testing.T) {
	r := porygonV38TruthRuntime("n1", "s0")
	defer porygonV38CleanupRuntime(r)
	state := &porygonPaperRuntimeState{
		baselineHeight:          0,
		txs:                     map[string]*PorygonTxRoundLifecycle{},
		proposalUpdates:         map[uint64][]PorygonProposalUpdate{},
		certifiedPartitionRoots: map[uint64]map[string]string{},
		certifiedRoots:          map[uint64]string{},
	}
	p := &porygonPipelineRuntime{baselineHeight: 0, orderedHeight: 34, executedHeight: 32, blocks: map[uint64]*porygonPipelineBlock{}}
	porygonV38InstallPaperState(r, state, p)
	if ready, _ := r.porygonV58SelectedLeaderReady(35); ready {
		t.Fatal("NEW-VIEW leader accepted B35 without certified E33")
	}
}
