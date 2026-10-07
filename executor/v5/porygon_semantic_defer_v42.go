package v5

import (
	"context"
	"fmt"
	"sync"

	"metaverse-chainlab/executor/realism/consensus/pbft"
)

// Porygon ordering is intentionally allowed to run ahead of execution. A
// correct backup can therefore receive the PRE-PREPARE for B_h before its local
// E(B_(h-2)) has produced the certified Proposal.T/Proposal.U truth required to
// validate B_h. This is a local readiness wait, not a missing-consensus-height
// condition, so it must never enter PBFT catch-up.
type porygonSemanticDeferredPrePrepareState struct {
	mu       sync.Mutex
	byHeight map[uint64]deferredPrePrepare
}

var porygonSemanticDeferredPrePrepareStates sync.Map // map[*NodeRuntime]*porygonSemanticDeferredPrePrepareState

func (r *NodeRuntime) porygonSemanticDeferredPrePrepareState() *porygonSemanticDeferredPrePrepareState {
	if value, ok := porygonSemanticDeferredPrePrepareStates.Load(r); ok {
		return value.(*porygonSemanticDeferredPrePrepareState)
	}
	created := &porygonSemanticDeferredPrePrepareState{byHeight: map[uint64]deferredPrePrepare{}}
	actual, _ := porygonSemanticDeferredPrePrepareStates.LoadOrStore(r, created)
	return actual.(*porygonSemanticDeferredPrePrepareState)
}

func porygonRememberSemanticDeferredPrePrepare(state *porygonSemanticDeferredPrePrepareState, pending deferredPrePrepare) (bool, error) {
	if state == nil || pending.Block.BlockHash == "" || pending.Block.Height == 0 || pending.FromNode == "" {
		return false, fmt.Errorf("invalid Porygon semantic-deferred PRE-PREPARE")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.byHeight == nil {
		state.byHeight = map[uint64]deferredPrePrepare{}
	}
	if existing, ok := state.byHeight[pending.Block.Height]; ok {
		if existing.View > pending.View {
			return false, nil
		}
		if existing.View == pending.View {
			if existing.Block.BlockHash != pending.Block.BlockHash {
				return false, fmt.Errorf("conflicting Porygon semantic-deferred PRE-PREPARE at view %d height %d", pending.View, pending.Block.Height)
			}
			return false, nil
		}
	}
	state.byHeight[pending.Block.Height] = pending
	return true, nil
}

// porygonTakeSemanticDeferredPrePrepare is deliberately view-safe. An old-view
// proposal is never replayed after PBFT installs a new view; a new-view proposal
// for the same height may replace it through porygonRememberSemanticDeferredPrePrepare.
func porygonTakeSemanticDeferredPrePrepare(state *porygonSemanticDeferredPrePrepareState, expectedHeight, currentView uint64) (deferredPrePrepare, bool, bool) {
	if state == nil || expectedHeight == 0 {
		return deferredPrePrepare{}, false, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	staleExpectedView := false
	for height, pending := range state.byHeight {
		if height < expectedHeight || pending.View < currentView {
			if height == expectedHeight && pending.View != currentView {
				staleExpectedView = true
			}
			delete(state.byHeight, height)
		}
	}
	pending, ok := state.byHeight[expectedHeight]
	if !ok {
		return deferredPrePrepare{}, false, staleExpectedView
	}
	if pending.View != currentView {
		delete(state.byHeight, expectedHeight)
		return deferredPrePrepare{}, false, true
	}
	delete(state.byHeight, expectedHeight)
	return pending, true, false
}

func (r *NodeRuntime) deferPorygonSemanticPrePrepare(pre pbft.PrePrepare) error {
	if r == nil {
		return fmt.Errorf("Porygon semantic-deferred PRE-PREPARE runtime missing")
	}
	currentView := r.currentPBFTView()
	expectedHeight := r.porygonConsensusNextHeight()
	if pre.View != currentView || pre.Height != expectedHeight || pre.Sequence != pre.Height {
		return fmt.Errorf("Porygon semantic-deferred PRE-PREPARE slot mismatch: got view=%d height=%d expected view=%d height=%d", pre.View, pre.Height, currentView, expectedHeight)
	}
	stored, err := porygonRememberSemanticDeferredPrePrepare(r.porygonSemanticDeferredPrePrepareState(), deferredPrePrepare{
		FromNode: pre.LeaderID, View: pre.View, Sequence: pre.Sequence, Block: pre.Block, Signature: pre.Signature,
	})
	if err != nil {
		return err
	}
	if stored {
		r.addPorygonRuntimeMetric("porygon_preprepare_local_execution_deferred_count", 1)
		r.addPorygonRuntimeMetric("porygon_preprepare_semantic_deferred_store_count", 1)
		r.logConsensus("PORYGON_PRE_PREPARE_SEMANTIC_DEFERRED", pre.LeaderID, pre.BlockHash, pre.Height)
	}
	return nil
}

func (r *NodeRuntime) replayPorygonSemanticDeferredPrePrepare(ctx context.Context) {
	if r == nil || ctx == nil {
		return
	}
	expectedHeight := r.porygonConsensusNextHeight()
	currentView := r.currentPBFTView()
	ready, _ := r.porygonPaperProposalValidationReady(expectedHeight)
	if !ready {
		return
	}
	pending, ok, staleView := porygonTakeSemanticDeferredPrePrepare(r.porygonSemanticDeferredPrePrepareState(), expectedHeight, currentView)
	if staleView {
		r.addPorygonRuntimeMetric("porygon_preprepare_semantic_deferred_stale_view_count", 1)
		return
	}
	if !ok {
		return
	}
	pre := pbft.PrePrepare{
		View: pending.View, Sequence: pending.Sequence, Height: pending.Block.Height,
		LeaderID: pending.FromNode, BlockHash: pending.Block.BlockHash, Block: pending.Block, Signature: pending.Signature,
	}
	envelope, err := pbftPrePrepareEnvelope(pre, pending.FromNode, r.node.ShardID)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	r.addPorygonRuntimeMetric("porygon_preprepare_semantic_deferred_replay_count", 1)
	r.logConsensus("PORYGON_PRE_PREPARE_SEMANTIC_REPLAY", pending.FromNode, pending.Block.BlockHash, pending.Block.Height)
	if err := r.handlePBFTPrePrepare(ctx, envelope); err != nil {
		r.setLastProposalError(err)
	}
}
