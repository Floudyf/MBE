package v5

import (
	"context"
	"fmt"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
)

// porygonV51SelectedPrepare is local liveness state only. The selected block
// and view are already authenticated by PBFT NEW-VIEW; this cache merely lets a
// backup defer its fresh-view PREPARE until the local Porygon execution frontier
// has the certified Proposal.T/U truth needed for semantic validation.
type porygonV51SelectedPrepare struct {
	View     uint64
	LeaderID string
	Block    realblock.Block
}

type porygonV51SelectedPrepareState struct {
	mu      sync.Mutex
	pending *porygonV51SelectedPrepare
}

var porygonV51SelectedPrepareStates sync.Map // map[*NodeRuntime]*porygonV51SelectedPrepareState

func (r *NodeRuntime) porygonV51SelectedPrepareState() *porygonV51SelectedPrepareState {
	if value, ok := porygonV51SelectedPrepareStates.Load(r); ok {
		return value.(*porygonV51SelectedPrepareState)
	}
	created := &porygonV51SelectedPrepareState{}
	actual, _ := porygonV51SelectedPrepareStates.LoadOrStore(r, created)
	return actual.(*porygonV51SelectedPrepareState)
}

func (r *NodeRuntime) porygonV51RememberSelectedPrepare(nv pbft.NewView, selected realblock.Block) error {
	if r == nil || nv.View == 0 || nv.LeaderID == "" || selected.BlockHash == "" || selected.Height == 0 {
		return fmt.Errorf("invalid Porygon v5.1 selected-proposal recovery state")
	}
	state := r.porygonV51SelectedPrepareState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pending != nil {
		prior := state.pending
		if prior.View > nv.View {
			return nil
		}
		if prior.View == nv.View && prior.Block.BlockHash != selected.BlockHash {
			return fmt.Errorf("conflicting Porygon v5.1 selected proposal at view %d", nv.View)
		}
	}
	copyBlock := selected
	state.pending = &porygonV51SelectedPrepare{View: nv.View, LeaderID: nv.LeaderID, Block: copyBlock}
	return nil
}

func (r *NodeRuntime) porygonV51TakeSelectedPrepare(currentView, expectedHeight uint64) (porygonV51SelectedPrepare, bool, bool) {
	if r == nil {
		return porygonV51SelectedPrepare{}, false, false
	}
	state := r.porygonV51SelectedPrepareState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pending == nil {
		return porygonV51SelectedPrepare{}, false, false
	}
	pending := *state.pending
	if pending.View != currentView || pending.Block.Height < expectedHeight {
		state.pending = nil
		return porygonV51SelectedPrepare{}, false, true
	}
	if pending.Block.Height != expectedHeight {
		return porygonV51SelectedPrepare{}, false, false
	}
	state.pending = nil
	return pending, true, false
}

// porygonV51RecoverSelectedPrepare restores the fresh-view PREPARE on backups
// directly from authenticated NEW-VIEW selected-proposal evidence. PBFT safety
// and quorum rules remain untouched: NEW-VIEW has already installed the exact
// selected digest into the PBFT state; this function only emits this replica's
// view-specific PREPARE once ordinary Porygon semantic validation is ready.
func (r *NodeRuntime) porygonV51RecoverSelectedPrepare(ctx context.Context, nv pbft.NewView, selected realblock.Block) error {
	if r == nil || ctx == nil || selected.BlockHash == "" {
		return nil
	}
	if r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID || r.isCurrentLeader() {
		return nil
	}
	state := r.pbftState()
	if state == nil || state.View() != nv.View || state.Leader() != nv.LeaderID {
		return fmt.Errorf("Porygon v5.1 NEW-VIEW recovery state mismatch")
	}
	if selected.Height != nv.Height || selected.Height != r.porygonConsensusNextHeight() {
		if selected.Height > r.porygonConsensusNextHeight() {
			if err := r.porygonV51RememberSelectedPrepare(nv, selected); err != nil {
				return err
			}
			r.requestCatchup(ctx)
			return nil
		}
		return nil
	}
	if !state.IsDuplicatePrePrepare(nv.View, selected.Height, selected.BlockHash) {
		return fmt.Errorf("Porygon v5.1 NEW-VIEW selected digest not installed in fresh view")
	}
	ready, reason := r.porygonPaperProposalValidationReady(selected.Height)
	if !ready {
		if err := r.porygonV51RememberSelectedPrepare(nv, selected); err != nil {
			return err
		}
		r.addPorygonRuntimeMetric("porygon_v51_new_view_selected_prepare_deferred_count", 1)
		r.setLastProposalError(fmt.Errorf("porygon v5.1 selected proposal local execution not ready at height %d: %s", selected.Height, reason))
		return nil
	}
	if err := r.validateConsensusBlockBody(selected); err != nil {
		return fmt.Errorf("Porygon v5.1 selected consensus block body: %w", err)
	}
	accepted, requestCatchup, err := r.validatePrePrepare(nv.LeaderID, selected)
	if err != nil {
		return err
	}
	if !accepted {
		if requestCatchup {
			if err := r.porygonV51RememberSelectedPrepare(nv, selected); err != nil {
				return err
			}
			r.requestCatchup(ctx)
		}
		return nil
	}

	r.rememberProposal(selected)
	r.proposalWorkUnits.Store(int64(r.estimateProposalValidationWork(selected)))
	r.clearLastProposalError()
	prepare := pbft.Prepare{
		View:      nv.View,
		Sequence:  selected.Height,
		Height:    selected.Height,
		NodeID:    r.node.NodeID,
		BlockHash: selected.BlockHash,
	}
	r.addPorygonRuntimeMetric("porygon_v51_new_view_selected_prepare_broadcast_count", 1)
	r.logConsensus("PORYGON_V51_NEW_VIEW_SELECTED_PREPARE", nv.LeaderID, selected.BlockHash, selected.Height)
	return r.broadcastPBFTPrepare(ctx, prepare)
}

// replayPorygonV51SelectedPrepare is woken by execution-frontier advancement.
// It does not invent PBFT evidence: the pending block came only from an already
// authenticated NEW-VIEW and is discarded on any later view change.
func (r *NodeRuntime) replayPorygonV51SelectedPrepare(ctx context.Context) {
	if r == nil || ctx == nil || r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return
	}
	view := r.currentPBFTView()
	height := r.porygonConsensusNextHeight()
	ready, _ := r.porygonPaperProposalValidationReady(height)
	if !ready {
		return
	}
	pending, ok, stale := r.porygonV51TakeSelectedPrepare(view, height)
	if stale {
		r.addPorygonRuntimeMetric("porygon_v51_new_view_selected_prepare_stale_count", 1)
		return
	}
	if !ok {
		return
	}
	nv := pbft.NewView{View: pending.View, LeaderID: pending.LeaderID, Height: pending.Block.Height}
	if err := r.porygonV51RecoverSelectedPrepare(ctx, nv, pending.Block); err != nil {
		r.setLastProposalError(err)
		return
	}
	r.addPorygonRuntimeMetric("porygon_v51_new_view_selected_prepare_replay_count", 1)
}
