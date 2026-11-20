package v5

import (
	"fmt"

	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
)

// MTCF_V1: The wire message is authenticated and hydrated BEFORE this gate.
// Full signed-frontier validation, PBFT safety checks, and all voting remain
// in handlePBFTPrePrepare; this gate may only retain an authenticated proposal.
func mtFrontierGateNeedsDeferralV1(height, durableHeight uint64, commitBusy bool) bool {
	if height == 0 || height <= durableHeight {
		return false
	}
	return height > durableHeight+1 || commitBusy
}

// mtGuardHydratedFrontierPrePrepareV2 executes after the normal PBFT envelope
// authentication AND compact transaction body hydration. It does not vote.
// A retained proposal must re-enter handlePBFTPrePrepare for all full checks.
func (r *NodeRuntime) mtGuardHydratedFrontierPrePrepareV2(wire p2p.MessageEnvelope, pre pbft.PrePrepare) (bool, error) {
	if r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != "time_or_count_block_producer" ||
		!metaTrackTransactionFrontierMetadataV656(pre.Block) {
		return false, nil
	}
	r.mu.Lock()
	durableHeight := r.committedHeight
	r.mu.Unlock()
	// commitMu is held until the parent's ordinal cache is published.
	// TryLock prevents blocking the network receiver on state-version traffic.
	commitBusy := !r.commitMu.TryLock()
	if !commitBusy {
		r.commitMu.Unlock()
	}
	if !mtFrontierGateNeedsDeferralV1(pre.Height, durableHeight, commitBusy) {
		return false, nil
	}
	if pre.View != r.currentPBFTView() {
		return false, nil // normal handler will validate/reject the stale view
	}
	if err := p2p.ValidateEnvelope(wire); err != nil {
		return true, fmt.Errorf("frontier deferral envelope: %w", err)
	}
	if pre.LeaderID != wire.FromNode || pre.LeaderID != r.leaderID(r.node.ShardID) ||
		wire.ShardID != r.node.ShardID || pre.Block.ShardID != r.node.ShardID ||
		wire.View != pre.View || wire.Height != pre.Height || wire.Sequence != pre.Sequence ||
		pre.Height != pre.Sequence || pre.Height != pre.Block.Height ||
		pre.BlockHash == "" || pre.BlockHash != pre.Block.BlockHash {
		return true, fmt.Errorf("frontier deferral PRE-PREPARE identity/view/height mismatch")
	}
	if pre.Block.ProposerID != pre.LeaderID && !r.pbftAllowsCarriedProposal(pre.Block) {
		return true, fmt.Errorf("frontier deferral proposer mismatch")
	}
	// The enclosing handlePBFTPrePrepare has already authenticated the
	// original signed envelope before hydration. Rechecking the expanded
	// body as a new signed message is unnecessary and can reject compact
	// transport formats whose wire bytes differ from their hydrated shape.
	if err := r.validateConsensusBlockStructuralV1(pre.Block); err != nil {
		return true, fmt.Errorf("frontier deferral signed block structure: %w", err)
	}
	r.mu.Lock()
	currentHeight := r.committedHeight
	if pre.Height <= currentHeight {
		r.mu.Unlock()
		return false, nil // was committed during authentication; normal stale handling
	}
	if pre.Height-currentHeight > 8 {
		r.mu.Unlock()
		return true, fmt.Errorf("frontier deferral height beyond bounded horizon")
	}
	if r.deferredPrePrepares == nil {
		r.deferredPrePrepares = map[uint64]deferredPrePrepare{}
	}
	if old, exists := r.deferredPrePrepares[pre.Height]; exists {
		if old.View > pre.View || (old.View == pre.View &&
			(old.Block.BlockHash != pre.BlockHash || old.Signature != pre.Signature || old.FromNode != wire.FromNode)) {
			r.mu.Unlock()
			return true, fmt.Errorf("frontier deferral conflicting PRE-PREPARE at same height/view")
		}
		if old.View == pre.View {
			r.mu.Unlock()
			return true, nil
		}
	}
	if len(r.deferredPrePrepares) >= 16 {
		r.mu.Unlock()
		return true, fmt.Errorf("frontier deferral queue capacity reached")
	}
	r.deferredPrePrepares[pre.Height] = deferredPrePrepare{
		FromNode: wire.FromNode, View: pre.View, Sequence: pre.Sequence,
		Block: pre.Block, Signature: pre.Signature,
	}
	r.incrementRuntimeMetricLocked("metatwine_frontier_preprepare_deferred_count")
	r.mu.Unlock()
	r.logConsensus("PBFT_PRE_PREPARE_FRONTIER_DEFERRED", wire.FromNode, pre.BlockHash, pre.Height)
	return true, nil
}
