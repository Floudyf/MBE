package v5

import (
	"context"
	"fmt"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const maxPBFTProposalRetransmits = 2

func initialLeaderID(plan Plan, shardID string) string {
	for _, item := range plan.NodeConfigs {
		if item.ShardID == shardID && item.Leader {
			return item.NodeID
		}
	}
	// Some focused tests (and defensive partial plans) carry the complete
	// validator set even when the leader's NodePlan is absent. The validator
	// list is compiled with the shard leader first, so preserve that authority
	// instead of promoting the first local NodePlan accidentally.
	for _, item := range plan.NodeConfigs {
		if item.ShardID == shardID && len(item.Validators) > 0 && item.Validators[0] != "" {
			return item.Validators[0]
		}
	}
	for _, item := range plan.NodeConfigs {
		if item.ShardID == shardID {
			return item.NodeID
		}
	}
	return ""
}

func (r *NodeRuntime) pbftState() *pbft.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consensus == nil {
		r.consensus = pbft.NewState(
			r.node.NodeID,
			r.node.ShardID,
			initialLeaderID(r.plan, r.node.ShardID),
			r.node.Validators,
		)
	}
	return r.consensus
}

func (r *NodeRuntime) currentPBFTView() uint64 {
	state := r.pbftState()
	if state == nil {
		return 0
	}
	return state.View()
}

func (r *NodeRuntime) isCurrentLeader() bool {
	r.mu.Lock()
	state := r.consensus
	legacyLeader := r.node.Leader
	r.mu.Unlock()
	if state == nil {
		// A small number of existing unit tests construct NodeRuntime literals
		// without running newNodeRuntime. Preserve their static-role semantics;
		// every production runtime initializes consensus eagerly.
		return legacyLeader
	}
	return state.Leader() == r.node.NodeID
}

// isCurrentLeaderLocked is used only while r.mu is already held.
func (r *NodeRuntime) isCurrentLeaderLocked() bool {
	if r.consensus == nil {
		return r.node.Leader
	}
	return r.consensus.Leader() == r.node.NodeID
}

func (r *NodeRuntime) broadcastPBFTEnvelope(ctx context.Context, envelope p2p.MessageEnvelope) []error {
	errs := []error{}
	seen := map[string]bool{}
	for _, nodeID := range r.node.Validators {
		if nodeID == "" || nodeID == r.node.NodeID || seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		if err := r.sendToNode(ctx, nodeID, envelope); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (r *NodeRuntime) pbftAllowsCarriedProposal(block realblock.Block) bool {
	state := r.pbftState()
	if state == nil || block.BlockHash == "" {
		return false
	}
	cert, carried, ok := state.PreparedCertificate(block.Height)
	return ok && cert.BlockHash == block.BlockHash && carried.BlockHash == block.BlockHash
}

func validatePBFTEnvelope(msg p2p.MessageEnvelope, view, sequence, height uint64, nodeID string) error {
	if msg.FromNode == "" || nodeID == "" || msg.FromNode != nodeID {
		return fmt.Errorf("pbft envelope sender mismatch")
	}
	if msg.View != view || msg.Sequence != sequence || msg.Height != height {
		return fmt.Errorf("pbft envelope slot mismatch")
	}
	if sequence == 0 || sequence != height {
		return fmt.Errorf("pbft sequence/height mismatch")
	}
	return nil
}

func (r *NodeRuntime) handlePBFTPrePrepare(ctx context.Context, msg p2p.MessageEnvelope) error {
	pre, err := p2p.DecodePayload[pbft.PrePrepare](msg)
	if err != nil {
		return err
	}
	if err := validatePBFTEnvelope(msg, pre.View, pre.Sequence, pre.Height, pre.LeaderID); err != nil {
		return err
	}
	if pre.BlockHash != pre.Block.BlockHash {
		return fmt.Errorf("pbft pre-prepare payload digest mismatch")
	}
	if err := r.verifyPBFTPrePrepareAuthentication(pre); err != nil {
		return fmt.Errorf("pbft pre-prepare authentication: %w", err)
	}
	if err := r.validateConsensusBlockBody(pre.Block); err != nil {
		return fmt.Errorf("pbft pre-prepare block body: %w", err)
	}

	state := r.pbftState()
	if handled, err := r.maybeRespondWithCertifiedCommit(ctx, msg.FromNode, pre); handled || err != nil {
		return err
	}
	if err := state.ValidatePrePrepare(pre); err != nil {
		r.setLastProposalError(err)
		return err
	}

	// A duplicate PRE-PREPARE for the same view/sequence/digest is a liveness
	// retransmission. The proposal was already fully validated, so immediately
	// re-send PREPARE instead of re-running expensive execution-plan evidence.
	if state.IsDuplicatePrePrepare(pre.View, pre.Sequence, pre.BlockHash) {
		prepare := pbft.Prepare{
			View:      pre.View,
			Sequence:  pre.Sequence,
			Height:    pre.Height,
			NodeID:    r.node.NodeID,
			BlockHash: pre.BlockHash,
		}
		r.logConsensus("PBFT_PRE_PREPARE_RETRANSMIT_ACCEPTED", msg.FromNode, pre.BlockHash, pre.Height)
		return r.broadcastPBFTPrepare(ctx, prepare)
	}

	accepted, requestCatchup, err := r.validatePrePrepare(msg.FromNode, pre.Block)
	r.logConsensus(msg.MessageType, msg.FromNode, pre.BlockHash, pre.Height)
	if err != nil {
		r.setLastProposalError(err)
		return err
	}
	if !accepted {
		if requestCatchup {
			return r.deferPBFTPrePrepare(ctx, msg.FromNode, pre, false)
		}
		return nil
	}

	prepare, err := state.OnPrePrepare(pre)
	if err != nil {
		r.setLastProposalError(err)
		return err
	}
	r.rememberProposal(pre.Block)
	r.proposalWorkUnits.Store(int64(r.estimateProposalValidationWork(pre.Block)))
	r.clearLastProposalError()
	return r.broadcastPBFTPrepare(ctx, prepare)
}

func (r *NodeRuntime) handlePBFTPrepare(ctx context.Context, msg p2p.MessageEnvelope) error {
	prepare, err := p2p.DecodePayload[pbft.Prepare](msg)
	if err != nil {
		return err
	}
	if err := validatePBFTEnvelope(msg, prepare.View, prepare.Sequence, prepare.Height, prepare.NodeID); err != nil {
		return err
	}
	if err := r.verifyPBFTPrepareAuthentication(prepare); err != nil {
		return fmt.Errorf("pbft prepare authentication: %w", err)
	}
	r.logConsensus(msg.MessageType, msg.FromNode, prepare.BlockHash, prepare.Height)

	reached, _, err := r.pbftState().AcceptPrepare(prepare)
	if err != nil {
		r.setLastProposalError(err)
		return err
	}
	r.mirrorPrepareVote(prepare)
	if reached {
		r.logConsensus("PBFT_PREPARED_CERTIFICATE", r.node.NodeID, prepare.BlockHash, prepare.Height)
		return r.broadcastPBFTCommit(ctx, pbft.Commit{
			View:      prepare.View,
			Sequence:  prepare.Sequence,
			Height:    prepare.Height,
			NodeID:    r.node.NodeID,
			BlockHash: prepare.BlockHash,
		})
	}
	return nil
}

func (r *NodeRuntime) handlePBFTCommit(ctx context.Context, msg p2p.MessageEnvelope) error {
	commit, err := p2p.DecodePayload[pbft.Commit](msg)
	if err != nil {
		return err
	}
	// Historical V5 tests used PBFT_COMMIT as a leader commit notification
	// carrying Proposal{Block: ...}. Preserve that wire shape only for a
	// single-validator shard, where one local vote is already the complete
	// PBFT quorum. Multi-validator runtimes reject this compatibility form.
	if commit.BlockHash == "" && commit.NodeID == "" {
		if r.pbftAuthenticationRequired() {
			return fmt.Errorf("legacy singleton pbft commit wire form is disabled when validator authentication is enabled")
		}
		return r.handleLegacySingletonCommit(ctx, msg)
	}
	if err := validatePBFTEnvelope(msg, commit.View, commit.Sequence, commit.Height, commit.NodeID); err != nil {
		return err
	}
	if err := r.verifyPBFTCommitAuthentication(commit); err != nil {
		return fmt.Errorf("pbft commit authentication: %w", err)
	}
	r.logConsensus(msg.MessageType, msg.FromNode, commit.BlockHash, commit.Height)

	reached, _, block, err := r.pbftState().AcceptCommit(commit)
	if err != nil {
		r.setLastProposalError(err)
		return err
	}
	if reached {
		return r.enqueuePBFTCommittedBlock(block)
	}
	return nil
}

func (r *NodeRuntime) handleLegacySingletonCommit(ctx context.Context, msg p2p.MessageEnvelope) error {
	if len(r.node.Validators) != 1 || r.node.Validators[0] != r.node.NodeID || msg.FromNode != r.node.NodeID {
		return fmt.Errorf("legacy pbft commit payload rejected outside single-validator shard")
	}
	proposal, err := p2p.DecodePayload[Proposal](msg)
	if err != nil {
		return err
	}
	block := proposal.Block
	if block.BlockHash == "" || block.Height == 0 || block.Height != msg.Height || msg.Sequence != block.Height {
		return fmt.Errorf("legacy pbft commit proposal identity mismatch")
	}
	state := r.pbftState()
	view := state.View()
	if msg.View != view || state.Leader() != r.node.NodeID {
		return fmt.Errorf("legacy pbft commit view/leader mismatch")
	}
	prepare, err := state.OnPrePrepare(pbft.PrePrepare{
		View: view, Sequence: block.Height, Height: block.Height,
		LeaderID: r.node.NodeID, BlockHash: block.BlockHash, Block: block,
	})
	if err != nil {
		return err
	}
	if _, _, err := state.AcceptPrepare(prepare); err != nil {
		return err
	}
	r.rememberProposal(block)
	reached, _, committedBlock, err := state.AcceptCommit(pbft.Commit{
		View: view, Sequence: block.Height, Height: block.Height,
		NodeID: r.node.NodeID, BlockHash: block.BlockHash,
	})
	if err != nil {
		return err
	}
	if reached {
		return r.enqueuePBFTCommittedBlock(committedBlock)
	}
	return nil
}

func (r *NodeRuntime) broadcastPBFTPrepare(ctx context.Context, prepare pbft.Prepare) error {
	if err := r.signPBFTPrepare(&prepare); err != nil {
		return fmt.Errorf("sign pbft prepare: %w", err)
	}
	reached, _, err := r.pbftState().AcceptPrepare(prepare)
	if err != nil {
		return err
	}
	r.mirrorPrepareVote(prepare)

	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTPrepare,
		r.node.NodeID,
		"",
		r.node.ShardID,
		prepare.Height,
		prepare.View,
		prepare.Sequence,
		prepare,
	)
	if err != nil {
		return err
	}
	errs := r.broadcastPBFTEnvelope(ctx, envelope)
	if len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}

	if reached {
		r.logConsensus("PBFT_PREPARED_CERTIFICATE", r.node.NodeID, prepare.BlockHash, prepare.Height)
		if err := r.broadcastPBFTCommit(ctx, pbft.Commit{
			View:      prepare.View,
			Sequence:  prepare.Sequence,
			Height:    prepare.Height,
			NodeID:    r.node.NodeID,
			BlockHash: prepare.BlockHash,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *NodeRuntime) broadcastPBFTCommit(ctx context.Context, commit pbft.Commit) error {
	if err := r.signPBFTCommit(&commit); err != nil {
		return fmt.Errorf("sign pbft commit: %w", err)
	}
	state := r.pbftState()
	reached, _, block, err := state.AcceptCommit(commit)
	if err != nil {
		return err
	}

	// COMMIT is idempotent and may be retransmitted. Rate-limit duplicate
	// broadcasts so a lost COMMIT cannot permanently prevent a certificate,
	// while ordinary late PREPARE messages do not create a broadcast storm.
	if state.ShouldBroadcastCommit(commit.View, commit.Sequence, commit.BlockHash, time.Second) {
		envelope, err := p2p.NewEnvelope(
			p2p.MessagePBFTCommit,
			r.node.NodeID,
			"",
			r.node.ShardID,
			commit.Height,
			commit.View,
			commit.Sequence,
			commit,
		)
		if err != nil {
			return err
		}
		errs := r.broadcastPBFTEnvelope(ctx, envelope)
		if len(errs) > 0 {
			r.setLastProposalError(errs[0])
		}
	}

	if reached {
		return r.enqueuePBFTCommittedBlock(block)
	}
	return nil
}

func (r *NodeRuntime) enqueuePBFTCommittedBlock(block realblock.Block) error {
	if block.BlockHash == "" {
		return nil
	}
	for _, item := range block.TxList {
		r.recordLifecycle(LifecycleEvent{
			TimestampMS: time.Now().UnixMilli(),
			TxID:        item.TxID,
			LogicalTxID: tx.SemanticID(item),
			Stage:       "quorum_committed",
			NodeID:      r.node.NodeID,
			ShardID:     r.node.ShardID,
			BlockHeight: block.Height,
			Success:     true,
		})
	}
	r.logConsensus("PBFT_COMMIT_CERTIFICATE", r.node.NodeID, block.BlockHash, block.Height)
	return r.enqueueCommitTask(commitTaskConsensus, block, CommitOriginConsensus)
}

func (r *NodeRuntime) mirrorPrepareVote(prepare pbft.Prepare) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.votes[prepare.BlockHash] == nil {
		r.votes[prepare.BlockHash] = map[string]bool{}
	}
	r.votes[prepare.BlockHash][prepare.NodeID] = true
}

func (r *NodeRuntime) beginPBFTProposal(ctx context.Context, block realblock.Block, workUnits int) error {
	state := r.pbftState()
	if state.Leader() != r.node.NodeID {
		return fmt.Errorf("pbft proposal from non-primary: node=%s leader=%s view=%d", r.node.NodeID, state.Leader(), state.View())
	}
	view := state.View()
	if view > 0 {
		if err := r.rebroadcastCurrentPBFTNewView(ctx); err != nil {
			return err
		}
	}
	pre := pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  r.node.NodeID,
		BlockHash: block.BlockHash,
		Block:     block,
	}
	if err := r.validateConsensusBlockBody(block); err != nil {
		return fmt.Errorf("primary consensus block body: %w", err)
	}
	if err := r.signPBFTPrePrepare(&pre); err != nil {
		return fmt.Errorf("sign pbft pre-prepare: %w", err)
	}
	prepare, err := state.OnPrePrepare(pre)
	if err != nil {
		return err
	}

	r.rememberProposal(block)
	r.mu.Lock()
	r.lastProposalError = ""
	r.proposalInFlight = true
	r.proposalInFlightHash = block.BlockHash
	r.proposalStartedAt = time.Now()
	r.proposalLastBroadcastAt = r.proposalStartedAt
	r.proposalRetransmitCount = 0
	r.proposalWorkUnits.Store(int64(workUnits))
	r.votes[block.BlockHash] = map[string]bool{}
	r.mu.Unlock()

	r.logConsensus("PBFT_PRE_PREPARE_LOCAL", r.node.NodeID, block.BlockHash, block.Height)
	if err := r.broadcastPBFTPrePrepare(ctx, pre); err != nil {
		return err
	}
	return r.broadcastPBFTPrepare(ctx, prepare)
}

func (r *NodeRuntime) broadcastPBFTPrePrepare(ctx context.Context, pre pbft.PrePrepare) error {
	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTPrePrepare,
		r.node.NodeID,
		"",
		r.node.ShardID,
		pre.Height,
		pre.View,
		pre.Sequence,
		pre,
	)
	if err != nil {
		return err
	}
	errs := r.broadcastPBFTEnvelope(ctx, envelope)
	if len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}
	// A minority send failure must not abort the local proposal. PBFT
	// liveness is determined by quorum and retransmission, not by every peer
	// accepting the first TCP send.
	return nil
}

func (r *NodeRuntime) rebroadcastCurrentPBFTNewView(ctx context.Context) error {
	state := r.pbftState()
	view := state.View()
	if view == 0 {
		return nil
	}
	nv, ok := state.NewViewProof(view)
	if !ok {
		return nil
	}
	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTNewView,
		r.node.NodeID,
		"",
		r.node.ShardID,
		nv.Height,
		nv.View,
		nv.Height,
		nv,
	)
	if err != nil {
		return err
	}
	if errs := r.broadcastPBFTEnvelope(ctx, envelope); len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("pbft_new_view_retransmit_count")
	r.mu.Unlock()
	return nil
}

func (r *NodeRuntime) retransmitCurrentPBFTProposal(ctx context.Context) error {
	r.mu.Lock()
	hash := r.proposalInFlightHash
	block := r.proposals[hash]
	if !r.proposalInFlight || hash == "" || block.BlockHash == "" {
		r.mu.Unlock()
		return nil
	}
	r.proposalRetransmitCount++
	r.proposalLastBroadcastAt = time.Now()
	r.incrementRuntimeMetricLocked("pbft_preprepare_retransmit_count")
	r.lastProposalError = "proposal_timeout_retransmit_same_digest"
	r.mu.Unlock()

	state := r.pbftState()
	view := state.View()
	if view > 0 {
		if err := r.rebroadcastCurrentPBFTNewView(ctx); err != nil {
			return err
		}
	}
	pre := pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  r.node.NodeID,
		BlockHash: block.BlockHash,
		Block:     block,
	}
	if err := r.signPBFTPrePrepare(&pre); err != nil {
		return fmt.Errorf("sign retransmitted pbft pre-prepare: %w", err)
	}
	prepare, err := state.OnPrePrepare(pre)
	if err != nil {
		return err
	}
	r.logConsensus("PBFT_PRE_PREPARE_RETRANSMIT", r.node.NodeID, block.BlockHash, block.Height)
	if err := r.broadcastPBFTPrePrepare(ctx, pre); err != nil {
		return err
	}
	// A peer may already have a commit certificate for this exact digest even
	// when this replica missed enough original COMMIT messages. Probe peers in
	// parallel with the same-digest retransmission.
	r.requestCatchup(ctx)
	return r.broadcastPBFTPrepare(ctx, prepare)
}

func (r *NodeRuntime) checkPBFTLiveness(ctx context.Context) {
	state := r.pbftState()
	if state == nil || len(r.node.Validators) <= 1 {
		return
	}

	r.mu.Lock()
	fatal := firstNonEmpty(r.fatalPersistenceError, r.fatalExecutionError)
	inFlight := r.proposalInFlight
	lastBroadcast := r.proposalLastBroadcastAt
	retransmits := r.proposalRetransmitCount
	viewTarget := r.viewChangeTarget
	viewStarted := r.viewChangeStartedAt
	viewLastBroadcast := r.viewChangeLastBroadcast
	viewRetransmits := r.viewChangeRetransmits
	r.mu.Unlock()
	if fatal != "" {
		return
	}

	timeout := r.proposalTimeout()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	if r.isCurrentLeader() && inFlight && !lastBroadcast.IsZero() && time.Since(lastBroadcast) >= timeout {
		if retransmits < maxPBFTProposalRetransmits {
			if err := r.retransmitCurrentPBFTProposal(ctx); err != nil {
				r.setLastProposalError(err)
			}
			return
		}
		r.initiatePBFTViewChange(ctx, state.View()+1)
		return
	}

	// A backup starts view change only while there is actual work/consensus
	// activity. An empty idle shard must not rotate primaries forever.
	active := inFlight || r.pool.Len() > 0 || state.Snapshot().Stage != pbft.StageIdle
	if !active {
		return
	}
	viewTimeout := time.Duration(maxPBFTProposalRetransmits+1) * timeout
	if viewTimeout < 15*time.Second {
		viewTimeout = 15 * time.Second
	}
	// PBFT liveness uses increasing timeouts in higher installed views so
	// correct replicas eventually remain together long enough to make progress.
	shift := state.View()
	if shift > 4 {
		shift = 4
	}
	viewTimeout *= time.Duration(uint64(1) << shift)

	if viewTarget > state.View() && !viewStarted.IsZero() {
		if viewRetransmits < 2 && (viewLastBroadcast.IsZero() || time.Since(viewLastBroadcast) >= viewTimeout/2) {
			if err := r.rebroadcastCurrentPBFTViewChange(ctx); err != nil {
				r.setLastProposalError(err)
			}
			return
		}
		if time.Since(viewStarted) >= 2*viewTimeout {
			r.initiatePBFTViewChange(ctx, viewTarget+1)
		}
		return
	}
	if last := state.LastProgress(); !last.IsZero() && time.Since(last) >= viewTimeout {
		r.initiatePBFTViewChange(ctx, state.View()+1)
	}
}

func (r *NodeRuntime) rebroadcastCurrentPBFTViewChange(ctx context.Context) error {
	r.mu.Lock()
	target := r.viewChangeTarget
	height := r.committedHeight + 1
	r.mu.Unlock()
	state := r.pbftState()
	if target <= state.View() || target == 0 {
		return nil
	}
	vc, err := state.BuildViewChange(target, height)
	if err != nil {
		return err
	}
	if err := r.signPBFTViewChange(&vc); err != nil {
		return fmt.Errorf("sign retransmitted pbft view-change: %w", err)
	}
	if _, _, err := state.AcceptViewChange(vc); err != nil {
		return err
	}
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTViewChange, r.node.NodeID, "", r.node.ShardID, height, vc.View, height, vc)
	if err != nil {
		return err
	}
	if errs := r.broadcastPBFTEnvelope(ctx, envelope); len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}
	r.mu.Lock()
	r.viewChangeLastBroadcast = time.Now()
	r.viewChangeRetransmits++
	r.incrementRuntimeMetricLocked("pbft_view_change_retransmit_count")
	r.mu.Unlock()
	return nil
}

func (r *NodeRuntime) initiatePBFTViewChange(ctx context.Context, newView uint64) {
	state := r.pbftState()
	if state == nil || newView <= state.View() {
		return
	}
	r.mu.Lock()
	height := r.committedHeight + 1
	r.mu.Unlock()

	vc, err := state.BuildViewChange(newView, height)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	if err := r.signPBFTViewChange(&vc); err != nil {
		r.setLastProposalError(fmt.Errorf("sign pbft view-change: %w", err))
		return
	}
	if !state.MarkViewChangeBroadcast(newView) {
		return
	}

	r.mu.Lock()
	now := time.Now()
	if r.viewChangeTarget != newView {
		r.viewChangeStartedAt = now
		r.viewChangeRetransmits = 0
	}
	r.viewChangeTarget = newView
	r.viewChangeLastBroadcast = now
	r.incrementRuntimeMetricLocked("pbft_view_change_started_count")
	r.lastProposalError = fmt.Sprintf("pbft_view_change_started:%d", newView)
	r.mu.Unlock()

	reached, _, err := state.AcceptViewChange(vc)
	if err != nil {
		r.setLastProposalError(err)
		return
	}

	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTViewChange,
		r.node.NodeID,
		"",
		r.node.ShardID,
		height,
		vc.View,
		height,
		vc,
	)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	if errs := r.broadcastPBFTEnvelope(ctx, envelope); len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}

	if reached && state.LeaderForView(newView) == r.node.NodeID {
		if err := r.broadcastPBFTNewView(ctx, newView, height); err != nil {
			r.setLastProposalError(err)
		}
	}
}

func (r *NodeRuntime) handlePBFTViewChange(ctx context.Context, msg p2p.MessageEnvelope) error {
	vc, err := p2p.DecodePayload[pbft.ViewChange](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != vc.NodeID || msg.Height != vc.Height || msg.View != vc.View {
		return fmt.Errorf("pbft view-change envelope mismatch")
	}
	if err := r.verifyPBFTViewChangeAuthentication(vc); err != nil {
		return fmt.Errorf("pbft view-change authentication: %w", err)
	}
	state := r.pbftState()
	reached, count, err := state.AcceptViewChange(vc)
	if err != nil {
		return err
	}
	r.logConsensus(msg.MessageType, msg.FromNode, preparedHash(vc.Prepared), vc.Height)

	// f+1 view-change messages are enough evidence that at least one correct
	// replica wants to leave the current view; join the same target view.
	if count >= pbft.FaultTolerance(len(r.node.Validators))+1 && !state.HasViewChangeFrom(vc.NewView, r.node.NodeID) {
		r.initiatePBFTViewChange(ctx, vc.NewView)
	}
	if reached && state.LeaderForView(vc.NewView) == r.node.NodeID {
		return r.broadcastPBFTNewView(ctx, vc.NewView, vc.Height)
	}
	return nil
}

func (r *NodeRuntime) broadcastPBFTNewView(ctx context.Context, newView, height uint64) error {
	state := r.pbftState()
	nv, err := state.BuildNewView(newView, height)
	if err != nil {
		return err
	}
	if err := r.signPBFTNewView(&nv); err != nil {
		return fmt.Errorf("sign pbft new-view: %w", err)
	}
	selected, hasSelected, err := state.AcceptNewView(nv)
	if err != nil {
		return err
	}

	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTNewView,
		r.node.NodeID,
		"",
		r.node.ShardID,
		height,
		newView,
		height,
		nv,
	)
	if err != nil {
		return err
	}
	if errs := r.broadcastPBFTEnvelope(ctx, envelope); len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}
	r.logConsensus("PBFT_NEW_VIEW_LOCAL", r.node.NodeID, selected.BlockHash, height)
	return r.onPBFTNewViewAccepted(ctx, nv, selected, hasSelected)
}

func (r *NodeRuntime) handlePBFTNewView(ctx context.Context, msg p2p.MessageEnvelope) error {
	nv, err := p2p.DecodePayload[pbft.NewView](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != nv.LeaderID || msg.View != nv.View || msg.Height != nv.Height {
		return fmt.Errorf("pbft new-view envelope mismatch")
	}
	if err := r.verifyPBFTNewViewAuthentication(nv); err != nil {
		return fmt.Errorf("pbft new-view authentication: %w", err)
	}
	selected, hasSelected, err := r.pbftState().AcceptNewView(nv)
	if err != nil {
		return err
	}
	r.logConsensus(msg.MessageType, msg.FromNode, selected.BlockHash, nv.Height)
	return r.onPBFTNewViewAccepted(ctx, nv, selected, hasSelected)
}

func (r *NodeRuntime) onPBFTNewViewAccepted(ctx context.Context, nv pbft.NewView, selected realblock.Block, hasSelected bool) error {
	r.mu.Lock()
	oldHash := r.proposalInFlightHash
	oldBlock := r.proposals[oldHash]
	if oldHash != "" && (!hasSelected || oldHash != selected.BlockHash) {
		delete(r.proposals, oldHash)
		delete(r.votes, oldHash)
	}
	r.proposalInFlight = false
	r.proposalInFlightHash = ""
	r.proposalStartedAt = time.Time{}
	r.proposalLastBroadcastAt = time.Time{}
	r.proposalRetransmitCount = 0
	r.proposalWorkUnits.Store(0)
	r.viewChangeStartedAt = time.Time{}
	r.viewChangeLastBroadcast = time.Time{}
	r.viewChangeRetransmits = 0
	r.viewChangeTarget = 0
	r.incrementRuntimeMetricLocked("pbft_new_view_accepted_count")
	r.lastProposalError = ""
	for height, pending := range r.deferredPrePrepares {
		if pending.View < nv.View {
			delete(r.deferredPrePrepares, height)
		}
	}
	r.mu.Unlock()

	if oldBlock.BlockHash != "" && (!hasSelected || oldBlock.BlockHash != selected.BlockHash) {
		r.pool.ReleaseReserved(oldBlock.TxList)
	}

	if !r.isCurrentLeader() {
		return nil
	}
	if !hasSelected {
		return nil
	}

	workUnits := r.estimateProposalValidationWork(selected)
	r.mu.Lock()
	r.proposalInFlight = true
	r.proposalInFlightHash = selected.BlockHash
	r.proposalStartedAt = time.Now()
	r.proposalLastBroadcastAt = r.proposalStartedAt
	r.proposalRetransmitCount = 0
	r.proposalWorkUnits.Store(int64(workUnits))
	r.proposals[selected.BlockHash] = selected
	r.votes[selected.BlockHash] = map[string]bool{}
	r.mu.Unlock()

	pre := pbft.PrePrepare{
		View:      nv.View,
		Sequence:  selected.Height,
		Height:    selected.Height,
		LeaderID:  r.node.NodeID,
		BlockHash: selected.BlockHash,
		Block:     selected,
	}
	if err := r.validateConsensusBlockBody(selected); err != nil {
		return fmt.Errorf("new-view selected consensus block body: %w", err)
	}
	if err := r.signPBFTPrePrepare(&pre); err != nil {
		return fmt.Errorf("sign new-view pbft pre-prepare: %w", err)
	}
	prepare, err := r.pbftState().OnPrePrepare(pre)
	if err != nil {
		return err
	}
	if err := r.broadcastPBFTPrePrepare(ctx, pre); err != nil {
		return err
	}
	return r.broadcastPBFTPrepare(ctx, prepare)
}

func (r *NodeRuntime) handlePBFTCheckpoint(_ context.Context, msg p2p.MessageEnvelope) error {
	checkpoint, err := p2p.DecodePayload[pbft.Checkpoint](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != checkpoint.NodeID || msg.Height != checkpoint.Height {
		return fmt.Errorf("pbft checkpoint envelope mismatch")
	}
	if err := r.verifyPBFTCheckpointAuthentication(checkpoint); err != nil {
		return fmt.Errorf("pbft checkpoint authentication: %w", err)
	}
	r.noteCatchupTarget(checkpoint.Height)
	stable, _, _, err := r.pbftState().AcceptCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	r.logConsensus(msg.MessageType, msg.FromNode, checkpoint.BlockHash, checkpoint.Height)
	if stable {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_stable_checkpoint_count")
		r.mu.Unlock()
	}
	return nil
}

func (r *NodeRuntime) maybeBroadcastPBFTCheckpoint(ctx context.Context, block realblock.Block) {
	state := r.pbftState()
	if state == nil || !state.CheckpointDue(block.Height) {
		return
	}
	stateRoot := r.plugins.StateStorage.Root(r.db)
	checkpoint := state.BuildCheckpoint(block.Height, block.BlockHash, stateRoot)
	if err := r.signPBFTCheckpoint(&checkpoint); err != nil {
		r.setLastProposalError(fmt.Errorf("sign pbft checkpoint: %w", err))
		return
	}
	stable, _, _, err := state.AcceptCheckpoint(checkpoint)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTCheckpoint,
		r.node.NodeID,
		"",
		r.node.ShardID,
		block.Height,
		state.View(),
		block.Height,
		checkpoint,
	)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	if errs := r.broadcastPBFTEnvelope(ctx, envelope); len(errs) > 0 {
		r.setLastProposalError(errs[0])
	}
	if stable {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_stable_checkpoint_count")
		r.mu.Unlock()
	}
}

func (r *NodeRuntime) maybeRespondWithCertifiedCommit(ctx context.Context, toNode string, pre pbft.PrePrepare) (bool, error) {
	r.mu.Lock()
	committedHeight := r.committedHeight
	r.mu.Unlock()
	if pre.Height == 0 || pre.Height > committedHeight || toNode == "" || toNode == r.node.NodeID {
		return false, nil
	}
	local, ok, err := r.store.ReadCommittedAtHeight(pre.Height)
	if err != nil {
		return false, err
	}
	if !ok || local.BlockHash != pre.BlockHash {
		return false, nil
	}
	cert, ok := r.pbftState().CommitCertificate(pre.Height)
	if !ok || cert.BlockHash != pre.BlockHash {
		return false, nil
	}
	if err := r.sendCertifiedCatchupBlock(ctx, toNode, local, cert); err != nil {
		return true, err
	}
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("pbft_commit_certificate_retransmit_count")
	r.mu.Unlock()
	return true, nil
}

func (r *NodeRuntime) sendCertifiedCatchupBlock(ctx context.Context, toNode string, block realblock.Block, cert pbft.CommitCertificate) error {
	envelope, err := p2p.NewEnvelope(catchupBlockMessage, r.node.NodeID, toNode, r.node.ShardID, block.Height, cert.View, cert.Sequence, CatchupBlock{Block: block, SourceNode: r.node.NodeID, Certificate: cert})
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, toNode, envelope)
}

func (r *NodeRuntime) handleCertifiedCatchupRequest(ctx context.Context, requester string, request CatchupRequest) error {
	if requester == "" || !r.pbftState().IsValidator(requester) || request.ShardID != r.node.ShardID || request.FromHeight == 0 || request.ToHeight < request.FromHeight {
		return fmt.Errorf("invalid certified catch-up request")
	}
	if request.ToHeight-request.FromHeight+1 > certifiedCatchupBatchSize {
		return fmt.Errorf("certified catch-up request exceeds batch limit")
	}
	r.mu.Lock()
	if r.catchupResponsesInFlight >= maxConcurrentCatchupResponses {
		r.incrementRuntimeMetricLocked("pbft_catchup_response_busy_count")
		r.mu.Unlock()
		return nil
	}
	r.catchupResponsesInFlight++
	committedHeight := r.committedHeight
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.catchupResponsesInFlight--
		r.mu.Unlock()
	}()
	if request.FromHeight > committedHeight {
		return nil
	}

	// Read only the requested durable height range. The storage layer builds a
	// lightweight height->offset index once and extends it only across newly
	// appended JSONL bytes, so a 1,000+ block Groundhog recovery does not
	// repeatedly parse and restore the entire 400-760 MB durable block log.
	toHeight := request.ToHeight
	if committedHeight < toHeight {
		toHeight = committedHeight
	}
	committedBlocks, err := r.store.ReadCommittedRange(request.FromHeight, toHeight)
	if err != nil {
		return err
	}
	blocksByHeight := make(map[uint64]realblock.Block, len(committedBlocks))
	for _, block := range committedBlocks {
		blocksByHeight[block.Height] = block
	}
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("pbft_catchup_indexed_range_read_count")
	r.mu.Unlock()

	sent := 0
	missingHeight := uint64(0)
	for height := request.FromHeight; height <= request.ToHeight && height <= committedHeight; height++ {
		cert, ok := r.pbftState().CommitCertificate(height)
		if !ok {
			missingHeight = height
			break
		}
		block, ok := blocksByHeight[height]
		if !ok || block.BlockHash != cert.BlockHash {
			missingHeight = height
			break
		}
		if err := r.sendCertifiedCatchupBlock(ctx, requester, block, cert); err != nil {
			return err
		}
		sent++
	}
	if sent > 0 {
		r.mu.Lock()
		if r.runtimeMetricCounts == nil {
			r.runtimeMetricCounts = map[string]int64{}
		}
		r.runtimeMetricCounts["pbft_catchup_blocks_served_count"] += int64(sent)
		r.mu.Unlock()
	}
	if sent == 0 || missingHeight != 0 {
		snapshot := r.pbftState().Snapshot()
		hint := CatchupUnavailable{SourceNode: r.node.NodeID, RequestedFromHeight: func() uint64 {
			if missingHeight != 0 {
				return missingHeight
			}
			return request.FromHeight
		}(), CommittedHeight: committedHeight, StableCheckpointHeight: snapshot.StableCheckpointHeight, Reason: "certified_catchup_proof_or_block_unavailable"}
		envelope, err := p2p.NewEnvelope(catchupUnavailableMessage, r.node.NodeID, requester, r.node.ShardID, request.FromHeight, snapshot.View, request.FromHeight, hint)
		if err != nil {
			return err
		}
		_ = r.sendToNode(ctx, requester, envelope)
	}
	return nil
}

func (r *NodeRuntime) handleCertifiedCatchupBlock(ctx context.Context, fromNode string, item CatchupBlock) error {
	if fromNode == "" || fromNode != item.SourceNode || !r.pbftState().IsValidator(fromNode) {
		return fmt.Errorf("certified catch-up source is not a validator")
	}
	if err := r.validateConsensusBlockBody(item.Block); err != nil {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_catchup_failure_count")
		r.mu.Unlock()
		return fmt.Errorf("certified catch-up block body: %w", err)
	}
	if err := r.verifyCommitCertificateAuthentication(item.Certificate); err != nil {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_catchup_failure_count")
		r.mu.Unlock()
		return fmt.Errorf("certified catch-up authentication: %w", err)
	}
	if err := r.pbftState().AcceptCommitCertificate(item.Certificate, item.Block); err != nil {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_catchup_failure_count")
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	expected := r.committedHeight + 1
	committedHash := r.committedHash
	r.mu.Unlock()
	if item.Block.Height < expected {
		return nil
	}
	if item.Block.Height == expected && item.Block.PreviousHash != committedHash {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_catchup_failure_count")
		r.mu.Unlock()
		return fmt.Errorf("certified catch-up parent mismatch at height %d", item.Block.Height)
	}
	r.rememberProposal(item.Block)
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("pbft_catchup_block_count")
	r.mu.Unlock()
	if err := r.enqueueCommitTask(commitTaskCatchUp, item.Block, CommitOriginCatchUp); err != nil {
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("pbft_catchup_failure_count")
		r.mu.Unlock()
		return err
	}
	return nil
}

func (r *NodeRuntime) deferPBFTPrePrepare(ctx context.Context, fromNode string, pre pbft.PrePrepare, replayed bool) error {
	disposition := r.deferPBFTPrePrepareWithDisposition(fromNode, pre)
	switch disposition {
	case prePrepareDeferralBecameCurrent:
		envelope, err := pbftPrePrepareEnvelope(pre, fromNode, r.node.ShardID)
		if err != nil {
			return err
		}
		return r.handlePBFTPrePrepare(ctx, envelope)
	case prePrepareDeferralStored:
		r.requestCatchup(ctx)
		return nil
	default:
		if !replayed {
			r.requestCatchup(ctx)
		}
		return nil
	}
}

func (r *NodeRuntime) deferPBFTPrePrepareWithDisposition(fromNode string, pre pbft.PrePrepare) prePrepareDeferralDisposition {
	block := pre.Block
	if block.BlockHash == "" || fromNode == "" {
		return prePrepareDeferralIgnored
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	expectedHeight := r.committedHeight + 1
	if block.Height < expectedHeight || block.Height > expectedHeight+8 {
		return prePrepareDeferralIgnored
	}
	if block.Height == expectedHeight {
		return prePrepareDeferralBecameCurrent
	}
	if r.deferredPrePrepares == nil {
		r.deferredPrePrepares = map[uint64]deferredPrePrepare{}
	}
	if existing, ok := r.deferredPrePrepares[block.Height]; ok {
		if existing.View > pre.View {
			return prePrepareDeferralIgnored
		}
		if existing.View == pre.View && existing.Block.BlockHash != block.BlockHash {
			r.lastProposalError = fmt.Sprintf("conflicting future pre-prepare at view %d height %d", pre.View, block.Height)
			return prePrepareDeferralIgnored
		}
	}
	r.deferredPrePrepares[block.Height] = deferredPrePrepare{
		FromNode:  fromNode,
		View:      pre.View,
		Sequence:  pre.Sequence,
		Block:     block,
		Signature: pre.Signature,
	}
	return prePrepareDeferralStored
}

func (r *NodeRuntime) replayDeferredPBFTPrePrepare(ctx context.Context) {
	r.mu.Lock()
	expectedHeight := r.committedHeight + 1
	for height := range r.deferredPrePrepares {
		if height < expectedHeight {
			delete(r.deferredPrePrepares, height)
		}
	}
	pending, ok := r.deferredPrePrepares[expectedHeight]
	if ok {
		delete(r.deferredPrePrepares, expectedHeight)
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	if pending.View != r.currentPBFTView() {
		return
	}
	pre := pbft.PrePrepare{
		View:      pending.View,
		Sequence:  pending.Sequence,
		Height:    pending.Block.Height,
		LeaderID:  pending.FromNode,
		BlockHash: pending.Block.BlockHash,
		Block:     pending.Block,
		Signature: pending.Signature,
	}
	envelope, err := pbftPrePrepareEnvelope(pre, pending.FromNode, r.node.ShardID)
	if err != nil {
		r.setLastProposalError(err)
		return
	}
	if err := r.handlePBFTPrePrepare(ctx, envelope); err != nil {
		r.setLastProposalError(err)
	}
}

func pbftPrePrepareEnvelope(pre pbft.PrePrepare, fromNode, shardID string) (p2p.MessageEnvelope, error) {
	return p2p.NewEnvelope(
		p2p.MessagePBFTPrePrepare,
		fromNode,
		"",
		shardID,
		pre.Height,
		pre.View,
		pre.Sequence,
		pre,
	)
}

func preparedHash(cert *pbft.PreparedCertificate) string {
	if cert == nil {
		return ""
	}
	return cert.BlockHash
}

func (r *NodeRuntime) setLastProposalError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastProposalError = err.Error()
	r.mu.Unlock()
}

func (r *NodeRuntime) clearLastProposalError() {
	r.mu.Lock()
	r.lastProposalError = ""
	r.mu.Unlock()
}
