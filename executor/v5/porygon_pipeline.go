package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

// Porygon's paper pipeline separates four protocol milestones that were
// previously collapsed into the shared durable-commit worker:
// Witness -> Ordering -> Execution -> Commit/Materialization.
// PBFT still owns Ordering. This file only lets later PBFT heights advance after
// an Ordering certificate while the Porygon-owned Execution/Commit stages run
// on separate workers. No PBFT quorum or message rule is changed here.

type porygonPipelinePhase string

const (
	porygonPipelineOrdered           porygonPipelinePhase = "ordered"
	porygonPipelineExecuted          porygonPipelinePhase = "executed"
	porygonPipelineProtocolCommitted porygonPipelinePhase = "protocol_committed"
	porygonPipelineDurable           porygonPipelinePhase = "durable"
	porygonPipelineFailed            porygonPipelinePhase = "failed"
)

type porygonPipelineBlock struct {
	Block               realblock.Block
	Phase               porygonPipelinePhase
	Generation          uint64
	Execution           BlockExecutionResult
	PreparedSnapshot    map[string]string
	PreparedRoot        string
	BaseSnapshotRoot    string
	PartitionUpdates    map[string][]PorygonStateUpdate
	ProtocolCertificate PorygonMultiShardUpdateCertificate
	OrderedAt           time.Time
	ExecutionStartedAt  time.Time
	ExecutionFinishedAt time.Time
	CommitStartedAt     time.Time
	CommitFinishedAt    time.Time
	DurableFinishedAt   time.Time
}

type porygonPipelineRuntime struct {
	mu sync.Mutex

	started bool
	execQ   chan realblock.Block
	commitQ chan uint64

	baselineHeight          uint64
	orderedHeight           uint64
	orderedHash             string
	executedHeight          uint64
	protocolCommittedHeight uint64
	durableHeight           uint64

	blocks          map[uint64]*porygonPipelineBlock
	rollbackDigests map[uint64]string
}

var porygonPipelineRuntimes sync.Map // map[*NodeRuntime]*porygonPipelineRuntime

func (r *NodeRuntime) porygonPipelineEnabledRuntime() bool {
	if r == nil || r.plugins.BlockExecutor == nil || r.plugins.BlockExecutor.ID() != porygonBlockExecutorID {
		return false
	}
	executor, ok := r.plugins.BlockExecutor.(porygonBlockExecutor)
	if !ok {
		return false
	}
	return porygonBool(executor.config, "pipeline_enabled", true)
}

func (r *NodeRuntime) porygonPipelineRuntime() *porygonPipelineRuntime {
	if value, ok := porygonPipelineRuntimes.Load(r); ok {
		return value.(*porygonPipelineRuntime)
	}
	r.mu.Lock()
	committedHeight := r.committedHeight
	committedHash := r.committedHash
	r.mu.Unlock()
	created := &porygonPipelineRuntime{
		execQ:                   make(chan realblock.Block, 16),
		commitQ:                 make(chan uint64, 16),
		baselineHeight:          committedHeight,
		orderedHeight:           committedHeight,
		orderedHash:             committedHash,
		executedHeight:          committedHeight,
		protocolCommittedHeight: committedHeight,
		durableHeight:           committedHeight,
		blocks:                  map[uint64]*porygonPipelineBlock{},
		rollbackDigests:         map[uint64]string{},
	}
	actual, _ := porygonPipelineRuntimes.LoadOrStore(r, created)
	return actual.(*porygonPipelineRuntime)
}

func (r *NodeRuntime) porygonPipelineEnsureWorkers() error {
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	if state.started {
		state.mu.Unlock()
		return nil
	}
	r.mu.Lock()
	ctx := r.commitWorkerContext
	r.mu.Unlock()
	if ctx == nil {
		state.mu.Unlock()
		return fmt.Errorf("porygon pipeline requires running commit worker context")
	}
	state.started = true
	state.mu.Unlock()
	go r.runPorygonPipelineExecutionWorker(ctx, state)
	go r.runPorygonPipelineCommitWorker(ctx, state)
	return nil
}

// porygonConsensusCursor is the parent of the next PBFT-ordered Porygon block.
// Other methods continue using durable committedHeight/committedHash.
func (r *NodeRuntime) porygonConsensusCursor() (uint64, string) {
	if !r.porygonPipelineEnabledRuntime() {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.committedHeight, r.committedHash
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.orderedHeight, state.orderedHash
}

func (r *NodeRuntime) porygonConsensusNextHeight() uint64 {
	height, _ := r.porygonConsensusCursor()
	return height + 1
}

// The paper's four-stage pipeline has at most two ordered-but-not-yet-M-committed
// transaction blocks ahead of the protocol commit frontier: while M(h) runs,
// O(h+2) may run, but O(h+3) belongs to the next protocol slot.  This bound is
// derived from W/O/E/M stage placement, not an empirical queue threshold.
func (r *NodeRuntime) porygonPipelineOrderingWindowOpen() bool {
	if !r.porygonPipelineEnabledRuntime() {
		return true
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.orderedHeight < state.protocolCommittedHeight+2
}

func (r *NodeRuntime) porygonPipelineBaselineHeight() uint64 {
	if !r.porygonPipelineEnabledRuntime() {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.committedHeight
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.baselineHeight
}

func porygonPipelinePlan(block realblock.Block) (porygonExecutionPlan, error) {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != porygonPlanAlgorithmID || len(block.ExecutionPlan.Payload) == 0 {
		return porygonExecutionPlan{}, fmt.Errorf("porygon pipeline block %d has no execution plan", block.Height)
	}
	var plan porygonExecutionPlan
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &plan); err != nil {
		return porygonExecutionPlan{}, fmt.Errorf("decode porygon pipeline plan at height %d: %w", block.Height, err)
	}
	if plan.BlockHeight != block.Height || plan.PlanDigest == "" || plan.PlanDigest != block.ExecutionPlan.PlanDigest {
		return porygonExecutionPlan{}, fmt.Errorf("porygon pipeline plan identity mismatch at height %d", block.Height)
	}
	return plan, nil
}

func porygonPendingEvidenceFromOrderedBlock(block realblock.Block) ([]porygonPendingTransactionEvidence, error) {
	plan, err := porygonPipelinePlan(block)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]tx.SignedTransaction, len(block.TxList))
	for _, item := range block.TxList {
		byID[item.TxID] = item
	}
	out := []porygonPendingTransactionEvidence{}
	rollbackMask := porygonRollbackMask(block.BlockHash)
	for _, assignment := range plan.Assignments {
		if !assignment.CrossShard || assignment.Abandoned || rollbackMask[assignment.TxID] {
			continue
		}
		item, ok := byID[assignment.TxID]
		if !ok {
			return nil, fmt.Errorf("porygon pending evidence missing ordered transaction %s", assignment.TxID)
		}
		out = append(out, porygonPendingTransactionEvidence{
			TxID: assignment.TxID, Height: block.Height,
			Accesses: porygonCanonicalAccesses(item), LockedKeys: append([]string(nil), assignment.LockedKeys...),
		})
	}
	return porygonCanonicalPendingEvidence(out), nil
}

func (r *NodeRuntime) porygonPipelineCrossRoundEvidence(releaseFrontier uint64) ([]porygonPendingTransactionEvidence, error) {
	if !r.porygonPipelineEnabledRuntime() {
		return nil, nil
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	heights := make([]uint64, 0, len(state.blocks))
	blocks := make(map[uint64]realblock.Block, len(state.blocks))
	for height, item := range state.blocks {
		if height <= releaseFrontier || height > state.orderedHeight || item == nil {
			continue
		}
		heights = append(heights, height)
		blocks[height] = item.Block
	}
	state.mu.Unlock()
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	out := []porygonPendingTransactionEvidence{}
	for _, height := range heights {
		items, err := porygonPendingEvidenceFromOrderedBlock(blocks[height])
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return porygonCanonicalPendingEvidence(out), nil
}

func (r *NodeRuntime) porygonPipelineReleaseFrontier() (PorygonMultiShardUpdateCertificate, bool) {
	if !r.porygonPipelineEnabledRuntime() {
		return PorygonMultiShardUpdateCertificate{}, false
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	height := state.protocolCommittedHeight
	if height == 0 || height <= state.durableHeight && state.blocks[height] == nil {
		return PorygonMultiShardUpdateCertificate{}, false
	}
	item := state.blocks[height]
	if item == nil || item.ProtocolCertificate.CertificateDigest == "" || item.ProtocolCertificate.RolledBack {
		return PorygonMultiShardUpdateCertificate{}, false
	}
	return item.ProtocolCertificate, true
}

// bindPorygonCrossRoundEvidence binds the paper's transaction-level pending
// set, then constructs the compact L/U/T Proposal body.  No block-level release
// frontier is used: each transaction leaves the set only when its later commit
// proposal is ordered (or it is abandoned/rolled back).
func (r *NodeRuntime) bindPorygonCrossRoundEvidence(block realblock.Block) (realblock.Block, error) {
	if !r.porygonPipelineEnabledRuntime() {
		return block, nil
	}
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return block, err
	}
	pending := r.porygonPaperPendingEvidenceForProposal(block.Height)
	evidence.PreviousPendingTransactions = pending
	evidence.PreviousPendingDigest = ""
	if len(pending) > 0 {
		evidence.PreviousPendingDigest = stableJSONDigest(pending)
	}
	evidence.ReleasedPendingCertificates = nil // legacy v1 frontier is intentionally retired.
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, evidence); err != nil {
		return block, err
	}
	realblock.AssignHash(&block)
	return r.porygonBindCompactProposalContext(block)
}

func (r *NodeRuntime) verifyPorygonCrossRoundEvidence(block realblock.Block) error {
	if !r.porygonPipelineEnabledRuntime() {
		return nil
	}
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return err
	}
	expected := r.porygonPaperPendingEvidenceForProposal(block.Height)
	actual := porygonCanonicalPendingEvidence(evidence.PreviousPendingTransactions)
	if stableJSONDigest(actual) != stableJSONDigest(expected) {
		return fmt.Errorf("porygon transaction-level pending evidence mismatch at height %d", block.Height)
	}
	if len(actual) == 0 {
		if evidence.PreviousPendingDigest != "" {
			return fmt.Errorf("porygon empty pending set has non-empty digest")
		}
	} else if evidence.PreviousPendingDigest != stableJSONDigest(actual) {
		return fmt.Errorf("porygon pending digest mismatch")
	}
	if evidence.CompactProposal != nil {
		proposal := evidence.CompactProposal
		if proposal.PendingTxDigest != evidence.PreviousPendingDigest {
			return fmt.Errorf("porygon L/U/T proposal pending digest mismatch")
		}
		if err := r.porygonPaperVerifyCertifiedProposalU(block.Height, proposal.U); err != nil {
			return err
		}
		expectedEC := porygonECDescriptorForHeight(r.plan.NodeConfigs, block.Height, block.ShardID, r.porygonExecutionShardCount(), r.porygonExecutionCommitteeCount())
		if proposal.ExecutionCommittee.CommitteeDigest != expectedEC.CommitteeDigest || stableJSONDigest(proposal.ExecutionCommittee) != stableJSONDigest(expectedEC) {
			return fmt.Errorf("porygon L/U/T proposal EC lifecycle mismatch at height %d", block.Height)
		}
		tHeight, tRoot, tPartitions := r.porygonPaperStateAnchorForProposal(block.Height)
		if tRoot == "" || proposal.TStateHeight != tHeight || proposal.TStateRoot != tRoot || stableJSONDigest(proposal.TPartitionRoots) != stableJSONDigest(tPartitions) {
			return fmt.Errorf("porygon L/U/T proposal T state mismatch at height %d", block.Height)
		}
	}
	return nil
}

func (r *NodeRuntime) porygonPipelineOnOrderingCertified(block realblock.Block) error {
	if !r.porygonPipelineEnabledRuntime() {
		return r.enqueueCommitTask(commitTaskConsensus, block, CommitOriginConsensus)
	}
	if err := r.porygonPipelineEnsureWorkers(); err != nil {
		return err
	}
	state := r.porygonPipelineRuntime()
	now := time.Now()
	state.mu.Lock()
	if existing := state.blocks[block.Height]; existing != nil {
		if existing.Block.BlockHash != block.BlockHash {
			state.mu.Unlock()
			return fmt.Errorf("porygon ordered-height conflict at %d", block.Height)
		}
		state.mu.Unlock()
		return nil
	}
	if block.Height != state.orderedHeight+1 || block.PreviousHash != state.orderedHash {
		wantHeight, wantHash := state.orderedHeight+1, state.orderedHash
		state.mu.Unlock()
		return fmt.Errorf("porygon ordering cursor mismatch got=%d/%s want=%d/%s", block.Height, block.PreviousHash, wantHeight, wantHash)
	}
	state.blocks[block.Height] = &porygonPipelineBlock{Block: block, Phase: porygonPipelineOrdered, Generation: 1, OrderedAt: now}
	state.orderedHeight = block.Height
	state.orderedHash = block.BlockHash
	state.mu.Unlock()
	if err := r.porygonPaperRegisterOrdered(block); err != nil {
		return err
	}

	// The proposer head tracks consensus Ordering, not physical durability, for
	// Porygon. This is what permits O(h+1) to overlap E/M(h).
	if r.proposer != nil {
		r.proposer.Confirm(block)
	}
	r.mu.Lock()
	if r.proposalInFlightHash == block.BlockHash {
		r.proposalInFlight = false
		r.proposalInFlightHash = ""
		r.proposalStartedAt = time.Time{}
		r.proposalLastBroadcastAt = time.Time{}
		r.proposalRetransmitCount = 0
		r.proposalWorkUnits.Store(0)
		r.lastProposalError = ""
	}
	r.incrementRuntimeMetricLocked("porygon_pipeline_ordered_count")
	r.mu.Unlock()

	select {
	case <-r.commitWorkerContext.Done():
		return r.commitWorkerContext.Err()
	case state.execQ <- block:
		return nil
	}
}

func (r *NodeRuntime) runPorygonPipelineExecutionWorker(ctx context.Context, state *porygonPipelineRuntime) {
	for {
		select {
		case <-ctx.Done():
			return
		case block := <-state.execQ:
			if err := r.executePorygonPipelineBlock(ctx, state, block); err != nil && ctx.Err() == nil {
				r.markFatalExecutionError(block, err)
				state.mu.Lock()
				if item := state.blocks[block.Height]; item != nil {
					item.Phase = porygonPipelineFailed
				}
				state.mu.Unlock()
				return
			}
		}
	}
}

func (r *NodeRuntime) porygonPipelineExecutionBase(block realblock.Block) (map[string]string, string, error) {
	proposal, err := porygonProposalFromBlock(block)
	if err != nil {
		return nil, "", err
	}
	partitionID := r.stateAccessPartitionID()
	partitionRoot := proposal.TPartitionRoots[partitionID]
	if partitionRoot == "" {
		return nil, "", fmt.Errorf("Porygon Proposal T missing partition root for %s", partitionID)
	}
	snapshot, err := r.porygonPaperPartitionSnapshot(proposal.TStateHeight, partitionID, partitionRoot)
	if err != nil {
		return nil, "", err
	}
	return snapshot, partitionRoot, nil
}

func (r *NodeRuntime) executePorygonPipelineBlock(ctx context.Context, state *porygonPipelineRuntime, block realblock.Block) error {
	base, baseRoot, err := r.porygonPipelineExecutionBase(block)
	if err != nil {
		return err
	}
	hydrated, err := r.porygonHydrateCompactProposal(ctx, block)
	if err != nil {
		return err
	}
	state.mu.Lock()
	item := state.blocks[block.Height]
	if item == nil || item.Block.BlockHash != block.BlockHash {
		state.mu.Unlock()
		return fmt.Errorf("porygon pipeline ordered block missing at height %d", block.Height)
	}
	if item.Phase != porygonPipelineOrdered {
		state.mu.Unlock()
		return nil // stale duplicate queue item after rollback/re-execution
	}
	generation := item.Generation
	if generation == 0 {
		generation = 1
		item.Generation = generation
	}
	item.ExecutionStartedAt = time.Now()
	executionStartedAt := item.ExecutionStartedAt
	state.mu.Unlock()

	shardCount := r.porygonExecutionShardCount()
	stateFetch := func(fetchCtx context.Context, transaction tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		homeShard := fmt.Sprintf("s%d", porygonStateShard(access.Key, shardCount))
		return r.porygonStateProjectionFetch(fetchCtx, hydrated, transaction, access, homeShard)
	}
	executed, err := r.plugins.BlockExecutor.ExecuteBlock(ctx, BlockExecutionInput{
		Block:                           hydrated,
		BaseStateSnapshot:               base,
		BaseStateCommitment:             nil,
		NodeID:                          r.node.NodeID,
		ShardID:                         r.node.ShardID,
		ExecutionShardID:                effectiveExecutionShardID(r.node),
		PorygonExecutionShardID:         r.porygonExecutionRoleShardID(block.Height),
		PorygonWaveExchange:             r.porygonWaveExchange,
		PorygonBatchExchange:            r.porygonBatchExchange,
		PorygonMultiShardUpdate:         r.porygonPaperPartitionRootProjection, // Paper2: later ESC applies U; Storage Roles authenticate the logical partition root.
		PorygonStateFetch:               stateFetch,
		PorygonCrossBatchWitnessOverlap: r.porygonCrossBatchWitnessOverlapObserved(block.Height),
		WorkerCount:                     blockExecutorWorkerCountFromProfile(r.pluginSnapshot),
		Execution:                       r.plugins.Execution,
		Scheduler:                       r.plugins.Scheduler,
		ExecutionPlanVerified:           r.hasVerifiedExecutionPlan(block),
		Progress:                        r.updateBlockExecutionProgress,
	})
	if err != nil {
		return err
	}
	if err := r.porygonPaperRegisterExecution(hydrated, executed); err != nil {
		return err
	}
	if proposal, proposalErr := porygonProposalFromBlock(block); proposalErr == nil {
		r.porygonReleaseTransactionBlockCache(proposal.L)
	}
	certifiedRoots := map[string]string{}
	if raw, ok := executed.ActualMetrics["porygon_certified_partition_roots"].(map[string]string); ok {
		certifiedRoots = raw
	} else if raw, ok := executed.ActualMetrics["porygon_certified_partition_roots"].(map[string]any); ok {
		for shard, value := range raw {
			if root, ok := value.(string); ok {
				certifiedRoots[shard] = root
			}
		}
	}
	certifiedSnapshot, certifiedRoot, certifyErr := r.porygonPaperRecordCertifiedDelta(block.Height, executed.StateDelta, certifiedRoots)
	if certifyErr != nil {
		return certifyErr
	}
	localCertifiedRoot, localRootReady := r.porygonPaperLocalCertifiedRootAtHeight(block.Height)
	if !localRootReady {
		return fmt.Errorf("Porygon canonical local partition root missing after execution at height %d", block.Height)
	}
	// Execution reads remain anchored at Proposal.T(h-2), but durable/materialized
	// state is canonical state(h-1)+this round's explicit writes. Export the
	// latter as StateRootAfter/StateUpdates so artifacts never report the stale
	// T-relative working snapshot as durable truth.
	executed.ExecutionResult.StateRootAfter = localCertifiedRoot
	executed.ExecutionResult.StateUpdates = copyRegistryStringMap(certifiedSnapshot)
	finished := time.Now()
	if executed.ActualMetrics == nil {
		executed.ActualMetrics = map[string]any{}
	}
	executed.ActualMetrics["porygon_pipeline_execution_stage_split"] = true
	executed.ActualMetrics["porygon_pipeline_execution_started_at_ns"] = executionStartedAt.UnixNano()
	executed.ActualMetrics["porygon_pipeline_execution_finished_at_ns"] = finished.UnixNano()
	executed.ActualMetrics["porygon_pipeline_ordered_height"] = block.Height

	state.mu.Lock()
	item = state.blocks[block.Height]
	if item == nil || item.Block.BlockHash != block.BlockHash || item.Generation != generation || item.Phase != porygonPipelineOrdered {
		state.mu.Unlock()
		r.addPorygonRuntimeMetric("porygon_pipeline_stale_execution_discard_count", 1)
		return nil
	}
	item.Execution = executed
	item.PreparedSnapshot = nil // paper structure uses Proposal T, never implicit predecessor forwarding.
	item.PreparedRoot = certifiedRoot
	item.BaseSnapshotRoot = baseRoot
	item.PartitionUpdates = nil
	item.ExecutionFinishedAt = finished
	item.Phase = porygonPipelineExecuted
	if block.Height > state.executedHeight {
		state.executedHeight = block.Height
	}
	state.mu.Unlock()
	r.addPorygonRuntimeMetric("porygon_pipeline_executed_count", 1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case state.commitQ <- block.Height:
		return nil
	}
}

func porygonPipelinePartitionUpdates(deltas []execution.TxDelta, shardCount int) map[string][]PorygonStateUpdate {
	updates := map[string][]PorygonStateUpdate{}
	for _, delta := range deltas {
		if !delta.Success || len(delta.WriteSet) == 0 {
			continue
		}
		keys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			partitionID := fmt.Sprintf("s%d", porygonStateShard(key, shardCount))
			updates[partitionID] = append(updates[partitionID], PorygonStateUpdate{
				TxID: delta.TxID, OriginalIndex: delta.OriginalIndex, Key: key, Value: delta.WriteSet[key],
			})
		}
	}
	for partitionID, rows := range updates {
		updates[partitionID] = porygonCanonicalStateUpdates(rows)
	}
	return updates
}

func (r *NodeRuntime) runPorygonPipelineCommitWorker(ctx context.Context, state *porygonPipelineRuntime) {
	for {
		select {
		case <-ctx.Done():
			return
		case height := <-state.commitQ:
			if err := r.commitPorygonPipelineBlock(ctx, state, height); err != nil && ctx.Err() == nil {
				state.mu.Lock()
				item := state.blocks[height]
				if item != nil {
					item.Phase = porygonPipelineFailed
				}
				state.mu.Unlock()
				if item != nil {
					r.markFatalExecutionError(item.Block, err)
				}
				return
			}
		}
	}
}

func (r *NodeRuntime) commitPorygonPipelineBlock(ctx context.Context, state *porygonPipelineRuntime, height uint64) error {
	state.mu.Lock()
	item := state.blocks[height]
	if item == nil {
		state.mu.Unlock()
		return fmt.Errorf("porygon pipeline execution artifact missing at height %d", height)
	}
	if item.Phase != porygonPipelineExecuted {
		state.mu.Unlock()
		return nil
	}
	block := item.Block
	item.CommitStartedAt = time.Now()
	finished := time.Now()
	item.CommitFinishedAt = finished
	item.Phase = porygonPipelineProtocolCommitted
	if item.Execution.ActualMetrics == nil {
		item.Execution.ActualMetrics = map[string]any{}
	}
	item.Execution.ActualMetrics["porygon_pipeline_commit_started_at_ns"] = item.CommitStartedAt.UnixNano()
	item.Execution.ActualMetrics["porygon_pipeline_commit_finished_at_ns"] = finished.UnixNano()
	item.Execution.ActualMetrics["porygon_multishard_update_handoff_enforced"] = false
	item.Execution.ActualMetrics["porygon_paper_update_transport"] = "proposal_carried_U_applied_by_following_EC"
	item.Execution.ActualMetrics["porygon_proposal_carried_update_enforced"] = true
	item.Execution.ActualMetrics["porygon_compact_proposal_txlist_omitted"] = len(block.TxList) == 0
	item.Execution.ActualMetrics["porygon_prepared_state_forwarding_used"] = false
	// Paper2 normal-path fidelity is exact here, but the original paper's fault
	// path (future same-shard ESC retries followed by a proposal-carried rollback
	// transaction after the bounded retry window) is intentionally fail-closed
	// rather than silently falling back to the retired v1 handoff/rollback RPCs.
	item.Execution.ActualMetrics["porygon_exact_multiround_rollback_claimed"] = false
	item.Execution.ActualMetrics["porygon_fault_recovery_truth_boundary"] = "fail_closed_until_future_ESC_retry_and_proposal_carried_rollback_are_observed"
	for key, value := range r.porygonPaperLifecycleMetrics() {
		item.Execution.ActualMetrics["porygon_paper_"+key] = value
	}
	if height > state.protocolCommittedHeight {
		state.protocolCommittedHeight = height
	}
	state.mu.Unlock()
	r.addPorygonRuntimeMetric("porygon_pipeline_protocol_stage_count", 1)
	return r.enqueueCommitTask(commitTaskConsensus, block, CommitOriginConsensus)
}

func (r *NodeRuntime) porygonPipelinePreparedExecution(block realblock.Block) (BlockExecutionResult, string, bool) {
	if !r.porygonPipelineEnabledRuntime() {
		return BlockExecutionResult{}, "", false
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	item := state.blocks[block.Height]
	if item == nil || item.Block.BlockHash != block.BlockHash || item.Phase != porygonPipelineProtocolCommitted {
		return BlockExecutionResult{}, "", false
	}
	executed := item.Execution
	if executed.ActualMetrics == nil {
		executed.ActualMetrics = map[string]any{}
	}
	orderedExecutionOverlap, executionCommitOverlap := porygonPipelineOverlapLocked(state, block.Height)
	full := orderedExecutionOverlap && executionCommitOverlap
	executed.ActualMetrics["porygon_ordering_execution_wall_clock_overlap"] = orderedExecutionOverlap
	executed.ActualMetrics["porygon_execution_commit_wall_clock_overlap"] = executionCommitOverlap
	executed.ActualMetrics["porygon_full_woec_pipeline_overlap_claimed"] = full
	if full {
		executed.ActualMetrics["porygon_pipeline_timing_truth_boundary"] = "real_cross_batch_witness_plus_ordering_execution_plus_execution_commit_wall_clock_overlap"
	} else {
		executed.ActualMetrics["porygon_pipeline_timing_truth_boundary"] = "real_cross_batch_witness_with_partial_woec_overlap"
	}
	return executed, item.BaseSnapshotRoot, true
}

func porygonPipelineOverlapLocked(state *porygonPipelineRuntime, height uint64) (bool, bool) {
	current := state.blocks[height]
	if current == nil {
		return false, false
	}
	orderingExecution := false
	if next := state.blocks[height+1]; next != nil && !current.ExecutionStartedAt.IsZero() && !current.ExecutionFinishedAt.IsZero() && !next.OrderedAt.IsZero() {
		orderingExecution = !next.OrderedAt.Before(current.ExecutionStartedAt) && next.OrderedAt.Before(current.ExecutionFinishedAt)
	}
	executionCommit := false
	if previous := state.blocks[height-1]; previous != nil && !current.ExecutionStartedAt.IsZero() && !current.ExecutionFinishedAt.IsZero() && !previous.CommitStartedAt.IsZero() && !previous.CommitFinishedAt.IsZero() {
		executionCommit = current.ExecutionStartedAt.Before(previous.CommitFinishedAt) && previous.CommitStartedAt.Before(current.ExecutionFinishedAt)
	}
	return orderingExecution, executionCommit
}

func (r *NodeRuntime) porygonPipelinePreparedSnapshot(epoch uint64) (map[string]string, string, bool) {
	// Retained only for upgrade compatibility with v1.x callers.  Paper2 never
	// exposes a speculative predecessor snapshot as execution truth.
	return nil, "", false
}

func (r *NodeRuntime) porygonPipelineSnapshotForStateFetch(ctx context.Context, epoch uint64) (map[string]string, string, error) {
	return nil, "", fmt.Errorf("Porygon speculative epoch state forwarding retired; use Proposal T state root")
}

func (r *NodeRuntime) porygonPipelineOnDurable(block realblock.Block) {
	if !r.porygonPipelineEnabledRuntime() {
		return
	}
	if err := r.porygonPaperValidateDurableSnapshot(block.Height, r.plugins.StateStorage.Snapshot(r.db)); err != nil {
		r.addPorygonRuntimeMetric("porygon_durable_snapshot_certified_root_mismatch_count", 1)
		r.markFatalExecutionError(block, err)
		return
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	if item := state.blocks[block.Height]; item != nil && item.Block.BlockHash == block.BlockHash {
		item.Phase = porygonPipelineDurable
		item.DurableFinishedAt = time.Now()
	}
	if block.Height > state.durableHeight {
		state.durableHeight = block.Height
	}
	state.mu.Unlock()
	r.addPorygonRuntimeMetric("porygon_pipeline_durable_count", 1)
}

func (r *NodeRuntime) porygonPipelineProposerAlreadyAdvanced(block realblock.Block) bool {
	if !r.porygonPipelineEnabledRuntime() {
		return false
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	item := state.blocks[block.Height]
	return item != nil && item.Block.BlockHash == block.BlockHash
}

func (r *NodeRuntime) porygonMarkPBFTDurableWhenSafe(block realblock.Block) {
	if !r.porygonPipelineEnabledRuntime() {
		r.pbftState().MarkDurableCommit(block)
		return
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	orderedHeight := state.orderedHeight
	state.mu.Unlock()
	// MarkDurableCommit resets the shared PBFT stage to Idle. Never do that
	// while a newer ordered height exists; PBFT certificates themselves remain
	// retained by pbft.State and are enough for the Porygon pipeline.
	if orderedHeight <= block.Height {
		r.pbftState().MarkDurableCommit(block)
	}
}

func (r *NodeRuntime) porygonPipelineSnapshotMetrics() map[string]any {
	if !r.porygonPipelineEnabledRuntime() {
		return nil
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	defer state.mu.Unlock()
	return map[string]any{
		"ordered_height":            state.orderedHeight,
		"executed_height":           state.executedHeight,
		"protocol_committed_height": state.protocolCommittedHeight,
		"durable_height":            state.durableHeight,
		"inflight_depth":            int64(state.orderedHeight - state.durableHeight),
	}
}

func porygonPipelinePlanShardCount(block realblock.Block) int {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != porygonPlanAlgorithmID {
		return 1
	}
	var plan porygonExecutionPlan
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &plan); err != nil || plan.ExecutionShardCount < 1 {
		return 1
	}
	return plan.ExecutionShardCount
}

func porygonPipelineDigest(block realblock.Block, executed BlockExecutionResult, certificate PorygonMultiShardUpdateCertificate) string {
	parts := []string{block.BlockHash, executed.PlanDigest, certificate.CertificateDigest, certificate.GlobalStateRoot}
	return stableTextDigest(strings.Join(parts, "|"))
}
