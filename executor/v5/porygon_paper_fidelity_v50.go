package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
	statepkg "metaverse-chainlab/executor/realism/state"
)

const (
	porygonPaperFidelityV50Version = "porygon_paper_fidelity_v5_0"
	porygonV50ECSlotCount          = 3
	porygonV50MaxUpdateFailures    = 2
)

// Porygon's sharded Multi-Shard Update section requires enough matching ESC
// execution results, e.g. strictly more than half of the ESC members. Keep this
// threshold distinct from witness/handoff thresholds and from fixed Storage
// Role root-majority authentication.
func porygonShardedExecutionResultThreshold(memberCount int) int {
	if memberCount < 1 {
		return 1
	}
	return memberCount/2 + 1
}

func porygonV50ECSlotForHeight(height uint64) int {
	if height == 0 {
		return 0
	}
	return int((height - 1) % porygonV50ECSlotCount)
}

type porygonV50ExecutionSlots struct {
	once   sync.Once
	queues [porygonV50ECSlotCount]chan realblock.Block
}

var porygonV50ExecutionSlotStates sync.Map // map[*NodeRuntime]*porygonV50ExecutionSlots

func (r *NodeRuntime) porygonV50ExecutionSlots() *porygonV50ExecutionSlots {
	if value, ok := porygonV50ExecutionSlotStates.Load(r); ok {
		return value.(*porygonV50ExecutionSlots)
	}
	created := &porygonV50ExecutionSlots{}
	actual, _ := porygonV50ExecutionSlotStates.LoadOrStore(r, created)
	return actual.(*porygonV50ExecutionSlots)
}

// Each paper EC lives for three rounds and at most three ECs coexist. MBE keeps
// the same physical validator budget but gives those three logical EC lifecycles
// independent runtime workers so business execution can overlap across heights.
func (r *NodeRuntime) porygonV50StartExecutionSlots(ctx context.Context, state *porygonPipelineRuntime) {
	slots := r.porygonV50ExecutionSlots()
	slots.once.Do(func() {
		for slot := 0; slot < porygonV50ECSlotCount; slot++ {
			slots.queues[slot] = make(chan realblock.Block, 8)
			go r.runPorygonV50ExecutionSlot(ctx, state, slot, slots.queues[slot])
		}
	})
}

func (r *NodeRuntime) porygonV50EnqueueExecution(ctx context.Context, state *porygonPipelineRuntime, block realblock.Block) error {
	slots := r.porygonV50ExecutionSlots()
	slot := porygonV50ECSlotForHeight(block.Height)
	if slots.queues[slot] == nil {
		return fmt.Errorf("Porygon v5 EC slot %d is not running", slot)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case slots.queues[slot] <- block:
		r.addPorygonRuntimeMetric("porygon_v50_ec_slot_enqueue_count", 1)
		return nil
	}
}

func (r *NodeRuntime) runPorygonV50ExecutionSlot(ctx context.Context, state *porygonPipelineRuntime, slot int, queue <-chan realblock.Block) {
	for {
		select {
		case <-ctx.Done():
			return
		case block := <-queue:
			r.addPorygonRuntimeMetric(fmt.Sprintf("porygon_v50_ec_slot_%d_execution_count", slot), 1)
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

// Business work for E(h+1) may overlap E(h), but the certified state frontier
// itself is height ordered because canonical state h is based on h-1. Wait only
// at the short finalization boundary, not during business execution.
func (r *NodeRuntime) porygonV50WaitExecutionFinalizeTurn(ctx context.Context, state *porygonPipelineRuntime, height uint64) error {
	if height <= state.baselineHeight+1 {
		return nil
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		state.mu.Lock()
		frontier := state.executedHeight
		state.mu.Unlock()
		if frontier >= height-1 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func porygonV50AdvanceExecutedFrontierLocked(state *porygonPipelineRuntime) {
	for {
		next := state.executedHeight + 1
		item := state.blocks[next]
		if item == nil {
			return
		}
		switch item.Phase {
		case porygonPipelineExecuted, porygonPipelineProtocolCommitted, porygonPipelineDurable:
			state.executedHeight = next
		default:
			return
		}
	}
}

// Execution slots may finish out of order. Protocol-commit enqueueing remains
// strictly height ordered so the shared durable commit worker sees one canonical
// chain while E(h+1) is free to overlap M/C(h).
func (r *NodeRuntime) runPorygonV50CommitWorker(ctx context.Context, state *porygonPipelineRuntime) {
	pending := map[uint64]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case height := <-state.commitQ:
			pending[height] = true
		}
		for {
			state.mu.Lock()
			next := state.protocolCommittedHeight + 1
			item := state.blocks[next]
			ready := pending[next] && item != nil && item.Phase == porygonPipelineExecuted
			state.mu.Unlock()
			if !ready {
				break
			}
			delete(pending, next)
			if err := r.commitPorygonPipelineBlock(ctx, state, next); err != nil && ctx.Err() == nil {
				state.mu.Lock()
				item = state.blocks[next]
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

// ---- Paper fault path: following-ESC retry and proposal-carried rollback ----

type porygonV50RecoveryRecord struct {
	TxID                string
	OriginHeight        uint64
	PartitionID         string
	FutureRetryFailures int
	LastFailureHeight   uint64
	NextProposalHeight  uint64
	Mode                string // retry or rollback
	RetryUpdate         PorygonProposalUpdate
	RollbackUpdates     []PorygonProposalUpdate
}

type porygonV50RecoveryState struct {
	mu            sync.Mutex
	pending       map[string]*porygonV50RecoveryRecord
	commitHeights map[string]uint64
	observed      bool
}

var porygonV50RecoveryStates sync.Map // map[*NodeRuntime]*porygonV50RecoveryState

func (r *NodeRuntime) porygonV50RecoveryState() *porygonV50RecoveryState {
	if value, ok := porygonV50RecoveryStates.Load(r); ok {
		return value.(*porygonV50RecoveryState)
	}
	created := &porygonV50RecoveryState{pending: map[string]*porygonV50RecoveryRecord{}, commitHeights: map[string]uint64{}}
	actual, _ := porygonV50RecoveryStates.LoadOrStore(r, created)
	return actual.(*porygonV50RecoveryState)
}

func porygonV50RecoveryKey(origin uint64, txID, partitionID string) string {
	return fmt.Sprintf("%d|%s|%s", origin, txID, partitionID)
}

// The initial failed U is not counted as a following-ESC retry failure. The
// next protocol height performs retry #1; if retry #1 fails, the next height
// performs retry #2; if retry #2 fails, the immediately following Proposal
// carries rollback. This makes the cross-round sequence explicit and testable.
func porygonV50NextRecoveryStep(carriedKind string, priorRetryFailures int, failedHeight uint64) (mode string, retryFailures int, nextProposalHeight uint64) {
	retryFailures = priorRetryFailures
	if carriedKind == "retry" {
		retryFailures++
	}
	mode = "retry"
	if retryFailures >= porygonV50MaxUpdateFailures {
		mode = "rollback"
	}
	return mode, retryFailures, failedHeight + 1
}

func porygonV50ShardIndex(partitionID string) (int, bool) {
	var shard int
	if _, err := fmt.Sscanf(strings.TrimSpace(partitionID), "s%d", &shard); err != nil || shard < 0 {
		return 0, false
	}
	return shard, true
}

func porygonV50FilterProposalUpdateForPartition(update PorygonProposalUpdate, partitionID string, shardCount int, kind string) (PorygonProposalUpdate, bool) {
	if shardCount < 1 {
		shardCount = 1
	}
	shardIndex, ok := porygonV50ShardIndex(partitionID)
	if !ok {
		return PorygonProposalUpdate{}, false
	}
	rows := make([]PorygonStateUpdate, 0)
	for _, row := range update.Updates {
		if row.Key == "" {
			continue
		}
		if porygonStateShard(row.Key, shardCount) == shardIndex {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		return PorygonProposalUpdate{}, false
	}
	out := PorygonProposalUpdate{
		TxID: update.TxID, OriginHeight: update.OriginHeight, Kind: kind,
		InvolvedShards: []int{shardIndex}, Updates: porygonCanonicalStateUpdates(rows),
	}
	out.UpdateDigest = porygonProposalUpdateDigest(out)
	return out, true
}

func (r *NodeRuntime) porygonV50CertifiedCommitUpdate(origin uint64, txID string) (PorygonProposalUpdate, bool) {
	value, ok := porygonPipelineRuntimes.Load(r)
	if !ok {
		return PorygonProposalUpdate{}, false
	}
	pipeline := value.(*porygonPipelineRuntime)
	pipeline.mu.Lock()
	item := pipeline.blocks[origin]
	if item == nil || item.Execution.PorygonCertifiedExecution == nil {
		pipeline.mu.Unlock()
		return PorygonProposalUpdate{}, false
	}
	truth := porygonCloneCertifiedExecutionTruth(*item.Execution.PorygonCertifiedExecution)
	pipeline.mu.Unlock()
	for _, update := range truth.ProposalU {
		if update.TxID == txID && update.OriginHeight == origin && update.Kind == "commit" {
			return update, true
		}
	}
	return PorygonProposalUpdate{}, false
}

func (r *NodeRuntime) porygonV50HistoricalCertifiedValue(ctx context.Context, height uint64, partitionID, key string) (string, error) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	root := state.certifiedPartitionRoots[height][partitionID]
	local := copyRegistryStringMap(state.certifiedSnapshots[height])
	state.mu.Unlock()
	if root == "" {
		return "", fmt.Errorf("Porygon v5 historical partition root unavailable at height %d partition %s", height, partitionID)
	}
	qualifiedKey := qualifyStateKey(partitionID, key)
	if partitionID == r.stateAccessPartitionID() {
		if local == nil || statepkg.RootOfSnapshot(local) != root {
			return "", fmt.Errorf("Porygon v5 local historical snapshot mismatch at height %d partition %s", height, partitionID)
		}
		value, exists := local[qualifiedKey]
		if !exists {
			return "", fmt.Errorf("Porygon v5 rollback prior value missing for %s at height %d", key, height)
		}
		return value, nil
	}
	targetNode := r.stateAccessLeaderID(partitionID)
	if targetNode == "" {
		return "", fmt.Errorf("Porygon v5 rollback Storage Role leader missing for %s", partitionID)
	}
	accessKind := fmt.Sprintf("%sread|state_height=%d|state_root=%s", porygonStateProjectionAccessKindPrefix, height, root)
	requestID := stableTextDigest(fmt.Sprintf("porygon-v50-rollback-history|%s|%d|%s|%s|%s", r.node.NodeID, height, partitionID, key, root))
	waiter := make(chan StateFetchResponse, 1)
	r.mu.Lock()
	if r.stateFetchWaiters == nil {
		r.stateFetchWaiters = map[string]chan StateFetchResponse{}
	}
	r.stateFetchWaiters[requestID] = waiter
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.stateFetchWaiters, requestID)
		r.mu.Unlock()
	}()
	request := StateFetchRequest{
		RequestID: requestID, TxID: "porygon-v50-rollback-history", BlockHash: stableTextDigest(fmt.Sprintf("rollback-history|%d|%s", height, root)),
		Key: key, HomeShard: partitionID, ExecutionShard: r.stateAccessPartitionID(), AccessKind: accessKind,
	}
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, height, r.currentPBFTView(), height, request)
	if err != nil {
		return "", err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		return "", err
	}
	timer := time.NewTimer(r.proposalTimeout())
	defer timer.Stop()
	select {
	case response := <-waiter:
		if !response.Success {
			return "", fmt.Errorf("Porygon v5 rollback historical fetch failed for %s: %s", key, response.Error)
		}
		if response.StateRoot != root || response.QualifiedKey != qualifiedKey || response.PorygonProof == nil || !porygonVerifyStateProof(*response.PorygonProof, qualifiedKey, response.Value, root) {
			return "", fmt.Errorf("Porygon v5 rollback historical proof mismatch for %s at height %d", key, height)
		}
		if response.WitnessDigest != porygonStateProjectionWitnessDigest(response, accessKind) {
			return "", fmt.Errorf("Porygon v5 rollback historical witness mismatch for %s", key)
		}
		if !response.PorygonProof.Exists {
			return "", fmt.Errorf("Porygon v5 rollback cannot restore absent pre-state key %s", key)
		}
		return response.Value, nil
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return "", fmt.Errorf("Porygon v5 rollback historical fetch timeout for %s", key)
	}
}

func (r *NodeRuntime) porygonV50RollbackUpdateForPartition(ctx context.Context, commit PorygonProposalUpdate, partitionID string) (PorygonProposalUpdate, error) {
	shardIndex, ok := porygonV50ShardIndex(partitionID)
	if !ok {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 invalid recovery partition %q", partitionID)
	}
	// The original commit U for a CTx ordered at h is first applied in B_(h+2).
	// Rollback restores the authenticated canonical version immediately before
	// that update: state height h+1. Remote values are fetched from the fixed
	// Storage Role and verified against the historical certified partition root.
	baseHeight := commit.OriginHeight + 1
	rows := make([]PorygonStateUpdate, 0)
	for _, row := range commit.Updates {
		if porygonStateShard(row.Key, r.porygonExecutionShardCount()) != shardIndex {
			continue
		}
		oldValue, err := r.porygonV50HistoricalCertifiedValue(ctx, baseHeight, partitionID, row.Key)
		if err != nil {
			return PorygonProposalUpdate{}, err
		}
		rows = append(rows, PorygonStateUpdate{TxID: row.TxID, OriginalIndex: row.OriginalIndex, Key: row.Key, Value: oldValue})
	}
	if len(rows) == 0 {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 rollback has no rows for %s", partitionID)
	}
	rollback := PorygonProposalUpdate{
		TxID: commit.TxID, OriginHeight: commit.OriginHeight, Kind: "rollback",
		InvolvedShards: []int{shardIndex}, Updates: porygonCanonicalStateUpdates(rows),
	}
	rollback.UpdateDigest = porygonProposalUpdateDigest(rollback)
	return rollback, nil
}

func (r *NodeRuntime) porygonV50RollbackUpdatesForCommit(ctx context.Context, commit PorygonProposalUpdate) ([]PorygonProposalUpdate, error) {
	shards := map[string]bool{}
	for _, row := range commit.Updates {
		if row.Key == "" {
			continue
		}
		shards[fmt.Sprintf("s%d", porygonStateShard(row.Key, r.porygonExecutionShardCount()))] = true
	}
	rows := make([]PorygonProposalUpdate, 0, len(shards))
	for _, partitionID := range sortedBoolKeys(shards) {
		rollback, err := r.porygonV50RollbackUpdateForPartition(ctx, commit, partitionID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, rollback)
	}
	return porygonV50MergeProposalUpdates(nil, rows), nil
}

func porygonV50RetryablePaperRootFailure(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "quorum timeout") || strings.Contains(text, "not ready") || strings.Contains(text, "deadline exceeded")
}

func porygonV50RemoveProposalUFromPartition(proposal PorygonProposalBody, partitionID string, shardCount int, items []PorygonStateUpdate) ([]PorygonStateUpdate, bool) {
	remove := map[string]bool{}
	for _, update := range proposal.U {
		// A rollback itself is the terminal recovery action. It must either obtain
		// the authenticated root or fail closed; it is never deferred again.
		if update.Kind == "rollback" {
			continue
		}
		part, ok := porygonV50FilterProposalUpdateForPartition(update, partitionID, shardCount, update.Kind)
		if !ok {
			continue
		}
		for _, row := range part.Updates {
			remove[row.TxID+"\x00"+row.Key] = true
		}
	}
	if len(remove) == 0 {
		return porygonCanonicalStateUpdates(items), false
	}
	out := make([]PorygonStateUpdate, 0, len(items))
	changed := false
	for _, row := range items {
		if remove[row.TxID+"\x00"+row.Key] {
			changed = true
			continue
		}
		out = append(out, row)
	}
	return porygonCanonicalStateUpdates(out), changed
}

// porygonV50RecordDeferredPartition runs only after the global authenticated ESC
// batch certificate agrees that this partition was deferred. Therefore every
// validator, including a future OC after view change, records the same recovery
// obligation. The initial failed U schedules the first following-ESC retry;
// two failed following-ESC retry rounds schedule a proposal-carried rollback.
func (r *NodeRuntime) porygonV50RecordDeferredPartition(ctx context.Context, proposal PorygonProposalBody, partitionID string, failedHeight uint64) error {
	state := r.porygonV50RecoveryState()
	for _, carried := range proposal.U {
		if carried.Kind == "rollback" {
			if _, ok := porygonV50FilterProposalUpdateForPartition(carried, partitionID, r.porygonExecutionShardCount(), carried.Kind); ok {
				return fmt.Errorf("Porygon v5 proposal-carried rollback failed root certification for %s", partitionID)
			}
			continue
		}
		if _, ok := porygonV50FilterProposalUpdateForPartition(carried, partitionID, r.porygonExecutionShardCount(), carried.Kind); !ok {
			continue
		}
		original, ok := r.porygonV50CertifiedCommitUpdate(carried.OriginHeight, carried.TxID)
		if !ok {
			return fmt.Errorf("Porygon v5 recovery missing certified original CTx %s@%d", carried.TxID, carried.OriginHeight)
		}
		retry, ok := porygonV50FilterProposalUpdateForPartition(original, partitionID, r.porygonExecutionShardCount(), "retry")
		if !ok {
			return fmt.Errorf("Porygon v5 recovery original CTx %s has no %s update", carried.TxID, partitionID)
		}
		key := porygonV50RecoveryKey(carried.OriginHeight, carried.TxID, partitionID)
		state.mu.Lock()
		record := state.pending[key]
		state.mu.Unlock()
		if record == nil {
			rollbacks, err := r.porygonV50RollbackUpdatesForCommit(ctx, original)
			if err != nil {
				return err
			}
			record = &porygonV50RecoveryRecord{
				TxID: carried.TxID, OriginHeight: carried.OriginHeight, PartitionID: partitionID,
				RetryUpdate: retry, RollbackUpdates: rollbacks,
			}
		}
		record.Mode, record.FutureRetryFailures, record.NextProposalHeight = porygonV50NextRecoveryStep(carried.Kind, record.FutureRetryFailures, failedHeight)
		record.LastFailureHeight = failedHeight
		if record.Mode == "rollback" {
			r.addPorygonRuntimeMetric("porygon_v50_proposal_rollback_scheduled_count", 1)
		} else {
			r.addPorygonRuntimeMetric("porygon_v50_future_esc_retry_scheduled_count", 1)
		}
		state.mu.Lock()
		state.pending[key] = record
		state.observed = true
		state.mu.Unlock()
	}
	return nil
}

func porygonV50RecoveryTxKey(origin uint64, txID string) string {
	return fmt.Sprintf("%d|%s", origin, txID)
}

func (r *NodeRuntime) porygonV50ResolvePartitionRecovery(proposal PorygonProposalBody, partitionID string) {
	state := r.porygonV50RecoveryState()
	for _, update := range proposal.U {
		if update.Kind != "retry" && update.Kind != "rollback" {
			continue
		}
		if _, ok := porygonV50FilterProposalUpdateForPartition(update, partitionID, r.porygonExecutionShardCount(), update.Kind); !ok {
			continue
		}
		state.mu.Lock()
		removed := 0
		if update.Kind == "rollback" {
			for key, record := range state.pending {
				if record != nil && record.OriginHeight == update.OriginHeight && record.TxID == update.TxID {
					delete(state.pending, key)
					removed++
				}
			}
		} else {
			key := porygonV50RecoveryKey(update.OriginHeight, update.TxID, partitionID)
			if _, ok := state.pending[key]; ok {
				delete(state.pending, key)
				removed = 1
			}
		}
		remaining := false
		for _, record := range state.pending {
			if record != nil && record.OriginHeight == update.OriginHeight && record.TxID == update.TxID {
				remaining = true
				break
			}
		}
		if removed > 0 {
			state.observed = true
			if !remaining {
				state.commitHeights[porygonV50RecoveryTxKey(update.OriginHeight, update.TxID)] = proposal.Height + 2
			}
		}
		state.mu.Unlock()
		if removed > 0 {
			if update.Kind == "rollback" {
				r.addPorygonRuntimeMetric("porygon_v50_proposal_rollback_applied_count", int64(removed))
			} else {
				r.addPorygonRuntimeMetric("porygon_v50_future_esc_retry_success_count", 1)
			}
		}
	}
}

func (r *NodeRuntime) porygonV50RecoveryProposalUpdates(height uint64) []PorygonProposalUpdate {
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	rows := make([]PorygonProposalUpdate, 0)
	for _, record := range state.pending {
		if record == nil || record.NextProposalHeight != height {
			continue
		}
		if record.Mode == "rollback" {
			rows = append(rows, record.RollbackUpdates...)
		} else {
			rows = append(rows, record.RetryUpdate)
		}
	}
	state.mu.Unlock()
	return porygonV50MergeProposalUpdates(nil, rows)
}

func (r *NodeRuntime) porygonV50RecoveryPending(nextHeight uint64) bool {
	if r == nil || nextHeight == 0 {
		return false
	}
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, record := range state.pending {
		if record != nil && record.NextProposalHeight >= nextHeight {
			return true
		}
	}
	for _, height := range state.commitHeights {
		if height >= nextHeight {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) porygonV50RecoveryTxPending(origin uint64, txID string, proposalHeight uint64) bool {
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, record := range state.pending {
		if record != nil && record.OriginHeight == origin && record.TxID == txID {
			return true
		}
	}
	return state.commitHeights[porygonV50RecoveryTxKey(origin, txID)] > proposalHeight
}

// If the immediately preceding proposal carries a retry/rollback whose outcome
// can schedule another recovery proposal, ordering the next height must wait for
// that execution result. Normal Porygon heights retain the h-2 dependency and
// full pipeline overlap.
func (r *NodeRuntime) porygonV50RecoveryBlocksNextProposal(nextHeight uint64, executedHeight uint64) bool {
	if nextHeight < 2 || executedHeight >= nextHeight-1 {
		return false
	}
	// If B_(h-1) carries a normal/retry U, its execution result determines
	// whether B_h must carry a following-ESC retry or rollback. Therefore only
	// this fault-decision edge waits for E(B_(h-1)); heights with no recoverable
	// U retain the paper's normal h-2 ordering dependency.
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	previous := pipeline.blocks[nextHeight-1]
	var previousBlock realblock.Block
	if previous != nil {
		previousBlock = previous.Block
	}
	pipeline.mu.Unlock()
	if previousBlock.BlockHash != "" {
		if proposal, err := porygonProposalFromBlock(previousBlock); err == nil {
			for _, update := range proposal.U {
				if update.Kind == "commit" || update.Kind == "retry" {
					return true
				}
			}
		}
	}
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, record := range state.pending {
		if record != nil && record.NextProposalHeight == nextHeight {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) porygonV50RecoveryObserved() bool {
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.observed
}

func porygonV50DeferredPartitionsFromBatchCertificate(cert PorygonESCBatchCertificate) map[string]bool {
	out := map[string]bool{}
	for _, entry := range cert.Entries {
		for _, partitionID := range entry.Result.DeferredPartitions {
			if partitionID != "" {
				out[partitionID] = true
			}
		}
	}
	return out
}

func (r *NodeRuntime) porygonV50ApplyRecoveryDeferredSet(ctx context.Context, proposal PorygonProposalBody, deferred map[string]bool) error {
	for partitionID := range deferred {
		if err := r.porygonV50RecordDeferredPartition(ctx, proposal, partitionID, proposal.Height); err != nil {
			return err
		}
	}
	for _, update := range proposal.U {
		if update.Kind != "retry" && update.Kind != "rollback" {
			continue
		}
		for _, shard := range update.InvolvedShards {
			partitionID := fmt.Sprintf("s%d", shard)
			if !deferred[partitionID] {
				r.porygonV50ResolvePartitionRecovery(proposal, partitionID)
			}
		}
	}
	return nil
}

func porygonV50DeferredPartitionsFromExecution(executed BlockExecutionResult) map[string]bool {
	out := map[string]bool{}
	if executed.ActualMetrics == nil {
		return out
	}
	raw := executed.ActualMetrics["porygon_v50_deferred_partitions"]
	switch values := raw.(type) {
	case []string:
		for _, value := range values {
			if value != "" {
				out[value] = true
			}
		}
	case []any:
		for _, value := range values {
			if text, ok := value.(string); ok && text != "" {
				out[text] = true
			}
		}
	}
	return out
}

func porygonV50ValidateDeferredCertifiedDisjoint(proposal PorygonProposalBody, deferred map[string]bool, shardCount int, certified map[string]porygonWaveResult) error {
	for partitionID := range deferred {
		keys := porygonV50DeferredUKeys(proposal, partitionID, shardCount)
		if len(keys) == 0 {
			continue
		}
		for _, result := range certified {
			for _, read := range result.Delta.ReadSet {
				if keys[read.Key] {
					return fmt.Errorf("Porygon v5 deferred U conflicts with certified current read: tx=%s key=%s", result.Item.TxID, read.Key)
				}
			}
			for key := range result.Delta.WriteSet {
				if keys[key] {
					return fmt.Errorf("Porygon v5 deferred U conflicts with certified current write: tx=%s key=%s", result.Item.TxID, key)
				}
			}
		}
	}
	return nil
}

func porygonV50UpdateKindRank(kind string) int {
	switch kind {
	case "commit":
		return 0
	case "retry":
		return 1
	case "rollback":
		return 2
	default:
		return 3
	}
}

func porygonV50MergeProposalUpdates(base, recovery []PorygonProposalUpdate) []PorygonProposalUpdate {
	rows := append(porygonCloneProposalUpdates(base), porygonCloneProposalUpdates(recovery)...)
	for i := range rows {
		rows[i].Updates = porygonCanonicalStateUpdates(rows[i].Updates)
		rows[i].InvolvedShards = append([]int(nil), rows[i].InvolvedShards...)
		sort.Ints(rows[i].InvolvedShards)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].OriginHeight != rows[j].OriginHeight {
			return rows[i].OriginHeight < rows[j].OriginHeight
		}
		left := porygonPaperProposalUpdateOrderIndex(rows[i])
		right := porygonPaperProposalUpdateOrderIndex(rows[j])
		if left != right {
			return left < right
		}
		if rows[i].TxID != rows[j].TxID {
			return rows[i].TxID < rows[j].TxID
		}
		return porygonV50UpdateKindRank(rows[i].Kind) < porygonV50UpdateKindRank(rows[j].Kind)
	})
	if len(rows) == 0 {
		return nil
	}
	dedup := rows[:0]
	seen := map[string]bool{}
	for _, row := range rows {
		key := fmt.Sprintf("%d|%s|%s|%v|%s", row.OriginHeight, row.TxID, row.Kind, row.InvolvedShards, row.UpdateDigest)
		if seen[key] {
			continue
		}
		seen[key] = true
		dedup = append(dedup, row)
	}
	return dedup
}

func porygonV50SplitProposalUpdates(items []PorygonProposalUpdate) (normal, recovery []PorygonProposalUpdate) {
	for _, item := range items {
		if item.Kind == "retry" || item.Kind == "rollback" {
			recovery = append(recovery, item)
		} else {
			normal = append(normal, item)
		}
	}
	return porygonV50MergeProposalUpdates(normal, nil), porygonV50MergeProposalUpdates(recovery, nil)
}

func (r *NodeRuntime) porygonV50ExpectedRecoveryUpdate(update PorygonProposalUpdate, proposalHeight uint64) (PorygonProposalUpdate, error) {
	if update.Kind != "retry" && update.Kind != "rollback" {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 invalid recovery kind %q", update.Kind)
	}
	if len(update.InvolvedShards) != 1 {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 recovery update must target exactly one shard")
	}
	partitionID := fmt.Sprintf("s%d", update.InvolvedShards[0])
	original, ok := r.porygonV50CertifiedCommitUpdate(update.OriginHeight, update.TxID)
	if !ok {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 recovery update missing certified origin %s@%d", update.TxID, update.OriginHeight)
	}
	if update.Kind == "retry" {
		minHeight := update.OriginHeight + 3
		maxHeight := update.OriginHeight + 2 + porygonV50MaxUpdateFailures
		if proposalHeight < minHeight || proposalHeight > maxHeight {
			return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 retry carried at wrong height: tx=%s got=%d allowed=[%d,%d]", update.TxID, proposalHeight, minHeight, maxHeight)
		}
		expected, ok := porygonV50FilterProposalUpdateForPartition(original, partitionID, r.porygonExecutionShardCount(), "retry")
		if !ok {
			return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 retry has no certified partition update")
		}
		return expected, nil
	}
	wantRollbackHeight := update.OriginHeight + 3 + porygonV50MaxUpdateFailures
	if proposalHeight != wantRollbackHeight {
		return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 rollback carried at wrong height: tx=%s got=%d want=%d", update.TxID, proposalHeight, wantRollbackHeight)
	}
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, record := range state.pending {
		if record == nil || record.OriginHeight != update.OriginHeight || record.TxID != update.TxID || record.Mode != "rollback" || record.NextProposalHeight != proposalHeight {
			continue
		}
		for _, candidate := range record.RollbackUpdates {
			if len(candidate.InvolvedShards) == 1 && candidate.InvolvedShards[0] == update.InvolvedShards[0] {
				return candidate, nil
			}
		}
	}
	return PorygonProposalUpdate{}, fmt.Errorf("Porygon v5 rollback has no synchronized historical recovery proof for %s/%s", update.TxID, partitionID)
}

func (r *NodeRuntime) porygonV50ValidateRecoveryProposalUpdates(height uint64, rows []PorygonProposalUpdate) error {
	seen := map[string]bool{}
	for _, row := range rows {
		if row.UpdateDigest == "" || row.UpdateDigest != porygonProposalUpdateDigest(row) {
			return fmt.Errorf("Porygon v5 recovery update digest mismatch for %s", row.TxID)
		}
		expected, err := r.porygonV50ExpectedRecoveryUpdate(row, height)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%d|%s|%v|%s", row.OriginHeight, row.TxID, row.InvolvedShards, row.Kind)
		if seen[key] {
			return fmt.Errorf("Porygon v5 duplicate recovery update %s", key)
		}
		seen[key] = true
		if porygonProposalUSemanticDigest([]PorygonProposalUpdate{row}) != porygonProposalUSemanticDigest([]PorygonProposalUpdate{expected}) {
			return fmt.Errorf("Porygon v5 recovery update semantic mismatch for %s", row.TxID)
		}
	}
	return nil
}

func porygonV50DeferredPartitionSet(cert PorygonMultiShardUpdateCertificate) map[string]bool {
	out := map[string]bool{}
	for _, shard := range cert.DeferredPartitions {
		if shard != "" {
			out[shard] = true
		}
	}
	return out
}

func porygonV50DeferredUKeys(proposal PorygonProposalBody, partitionID string, shardCount int) map[string]bool {
	out := map[string]bool{}
	for _, update := range proposal.U {
		if update.Kind == "rollback" {
			continue
		}
		part, ok := porygonV50FilterProposalUpdateForPartition(update, partitionID, shardCount, update.Kind)
		if !ok {
			continue
		}
		for _, row := range part.Updates {
			out[row.Key] = true
		}
	}
	return out
}

func porygonV50RestoreDeferredProposalU(base, working map[string]string, commitment *statepkg.Commitment, proposal PorygonProposalBody, partitionID string, shardCount int) error {
	for _, update := range proposal.U {
		if update.Kind == "rollback" {
			continue
		}
		part, ok := porygonV50FilterProposalUpdateForPartition(update, partitionID, shardCount, update.Kind)
		if !ok {
			continue
		}
		for _, row := range part.Updates {
			storageKey := qualifyStateKey(partitionID, row.Key)
			value, exists := base[storageKey]
			if !exists {
				return fmt.Errorf("Porygon v5 deferred U base value missing for %s", row.Key)
			}
			working[storageKey] = value
			commitment.Set(storageKey, value)
		}
	}
	return nil
}

// Durable materialization is canonical state(h-1) plus successful U rows (except
// a shard explicitly deferred to the following ESC) plus current ITx writes.
func porygonPaperDurableStateDeltaV50(
	proposal PorygonProposalBody,
	deltas []execution.TxDelta,
	assignments []porygonTxAssignment,
	orderingDomain string,
	storagePartitionID string,
	shardCount int,
	deferredShards map[string]bool,
) ([]statepkg.StateKV, error) {
	assignmentByID := make(map[string]porygonTxAssignment, len(assignments))
	for _, assignment := range assignments {
		if assignment.TxID != "" {
			assignmentByID[assignment.TxID] = assignment
		}
	}
	ordered := make([]PorygonStateUpdate, 0)
	for _, proposalUpdate := range proposal.U {
		for _, row := range proposalUpdate.Updates {
			if strings.TrimSpace(row.Key) == "" {
				continue
			}
			home := fmt.Sprintf("s%d", porygonStateShard(row.Key, shardCount))
			if deferredShards[home] && proposalUpdate.Kind != "rollback" {
				continue
			}
			if strings.TrimSpace(storagePartitionID) != "" && home != storagePartitionID {
				continue
			}
			ordered = append(ordered, row)
		}
	}
	current := append([]execution.TxDelta(nil), deltas...)
	sort.SliceStable(current, func(i, j int) bool {
		if current[i].OriginalIndex != current[j].OriginalIndex {
			return current[i].OriginalIndex < current[j].OriginalIndex
		}
		return current[i].TxID < current[j].TxID
	})
	for _, delta := range current {
		if !delta.Success || len(delta.WriteSet) == 0 {
			continue
		}
		assignment, ok := assignmentByID[delta.TxID]
		if !ok {
			return nil, fmt.Errorf("Porygon v5 materialization missing assignment for %s", delta.TxID)
		}
		if assignment.Abandoned || assignment.CrossShard {
			continue
		}
		keys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			home := fmt.Sprintf("s%d", porygonStateShard(key, shardCount))
			if strings.TrimSpace(storagePartitionID) != "" && home != storagePartitionID {
				continue
			}
			ordered = append(ordered, PorygonStateUpdate{TxID: delta.TxID, OriginalIndex: delta.OriginalIndex, Key: key, Value: delta.WriteSet[key]})
		}
	}
	collapsed := porygonPaperCollapseOrderedStateUpdates(ordered)
	if err := porygonPaperValidateCollapsedStateUpdates(collapsed); err != nil {
		return nil, err
	}
	out := make([]statepkg.StateKV, 0, len(collapsed))
	for _, update := range collapsed {
		storageKey := qualifyStateKey(orderingDomain, update.Key)
		if strings.TrimSpace(storagePartitionID) != "" {
			storageKey = qualifyStateKey(storagePartitionID, update.Key)
		}
		out = append(out, statepkg.StateKV{Key: storageKey, Value: update.Value})
	}
	return out, nil
}

// porygonV50NormalProposalUpdates returns only the frozen normal-path U truth
// derived from E(B_(h-2)). Recovery retry/rollback rows are deliberately not
// read from replica-local recovery state so backups can validate a proposal
// solely from certified historical execution/state.
