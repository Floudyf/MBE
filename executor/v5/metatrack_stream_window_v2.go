package v5

import (
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_MECHPACK_V2_STREAMING_CONSENSUS
//
// V669 originally closed an adaptive N/L window on the client before any
// transaction in that window was submitted. That preserved a globally signed
// window certificate, but it also put future-RouteBatch wait time directly on
// the submission critical path. V2 keeps the same threshold-free V669 rule and
// the same signed dependency/round metadata, but signs a cumulative prefix
// certificate on each RouteBatch and submits that RouteBatch immediately.
//
// The leader may then aggregate only complete signed shard projections whose
// prefix certificates say that they belong to the same V669 window. PBFT itself
// is unchanged.
const metaTrackStreamingConsensusPolicyV2 = "leader_streaming_signed_prefix_n_over_l_v2"

func metaTrackAnnotateStreamingPrefixV2(p *metaTrackCriticalWidthWindowPlannerV6568, batch []metaTrackPreparedRecordV6568) []metaTrackPreparedRecordV6568 {
	if p == nil || len(batch) == 0 {
		return nil
	}
	out := make([]metaTrackPreparedRecordV6568, len(batch))
	copy(out, batch)
	routeBatchCount := int(p.endBatch - p.startBatch + 1)
	for i := range out {
		r := &out[i].Record
		r.ConsensusWindowSequence = p.sequence
		r.ConsensusWindowStartBatchSequence = p.startBatch
		// A streaming certificate describes the prefix known at the instant
		// this RouteBatch is signed. It is never rewritten later.
		r.ConsensusWindowEndBatchSequence = r.RouteBatchSequence
		r.ConsensusWindowRouteBatchCount = routeBatchCount
		r.ConsensusWindowTransactionCount = p.transactionCount
		r.ConsensusWindowShardTransactionCount = p.shardCounts[r.ExecutionShard]
		r.ConsensusWindowCriticalPath = p.criticalPath
		if r.ControlPolicy == metaTrackDeclaredAccessFrontierPolicy {
			r.FrontierDigest = metaTrackDeclaredAccessFrontierDigestV6568(*r, r.ExecutionShard)
		}
	}
	return out
}

// PushBatchAdaptiveNLV669StreamingV2 evaluates exactly the same V669 admission
// rule as PushBatchAdaptiveNLV669, but returns the current RouteBatch immediately
// after assigning a signed cumulative-prefix certificate. No future batch is
// waited for on the client submission path.
func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchAdaptiveNLV669StreamingV2(batch []metaTrackPreparedRecordV6568, blockLimit int) ([]metaTrackPreparedRecordV6568, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	if p == nil {
		return nil, fmt.Errorf("metatrack v2 streaming V669 planner is nil")
	}
	seq := batch[0].Record.RouteBatchSequence
	for _, prepared := range batch {
		if prepared.Record.RouteBatchSequence != seq {
			return nil, fmt.Errorf("metatrack v2 streaming V669 mixed route-batch sequence")
		}
		if prepared.Record.ConsensusExecutionRound <= 0 {
			return nil, fmt.Errorf("metatrack v2 streaming V669 missing signed global execution round")
		}
	}
	if len(p.records) == 0 {
		if err := p.resetWithBatch(batch); err != nil {
			return nil, err
		}
		if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
			return nil, fmt.Errorf("metatrack v2 streaming V669 one route-batch projection exceeds block limit")
		}
		return metaTrackAnnotateStreamingPrefixV2(p, batch), nil
	}
	if seq != p.endBatch+1 {
		return nil, fmt.Errorf("metatrack v2 streaming V669 route batches are not contiguous")
	}
	candidateDepth, candidateL := metaTrackExtendCriticalPathV6568(p.depthByOrdinal, batch)
	candidateCounts := metaTrackShardCountsAfterV6568(p.shardCounts, batch)
	candidateN := p.transactionCount + len(batch)
	join := metaTrackCountsFitBlockV6568(candidateCounts, blockLimit) &&
		metaTrackDependencyClosedJoinV662(p.records, batch) &&
		metaTrackCriticalWidthImprovesV6568(p.transactionCount, p.criticalPath, candidateN, candidateL)
	if join {
		p.records = append(p.records, batch...)
		p.depthByOrdinal = candidateDepth
		p.criticalPath = candidateL
		p.transactionCount = candidateN
		p.shardCounts = candidateCounts
		p.endBatch = seq
		return metaTrackAnnotateStreamingPrefixV2(p, batch), nil
	}

	// The previous prefix is already signed/submitted, so closing it requires no
	// mutation. Start a new logical V669 window for this RouteBatch.
	p.sequence++
	if err := p.resetWithBatch(batch); err != nil {
		return nil, err
	}
	if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
		return nil, fmt.Errorf("metatrack v2 streaming V669 one route-batch projection exceeds block limit")
	}
	return metaTrackAnnotateStreamingPrefixV2(p, batch), nil
}

type metaTrackStreamingProjectionV2 struct {
	identity metaTrackBatchProjectionIdentity
	items    []tx.SignedTransaction
	first    *tx.ExecutionRoutingMetadata
}

func metaTrackStreamingProjectionsV2(items []tx.SignedTransaction, shardID string) ([]metaTrackStreamingProjectionV2, error) {
	bySeq := map[uint64][]tx.SignedTransaction{}
	ids := map[uint64]metaTrackBatchProjectionIdentity{}
	seqs := []uint64{}
	for _, item := range items {
		id, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return nil, err
		}
		if id.ExecutionShard != shardID {
			return nil, fmt.Errorf("metatrack v2 transaction routed to %s but reserved on %s", id.ExecutionShard, shardID)
		}
		if existing, ok := ids[id.Sequence]; ok {
			if existing != id {
				return nil, fmt.Errorf("metatrack v2 route-batch identity mismatch sequence=%d", id.Sequence)
			}
		} else {
			ids[id.Sequence] = id
			seqs = append(seqs, id.Sequence)
		}
		bySeq[id.Sequence] = append(bySeq[id.Sequence], item)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	out := make([]metaTrackStreamingProjectionV2, 0, len(seqs))
	for _, seq := range seqs {
		group := append([]tx.SignedTransaction(nil), bySeq[seq]...)
		id := ids[seq]
		if len(group) < id.ShardTransactionCount {
			if len(out) == 0 {
				return nil, fmt.Errorf("%w: sequence=%d shard=%s have=%d expected=%d", errMetaTrackBatchProjectionIncomplete, seq, shardID, len(group), id.ShardTransactionCount)
			}
			break
		}
		if len(group) > id.ShardTransactionCount {
			return nil, fmt.Errorf("metatrack v2 projection overfilled sequence=%d shard=%s have=%d expected=%d", seq, shardID, len(group), id.ShardTransactionCount)
		}
		sort.SliceStable(group, func(i, j int) bool {
			return group[i].ExecutionRouting.RoutingOrdinal < group[j].ExecutionRouting.RoutingOrdinal
		})
		first := group[0].ExecutionRouting
		if first == nil || first.ConsensusWindowSequence == 0 || first.ConsensusWindowStartBatchSequence == 0 ||
			first.ConsensusWindowEndBatchSequence != seq || first.ConsensusWindowRouteBatchCount <= 0 ||
			first.ConsensusWindowTransactionCount <= 0 || first.ConsensusWindowShardTransactionCount <= 0 ||
			first.ConsensusWindowCriticalPath <= 0 || first.ConsensusExecutionRound <= 0 {
			return nil, fmt.Errorf("metatrack v2 incomplete signed streaming prefix sequence=%d", seq)
		}
		for _, item := range group {
			r := item.ExecutionRouting
			if r == nil {
				return nil, fmt.Errorf("metatrack v2 missing execution routing")
			}
			if err := tx.ValidateExecutionRouting(item); err != nil {
				return nil, err
			}
			if r.ConsensusWindowSequence != first.ConsensusWindowSequence ||
				r.ConsensusWindowStartBatchSequence != first.ConsensusWindowStartBatchSequence ||
				r.ConsensusWindowEndBatchSequence != first.ConsensusWindowEndBatchSequence ||
				r.ConsensusWindowRouteBatchCount != first.ConsensusWindowRouteBatchCount ||
				r.ConsensusWindowTransactionCount != first.ConsensusWindowTransactionCount ||
				r.ConsensusWindowShardTransactionCount != first.ConsensusWindowShardTransactionCount ||
				r.ConsensusWindowCriticalPath != first.ConsensusWindowCriticalPath {
				return nil, fmt.Errorf("metatrack v2 mixed streaming prefix metadata sequence=%d", seq)
			}
			if r.ConsensusExecutionRound <= 0 {
				return nil, fmt.Errorf("metatrack v2 missing global execution round sequence=%d", seq)
			}
		}
		expectedPrefixCount := int(seq - first.ConsensusWindowStartBatchSequence + 1)
		if first.ConsensusWindowStartBatchSequence > seq || first.ConsensusWindowRouteBatchCount != expectedPrefixCount {
			return nil, fmt.Errorf("metatrack v2 invalid prefix batch count sequence=%d", seq)
		}
		if first.ConsensusWindowShardTransactionCount < id.ShardTransactionCount ||
			first.ConsensusWindowTransactionCount < id.TransactionCount {
			return nil, fmt.Errorf("metatrack v2 cumulative prefix count smaller than current projection sequence=%d", seq)
		}
		out = append(out, metaTrackStreamingProjectionV2{identity: id, items: group, first: first})
	}
	return out, nil
}

func metaTrackStreamingPrefixJoinValidV2(previous, current metaTrackStreamingProjectionV2) bool {
	a, b := previous.first, current.first
	if a == nil || b == nil {
		return false
	}
	// MBE_METATRACK_MECHPACK_V24_SPARSE_PROJECTION
	// RouteBatch sequence is global, while a physical execution shard may have
	// zero transactions in one or more intermediate RouteBatches. Therefore the
	// next *local* projection need only advance the immutable global prefix; it
	// need not be exactly End+1. The cumulative shard-count delta proves that
	// every skipped global batch contributed zero transactions to this shard.
	if b.ConsensusWindowSequence != a.ConsensusWindowSequence ||
		b.ConsensusWindowStartBatchSequence != a.ConsensusWindowStartBatchSequence ||
		b.ConsensusWindowEndBatchSequence <= a.ConsensusWindowEndBatchSequence {
		return false
	}
	globalDelta := b.ConsensusWindowTransactionCount - a.ConsensusWindowTransactionCount
	if globalDelta < current.identity.TransactionCount {
		return false
	}
	if b.ConsensusWindowShardTransactionCount-a.ConsensusWindowShardTransactionCount != current.identity.ShardTransactionCount {
		return false
	}
	if b.ConsensusWindowCriticalPath < a.ConsensusWindowCriticalPath {
		return false
	}
	// Same exact threshold-free V669 N/L admission test, now checked over signed
	// cumulative prefixes instead of requiring the client to wait for closure.
	return int64(b.ConsensusWindowTransactionCount)*int64(a.ConsensusWindowCriticalPath) >
		int64(a.ConsensusWindowTransactionCount)*int64(b.ConsensusWindowCriticalPath)
}

// selectMetaTrackStreamingWindowV2 selects as many currently available complete
// signed RouteBatch projections as belong to one streaming V669 window. It never
// skips an incomplete earlier projection and never crosses a signed window
// boundary. The normal block limit remains the only capacity ceiling.
func selectMetaTrackStreamingWindowV2(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v2 requires positive block limit")
	}
	projections, err := metaTrackStreamingProjectionsV2(items, shardID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(projections) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v2 no complete projection")
	}
	selectedIDs := map[string]bool{}
	selected := make([]tx.SignedTransaction, 0, limit)
	summaries := make([]metaTrackBatchProjectionIdentity, 0, len(projections))
	firstWindow := projections[0].first.ConsensusWindowSequence
	var previous *metaTrackStreamingProjectionV2
	for index := range projections {
		projection := &projections[index]
		if projection.first.ConsensusWindowSequence != firstWindow {
			break
		}
		if previous != nil && !metaTrackStreamingPrefixJoinValidV2(*previous, *projection) {
			return nil, nil, nil, fmt.Errorf("metatrack v2 signed prefix join mismatch sequence=%d", projection.identity.Sequence)
		}
		if len(selected)+len(projection.items) > limit {
			break
		}
		for _, item := range projection.items {
			selected = append(selected, item)
			selectedIDs[item.TxID] = true
		}
		summaries = append(summaries, projection.identity)
		previous = projection
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v2 no streaming projection fits block limit")
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if !selectedIDs[item.TxID] {
			deferred = append(deferred, item)
		}
	}
	return selected, deferred, summaries, nil
}

func metaTrackStreamingWindowMetadataV2(block realblock.Block) bool {
	if len(block.TxList) == 0 {
		return false
	}
	for _, item := range block.TxList {
		r := item.ExecutionRouting
		if r == nil || r.ConsensusWindowSequence == 0 || r.ConsensusWindowEndBatchSequence != r.RouteBatchSequence ||
			r.ConsensusWindowRouteBatchCount <= 0 || r.ConsensusWindowTransactionCount <= 0 ||
			r.ConsensusWindowShardTransactionCount <= 0 || r.ConsensusWindowCriticalPath <= 0 {
			return false
		}
	}
	return true
}

func (r *NodeRuntime) validateMetaTrackStreamingWindowV2(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	if !metaTrackStreamingWindowMetadataV2(block) {
		return nil, fmt.Errorf("metatrack v2 streaming metadata missing")
	}
	projections, err := metaTrackStreamingProjectionsV2(block.TxList, block.ShardID)
	if err != nil {
		return nil, err
	}
	if len(projections) == 0 {
		return nil, fmt.Errorf("metatrack v2 empty projection set")
	}
	window := projections[0].first.ConsensusWindowSequence
	total := 0
	summaries := make([]metaTrackBatchProjectionIdentity, 0, len(projections))
	for index, projection := range projections {
		if projection.first.ConsensusWindowSequence != window {
			return nil, fmt.Errorf("metatrack v2 PBFT block crosses streaming windows")
		}
		if index > 0 && !metaTrackStreamingPrefixJoinValidV2(projections[index-1], projection) {
			return nil, fmt.Errorf("metatrack v2 invalid signed prefix progression")
		}
		total += len(projection.items)
		summaries = append(summaries, projection.identity)
	}
	if total != len(block.TxList) {
		return nil, fmt.Errorf("metatrack v2 projection count mismatch")
	}
	// Streaming aggregation can place several complete RouteBatch projections in
	// one PBFT proposal. ConsensusExecutionDepth is intentionally *batch-local*,
	// so a cross-RouteBatch predecessor must be ordered/committed but must not
	// inflate the signed batch-local depth. All other v6.5.6 frontier checks stay
	// fail-closed.
	if _, err := r.validateMetaTrackStreamingTransactionFrontierV24(block); err != nil {
		return nil, err
	}
	return summaries, nil
}

// validateMetaTrackStreamingTransactionFrontierV24 preserves the v6.5.6
// predecessor/exact-version safety contract while respecting the original
// meaning of ConsensusExecutionDepth: depth inside one RouteBatch only.
// Cross-RouteBatch predecessors that are coalesced into the same streaming PBFT
// block are still required to be earlier in the block (or already committed),
// but they do not contribute to this batch-local depth field.
func (r *NodeRuntime) validateMetaTrackStreamingTransactionFrontierV24(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	if !metaTrackTransactionFrontierMetadataV656(block) {
		return nil, fmt.Errorf("metatrack v2.4 signed transaction-frontier metadata missing")
	}
	committedState, err := r.committedOrdinalsV656()
	if err != nil {
		return nil, err
	}
	committedState.mu.Lock()
	committed := make(map[uint64]bool, len(committedState.committed))
	for ordinal := range committedState.committed {
		committed[ordinal] = true
	}
	committedState.mu.Unlock()

	inBlock := map[uint64]int{}
	batchDepths := map[uint64]int{}
	produced := map[metaTrackVersionIdentity]uint64{}
	lastOrdinal := uint64(0)
	for index, item := range block.TxList {
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, err
		}
		routing := item.ExecutionRouting
		if routing.ExecutionShard != block.ShardID || routing.ConsensusExecutionDepth <= 0 {
			return nil, fmt.Errorf("metatrack v2.4 invalid execution shard/frontier metadata")
		}
		if lastOrdinal != 0 && routing.RoutingOrdinal <= lastOrdinal {
			return nil, fmt.Errorf("metatrack v2.4 block ordinals are not strictly increasing")
		}
		lastOrdinal = routing.RoutingOrdinal
		inBlock[routing.RoutingOrdinal] = index

		candidateBatchDepth := 1
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if predIndex, ok := inBlock[pred]; ok {
				if predIndex >= index {
					return nil, fmt.Errorf("metatrack v2.4 ordering predecessor not earlier")
				}
			} else if !committed[pred] {
				return nil, fmt.Errorf("metatrack v2.4 omitted ordering predecessor ordinal=%d", pred)
			}
		}
		for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
			if predIndex, ok := inBlock[pred]; ok {
				if predIndex >= index {
					return nil, fmt.Errorf("metatrack v2.4 execution predecessor not earlier")
				}
				predRouting := block.TxList[predIndex].ExecutionRouting
				if predRouting == nil {
					return nil, fmt.Errorf("metatrack v2.4 predecessor missing execution routing")
				}
				// The signed depth is RouteBatch-local by construction. Only an
				// execution predecessor from the same RouteBatch contributes.
				if predRouting.RouteBatchSequence == routing.RouteBatchSequence && batchDepths[pred]+1 > candidateBatchDepth {
					candidateBatchDepth = batchDepths[pred] + 1
				}
			} else if !committed[pred] {
				return nil, fmt.Errorf("metatrack v2.4 omitted execution predecessor ordinal=%d", pred)
			}
		}
		if candidateBatchDepth > routing.ConsensusExecutionDepth {
			return nil, fmt.Errorf("metatrack v2.4 batch-local dependency depth %d exceeds signed batch-local depth %d", candidateBatchDepth, routing.ConsensusExecutionDepth)
		}
		batchDepths[routing.RoutingOrdinal] = candidateBatchDepth

		for _, dep := range routing.StateVersions {
			if dep.ProducedVersion > 0 {
				if dep.ProducedVersion != routing.RoutingOrdinal {
					return nil, fmt.Errorf("metatrack v2.4 produced version mismatch")
				}
				id := metaTrackVersionIdentity{Key: dep.Key, Version: dep.ProducedVersion}
				if _, exists := produced[id]; exists {
					return nil, fmt.Errorf("metatrack v2.4 duplicate producer")
				}
				produced[id] = routing.RoutingOrdinal
			}
			if dep.RequiredVersion > 0 && dep.RequiredVersion >= routing.RoutingOrdinal {
				return nil, fmt.Errorf("metatrack v2.4 future/non-monotonic exact-version requirement")
			}
		}
	}
	for _, item := range block.TxList {
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.RequiredVersion == 0 {
				continue
			}
			if producer, ok := produced[metaTrackVersionIdentity{Key: dep.Key, Version: dep.RequiredVersion}]; ok && producer >= item.ExecutionRouting.RoutingOrdinal {
				return nil, fmt.Errorf("metatrack v2.4 local exact predecessor order mismatch")
			}
		}
	}
	return metaTrackProjectionSummariesV656(block)
}
