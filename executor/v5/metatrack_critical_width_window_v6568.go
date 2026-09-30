package v5

import (
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackCriticalWidthWindowPolicyV6568 = "critical_width_consensus_window_v6568"

type metaTrackPreparedRecordV6568 struct {
	Record WorkloadRecord
	Route  RoutingDecision
}

type metaTrackCriticalWidthWindowPlannerV6568 struct {
	sequence         uint64
	records          []metaTrackPreparedRecordV6568
	depthByOrdinal   map[uint64]int
	transactionCount int
	criticalPath     int
	shardCounts      map[string]int
	startBatch       uint64
	endBatch         uint64
}

func newMetaTrackCriticalWidthWindowPlannerV6568() *metaTrackCriticalWidthWindowPlannerV6568 {
	return &metaTrackCriticalWidthWindowPlannerV6568{sequence: 1}
}

func metaTrackExecutionPredecessorsV6568(record WorkloadRecord) []uint64 {
	seen := map[uint64]bool{}
	for _, pred := range record.ConsensusExecutionPredecessorOrdinals {
		if pred > 0 && pred < record.RoutingOrdinal {
			seen[pred] = true
		}
	}
	for _, dep := range record.StateVersions {
		if dep.RequiredVersion > 0 && dep.RequiredExecutionRound > 0 && dep.RequiredVersion < record.RoutingOrdinal {
			seen[dep.RequiredVersion] = true
		}
	}
	out := make([]uint64, 0, len(seen))
	for pred := range seen {
		out = append(out, pred)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func metaTrackExtendCriticalPathV6568(base map[uint64]int, batch []metaTrackPreparedRecordV6568) (map[uint64]int, int) {
	depths := make(map[uint64]int, len(base)+len(batch))
	maxDepth := 0
	for ordinal, depth := range base {
		depths[ordinal] = depth
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	ordered := append([]metaTrackPreparedRecordV6568(nil), batch...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Record.RoutingOrdinal < ordered[j].Record.RoutingOrdinal })
	for _, prepared := range ordered {
		record := prepared.Record
		depth := 1
		for _, pred := range metaTrackExecutionPredecessorsV6568(record) {
			if predDepth := depths[pred]; predDepth+1 > depth {
				depth = predDepth + 1
			}
		}
		depths[record.RoutingOrdinal] = depth
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return depths, maxDepth
}

func metaTrackCriticalWidthImprovesV6568(currentN, currentL, candidateN, candidateL int) bool {
	if currentN <= 0 || currentL <= 0 {
		return true
	}
	if candidateN <= currentN || candidateL <= 0 {
		return false
	}
	// Compare N/L exactly. No floating point and no empirical threshold.
	return int64(candidateN)*int64(currentL) > int64(currentN)*int64(candidateL)
}

func metaTrackShardCountsAfterV6568(base map[string]int, batch []metaTrackPreparedRecordV6568) map[string]int {
	out := make(map[string]int, len(base)+2)
	for shard, count := range base {
		out[shard] = count
	}
	for _, prepared := range batch {
		out[prepared.Record.ExecutionShard]++
	}
	return out
}

func metaTrackCountsFitBlockV6568(counts map[string]int, limit int) bool {
	if limit <= 0 {
		return false
	}
	for _, count := range counts {
		if count > limit {
			return false
		}
	}
	return true
}

func (p *metaTrackCriticalWidthWindowPlannerV6568) resetWithBatch(batch []metaTrackPreparedRecordV6568) error {
	if len(batch) == 0 {
		return fmt.Errorf("metatrack v6.5.6.8 empty route batch")
	}
	if p.sequence == 0 {
		p.sequence = 1
	}
	p.records = append([]metaTrackPreparedRecordV6568(nil), batch...)
	p.depthByOrdinal, p.criticalPath = metaTrackExtendCriticalPathV6568(nil, batch)
	p.transactionCount = len(batch)
	p.shardCounts = metaTrackShardCountsAfterV6568(nil, batch)
	p.startBatch = batch[0].Record.RouteBatchSequence
	p.endBatch = p.startBatch
	return nil
}

func (p *metaTrackCriticalWidthWindowPlannerV6568) finalizeCurrent() []metaTrackPreparedRecordV6568 {
	if len(p.records) == 0 {
		return nil
	}
	routeBatchCount := int(p.endBatch - p.startBatch + 1)
	out := make([]metaTrackPreparedRecordV6568, len(p.records))
	copy(out, p.records)
	for i := range out {
		r := &out[i].Record
		r.ConsensusWindowSequence = p.sequence
		r.ConsensusWindowStartBatchSequence = p.startBatch
		r.ConsensusWindowEndBatchSequence = p.endBatch
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

func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatch(batch []metaTrackPreparedRecordV6568, blockLimit int) ([]metaTrackPreparedRecordV6568, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	if p == nil {
		return nil, fmt.Errorf("metatrack v6.5.6.8 window planner is nil")
	}
	seq := batch[0].Record.RouteBatchSequence
	for _, prepared := range batch {
		if prepared.Record.RouteBatchSequence != seq {
			return nil, fmt.Errorf("metatrack v6.5.6.8 mixed route-batch sequence")
		}
		if prepared.Record.ConsensusExecutionRound <= 0 {
			return nil, fmt.Errorf("metatrack v6.5.6.8 missing signed global execution round")
		}
	}
	if len(p.records) == 0 {
		if err := p.resetWithBatch(batch); err != nil {
			return nil, err
		}
		if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
			return nil, fmt.Errorf("metatrack v6.5.6.8 one route-batch projection exceeds block limit")
		}
		return nil, nil
	}
	if seq != p.endBatch+1 {
		return nil, fmt.Errorf("metatrack v6.5.6.8 route batches are not contiguous")
	}
	candidateDepth, candidateL := metaTrackExtendCriticalPathV6568(p.depthByOrdinal, batch)
	candidateCounts := metaTrackShardCountsAfterV6568(p.shardCounts, batch)
	candidateN := p.transactionCount + len(batch)
	join := metaTrackCountsFitBlockV6568(candidateCounts, blockLimit) && metaTrackCriticalWidthImprovesV6568(p.transactionCount, p.criticalPath, candidateN, candidateL)
	if join {
		p.records = append(p.records, batch...)
		p.depthByOrdinal = candidateDepth
		p.criticalPath = candidateL
		p.transactionCount = candidateN
		p.shardCounts = candidateCounts
		p.endBatch = seq
		return nil, nil
	}
	closed := p.finalizeCurrent()
	p.sequence++
	if err := p.resetWithBatch(batch); err != nil {
		return nil, err
	}
	if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
		return nil, fmt.Errorf("metatrack v6.5.6.8 one route-batch projection exceeds block limit")
	}
	return closed, nil
}

func (p *metaTrackCriticalWidthWindowPlannerV6568) Flush() []metaTrackPreparedRecordV6568 {
	if p == nil || len(p.records) == 0 {
		return nil
	}
	out := p.finalizeCurrent()
	p.records = nil
	p.depthByOrdinal = nil
	p.transactionCount = 0
	p.criticalPath = 0
	p.shardCounts = nil
	p.startBatch = 0
	p.endBatch = 0
	return out
}

func metaTrackCriticalWidthWindowMetadataV6568(block realblock.Block) bool {
	if len(block.TxList) == 0 {
		return false
	}
	for _, item := range block.TxList {
		r := item.ExecutionRouting
		if r == nil || r.ConsensusWindowSequence == 0 || r.ConsensusWindowTransactionCount <= 0 || r.ConsensusWindowShardTransactionCount <= 0 || r.ConsensusWindowCriticalPath <= 0 {
			return false
		}
	}
	return true
}

func selectMetaTrackCriticalWidthWindowV6568(items []tx.SignedTransaction, limit int, shardID string, _ *mempool.Mempool) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 requires positive block limit")
	}
	ordered := append([]tx.SignedTransaction(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		li, lj := ordered[i].ExecutionRouting, ordered[j].ExecutionRouting
		if li == nil || lj == nil {
			return i < j
		}
		if li.ConsensusWindowSequence != lj.ConsensusWindowSequence {
			return li.ConsensusWindowSequence < lj.ConsensusWindowSequence
		}
		if li.RouteBatchSequence != lj.RouteBatchSequence {
			return li.RouteBatchSequence < lj.RouteBatchSequence
		}
		return li.RoutingOrdinal < lj.RoutingOrdinal
	})
	first := ordered[0].ExecutionRouting
	if first == nil || first.ConsensusWindowSequence == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 missing consensus window metadata")
	}
	window := first.ConsensusWindowSequence
	expectedShard := first.ConsensusWindowShardTransactionCount
	expectedGlobal := first.ConsensusWindowTransactionCount
	expectedStart := first.ConsensusWindowStartBatchSequence
	expectedEnd := first.ConsensusWindowEndBatchSequence
	expectedBatches := first.ConsensusWindowRouteBatchCount
	expectedCritical := first.ConsensusWindowCriticalPath
	selected := make([]tx.SignedTransaction, 0, expectedShard)
	selectedIDs := map[string]bool{}
	for _, item := range ordered {
		r := item.ExecutionRouting
		if r == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 missing execution routing")
		}
		if r.ConsensusWindowSequence != window {
			continue
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, nil, nil, err
		}
		if r.ExecutionShard != shardID {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 window shard mismatch")
		}
		if r.ConsensusWindowShardTransactionCount != expectedShard || r.ConsensusWindowTransactionCount != expectedGlobal || r.ConsensusWindowStartBatchSequence != expectedStart || r.ConsensusWindowEndBatchSequence != expectedEnd || r.ConsensusWindowRouteBatchCount != expectedBatches || r.ConsensusWindowCriticalPath != expectedCritical {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 inconsistent signed consensus window metadata")
		}
		if r.RouteBatchSequence < expectedStart || r.RouteBatchSequence > expectedEnd {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 route batch outside signed consensus window")
		}
		selected = append(selected, item)
		selectedIDs[item.TxID] = true
	}
	if expectedShard > limit {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 signed consensus window exceeds block limit")
	}
	if len(selected) < expectedShard {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 consensus window incomplete: have=%d expected=%d", len(selected), expectedShard)
	}
	if len(selected) > expectedShard {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.8 consensus window overfilled: have=%d expected=%d", len(selected), expectedShard)
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if !selectedIDs[item.TxID] {
			deferred = append(deferred, item)
		}
	}
	summaries, err := validateMetaTrackAggregatedBatchProjections(realblock.Block{ShardID: shardID, TxList: selected}, true)
	return selected, deferred, summaries, err
}

func (r *NodeRuntime) validateMetaTrackCriticalWidthWindowV6568(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	summaries, err := validateMetaTrackAggregatedBatchProjections(block, true)
	if err != nil {
		return nil, err
	}
	if !metaTrackCriticalWidthWindowMetadataV6568(block) {
		return nil, fmt.Errorf("metatrack v6.5.6.8 consensus window metadata missing")
	}
	first := block.TxList[0].ExecutionRouting
	count := 0
	lastBatch := uint64(0)
	for _, item := range block.TxList {
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, err
		}
		m := item.ExecutionRouting
		if m.ConsensusWindowSequence != first.ConsensusWindowSequence || m.ConsensusWindowTransactionCount != first.ConsensusWindowTransactionCount || m.ConsensusWindowShardTransactionCount != first.ConsensusWindowShardTransactionCount || m.ConsensusWindowStartBatchSequence != first.ConsensusWindowStartBatchSequence || m.ConsensusWindowEndBatchSequence != first.ConsensusWindowEndBatchSequence || m.ConsensusWindowRouteBatchCount != first.ConsensusWindowRouteBatchCount || m.ConsensusWindowCriticalPath != first.ConsensusWindowCriticalPath {
			return nil, fmt.Errorf("metatrack v6.5.6.8 mixed consensus window metadata")
		}
		if m.RouteBatchSequence < first.ConsensusWindowStartBatchSequence || m.RouteBatchSequence > first.ConsensusWindowEndBatchSequence {
			return nil, fmt.Errorf("metatrack v6.5.6.8 block contains route batch outside consensus window")
		}
		if lastBatch != 0 && m.RouteBatchSequence < lastBatch {
			return nil, fmt.Errorf("metatrack v6.5.6.8 route-batch sequence regression")
		}
		lastBatch = m.RouteBatchSequence
		count++
	}
	if count != first.ConsensusWindowShardTransactionCount {
		return nil, fmt.Errorf("metatrack v6.5.6.8 incomplete shard consensus window")
	}
	if int(first.ConsensusWindowEndBatchSequence-first.ConsensusWindowStartBatchSequence+1) != first.ConsensusWindowRouteBatchCount {
		return nil, fmt.Errorf("metatrack v6.5.6.8 consensus window batch count mismatch")
	}
	return summaries, nil
}
