package v5

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackTransactionFrontierPolicyV656 = "signed_transaction_dependency_frontier_v656"

type metaTrackConsensusOrdinalInfoV656 struct {
	shard    string
	sequence uint64
	depth    int
	round    int
}

type metaTrackConsensusShardTrackerV656 struct {
	lastWriter         map[string]uint64
	commutativeWriters map[string][]uint64
	readers            map[string][]uint64
	lastSender         map[string]uint64
}

type metaTrackConsensusPredecessorTrackerV656 struct {
	byShard   map[string]*metaTrackConsensusShardTrackerV656
	byOrdinal map[uint64]metaTrackConsensusOrdinalInfoV656
}

func newMetaTrackConsensusPredecessorTrackerV656() *metaTrackConsensusPredecessorTrackerV656 {
	return &metaTrackConsensusPredecessorTrackerV656{
		byShard:   map[string]*metaTrackConsensusShardTrackerV656{},
		byOrdinal: map[uint64]metaTrackConsensusOrdinalInfoV656{},
	}
}

func (t *metaTrackConsensusPredecessorTrackerV656) shardState(shard string) *metaTrackConsensusShardTrackerV656 {
	state := t.byShard[shard]
	if state == nil {
		state = &metaTrackConsensusShardTrackerV656{
			lastWriter: map[string]uint64{}, commutativeWriters: map[string][]uint64{},
			readers: map[string][]uint64{}, lastSender: map[string]uint64{},
		}
		t.byShard[shard] = state
	}
	return state
}

func metaTrackRecordAccessesV656(record WorkloadRecord) []tx.AccessItem {
	if len(record.SchedulingAccessList) > 0 {
		return record.SchedulingAccessList
	}
	return record.AccessList
}

func appendOrdinalV656(set map[uint64]bool, value uint64) {
	if value > 0 {
		set[value] = true
	}
}

func sortedOrdinalsV656(set map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// annotate binds only information already known before execution. It mirrors the
// execution scheduler's RAW/commutative/nonce rules, while WAW/WAR remain
// ordering-only. Cross-execution-shard exact predecessors are StateReady inputs,
// not local PBFT ordering predecessors.
func (t *metaTrackConsensusPredecessorTrackerV656) annotate(shard, sender string, record *WorkloadRecord) error {
	if t == nil || record == nil {
		return fmt.Errorf("metatrack v6.5.6 predecessor tracker is nil")
	}
	shard = strings.TrimSpace(shard)
	if shard == "" || record.RoutingOrdinal == 0 || record.RouteBatchSequence == 0 {
		return fmt.Errorf("metatrack v6.5.6 predecessor metadata requires execution shard, routing ordinal and route-batch sequence")
	}
	state := t.shardState(shard)
	execution := map[uint64]bool{}
	ordering := map[uint64]bool{}
	if sender = strings.TrimSpace(sender); sender != "" {
		appendOrdinalV656(execution, state.lastSender[sender])
		state.lastSender[sender] = record.RoutingOrdinal
	}
	accessByKey := map[string]tx.AccessItem{}
	for _, access := range metaTrackRecordAccessesV656(*record) {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			continue
		}
		accessByKey[key] = access
		switch access.Mode {
		case tx.AccessCommutativeDelta:
			appendOrdinalV656(ordering, state.lastWriter[key])
			for _, reader := range state.readers[key] {
				appendOrdinalV656(ordering, reader)
			}
			state.commutativeWriters[key] = append(state.commutativeWriters[key], record.RoutingOrdinal)
		case tx.AccessRead:
			appendOrdinalV656(execution, state.lastWriter[key])
			for _, writer := range state.commutativeWriters[key] {
				appendOrdinalV656(execution, writer)
			}
			state.readers[key] = append(state.readers[key], record.RoutingOrdinal)
		case tx.AccessWrite:
			appendOrdinalV656(ordering, state.lastWriter[key])
			for _, writer := range state.commutativeWriters[key] {
				appendOrdinalV656(ordering, writer)
			}
			for _, reader := range state.readers[key] {
				appendOrdinalV656(ordering, reader)
			}
			state.readers[key] = nil
			state.commutativeWriters[key] = nil
			state.lastWriter[key] = record.RoutingOrdinal
		case tx.AccessReadWrite:
			appendOrdinalV656(execution, state.lastWriter[key])
			for _, writer := range state.commutativeWriters[key] {
				appendOrdinalV656(execution, writer)
			}
			for _, reader := range state.readers[key] {
				appendOrdinalV656(ordering, reader)
			}
			state.readers[key] = nil
			state.commutativeWriters[key] = nil
			state.lastWriter[key] = record.RoutingOrdinal
		default:
			return fmt.Errorf("metatrack v6.5.6 unknown scheduling access mode for %s", key)
		}
	}
	for index := range record.StateVersions {
		dependency := &record.StateVersions[index]
		dependency.RequiredExecutionRound = 0
		if dependency.Key == "" || dependency.RequiredVersion == 0 {
			continue
		}
		access, ok := accessByKey[dependency.Key]
		if !ok || !requiresExactStateValue(access) {
			continue
		}
		info, ok := t.byOrdinal[dependency.RequiredVersion]
		if !ok || info.round <= 0 {
			return fmt.Errorf("metatrack v6.5.6.7 exact predecessor ordinal %d missing signed round", dependency.RequiredVersion)
		}
		dependency.RequiredExecutionRound = info.round
		if info.shard == shard {
			appendOrdinalV656(execution, dependency.RequiredVersion)
		}
	}
	// An execution predecessor dominates an ordering-only edge between the same pair.
	for ordinal := range execution {
		delete(ordering, ordinal)
	}
	depth := 1
	for ordinal := range execution {
		info, ok := t.byOrdinal[ordinal]
		if !ok || info.shard != shard || info.sequence != record.RouteBatchSequence {
			continue
		}
		if candidate := info.depth + 1; candidate > depth {
			depth = candidate
		}
	}
	globalRound := 1
	for ordinal := range execution {
		info, ok := t.byOrdinal[ordinal]
		if !ok || info.round <= 0 {
			return fmt.Errorf("metatrack v6.5.6.7 execution predecessor ordinal %d missing signed round", ordinal)
		}
		if candidate := info.round + 1; candidate > globalRound {
			globalRound = candidate
		}
	}
	for ordinal := range ordering {
		info, ok := t.byOrdinal[ordinal]
		if !ok || info.round <= 0 {
			return fmt.Errorf("metatrack v6.5.6.7 ordering predecessor ordinal %d missing signed round", ordinal)
		}
		if candidate := info.round + 1; candidate > globalRound {
			globalRound = candidate
		}
	}
	for _, dependency := range record.StateVersions {
		if dependency.RequiredExecutionRound > 0 && dependency.RequiredExecutionRound+1 > globalRound {
			globalRound = dependency.RequiredExecutionRound + 1
		}
	}
	record.ConsensusExecutionPredecessorOrdinals = sortedOrdinalsV656(execution)
	record.ConsensusOrderingPredecessorOrdinals = sortedOrdinalsV656(ordering)
	record.ConsensusExecutionDepth = depth
	record.ConsensusExecutionRound = globalRound
	t.byOrdinal[record.RoutingOrdinal] = metaTrackConsensusOrdinalInfoV656{shard: shard, sequence: record.RouteBatchSequence, depth: depth, round: globalRound}
	return nil
}

func metaTrackTransactionFrontierAnyMetadataV656(block realblock.Block) bool {
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		routing := item.ExecutionRouting
		if routing.ConsensusExecutionDepth > 0 || len(routing.ConsensusExecutionPredecessorOrdinals) > 0 || len(routing.ConsensusOrderingPredecessorOrdinals) > 0 {
			return true
		}
	}
	return false
}

func metaTrackTransactionFrontierMetadataV656(block realblock.Block) bool {
	if len(block.TxList) == 0 {
		return false
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ConsensusExecutionDepth <= 0 {
			return false
		}
	}
	return true
}

func metaTrackProjectionSummariesV656(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	bySeq := map[uint64]metaTrackBatchProjectionIdentity{}
	seqs := []uint64{}
	for _, item := range block.TxList {
		id, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return nil, err
		}
		if id.ExecutionShard != block.ShardID {
			return nil, fmt.Errorf("metatrack v6.5.6 shard mismatch")
		}
		if previous, ok := bySeq[id.Sequence]; ok && previous != id {
			return nil, fmt.Errorf("metatrack v6.5.6 route-batch identity mismatch in sequence %d", id.Sequence)
		} else if !ok {
			bySeq[id.Sequence] = id
			seqs = append(seqs, id.Sequence)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]metaTrackBatchProjectionIdentity, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, bySeq[seq])
	}
	return out, nil
}

// Leader-side selection. RouteBatch remains the routing-analysis window, but is
// no longer a consensus atom. The only capacity bound is the existing block size.
func selectMetaTrackTransactionFrontierV656(items []tx.SignedTransaction, limit int, shardID string, _ *mempool.Mempool) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6 requires positive block limit")
	}
	ordered := append([]tx.SignedTransaction(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].ExecutionRouting.RoutingOrdinal < ordered[j].ExecutionRouting.RoutingOrdinal
	})
	active := map[uint64]bool{}
	for _, item := range ordered {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6 missing execution routing")
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, nil, nil, err
		}
		if item.ExecutionRouting.ExecutionShard != shardID || item.ExecutionRouting.ConsensusExecutionDepth <= 0 {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6 invalid signed transaction-frontier metadata")
		}
		active[item.ExecutionRouting.RoutingOrdinal] = true
	}
	selectedOrd := map[uint64]bool{}
	depth := map[uint64]int{}
	selected := make([]tx.SignedTransaction, 0, limit)
	selectedIDs := map[string]bool{}
	for _, item := range ordered {
		if len(selected) >= limit {
			break
		}
		routing := item.ExecutionRouting
		blocked := false
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if active[pred] && !selectedOrd[pred] {
				blocked = true
				break
			}
		}
		candidateDepth := 1
		if !blocked {
			for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
				if active[pred] && !selectedOrd[pred] {
					blocked = true
					break
				}
				if selectedOrd[pred] && depth[pred]+1 > candidateDepth {
					candidateDepth = depth[pred] + 1
				}
			}
		}
		if blocked || candidateDepth > routing.ConsensusExecutionDepth {
			continue
		}
		ordinal := routing.RoutingOrdinal
		selectedOrd[ordinal] = true
		depth[ordinal] = candidateDepth
		selected = append(selected, item)
		selectedIDs[item.TxID] = true
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6 found no dependency-closed transaction frontier")
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if !selectedIDs[item.TxID] {
			deferred = append(deferred, item)
		}
	}
	summaries, err := metaTrackProjectionSummariesV656(realblock.Block{ShardID: shardID, TxList: selected})
	return selected, deferred, summaries, err
}

type metaTrackCommittedOrdinalsV656 struct {
	mu        sync.Mutex
	loaded    bool
	committed map[uint64]bool
}

var metaTrackCommittedOrdinalCacheV656 sync.Map // *NodeRuntime -> *metaTrackCommittedOrdinalsV656

func (r *NodeRuntime) committedOrdinalsV656() (*metaTrackCommittedOrdinalsV656, error) {
	raw, _ := metaTrackCommittedOrdinalCacheV656.LoadOrStore(r, &metaTrackCommittedOrdinalsV656{committed: map[uint64]bool{}})
	state := raw.(*metaTrackCommittedOrdinalsV656)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.loaded {
		return state, nil
	}
	if r.store != nil {
		blocks, err := r.store.ReadCommitted()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		for _, block := range blocks {
			for _, item := range block.TxList {
				if item.ExecutionRouting != nil && item.ExecutionRouting.ConsensusExecutionDepth > 0 {
					state.committed[item.ExecutionRouting.RoutingOrdinal] = true
				}
			}
		}
	}
	state.loaded = true
	return state, nil
}

func (r *NodeRuntime) recordMetaTrackCommittedRoutingOrdinalsV656(block realblock.Block) {
	if !metaTrackTransactionFrontierMetadataV656(block) {
		return
	}
	state, err := r.committedOrdinalsV656()
	if err != nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, item := range block.TxList {
		state.committed[item.ExecutionRouting.RoutingOrdinal] = true
	}
}

func (r *NodeRuntime) validateMetaTrackTransactionFrontierV656(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	if !metaTrackTransactionFrontierMetadataV656(block) {
		return nil, fmt.Errorf("metatrack v6.5.6 signed transaction-frontier metadata missing")
	}
	committedState, err := r.committedOrdinalsV656()
	if err != nil {
		return nil, err
	}
	committedState.mu.Lock()
	committed := make(map[uint64]bool, len(committedState.committed))
	for x := range committedState.committed {
		committed[x] = true
	}
	committedState.mu.Unlock()
	inBlock := map[uint64]int{}
	depths := map[uint64]int{}
	produced := map[metaTrackVersionIdentity]uint64{}
	lastOrdinal := uint64(0)
	for index, item := range block.TxList {
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, err
		}
		routing := item.ExecutionRouting
		if routing.ExecutionShard != block.ShardID || routing.ConsensusExecutionDepth <= 0 {
			return nil, fmt.Errorf("metatrack v6.5.6 invalid execution shard/frontier metadata")
		}
		if lastOrdinal != 0 && routing.RoutingOrdinal <= lastOrdinal {
			return nil, fmt.Errorf("metatrack v6.5.6 block ordinals are not strictly increasing")
		}
		lastOrdinal = routing.RoutingOrdinal
		inBlock[routing.RoutingOrdinal] = index
		candidateDepth := 1
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if predIndex, ok := inBlock[pred]; ok {
				if predIndex >= index {
					return nil, fmt.Errorf("metatrack v6.5.6 ordering predecessor not earlier")
				}
			} else if !committed[pred] {
				return nil, fmt.Errorf("metatrack v6.5.6 omitted ordering predecessor ordinal=%d", pred)
			}
		}
		for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
			if predIndex, ok := inBlock[pred]; ok {
				if predIndex >= index {
					return nil, fmt.Errorf("metatrack v6.5.6 execution predecessor not earlier")
				}
				if depths[pred]+1 > candidateDepth {
					candidateDepth = depths[pred] + 1
				}
			} else if !committed[pred] {
				return nil, fmt.Errorf("metatrack v6.5.6 omitted execution predecessor ordinal=%d", pred)
			}
		}
		if candidateDepth > routing.ConsensusExecutionDepth {
			return nil, fmt.Errorf("metatrack v6.5.6 block-local dependency depth %d exceeds signed batch-local depth %d", candidateDepth, routing.ConsensusExecutionDepth)
		}
		depths[routing.RoutingOrdinal] = candidateDepth
		for _, dep := range routing.StateVersions {
			if dep.ProducedVersion > 0 {
				if dep.ProducedVersion != routing.RoutingOrdinal {
					return nil, fmt.Errorf("metatrack v6.5.6 produced version mismatch")
				}
				id := metaTrackVersionIdentity{Key: dep.Key, Version: dep.ProducedVersion}
				if _, exists := produced[id]; exists {
					return nil, fmt.Errorf("metatrack v6.5.6 duplicate producer")
				}
				produced[id] = routing.RoutingOrdinal
			}
			if dep.RequiredVersion > 0 && dep.RequiredVersion >= routing.RoutingOrdinal {
				return nil, fmt.Errorf("metatrack v6.5.6 future/non-monotonic exact-version requirement")
			}
		}
	}
	for _, item := range block.TxList {
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.RequiredVersion == 0 {
				continue
			}
			if producer, ok := produced[metaTrackVersionIdentity{Key: dep.Key, Version: dep.RequiredVersion}]; ok && producer >= item.ExecutionRouting.RoutingOrdinal {
				return nil, fmt.Errorf("metatrack v6.5.6 local exact predecessor order mismatch")
			}
		}
	}
	return metaTrackProjectionSummariesV656(block)
}
