package v5

import (
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackPartitionInvariantPolicyV658 = "stream_partition_invariant_v658"

// metaTrackPartitionInvariantConsensusV658Enabled is runtime-local configuration
// evidence.  The PBFT protocol itself is unchanged; this flag only selects the
// MetaTrack candidate/validation rule used before PBFT.
func (r *NodeRuntime) metaTrackPartitionInvariantConsensusV658Enabled() bool {
	if r == nil || r.pluginSnapshot == nil {
		return false
	}
	cfg, ok := r.pluginSnapshot["block_producer"]
	return ok && cfg.Config != nil && boolFromAny(cfg.Config["partition_invariant_consensus_v658"])
}

// selectMetaTrackPartitionInvariantFrontierV658 chooses the largest currently
// admissible dependency-closed frontier under the existing physical block limit.
// RouteBatchSequence is deliberately NOT a selection atom.  Signed global rounds
// prove dependency direction; same-shard execution/ordering predecessors must be
// selected earlier when they are still present in the current candidate pool.
func selectMetaTrackPartitionInvariantFrontierV658(items []tx.SignedTransaction, limit int, shardID string, _ *mempool.Mempool) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 requires positive block limit")
	}
	ordered := append([]tx.SignedTransaction(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		li, lj := ordered[i].ExecutionRouting, ordered[j].ExecutionRouting
		if li == nil || lj == nil {
			return i < j
		}
		return li.RoutingOrdinal < lj.RoutingOrdinal
	})

	active := map[uint64]bool{}
	activeRound := map[uint64]int{}
	for _, item := range ordered {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 missing execution routing")
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, nil, nil, err
		}
		routing := item.ExecutionRouting
		if routing.ExecutionShard != shardID || routing.RoutingOrdinal == 0 || routing.ConsensusExecutionRound <= 0 {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 invalid signed streaming frontier metadata")
		}
		active[routing.RoutingOrdinal] = true
		activeRound[routing.RoutingOrdinal] = routing.ConsensusExecutionRound
	}

	selectedOrd := map[uint64]bool{}
	selectedIDs := map[string]bool{}
	selectedCapacity := limit
	if len(ordered) < selectedCapacity {
		selectedCapacity = len(ordered)
	}
	selected := make([]tx.SignedTransaction, 0, selectedCapacity)
	for _, item := range ordered {
		if len(selected) >= limit {
			break
		}
		routing := item.ExecutionRouting
		blocked := false
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if predRound, ok := activeRound[pred]; ok && predRound >= routing.ConsensusExecutionRound {
				return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 ordering predecessor round is not lower")
			}
			if active[pred] && !selectedOrd[pred] {
				blocked = true
				break
			}
		}
		if !blocked {
			for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
				if predRound, ok := activeRound[pred]; ok && predRound >= routing.ConsensusExecutionRound {
					return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 execution predecessor round is not lower")
				}
				if active[pred] && !selectedOrd[pred] {
					blocked = true
					break
				}
			}
		}
		if blocked {
			continue
		}
		for _, dep := range routing.StateVersions {
			if dep.RequiredVersion == 0 || !metaTrackExactAccessForDependencyV6566(item, dep.Key) {
				continue
			}
			if dep.RequiredVersion >= routing.RoutingOrdinal || dep.RequiredExecutionRound <= 0 || dep.RequiredExecutionRound >= routing.ConsensusExecutionRound {
				return nil, nil, nil, fmt.Errorf("metatrack v6.5.8 exact predecessor round contract violated")
			}
		}
		selected = append(selected, item)
		selectedOrd[routing.RoutingOrdinal] = true
		selectedIDs[item.TxID] = true
	}
	if len(selected) == 0 {
		return nil, nil, nil, errMetaTrackBatchProjectionIncomplete
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

func (r *NodeRuntime) validateMetaTrackPartitionInvariantFrontierV658(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	if len(block.TxList) == 0 {
		return nil, fmt.Errorf("metatrack v6.5.8 empty transaction frontier")
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
	roundByOrdinal := map[uint64]int{}
	producerRound := map[metaTrackVersionIdentity]int{}
	lastOrdinal := uint64(0)
	for index, item := range block.TxList {
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, err
		}
		routing := item.ExecutionRouting
		if routing == nil || routing.ExecutionShard != block.ShardID || routing.RoutingOrdinal == 0 || routing.ConsensusExecutionRound <= 0 {
			return nil, fmt.Errorf("metatrack v6.5.8 invalid block routing metadata")
		}
		if lastOrdinal != 0 && routing.RoutingOrdinal <= lastOrdinal {
			return nil, fmt.Errorf("metatrack v6.5.8 block ordinals are not strictly increasing")
		}
		lastOrdinal = routing.RoutingOrdinal
		inBlock[routing.RoutingOrdinal] = index
		roundByOrdinal[routing.RoutingOrdinal] = routing.ConsensusExecutionRound
		for _, dep := range routing.StateVersions {
			if dep.ProducedVersion > 0 {
				if dep.ProducedVersion != routing.RoutingOrdinal {
					return nil, fmt.Errorf("metatrack v6.5.8 produced version mismatch")
				}
				producerRound[metaTrackVersionIdentity{Key: dep.Key, Version: dep.ProducedVersion}] = routing.ConsensusExecutionRound
			}
		}
	}

	for index, item := range block.TxList {
		routing := item.ExecutionRouting
		round := routing.ConsensusExecutionRound
		checkPred := func(pred uint64, kind string) error {
			if predIndex, ok := inBlock[pred]; ok {
				if predIndex >= index {
					return fmt.Errorf("metatrack v6.5.8 %s predecessor not earlier", kind)
				}
				if predRound := roundByOrdinal[pred]; predRound <= 0 || predRound >= round {
					return fmt.Errorf("metatrack v6.5.8 %s predecessor round is not lower", kind)
				}
				return nil
			}
			if !committed[pred] {
				return fmt.Errorf("metatrack v6.5.8 omitted uncommitted %s predecessor ordinal=%d", kind, pred)
			}
			return nil
		}
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if err := checkPred(pred, "ordering"); err != nil {
				return nil, err
			}
		}
		for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
			if err := checkPred(pred, "execution"); err != nil {
				return nil, err
			}
		}
		for _, dep := range routing.StateVersions {
			if dep.RequiredVersion == 0 {
				continue
			}
			if dep.RequiredVersion >= routing.RoutingOrdinal {
				return nil, fmt.Errorf("metatrack v6.5.8 future/non-monotonic exact version")
			}
			if !metaTrackExactAccessForDependencyV6566(item, dep.Key) {
				continue
			}
			if dep.RequiredExecutionRound <= 0 || dep.RequiredExecutionRound >= round {
				return nil, fmt.Errorf("metatrack v6.5.8 exact predecessor round contract violated")
			}
			if actual, ok := producerRound[metaTrackVersionIdentity{Key: dep.Key, Version: dep.RequiredVersion}]; ok && actual != dep.RequiredExecutionRound {
				return nil, fmt.Errorf("metatrack v6.5.8 exact predecessor round evidence mismatch")
			}
		}
	}
	return metaTrackProjectionSummariesV656(block)
}
