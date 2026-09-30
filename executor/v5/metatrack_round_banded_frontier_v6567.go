package v5

import (
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackRoundBandedPolicyV6567 = "signed_global_round_banded_frontier_v6567"

func metaTrackRoundBandAllowanceV6567(count, workers int) int {
	if count <= 0 {
		return 0
	}
	if workers < 1 {
		workers = 1
	}
	byWork := (count + workers - 1) / workers
	if byWork < 1 {
		byWork = 1
	}
	if byWork > workers {
		return workers
	}
	return byWork
}

func metaTrackRoundSpanV6567(minRound, maxRound int) int {
	if minRound <= 0 || maxRound < minRound {
		return 0
	}
	return maxRound - minRound + 1
}

func metaTrackRoundBandedFrontierMetadataV6567(block realblock.Block) bool {
	if len(block.TxList) == 0 {
		return false
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ConsensusExecutionRound <= 0 {
			return false
		}
	}
	return true
}

func copyOrdinalBoolMapV6567(src map[uint64]bool) map[uint64]bool {
	out := make(map[uint64]bool, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func copyOrdinalIntMapV6567(src map[uint64]int) map[uint64]int {
	out := make(map[uint64]int, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

// selectMetaTrackRoundBandedTransactionFrontierV6567 removes the v6.5.6.6
// "remote exact version must already be published" gate. Instead, every signed
// execution dependency receives a global round and every exact cross-shard edge
// carries its predecessor round. Since every wait edge must point to a strictly
// lower round, the cross-shard block wait graph cannot contain a cycle.
//
// To avoid solving liveness by swallowing an arbitrarily long chain, a proposal
// may span only a structural round band. The band width is bounded by both the
// configured worker count and ceil(selected/workers). No transaction-count,
// RouteBatch-count, or hand-tuned chain threshold is introduced.
func selectMetaTrackRoundBandedTransactionFrontierV6567(items []tx.SignedTransaction, limit int, shardID string, _ *mempool.Mempool, workers int) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 requires positive block limit")
	}
	if workers < 1 {
		workers = 1
	}

	ordered := append([]tx.SignedTransaction(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i].ExecutionRouting, ordered[j].ExecutionRouting
		if left == nil || right == nil {
			return i < j
		}
		return left.RoutingOrdinal < right.RoutingOrdinal
	})

	active := map[uint64]bool{}
	activeRound := map[uint64]int{}
	byRound := map[int][]tx.SignedTransaction{}
	rounds := []int{}
	seenRound := map[int]bool{}
	for _, item := range ordered {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 missing execution routing")
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, nil, nil, err
		}
		routing := item.ExecutionRouting
		if routing.ExecutionShard != shardID || routing.ConsensusExecutionDepth <= 0 || routing.ConsensusExecutionRound <= 0 {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 invalid signed round-frontier metadata")
		}
		active[routing.RoutingOrdinal] = true
		activeRound[routing.RoutingOrdinal] = routing.ConsensusExecutionRound
		round := routing.ConsensusExecutionRound
		if !seenRound[round] {
			seenRound[round] = true
			rounds = append(rounds, round)
		}
		byRound[round] = append(byRound[round], item)
	}
	sort.Ints(rounds)

	selectedOrd := map[uint64]bool{}
	depth := map[uint64]int{}
	selected := make([]tx.SignedTransaction, 0, limit)
	selectedIDs := map[string]bool{}
	minRound, maxRound := 0, 0

	for _, round := range rounds {
		if len(selected) >= limit {
			break
		}
		tempOrd := copyOrdinalBoolMapV6567(selectedOrd)
		tempDepth := copyOrdinalIntMapV6567(depth)
		group := make([]tx.SignedTransaction, 0, len(byRound[round]))

		for _, item := range byRound[round] {
			if len(selected)+len(group) >= limit {
				break
			}
			routing := item.ExecutionRouting
			blocked := false
			for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
				if predRound, ok := activeRound[pred]; ok && predRound >= routing.ConsensusExecutionRound {
					return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 ordering predecessor round is not lower")
				}
				if active[pred] && !tempOrd[pred] {
					blocked = true
					break
				}
			}
			candidateDepth := 1
			if !blocked {
				for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
					if predRound, ok := activeRound[pred]; ok && predRound >= routing.ConsensusExecutionRound {
						return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 execution predecessor round is not lower")
					}
					if active[pred] && !tempOrd[pred] {
						blocked = true
						break
					}
					if tempOrd[pred] && tempDepth[pred]+1 > candidateDepth {
						candidateDepth = tempDepth[pred] + 1
					}
				}
			}
			if blocked || candidateDepth > routing.ConsensusExecutionDepth {
				continue
			}
			ordinal := routing.RoutingOrdinal
			tempOrd[ordinal] = true
			tempDepth[ordinal] = candidateDepth
			group = append(group, item)
		}
		if len(group) == 0 {
			continue
		}

		tentativeCount := len(selected) + len(group)
		tentativeMin, tentativeMax := minRound, maxRound
		if tentativeMin == 0 || round < tentativeMin {
			tentativeMin = round
		}
		if round > tentativeMax {
			tentativeMax = round
		}
		span := metaTrackRoundSpanV6567(tentativeMin, tentativeMax)
		allowed := metaTrackRoundBandAllowanceV6567(tentativeCount, workers)
		if span > allowed {
			continue
		}

		selectedOrd = tempOrd
		depth = tempDepth
		minRound, maxRound = tentativeMin, tentativeMax
		for _, item := range group {
			selected = append(selected, item)
			selectedIDs[item.TxID] = true
		}
	}

	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.7 found no dependency-closed round-banded transaction frontier")
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

func (r *NodeRuntime) validateMetaTrackRoundBandedTransactionFrontierV6567(block realblock.Block) ([]metaTrackBatchProjectionIdentity, error) {
	summaries, err := r.validateMetaTrackTransactionFrontierV656(block)
	if err != nil {
		return nil, err
	}
	if !metaTrackRoundBandedFrontierMetadataV6567(block) {
		return nil, fmt.Errorf("metatrack v6.5.6.7 signed execution round metadata missing")
	}
	workers := blockExecutorWorkerCountFromProfile(r.pluginSnapshot)
	if workers < 1 {
		workers = 1
	}

	roundByOrdinal := map[uint64]int{}
	producerRound := map[metaTrackVersionIdentity]int{}
	minRound, maxRound := 0, 0
	for _, item := range block.TxList {
		routing := item.ExecutionRouting
		round := routing.ConsensusExecutionRound
		if round <= 0 {
			return nil, fmt.Errorf("metatrack v6.5.6.7 invalid execution round")
		}
		roundByOrdinal[routing.RoutingOrdinal] = round
		if minRound == 0 || round < minRound {
			minRound = round
		}
		if round > maxRound {
			maxRound = round
		}
		for _, dependency := range routing.StateVersions {
			if dependency.ProducedVersion > 0 {
				producerRound[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}] = round
			}
		}
	}

	for _, item := range block.TxList {
		routing := item.ExecutionRouting
		round := routing.ConsensusExecutionRound
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if predRound, ok := roundByOrdinal[pred]; ok && predRound >= round {
				return nil, fmt.Errorf("metatrack v6.5.6.7 block ordering predecessor round is not lower")
			}
		}
		for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
			if predRound, ok := roundByOrdinal[pred]; ok && predRound >= round {
				return nil, fmt.Errorf("metatrack v6.5.6.7 block execution predecessor round is not lower")
			}
		}
		for _, dependency := range routing.StateVersions {
			if dependency.RequiredVersion == 0 || !metaTrackExactAccessForDependencyV6566(item, dependency.Key) {
				continue
			}
			if dependency.RequiredExecutionRound <= 0 || dependency.RequiredExecutionRound >= round {
				return nil, fmt.Errorf("metatrack v6.5.6.7 exact predecessor round contract violated")
			}
			if actual, ok := producerRound[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}]; ok && actual != dependency.RequiredExecutionRound {
				return nil, fmt.Errorf("metatrack v6.5.6.7 exact predecessor round evidence mismatch")
			}
		}
	}

	span := metaTrackRoundSpanV6567(minRound, maxRound)
	allowed := metaTrackRoundBandAllowanceV6567(len(block.TxList), workers)
	if span > allowed {
		return nil, fmt.Errorf("metatrack v6.5.6.7 round band span %d exceeds structural allowance %d", span, allowed)
	}
	return summaries, nil
}
