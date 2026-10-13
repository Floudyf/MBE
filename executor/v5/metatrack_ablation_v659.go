package v5

import "fmt"

const (
	metaTrackAblationForceAllConservativeV659      = "ablation_force_all_conservative_v659"
	metaTrackAblationDisableCriticalWidthV659      = "ablation_disable_critical_width_window_v659"
	metaTrackAblationIgnoreExactReadyRoutingV660   = "ablation_ignore_exact_ready_routing_v660"
	metaTrackAblationSingleRouteBatchConsensusV660 = "ablation_single_route_batch_consensus_v660"
	metaTrackAblationForceHomeExactV660            = "ablation_force_home_exact_v660"
	metaTrackAblationIgnoreCoaccessRoutingV661    = "ablation_ignore_coaccess_routing_v661"
	metaTrackAblationSingleReadyQueueV661         = "ablation_single_ready_queue_v661"
	metaTrackAblationOnDemandStateFetchV661       = "ablation_on_demand_state_fetch_v661"
	metaTrackAblationHashRoutingV662            = "ablation_hash_routing_v662"
)

func metaTrackForceAllConservativeEnabledV659(execution ExecutionPlugin) bool {
	if execution != nil && execution.ID() == metaTrackSingleConservativeExecutionID {
		return true
	}
	switch plugin := execution.(type) {
	case dualTrackExecution:
		return boolFromAny(plugin.config[metaTrackAblationForceAllConservativeV659])
	case *dualTrackExecution:
		return plugin != nil && boolFromAny(plugin.config[metaTrackAblationForceAllConservativeV659])
	default:
		return false
	}
}

// applyMetaTrackForceAllConservativeV659 is ablation-only. It keeps the same
// classification dependency DAG and StateReady tokens, but removes the
// Fast/Conservative split by forcing every classified transaction onto the
// conservative lane. The flag defaults off and the full MetaTrack profile never
// enables it.
func applyMetaTrackForceAllConservativeV659(execution ExecutionPlugin, result *BatchClassificationResult) {
	if result == nil || !metaTrackForceAllConservativeEnabledV659(execution) {
		return
	}
	for txID, decision := range result.Decisions {
		reasons := append([]string(nil), result.ReasonCodes[txID]...)
		reasons = append(reasons, metaTrackAblationForceAllConservativeV659)
		reasons = uniqueStrings(reasons)
		result.ReasonCodes[txID] = reasons
		decision.Track = "conservative"
		decision.Reason = metaTrackAblationForceAllConservativeV659
		result.Decisions[txID] = decision
	}
	result.FastDependencyBlockedCount = 0
	result.EffectiveFrontierTrackDemotionCount = 0
}

func metaTrackCriticalWidthDisabledV659(config map[string]any) bool {
	return boolFromAny(config[metaTrackAblationDisableCriticalWidthV659])
}

// metaTrackAblationReadyRankV660 keeps the exact same incremental planner and
// history state but neutralizes only the exact-version ready-rank criterion for
// the routing ablation.  Co-access locality, predicted remote cost, load,
// capacity, and all historical indices remain unchanged.
func metaTrackAblationReadyRankV660(ignoreExactReady bool, record WorkloadRecord, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	if ignoreExactReady {
		return 0
	}
	return metaTrackIncrementalReadyRankV651(record, shard, state)
}

// PushBatchSingleRouteBatchV660 is the clean consensus-window ablation.  It
// uses the same v6.5.6.8 signed window metadata and node-side validator as Full,
// but never joins two RouteBatches into one consensus window.
func (p *metaTrackCriticalWidthWindowPlannerV6568) PushBatchSingleRouteBatchV660(batch []metaTrackPreparedRecordV6568, blockLimit int) ([]metaTrackPreparedRecordV6568, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	if p == nil {
		return nil, fmt.Errorf("metatrack v6.6.0 single-route-batch planner is nil")
	}
	seq := batch[0].Record.RouteBatchSequence
	for _, prepared := range batch {
		if prepared.Record.RouteBatchSequence != seq {
			return nil, fmt.Errorf("metatrack v6.6.0 mixed route-batch sequence")
		}
		if prepared.Record.ConsensusExecutionRound <= 0 {
			return nil, fmt.Errorf("metatrack v6.6.0 missing signed global execution round")
		}
	}
	if len(p.records) == 0 {
		if err := p.resetWithBatch(batch); err != nil {
			return nil, err
		}
		if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
			return nil, fmt.Errorf("metatrack v6.6.0 one route-batch projection exceeds block limit")
		}
		return nil, nil
	}
	if seq != p.endBatch+1 {
		return nil, fmt.Errorf("metatrack v6.6.0 route batches are not contiguous")
	}
	closed := p.finalizeCurrent()
	p.sequence++
	if err := p.resetWithBatch(batch); err != nil {
		return nil, err
	}
	if !metaTrackCountsFitBlockV6568(p.shardCounts, blockLimit) {
		return nil, fmt.Errorf("metatrack v6.6.0 one route-batch projection exceeds block limit")
	}
	return closed, nil
}

// metaTrackAblationHomePublicationClassV660 preserves Version Liveness itself.
// Only a local-transient exact value that has a local value successor is forced
// through its persistent Home when local execution-domain handoff is disabled.
// Ordering-only successors and dead intermediates keep their original class.
func metaTrackAblationHomePublicationClassV660(forceHome bool, localValueSuccessorCount int, effectiveClass string) string {
	if forceHome && effectiveClass == metaTrackVersionClassLocalTransient && localValueSuccessorCount > 0 {
		return metaTrackVersionClassRemoteLive
	}
	return effectiveClass
}


// metaTrackAblationCoaccessLocalityV661 keeps the exact same incremental
// routing history and candidate set. The main co-access ablation neutralizes
// only the co-occurrence-locality criterion; exact-version readiness, predicted
// remote cost, load, and the workload-derived capacity remain unchanged.
func metaTrackAblationCoaccessLocalityV661(ignoreCoaccess bool, keys []string, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	if ignoreCoaccess {
		return 0
	}
	return metaTrackIncrementalPairLocalityV650(keys, shard, state)
}


// planHashBaselineAblationV662 replaces only MetaTrack's workload/dependency-aware
// execution-shard choice with the repository's deterministic hash baseline.  It
// intentionally stays inside metaTrackRouting so signed execution-routing,
// projection metadata, dependency closure, StateReady and Version Liveness keep
// exactly the same MetaTrack capability contract.
// MBE_MT_FLOW_ABL_V1: routing-only fixed hash replacement for the complete
// dependency-aware incremental execution-sharding mechanism. Keep MetaTrack's
// signed placement/version carrier, state ownership and PBFT untouched.
// Deterministic capacity is shared across RouteBatches, just as in Full.
func (p *metaTrackRouting) planHashBaselineAblationV662(input BatchRoutingInput) BatchRoutingPlan {
	p.incrementalMu.Lock()
	defer p.incrementalMu.Unlock()

	newRun := input.BatchIndex == 0 && p.incrementalV650 != nil && p.incrementalV650.Processed > 0
	if p.incrementalV650 == nil || !p.incrementalV650.compatible(input) || newRun {
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(input)
	}
	state := p.incrementalV650
	base := statelessHashRouting{basicPlugin: p.basicPlugin}.PlanBatch(input)
	base.StateStorageUnitCount = p.StateStorageUnitCount(input.ShardIDs)
	base.PlacementPolicy = "metatrack_deterministic_hash_ablation_v662"
	base.TransactionPolicy = "source_hash_or_state_hash_capacity_v662"
	base.IncrementalRoutingPolicy = "metatrack_capacity_hash_routing_ablation_v662"
	base.IncrementalExpectedTransactionCount = input.ExpectedTransactionCount
	base.IncrementalExecutionShardCapacity = state.Capacity
	base.IncrementalHistoryTransactionCountBefore = state.Processed
	base.IncrementalHistoryDigestBefore = state.HistoryDigest
	base.IncrementalBatchPartitionInvariant = true
	if policy, ok := p.config["control_policy"].(string); ok {
		base.ControlPolicy = policy
	}
	base.ShardLoadBefore = make(map[string]int, len(input.ShardIDs))
	for _, shard := range input.ShardIDs {
		base.ShardLoadBefore[shard] = state.ShardLoad[shard]
	}

	placementIndex := make(map[int]int, len(base.TransactionPlacements))
	for i, placement := range base.TransactionPlacements {
		placementIndex[placement.TxIndex] = i
	}
	base.RemoteAccessEstimate = 0
	for _, record := range input.Records {
		index, ok := placementIndex[record.Index]
		if !ok {
			continue
		}
		placement := base.TransactionPlacements[index]
		chosen := placement.ExecutionShard // Deterministic source/state hash only.
		preferred := -1
		for i, shard := range input.ShardIDs {
			if shard == chosen {
				preferred = i
				break
			}
		}
		if preferred < 0 && len(input.ShardIDs) > 0 {
			preferred = 0
			chosen = input.ShardIDs[0]
		}
		if state.Capacity > 0 && state.ShardLoad[chosen] >= state.Capacity {
			for offset := 1; offset < len(input.ShardIDs); offset++ {
				candidate := input.ShardIDs[(preferred+offset)%len(input.ShardIDs)]
				if state.ShardLoad[candidate] < state.Capacity {
					chosen = candidate
					base.IncrementalCapacityForcedChoiceCount++
					break
				}
			}
		}
		placement.ExecutionShard = chosen
		if chosen == input.ShardIDs[preferred] {
			placement.Reason = "deterministic_hash_execution_v662"
		} else {
			placement.Reason = "deterministic_hash_capacity_probe_v662"
		}
		placement.QueueLoadBefore = state.ShardLoad[chosen]
		ctx := metaTrackBuildRoutingRecordContextV669(p, record, input.Sharding, input.ShardIDs, state)
		reads, writes := metaTrackRemoteCountsFromContextV669(ctx, chosen)
		placement.PredictedRemoteReads = reads
		placement.PredictedRemoteWrites = writes
		placement.RemoteAccessCount = reads + writes
		base.RemoteAccessEstimate += reads + writes
		base.TransactionPlacements[index] = placement

		seenPred := map[uint64]bool{}
		for _, dep := range ctx.ExactDeps {
			base.IncrementalExactStateEdgeCount++
			if dep.Producer.Shard == chosen {
				base.IncrementalExactLocalStateEdgeCount++
			} else {
				base.IncrementalExactCrossShardStateEdgeCount++
			}
			if !seenPred[dep.Producer.Ordinal] {
				seenPred[dep.Producer.Ordinal] = true
				base.IncrementalExactPredecessorEdgeCount++
				if dep.Producer.Shard == chosen {
					base.IncrementalExactLocalPredecessorCount++
				} else {
					base.IncrementalExactCrossShardPredecessorCount++
				}
			}
		}
		state.ShardLoad[chosen]++
		state.Processed++
		for _, version := range record.StateVersions {
			if version.Key != "" && version.ProducedVersion > 0 {
				state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: version.Key, Version: version.ProducedVersion}] = metaTrackIncrementalProducerV650{
					Shard: chosen, Ordinal: record.RoutingOrdinal,
				}
			}
		}
		state.HistoryDigest = stableDigest(struct {
			Previous       string `json:"previous"`
			Ordinal        uint64 `json:"ordinal"`
			LogicalID      string `json:"logical_id"`
			ExecutionShard string `json:"execution_shard"`
		}{state.HistoryDigest, record.RoutingOrdinal, placement.LogicalID, chosen})
	}
	base.ShardLoadAfter = make(map[string]int, len(input.ShardIDs))
	for _, shard := range input.ShardIDs {
		base.ShardLoadAfter[shard] = state.ShardLoad[shard]
	}
	base.IncrementalExecutionShardSwitchCount = base.IncrementalExactCrossShardPredecessorCount
	base.PlacementCapacity = state.Capacity
	base.PlacementMinBudget = 1
	base.PlacementBudget = len(base.StatePlacements)
	base.IncrementalHistoryTransactionCountAfter = state.Processed
	base.IncrementalHistoryDigestAfter = state.HistoryDigest
	base.RoutingOverhead = base.RemoteAccessEstimate + base.IncrementalExactStateEdgeCount
	if !input.DeferFinalSeal {
		if metaTrackDeclaredAccessFrontierPolicyEnabled(p.config) {
			applyMetaTrackDeclaredAccessFrontierV2(&base, input.Records)
		}
		base.PlanDigest = routingPlanDigest(base)
	}
	return base
}
