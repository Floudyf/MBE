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
)

func metaTrackForceAllConservativeEnabledV659(execution ExecutionPlugin) bool {
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
