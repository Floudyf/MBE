package v5

// MT_LOADISO_V1: standalone online candidate, NOT a modification of Full or Bound.
// The script copies the proven Full planner below and changes ONLY its load
// ranking input. Physical capacity, actual producer provenance, exact edges,
// coaccess and signed plan continue to use the actual committed routing state.
import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

const mtLoadIsoPolicyV1 = "metatrack_shadow_loadiso_v1"

type mtLoadIsoStateV1 struct {
	mu               sync.Mutex
	shadow           *metaTrackRouting
	batches          int
	interventions    int
	fullCalls        int
	shadowNS         int64
	actualNS         int64
	aligned          bool
	reuseFullBatches int
	donorCalls       int
	replayCalls      int
}

// mtLoadIsoDonorV1 uses the SAME current RouteBatch, never a future RouteBatch.
// Per-record snapshots are reconstructed from Full's placement sequence, so
// the load seen by record i excludes placement of record i and later records.
func mtLoadIsoDonorV1(shadow *metaTrackRouting, in BatchRoutingInput) ([]map[string]int, BatchRoutingPlan) {
	before := map[string]int{}
	for _, sh := range in.ShardIDs {
		before[sh] = 0
	}
	if shadow.incrementalV650 != nil {
		for _, sh := range in.ShardIDs {
			before[sh] = shadow.incrementalV650.ShardLoad[sh]
		}
	}
	plan := shadow.planIncrementalExactContinuityV650(in)
	if len(plan.TransactionPlacements) != len(in.Records) {
		panic("MT_LOADISO_STOP: shadow placement count")
	}
	snapshots := make([]map[string]int, len(in.Records))
	for i, placement := range plan.TransactionPlacements {
		if placement.TxIndex != in.Records[i].Index || placement.LogicalID != firstNonEmpty(in.Records[i].LogicalID, fmt.Sprintf("tx-%d", in.Records[i].Index)) {
			panic("MT_LOADISO_STOP: shadow placement order")
		}
		snap := make(map[string]int, len(before))
		for sh, load := range before {
			snap[sh] = load
		}
		snapshots[i] = snap
		if _, ok := before[placement.ExecutionShard]; !ok {
			panic("MT_LOADISO_STOP: shadow selected unknown shard")
		}
		before[placement.ExecutionShard]++
	}
	for _, sh := range in.ShardIDs {
		if before[sh] != shadow.incrementalV650.ShardLoad[sh] {
			panic("MT_LOADISO_STOP: shadow load reconstruction mismatch")
		}
	}
	return snapshots, plan
}

// mtLoadIsoSelectBoundAndFullV2 retains the already-computed Full fork.
// It makes the old micro10 comparison once, with the same candidates and guard.
func mtLoadIsoSelectBoundAndFullV2(p *metaTrackRouting, in BatchRoutingInput) (mtBoundTrialV1, mtBoundTrialV1) {
	base := p.incrementalV650
	full := mtBoundForkV1(p, base, in, "full")
	selected := full
	if len(in.Records) == 100 && len(in.ShardIDs) == 2 && in.ExpectedTransactionCount > 0 {
		for _, mode := range []string{"exact", "remote"} {
			alt := mtBoundForkV1(p, base, in, mode)
			if !mtBoundBatchSafeV1(alt, full) {
				continue
			}
			if mtBoundPlanSwitchesV1(alt.plan, full.plan) > 10 {
				continue
			}
			moves, pairs := mtBoundChangesV1(alt, full, in)
			if moves > 50 || pairs > 100 || mtBoundSkewV1(alt.state) > mtBoundSkewV1(full.state) {
				continue
			}
			if selected.mode == "full" || mtBoundLessV1(alt, selected) {
				selected = alt
			}
		}
	}
	return selected, full
}

// MT_LOADCOW_V1: reuse the production Bounded COW V3 engine in LoadIso.
// This is selection-only: all trials roll back and the caller commits exactly
// one result. In the diverged case Full must NOT be committed before the
// real-capacity Shadow-load replay. The deep-fork oracle above stays unchanged.
func mtLoadIsoSelectBoundCOWV1(p *metaTrackRouting, in BatchRoutingInput) (mtBoundSavedTrialV2, mtBoundSavedTrialV2) {
	base := p.incrementalV650
	touched := mtBoundTouchedForBatchV2(in)
	full := mtBoundRunRollbackV3(p, base, in, "full", touched)
	selected := full
	// MT_LOADP1_V1: batch-local lazy immutable context shared by the two prune passes.
	// Full and candidate planners are untouched; exact producers always re-resolve.
	staticCache := make([]*mtLoadP1StaticV1, len(in.Records))
	if len(in.Records) == 100 && len(in.ShardIDs) == 2 && in.ExpectedTransactionCount > 0 {
		for _, mode := range []string{"exact", "remote"} {
			// MT_LOADP0_V1: use the existing Bound monotone impossibility gate.
			// If the gate cannot prove rejection it returns false, and the
			// original full candidate evaluation and all acceptance guards run.
			if mtLoadP2PruneImpossibleV1(p, base, in, mode, full.trial, selected.trial, touched, staticCache) {
				continue
			}
			alt := mtBoundRunRollbackV3(p, base, in, mode, touched)
			if !mtBoundBatchSafeV1(alt.trial, full.trial) {
				continue
			}
			if mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) > 10 {
				continue
			}
			moves, pairs := mtBoundDeltaChangesV2(alt.after, full.after)
			if moves > 50 || pairs > 100 || alt.trial.skew > full.trial.skew {
				continue
			}
			if selected.trial.mode == "full" || mtBoundLessV1(alt.trial, selected.trial) {
				selected = alt
			}
		}
	}
	return selected, full
}

// Keep the original helper as the test's legacy reference entry point.
func mtLoadIsoSelectBoundV1(p *metaTrackRouting, in BatchRoutingInput) mtBoundTrialV1 {
	chosen, _ := mtLoadIsoSelectBoundAndFullV2(p, in)
	return chosen
}

// Same-history reuse is exact, not inferred from a truncated hash:
// when shadow and live refer to the same committed state object, one Full
// counterfactual plan serves both. Trials use batch-touched COW and roll back.
// Once a micro10 alternative is accepted, shadow and live histories remain
// separate and the original shadow-load replay semantics are preserved.
func (p *metaTrackRouting) planLoadIsoV1(in BatchRoutingInput) BatchRoutingPlan {
	p.loadIsoOnceV1.Do(func() { p.loadIsoV1 = &mtLoadIsoStateV1{} })
	iso := p.loadIsoV1
	iso.mu.Lock()
	defer iso.mu.Unlock()
	reset := in.BatchIndex == 0 && iso.batches > 0
	if iso.shadow == nil || reset {
		iso.shadow = mtShadowFullFromBoundV1(p)
		delete(iso.shadow.basicPlugin.config, "load_iso_shadow_v1")
		iso.batches, iso.interventions, iso.fullCalls = 0, 0, 0
		iso.shadowNS, iso.actualNS = 0, 0
		iso.reuseFullBatches, iso.donorCalls, iso.replayCalls = 0, 0, 0
		iso.aligned = true
		p.incrementalV650 = nil
	}
	if p.incrementalV650 == nil || !p.incrementalV650.compatible(in) {
		if iso.batches != 0 {
			panic("MT_LOADISO_STOP: unexpected live history reset")
		}
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
	}
	if iso.batches == 0 {
		// The two histories are deterministically identical at genesis.
		iso.shadow.incrementalV650 = p.incrementalV650
	}
	if len(in.ShardIDs) != 2 || in.ExpectedTransactionCount <= 0 || len(in.Records) != 100 {
		panic("MT_LOADISO_STOP: requires 100 records, 2 shards and known transaction count")
	}
	alignedBefore := iso.aligned
	if alignedBefore && p.incrementalV650 != iso.shadow.incrementalV650 {
		panic("MT_LOADISO_STOP: aligned histories no longer share one state")
	}
	var chosen mtBoundTrialV1
	var shadowPlan BatchRoutingPlan
	var result BatchRoutingPlan
	var shadowNS, chooseNS, replayNS int64
	if alignedBefore {
		// On a shared prefix the Full donor load and the physical Full load
		// are identical at EVERY record. The old LoadIso re-plan would therefore
		// produce exactly the same result as the micro10 Full fork.
		started := time.Now()
		selected, full := mtLoadIsoSelectBoundCOWV1(p, in)
		chooseNS = time.Since(started).Nanoseconds()
		chosen = selected.trial
		shadowPlan = full.trial.plan
		if chosen.mode == "full" {
			mtBoundCOWApplyV3(p.incrementalV650, full.after)
			iso.shadow.incrementalV650 = p.incrementalV650
			result = full.trial.plan
			iso.reuseFullBatches++
		} else {
			// Allocate a separate physical history ONCE at first divergence.
			// Shadow commits the Full delta; live commits the selected delta.
			shadowHistory := mtBoundCloneStateV1(p.incrementalV650)
			mtBoundCOWApplyV3(shadowHistory, full.after)
			mtBoundCOWApplyV3(p.incrementalV650, selected.after)
			iso.shadow.incrementalV650 = shadowHistory
			result = chosen.plan
			for i := range result.TransactionPlacements {
				result.TransactionPlacements[i].Reason += ":bounded_batch_" + chosen.mode
			}
			iso.interventions++
			iso.aligned = false
		}
	} else {
		// Preserve donor/replay semantics after divergence; only trials use COW.
		donorAt := time.Now()
		var snapshots []map[string]int
		snapshots, shadowPlan = mtLoadIsoDonorV1(iso.shadow, in)
		shadowNS = time.Since(donorAt).Nanoseconds()
		iso.donorCalls++
		selectAt := time.Now()
		selected, _ := mtLoadIsoSelectBoundCOWV1(p, in)
		chooseNS = time.Since(selectAt).Nanoseconds()
		chosen = selected.trial
		if chosen.mode != "full" {
			mtBoundCOWApplyV3(p.incrementalV650, selected.after)
			result = chosen.plan
			for i := range result.TransactionPlacements {
				result.TransactionPlacements[i].Reason += ":bounded_batch_" + chosen.mode
			}
			iso.interventions++
		} else {
			replayAt := time.Now()
			result = p.planLoadIsoExactV1(in, snapshots)
			replayNS = time.Since(replayAt).Nanoseconds()
			iso.fullCalls++
			iso.replayCalls++
		}
	}
	accepted := chosen.mode != "full"
	result.IncrementalRoutingPolicy = mtLoadIsoPolicyV1
	result.TransactionPolicy = "micro10_intervention_else_full_shadow_load_rank_real_capacity_v1"
	if !in.DeferFinalSeal {
		result.PlanDigest = routingPlanDigest(result)
	}
	if p.incrementalV650 == nil || iso.shadow.incrementalV650 == nil ||
		p.incrementalV650.Processed != iso.shadow.incrementalV650.Processed ||
		p.incrementalV650.Capacity != iso.shadow.incrementalV650.Capacity ||
		shadowPlan.IncrementalHistoryTransactionCountAfter != result.IncrementalHistoryTransactionCountAfter {
		panic("MT_LOADISO_STOP: history length/capacity mismatch")
	}
	sum := 0
	for _, sh := range in.ShardIDs {
		v := p.incrementalV650.ShardLoad[sh]
		if v < 0 || (p.incrementalV650.Capacity > 0 && v > p.incrementalV650.Capacity) {
			panic("MT_LOADISO_STOP: physical shard exceeds capacity")
		}
		sum += v
	}
	if sum != p.incrementalV650.Processed {
		panic("MT_LOADISO_STOP: physical load not conserved")
	}
	iso.batches++
	iso.shadowNS += shadowNS
	iso.actualNS += chooseNS + replayNS
	drift := mtShadowPlacementDriftV1(result, shadowPlan)
	if drift < 0 {
		panic("MT_LOADISO_STOP: transaction order mismatch")
	}
	log.Printf("[MT_LOADISO_V1] batch=%d tx=%d mode=%s adopted=%t cumulative_adopted=%d live_exact=%d live_remote=%d shadow_exact=%d shadow_remote=%d drift_now=%d real_skew=%d shadow_skew=%d shadow_us=%d live_us=%d shadow_history=%s live_history=%s aligned_before=%t reused_full_batches=%d donor_calls=%d replay_calls=%d choose_us=%d replay_us=%d decision_path=%s",
		in.BatchIndex, len(in.Records), chosen.mode, accepted, iso.interventions,
		result.IncrementalExactCrossShardStateEdgeCount, result.RemoteAccessEstimate,
		shadowPlan.IncrementalExactCrossShardStateEdgeCount, shadowPlan.RemoteAccessEstimate,
		drift, mtBoundSkewV1(p.incrementalV650), mtBoundSkewV1(iso.shadow.incrementalV650),
		shadowNS/1000, (chooseNS+replayNS)/1000,
		iso.shadow.incrementalV650.HistoryDigest, p.incrementalV650.HistoryDigest,
		alignedBefore, iso.reuseFullBatches, iso.donorCalls, iso.replayCalls,
		chooseNS/1000, replayNS/1000,
		map[bool]string{true: "shared_history_reuse", false: "original_diverged_path"}[alignedBefore])
	return result
}

// -- COPIED_FROZEN_FULL_PLANNER_BELOW --

func (p *metaTrackRouting) planLoadIsoExactV1(input BatchRoutingInput, shadowLoads []map[string]int) BatchRoutingPlan {
	p.incrementalMu.Lock()
	defer p.incrementalMu.Unlock()

	// A planner instance normally belongs to one client run, but reset on a new
	// BatchIndex=0 as a defensive run-boundary guard so cumulative routing
	// history can never leak into a later experiment that reuses the plugin.
	newRun := input.BatchIndex == 0 && p.incrementalV650 != nil && p.incrementalV650.Processed > 0
	if p.incrementalV650 == nil || !p.incrementalV650.compatible(input) || newRun {
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(input)
	}
	state := p.incrementalV650
	// MBE_MT_NOCAP_V1: experimental method only. Zero disables the
	// arbitrary N/shards cumulative admission ceiling; it does NOT remove
	// structural load ranking, worker limits, or per-block PBFT capacity.
	if boolFromAny(p.config["experimental_no_global_capacity_v1"]) {
		state.Capacity = 0
	}
	ignoreCoaccessV661 := boolFromAny(p.config[metaTrackAblationIgnoreCoaccessRoutingV661])
	noExactV1 := boolFromAny(p.config["ablation_no_exact_version_routing_v1"]) // MBE_MT_NOEXACT_ABL_V1
	selector := metaTrackIncrementalRankCandidatesV651
	if noExactV1 {
		selector = metaTrackRankWithoutExactV1
	}
	// Only the experimental fork sets this key. Full's selector is unchanged.
	if mode, ok := p.config["bound_selector_v1"].(string); ok && (mode == "exact" || mode == "remote") {
		selector = func(items []metaTrackIncrementalCandidateV650) []metaTrackIncrementalCandidateV650 {
			return mtBoundAlternativeRankV1(items, mode)
		}
	}
	// MBE_MT_ONLINE_V1: only candidate ordering changes. No new signed
	// state readiness token and no modification to exact dependency metadata.
	onlineConfiguredV1 := boolFromAny(p.config["online_state_ready_routing_v1"])
	feedbackColdstartV1 := onlineConfiguredV1 && boolFromAny(p.config["online_feedback_coldstart_v2"])
	capacityGuardV1 := onlineConfiguredV1 && boolFromAny(p.config["online_capacity_guard_v2"])
	onlineUsedV1 := false  // MBE_MT_ONLINE_FIX_V1 reset per candidate selection
	onlineProxyV1 := false // MBE_MT_ONLINE_DUAL_V1: reset per candidate selection
	if onlineConfiguredV1 {
		observed := input.OnlineFeedback
		nowMS := time.Now().UnixMilli()
		selector = func(candidates []metaTrackIncrementalCandidateV650) []metaTrackIncrementalCandidateV650 {
			var ranked []metaTrackIncrementalCandidateV650
			var valid bool
			var proxy bool
			if feedbackColdstartV1 {
				ranked, valid, proxy = mtOnlineRankCandidatesColdstartV1(candidates, observed, nowMS)
			} else {
				ranked, valid = mtOnlineRankCandidatesV1(candidates, observed, nowMS)
			}
			if valid {
				onlineUsedV1 = true
				onlineProxyV1 = proxy
			} else {
				ranked = metaTrackIncrementalRankCandidatesV651(candidates)
				onlineProxyV1 = false
			}
			// V2: keep the measured/proxy completion ranking intact. An
			// independent, guarded choice is made after the candidate sort.
			return ranked
		}
	}
	plan := BatchRoutingPlan{
		BatchIndex:                               input.BatchIndex,
		ShardingPluginID:                         shardingPluginID(input.Sharding),
		StateStorageUnitCount:                    p.StateStorageUnitCount(input.ShardIDs),
		PlacementPolicy:                          "incremental_rank_balanced_history_v651",
		TransactionPolicy:                        "ready_remote_coaccess_load_minimax_rank_v651",
		PlacementMu:                              "1.0",
		ShardLoadBefore:                          map[string]int{},
		ShardLoadAfter:                           map[string]int{},
		IncrementalRoutingPolicy:                 metaTrackIncrementalExactContinuityPolicyV650,
		IncrementalExpectedTransactionCount:      input.ExpectedTransactionCount,
		IncrementalExecutionShardCapacity:        state.Capacity,
		IncrementalHistoryTransactionCountBefore: state.Processed,
		IncrementalHistoryDigestBefore:           state.HistoryDigest,
		IncrementalBatchPartitionInvariant:       true,
	}
	if noExactV1 {
		plan.TransactionPolicy = "remote_coaccess_load_minimax_noexact_v1"
	}
	if onlineConfiguredV1 {
		plan.IncrementalRoutingPolicy = mtOnlinePolicyV1
		plan.TransactionPolicy = "online_joint_ready_with_explicit_legacy_fallback_v1"
	}
	for _, shard := range input.ShardIDs {
		plan.ShardLoadBefore[shard] = state.ShardLoad[shard]
		plan.ShardLoadAfter[shard] = state.ShardLoad[shard]
	}
	if len(input.ShardIDs) == 0 {
		return plan
	}

	batchFrequency := map[string]*StateFrequencyRow{}
	batchCoaccess := map[string]int{}
	batchKeys := map[string]bool{}
	routingEpoch := uint64(maxInt(0, intValue(p.config["routing_epoch"])))

	// MBE_MT_ONLINE_PROTECT_V1: Full and protected Online use the same
	// incremental history; only committed placement updates it.
	for recordIndex, record := range input.Records {
		if recordIndex >= len(shadowLoads) || shadowLoads[recordIndex] == nil {
			panic("MT_LOADISO_STOP: missing donor prefix load")
		}
		routingCtx := metaTrackBuildRoutingRecordContextV669(p, record, input.Sharding, input.ShardIDs, state)
		accesses := routingCtx.Accesses
		keys := routingCtx.Keys
		logicalID := firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index))
		for _, access := range accesses {
			if access.Key == "" {
				continue
			}
			plan.AccessMatrix = append(plan.AccessMatrix, AccessMatrixRow{LogicalID: logicalID, TxIndex: record.Index, Key: access.Key, Mode: access.Mode})
			row := batchFrequency[access.Key]
			if row == nil {
				row = &StateFrequencyRow{Key: access.Key}
				batchFrequency[access.Key] = row
			}
			row.Frequency++
			if isReadMode(access.Mode) {
				row.ReadCount++
			}
			if isWriteMode(access.Mode) {
				row.WriteCount++
			}
			batchKeys[access.Key] = true
		}
		for _, pair := range routingCtx.Pairs {
			batchCoaccess[pair]++
		}

		candidates := make([]metaTrackIncrementalCandidateV650, 0, len(input.ShardIDs))
		anyAdmissible := false
		for _, shard := range input.ShardIDs {
			admissible := state.Capacity <= 0 || state.ShardLoad[shard] < state.Capacity
			if admissible {
				anyAdmissible = true
			}
			reads, writes := metaTrackRemoteCountsFromContextV669(routingCtx, shard)
			candidates = append(candidates, metaTrackIncrementalCandidateV650{
				Shard:            shard,
				Admissible:       admissible,
				ReadyRank:        metaTrackReadyRankFromContextV669(routingCtx, shard),
				ExactCross:       metaTrackExactCrossFromContextV669(routingCtx, shard),
				CoaccessLocality: metaTrackCoaccessFromContextV669(ignoreCoaccessV661, routingCtx, shard, state),
				RemoteReads:      reads,
				RemoteWrites:     writes,
				RemoteCost:       reads + writes,
				Load:             shadowLoads[recordIndex][shard],
			})
		}
		filtered := make([]metaTrackIncrementalCandidateV650, 0, len(candidates))
		if anyAdmissible {
			for _, candidate := range candidates {
				if candidate.Admissible {
					filtered = append(filtered, candidate)
				}
			}
		} else {
			filtered = append(filtered, candidates...)
		}
		// Capture the original five-criterion decision before the online
		// selector mutates ranking fields, for true same-input counterfactual.
		legacyShardV1 := ""
		if onlineConfiguredV1 {
			legacy := metaTrackIncrementalRankCandidatesV651(filtered)
			sort.Slice(legacy, func(i, j int) bool { return metaTrackIncrementalCandidateLessV650(legacy[i], legacy[j]) })
			legacyShardV1 = legacy[0].Shard
		}
		onlineUsedV1 = false
		filtered = selector(filtered)
		// The unconstrained diagnostic calls selector again below; preserve
		// the decision made with the actual capacity filter.
		selectedOnlineV1 := onlineUsedV1
		selectedProxyV1 := onlineProxyV1
		sort.Slice(filtered, func(i, j int) bool { return metaTrackIncrementalCandidateLessV650(filtered[i], filtered[j]) })
		selected := filtered[0]
		protectedDecisionV1 := ""
		if capacityGuardV1 && selectedOnlineV1 {
			if feedbackColdstartV1 {
				// MBE_MT_ONLINE_PROTECT_V1: joint Online only. No V3 post-score
				// capacity override or same-batch override can bypass this gate.
				// The Full candidate is from the SAME incremental prefix,
				// evaluated before any Online score mutation.
				selected, protectedDecisionV1 = mtOnlineProtectedChoiceV1(filtered, legacyShardV1, routingCtx.ExactDeps, input.OnlineFeedback, time.Now().UnixMilli())
			} else {
				// Preserve the independently selectable balance-only method.
				selected = mtOnlineBalanceChooseV2(filtered, len(input.Records), input.OnlineFeedback)
			}
		}
		selectedChangedV1 := selectedOnlineV1 && selected.Shard != legacyShardV1

		// Capacity remains the only hard constraint. To observe when it changes
		// the rank-balanced answer, compute the same ordinal selector without the
		// capacity filter. No empirical threshold is used.
		if anyAdmissible {
			unconstrained := selector(candidates)
			sort.Slice(unconstrained, func(i, j int) bool { return metaTrackIncrementalCandidateLessV650(unconstrained[i], unconstrained[j]) })
			unconstrainedChoiceV2 := unconstrained[0]
			if capacityGuardV1 && onlineUsedV1 {
				unconstrainedChoiceV2 = mtOnlineBalanceChooseV2(unconstrained, len(input.Records), input.OnlineFeedback)
			}
			if !unconstrainedChoiceV2.Admissible && unconstrainedChoiceV2.Shard != selected.Shard {
				plan.IncrementalCapacityForcedChoiceCount++
			}
		}
		// Legacy counters remain as compatibility observability, but no criterion
		// has absolute priority in v6.5.1. Each counter records a uniquely best
		// criterion that is also chosen by the rank-balanced selector.
		if !noExactV1 && metaTrackIncrementalUniqueBestV651(filtered, selected, func(left, right metaTrackIncrementalCandidateV650) bool { return left.ExactCross < right.ExactCross }) {
			plan.IncrementalExactFirstChoiceCount++
		}
		if metaTrackIncrementalUniqueBestV651(filtered, selected, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.CoaccessLocality > right.CoaccessLocality
		}) {
			plan.IncrementalCoaccessTiebreakCount++
		}
		if metaTrackIncrementalUniqueBestV651(filtered, selected, func(left, right metaTrackIncrementalCandidateV650) bool { return left.RemoteCost < right.RemoteCost }) {
			plan.IncrementalRemoteTiebreakCount++
		}
		if metaTrackIncrementalUniqueBestV651(filtered, selected, func(left, right metaTrackIncrementalCandidateV650) bool { return left.Load < right.Load }) {
			plan.IncrementalLoadTiebreakCount++
		}

		if onlineConfiguredV1 && selectedOnlineV1 {
			plan.TransactionPolicy = "online_joint_readiness_observed_v1"
		}
		remoteReads, remoteWrites := selected.RemoteReads, selected.RemoteWrites
		plan.RemoteAccessEstimate += remoteReads + remoteWrites
		homeShard := firstNonEmpty(record.SourceShard, shardFor(input.Sharding, record.StateKeys, input.ShardIDs))
		targetShard := record.TargetShard
		if targetShard == "" && record.CrossShard && len(input.ShardIDs) > 1 {
			targetShard = nextShardAfter(homeShard, input.ShardIDs)
		}
		plan.TransactionPlacements = append(plan.TransactionPlacements, TransactionPlacement{
			LogicalID:             logicalID,
			TxIndex:               record.Index,
			SenderGroupID:         metaTrackSenderGroupID(record),
			RoutingEpoch:          routingEpoch,
			HomeShard:             homeShard,
			ExecutionShard:        selected.Shard,
			TargetShard:           targetShard,
			CoaccessGroup:         strings.Join(keys, "+"),
			Reason:                mtOnlineProtectedReasonV1(mtOnlineDualReasonV1(mtOnlinePlacementReasonFixedV1(onlineConfiguredV1, selectedOnlineV1, selectedChangedV1, selected), selectedProxyV1, capacityGuardV1), protectedDecisionV1),
			PredictedRemoteReads:  remoteReads,
			PredictedRemoteWrites: remoteWrites,
			RemoteAccessCount:     remoteReads + remoteWrites,
			MajorityCoverage:      selected.CoaccessLocality,
			MajorityTie:           false,
			QueueLoadBefore:       state.ShardLoad[selected.Shard],
		})

		// Exact-version edge metrics are computed against the same pre-decision
		// producer index used by routing, so they describe the actual choice.
		seenPred := map[uint64]bool{}
		for _, dep := range routingCtx.ExactDeps {
			producer := dep.Producer
			plan.IncrementalExactStateEdgeCount++
			if producer.Shard == selected.Shard {
				plan.IncrementalExactLocalStateEdgeCount++
			} else {
				plan.IncrementalExactCrossShardStateEdgeCount++
			}
			if !seenPred[producer.Ordinal] {
				seenPred[producer.Ordinal] = true
				plan.IncrementalExactPredecessorEdgeCount++
				if producer.Shard == selected.Shard {
					plan.IncrementalExactLocalPredecessorCount++
				} else {
					plan.IncrementalExactCrossShardPredecessorCount++
				}
			}
		}

		state.ShardLoad[selected.Shard]++
		state.Processed++
		for _, key := range keys {
			if state.KeyShardSupport[key] == nil {
				state.KeyShardSupport[key] = map[string]int{}
			}
			state.KeyShardSupport[key][selected.Shard]++
		}
		for _, pair := range routingCtx.Pairs {
			if state.PairShardSupport[pair] == nil {
				state.PairShardSupport[pair] = map[string]int{}
			}
			state.PairShardSupport[pair][selected.Shard]++
			plan.IncrementalCoaccessPairUpdateCount++
		}
		for _, dep := range record.StateVersions {
			if dep.Key == "" || dep.ProducedVersion == 0 {
				continue
			}
			state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.ProducedVersion}] = metaTrackIncrementalProducerV650{Shard: selected.Shard, Ordinal: record.RoutingOrdinal, FinishRank: selected.ReadyRank + 1}
		}
		state.HistoryDigest = stableDigest(struct {
			Previous       string `json:"previous"`
			Ordinal        uint64 `json:"ordinal"`
			LogicalID      string `json:"logical_id"`
			ExecutionShard string `json:"execution_shard"`
		}{state.HistoryDigest, record.RoutingOrdinal, logicalID, selected.Shard})
	}

	for _, row := range batchFrequency {
		plan.StateFrequency = append(plan.StateFrequency, *row)
		plan.PlacementTotalFrequency += row.Frequency
		if row.Frequency > plan.PlacementMaxFrequency {
			plan.PlacementMaxFrequency = row.Frequency
		}
	}
	sort.Slice(plan.StateFrequency, func(i, j int) bool { return plan.StateFrequency[i].Key < plan.StateFrequency[j].Key })
	for pair, weight := range batchCoaccess {
		left, right, _ := strings.Cut(pair, "\x00")
		plan.CoaccessEdges = append(plan.CoaccessEdges, CoaccessEdge{LeftKey: left, RightKey: right, Weight: weight})
	}
	sort.Slice(plan.CoaccessEdges, func(i, j int) bool {
		if plan.CoaccessEdges[i].LeftKey != plan.CoaccessEdges[j].LeftKey {
			return plan.CoaccessEdges[i].LeftKey < plan.CoaccessEdges[j].LeftKey
		}
		return plan.CoaccessEdges[i].RightKey < plan.CoaccessEdges[j].RightKey
	})
	for key := range batchKeys {
		home := p.LogicalStateHome(key, input.Sharding, input.ShardIDs)
		best := ""
		bestSupport := -1
		for _, shard := range input.ShardIDs {
			support := state.KeyShardSupport[key][shard]
			if best == "" || support > bestSupport || (support == bestSupport && shard < best) {
				best, bestSupport = shard, support
			}
		}
		placement := StatePlacement{Key: key, HomeStateUnit: home.StateUnitID, HomeShard: home.ServingShard, ExecutionShard: best, Frequency: batchFrequency[key].Frequency, Reason: "incremental_rank_only_historical_key_affinity"}
		plan.StatePlacements = append(plan.StatePlacements, placement)
	}
	sort.Slice(plan.StatePlacements, func(i, j int) bool { return plan.StatePlacements[i].Key < plan.StatePlacements[j].Key })
	for _, shard := range input.ShardIDs {
		plan.ShardLoadAfter[shard] = state.ShardLoad[shard]
	}
	plan.PlacementCapacity = state.Capacity
	plan.PlacementMinBudget = 1
	plan.PlacementBudget = len(plan.StatePlacements)
	plan.IncrementalExecutionShardSwitchCount = plan.IncrementalExactCrossShardPredecessorCount
	plan.RoutingOverhead = plan.RemoteAccessEstimate + plan.IncrementalExactStateEdgeCount + plan.IncrementalCoaccessPairUpdateCount
	plan.IncrementalHistoryTransactionCountAfter = state.Processed
	plan.IncrementalHistoryDigestAfter = state.HistoryDigest
	if !input.DeferFinalSeal {
		if metaTrackDeclaredAccessFrontierPolicyEnabled(p.config) {
			applyMetaTrackDeclaredAccessFrontierV2(&plan, input.Records)
		}
		plan.PlanDigest = routingPlanDigest(plan)
	}
	return plan
}
