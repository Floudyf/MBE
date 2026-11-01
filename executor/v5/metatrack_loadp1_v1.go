package v5

// MT_LOADP1_V1: LoadIso-only batch-local static context sharing for its two
// already existing P0 prune probes. The Full/Bound/COW algorithms are untouched.
// The first probe calls the original V669 constructor; later probes reuse
// ONLY normalized accesses, sorted keys/pairs and static home-counts.
// Exact-version predecessor resolution always reads the *current* working
// producer index, preserving intra-batch causality.
import (
	"sort"
	"strings"
)

type mtLoadP1StaticV1 struct {
	ctx           metaTrackRoutingRecordContextV669
	homes         map[string]string
	exactEligible map[string]bool
}

func mtLoadP1ContextV1(p *metaTrackRouting, record WorkloadRecord, sharding ShardingPlugin, shardIDs []string, state *metaTrackIncrementalRoutingStateV650, slot **mtLoadP1StaticV1) metaTrackRoutingRecordContextV669 {
	if *slot == nil {
		// First access intentionally uses the original implementation; no
		// static parsing or Home resolution is reimplemented.
		ctx := metaTrackBuildRoutingRecordContextV669(p, record, sharding, shardIDs, state)
		cache := &mtLoadP1StaticV1{
			ctx:           ctx,
			homes:         make(map[string]string, len(ctx.Keys)),
			exactEligible: make(map[string]bool, len(ctx.Keys)),
		}
		cache.ctx.ExactDeps = nil // Dynamic! Never cache producer provenance.
		// Original V669 takes the FIRST normalized access of each key.
		for _, a := range ctx.Accesses {
			if a.Key == "" {
				continue
			}
			if _, ok := cache.exactEligible[a.Key]; !ok {
				cache.exactEligible[a.Key] = requiresExactStateValue(a)
			}
		}
		for _, key := range ctx.Keys {
			cache.homes[key] = state.HomeShardByKey[key]
		}
		*slot = cache
		return ctx
	}
	c := *slot
	// HomeShardByKey is a COW-managed side effect in V669. Re-create the exact
	// entries at the same record boundary; if unexpected home history exists,
	// fail open to the original constructor, never silently trust stale cache.
	if state.HomeShardByKey == nil {
		state.HomeShardByKey = map[string]string{}
	}
	for _, key := range c.ctx.Keys {
		if old, ok := state.HomeShardByKey[key]; ok && old != c.homes[key] {
			return metaTrackBuildRoutingRecordContextV669(p, record, sharding, shardIDs, state)
		}
		state.HomeShardByKey[key] = c.homes[key]
	}
	ctx := c.ctx
	ctx.ExactDeps = nil
	for _, dep := range record.StateVersions {
		if dep.Key == "" || dep.RequiredVersion == 0 || !c.exactEligible[dep.Key] {
			continue
		}
		producer, ok := state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.RequiredVersion}]
		if !ok || strings.TrimSpace(producer.Shard) == "" {
			continue
		}
		ctx.ExactDeps = append(ctx.ExactDeps, metaTrackResolvedExactDependencyV669{Key: dep.Key, Producer: producer})
	}
	return ctx
}

// mtLoadP1PruneImpossibleV1 is the exact Bound P0 rejection predicate, but it
// reuses static V669 record context between Exact/Remote pruning probes.
// Unlike Full or full candidate trials, only this LoadIso-specific read-only
// preflight path opts in; the candidate decision and guard remain unchanged.

// mtLoadP2ProbeV1 retains the P1 prefix walk and COW rollback, but returns
// its computed final metrics. It does not build or commit a plan.
type mtLoadP2ProbeResultV1 struct {
	monotoneReject bool
	complete       bool
	switches       int
	remote         int
	exact          int
	ready          int
	peak           int
}

// This is the original P1 signature and original P1 monotone rejection
// semantics. P1 tests continue to compare it with the unchanged Bound oracle.
func mtLoadP1PruneImpossibleV1(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string, full mtBoundTrialV1, touched mtBoundTouchedV2, cache []*mtLoadP1StaticV1) bool {
	return mtLoadP2ProbeV1(p, base, input, mode, full, touched, cache).monotoneReject
}

// P2 stops the second (full) candidate execution only after an exhaustive
// prefix walk proves the completed candidate cannot win. All real winners
// continue through the original planner and all exact safety checks.
// Only complete deterministic probes may prove a strict-gain impossibility.
// A surviving candidate with the same placements or with no strict improvement
// in any of the actual Bound safety metrics cannot be adopted.
func mtLoadP2NoStrictGainV1(probe mtLoadP2ProbeResultV1, full mtBoundTrialV1) bool {
	if !probe.complete {
		return false
	}
	if probe.switches == 0 {
		return true
	}
	return probe.remote == full.plan.RemoteAccessEstimate &&
		probe.exact == full.plan.IncrementalExactCrossShardStateEdgeCount &&
		probe.peak >= full.peak
}

func mtLoadP2PruneImpossibleV1(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string, full, best mtBoundTrialV1, touched mtBoundTouchedV2, cache []*mtLoadP1StaticV1) bool {
	probe := mtLoadP2ProbeV1(p, base, input, mode, full, touched, cache)
	if probe.monotoneReject {
		return true
	}
	if !probe.complete { // Unsupported or inconclusive: fail OPEN to original planner.
		return false
	}
	if mtLoadP2NoStrictGainV1(probe, full) {
		return true
	}
	// In the existing selection loop, a candidate can replace an already
	// accepted Exact only when mtBoundLessV1 would rank it above the best.
	// A Full incumbent is different (first eligible alternative wins), so
	// intentionally do NOT use this shortcut while best.mode == "full".
	if best.mode != "full" {
		trial := mtBoundTrialV1{mode: mode, ready: probe.ready, peak: probe.peak,
			plan: BatchRoutingPlan{RemoteAccessEstimate: probe.remote,
				IncrementalExactCrossShardStateEdgeCount: probe.exact}}
		if !mtBoundLessV1(trial, best) {
			return true
		}
	}
	return false
}

func mtLoadP2ProbeV1(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string, full mtBoundTrialV1, touched mtBoundTouchedV2, cache []*mtLoadP1StaticV1) mtLoadP2ProbeResultV1 {

	if mode != "exact" && mode != "remote" {
		return mtLoadP2ProbeResultV1{}
	}
	// Unsupported experimental combinations fall back to the original full
	// candidate planner; never guess whether a different selector is equivalent.
	if boolFromAny(p.config[metaTrackAblationIgnoreCoaccessRoutingV661]) ||
		boolFromAny(p.config["ablation_no_exact_version_routing_v1"]) ||
		boolFromAny(p.config["online_state_ready_routing_v1"]) {
		return mtLoadP2ProbeResultV1{}
	}
	before := mtBoundCOWBeginV3(base, touched)
	defer mtBoundCOWApplyV3(base, before)

	remoteTotal, exactCrossTotal, readyMax, switches := 0, 0, 0, 0
	for i, record := range input.Records {
		ctx := mtLoadP1ContextV1(p, record, input.Sharding, input.ShardIDs, base, &cache[i])
		candidates := make([]metaTrackIncrementalCandidateV650, 0, len(input.ShardIDs))
		anyAdmissible := false
		for _, shard := range input.ShardIDs {
			admissible := base.Capacity <= 0 || base.ShardLoad[shard] < base.Capacity
			if admissible {
				anyAdmissible = true
			}
			reads, writes := metaTrackRemoteCountsFromContextV669(ctx, shard)
			candidates = append(candidates, metaTrackIncrementalCandidateV650{
				Shard: shard, Admissible: admissible,
				ReadyRank:        metaTrackReadyRankFromContextV669(ctx, shard),
				ExactCross:       metaTrackExactCrossFromContextV669(ctx, shard),
				CoaccessLocality: metaTrackCoaccessFromContextV669(false, ctx, shard, base),
				RemoteReads:      reads, RemoteWrites: writes, RemoteCost: reads + writes,
				Load: base.ShardLoad[shard],
			})
		}
		filtered := make([]metaTrackIncrementalCandidateV650, 0, len(candidates))
		for _, c := range candidates {
			if !anyAdmissible || c.Admissible {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) == 0 {
			return mtLoadP2ProbeResultV1{} // Inconclusive: fall back to full planner.
		}
		ranked := mtBoundAlternativeRankV1(filtered, mode)
		sort.Slice(ranked, func(i, j int) bool {
			return metaTrackIncrementalCandidateLessV650(ranked[i], ranked[j])
		})
		selected := ranked[0]
		remoteTotal += selected.RemoteCost
		exactCrossTotal += selected.ExactCross
		if selected.ReadyRank > readyMax {
			readyMax = selected.ReadyRank
		}
		if i >= len(full.plan.TransactionPlacements) {
			return mtLoadP2ProbeResultV1{}
		}
		if full.plan.TransactionPlacements[i].ExecutionShard != selected.Shard {
			switches++
		}
		// All five checks are monotone along the remaining batch.  Never use
		// delta pair/version budgets here: their discrepancies can cancel later.
		if switches > 10 || remoteTotal > full.plan.RemoteAccessEstimate ||
			exactCrossTotal > full.plan.IncrementalExactCrossShardStateEdgeCount ||
			readyMax > full.ready || base.ShardLoad[selected.Shard]+1 > full.peak {
			return mtLoadP2ProbeResultV1{monotoneReject: true}
		}
		base.ShardLoad[selected.Shard]++
		base.Processed++
		for _, key := range ctx.Keys {
			if base.KeyShardSupport[key] == nil {
				base.KeyShardSupport[key] = map[string]int{}
			}
			base.KeyShardSupport[key][selected.Shard]++
		}
		for _, pair := range ctx.Pairs {
			if base.PairShardSupport[pair] == nil {
				base.PairShardSupport[pair] = map[string]int{}
			}
			base.PairShardSupport[pair][selected.Shard]++
		}
		for _, dep := range record.StateVersions {
			if dep.Key == "" || dep.ProducedVersion == 0 {
				continue
			}
			base.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.ProducedVersion}] =
				metaTrackIncrementalProducerV650{Shard: selected.Shard, Ordinal: record.RoutingOrdinal, FinishRank: selected.ReadyRank + 1}
		}
	}
	return mtLoadP2ProbeResultV1{
		complete: true, switches: switches, remote: remoteTotal,
		exact: exactCrossTotal, ready: readyMax, peak: mtBoundPeakV1(base),
	}
}
