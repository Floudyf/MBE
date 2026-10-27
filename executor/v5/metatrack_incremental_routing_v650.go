package v5

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_CRITICAL_PATH_BALANCED_ROUTING_V651
//
// This planner makes the execution-shard choice independent from RouteBatch
// boundaries. RouteBatch remains a signed/liveness packaging boundary, while
// routing history persists across calls on the unique client-side planner.
//
// v6.5.1 removes the v6.5.0 "exact continuity always wins" ordering. Each
// admissible shard is ranked independently on four structural criteria:
// predicted exact-version ready rank, ordinary remote state cost, cumulative
// co-access locality, and cumulative transaction load. Selection minimizes the
// worst criterion rank and then the sum of ranks; a stable shard ID is used only
// for a complete tie. No chain-length threshold, timed window, decay constant,
// or empirically tuned weight is introduced. Historical co-access magnitude can
// therefore no longer dominate the other signals merely because it accumulated
// across many RouteBatches. A deterministic global capacity is still derived
// only from the declared workload transaction count and execution-shard count.

// MBE_METATRACK_MECHPACK_V1_ROUTING
// Superseded by v2 below: v1 exact-first priority could over-concentrate chains.
const metaTrackIncrementalExactContinuityPolicyV650 = "incremental_exact_ready_balanced_coaccess_v2"

type metaTrackIncrementalVersionSlotV650 struct {
	Key     string
	Version uint64
}

type metaTrackIncrementalProducerV650 struct {
	Shard      string
	Ordinal    uint64
	FinishRank int
}

type metaTrackIncrementalRoutingStateV650 struct {
	ShardSignature           string
	ExpectedTransactionCount int
	Capacity                 int
	Processed                int
	ShardLoad                map[string]int
	ProducerByVersion        map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650
	PairShardSupport         map[string]map[string]int
	KeyShardSupport          map[string]map[string]int
	HomeShardByKey           map[string]string
	HistoryDigest            string
}

type metaTrackIncrementalCandidateV650 struct {
	Shard            string
	Admissible       bool
	ReadyRank        int
	ExactCross       int
	CoaccessLocality int
	RemoteReads      int
	RemoteWrites     int
	RemoteCost       int
	Load             int
	WorstRank        int
	RankSum          int
}

type metaTrackResolvedExactDependencyV669 struct {
	Key      string
	Producer metaTrackIncrementalProducerV650
}

type metaTrackRoutingRecordContextV669 struct {
	Accesses        []tx.AccessItem
	Keys            []string
	Pairs           []string
	ExactDeps       []metaTrackResolvedExactDependencyV669
	ReadHomeCount   map[string]int
	WriteHomeCount  map[string]int
	TotalHomeReads  int
	TotalHomeWrites int
}

func metaTrackBuildRoutingRecordContextV669(p *metaTrackRouting, record WorkloadRecord, sharding ShardingPlugin, shardIDs []string, state *metaTrackIncrementalRoutingStateV650) metaTrackRoutingRecordContextV669 {
	if state.HomeShardByKey == nil {
		state.HomeShardByKey = map[string]string{}
	}
	ctx := metaTrackRoutingRecordContextV669{
		Accesses:       normalizedAccessItems(record),
		ReadHomeCount:  map[string]int{},
		WriteHomeCount: map[string]int{},
	}
	accessByKey := map[string]tx.AccessItem{}
	seenKey := map[string]bool{}
	for _, access := range ctx.Accesses {
		if access.Key == "" {
			continue
		}
		if _, ok := accessByKey[access.Key]; !ok {
			accessByKey[access.Key] = access
		}
		if !seenKey[access.Key] {
			seenKey[access.Key] = true
			ctx.Keys = append(ctx.Keys, access.Key)
		}
		home, cached := state.HomeShardByKey[access.Key]
		if !cached {
			home = p.LogicalStateHome(access.Key, sharding, shardIDs).ServingShard
			state.HomeShardByKey[access.Key] = home
		}
		if home == "" {
			continue
		}
		if isReadMode(access.Mode) {
			ctx.ReadHomeCount[home]++
			ctx.TotalHomeReads++
		}
		if isWriteMode(access.Mode) {
			ctx.WriteHomeCount[home]++
			ctx.TotalHomeWrites++
		}
	}
	sort.Strings(ctx.Keys)
	for left := 0; left < len(ctx.Keys); left++ {
		for right := left + 1; right < len(ctx.Keys); right++ {
			ctx.Pairs = append(ctx.Pairs, keyPair(ctx.Keys[left], ctx.Keys[right]))
		}
	}
	for _, dep := range record.StateVersions {
		if dep.Key == "" || dep.RequiredVersion == 0 {
			continue
		}
		access, ok := accessByKey[dep.Key]
		if !ok || !requiresExactStateValue(access) {
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

func metaTrackRemoteCountsFromContextV669(ctx metaTrackRoutingRecordContextV669, shard string) (int, int) {
	return ctx.TotalHomeReads - ctx.ReadHomeCount[shard], ctx.TotalHomeWrites - ctx.WriteHomeCount[shard]
}

func metaTrackReadyRankFromContextV669(ctx metaTrackRoutingRecordContextV669, shard string) int {
	readyRank := 0
	seenProducer := map[uint64]bool{}
	for _, dep := range ctx.ExactDeps {
		producer := dep.Producer
		if seenProducer[producer.Ordinal] {
			continue
		}
		seenProducer[producer.Ordinal] = true
		candidateRank := producer.FinishRank
		if producer.Shard != shard {
			candidateRank++
		}
		if candidateRank > readyRank {
			readyRank = candidateRank
		}
	}
	return readyRank
}

func metaTrackExactCrossFromContextV669(ctx metaTrackRoutingRecordContextV669, shard string) int {
	cross := 0
	for _, dep := range ctx.ExactDeps {
		if dep.Producer.Shard != shard {
			cross++
		}
	}
	return cross
}

func metaTrackCoaccessFromContextV669(ignoreCoaccess bool, ctx metaTrackRoutingRecordContextV669, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	if ignoreCoaccess {
		return 0
	}
	score := 0
	for _, pair := range ctx.Pairs {
		score += state.PairShardSupport[pair][shard]
	}
	if len(ctx.Keys) == 1 {
		score += state.KeyShardSupport[ctx.Keys[0]][shard]
	}
	return score
}

func newMetaTrackIncrementalRoutingStateV650(input BatchRoutingInput) *metaTrackIncrementalRoutingStateV650 {
	shards := append([]string(nil), input.ShardIDs...)
	sort.Strings(shards)
	capacity := 0
	if len(shards) > 0 && input.ExpectedTransactionCount > 0 {
		capacity = (input.ExpectedTransactionCount + len(shards) - 1) / len(shards)
	}
	load := make(map[string]int, len(shards))
	for _, shard := range shards {
		load[shard] = 0
	}
	return &metaTrackIncrementalRoutingStateV650{
		ShardSignature:           strings.Join(shards, "\x00"),
		ExpectedTransactionCount: input.ExpectedTransactionCount,
		Capacity:                 capacity,
		ShardLoad:                load,
		ProducerByVersion:        map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650{},
		PairShardSupport:         map[string]map[string]int{},
		KeyShardSupport:          map[string]map[string]int{},
		HomeShardByKey:           map[string]string{},
		HistoryDigest:            stableDigest("metatrack_incremental_exact_continuity_v650:genesis"),
	}
}

func (s *metaTrackIncrementalRoutingStateV650) compatible(input BatchRoutingInput) bool {
	shards := append([]string(nil), input.ShardIDs...)
	sort.Strings(shards)
	return s != nil &&
		s.ShardSignature == strings.Join(shards, "\x00") &&
		s.ExpectedTransactionCount == input.ExpectedTransactionCount
}

func metaTrackIncrementalKeysV650(record WorkloadRecord) []string {
	seen := map[string]bool{}
	keys := []string{}
	for _, access := range normalizedAccessItems(record) {
		if access.Key == "" || seen[access.Key] {
			continue
		}
		seen[access.Key] = true
		keys = append(keys, access.Key)
	}
	sort.Strings(keys)
	return keys
}

func metaTrackIncrementalAccessForKeyV650(record WorkloadRecord, key string) (tx.AccessItem, bool) {
	for _, access := range normalizedAccessItems(record) {
		if access.Key == key {
			return access, true
		}
	}
	return tx.AccessItem{}, false
}

func metaTrackIncrementalExactProducersV650(record WorkloadRecord, state *metaTrackIncrementalRoutingStateV650) ([]metaTrackIncrementalProducerV650, int) {
	seenProducer := map[uint64]bool{}
	producers := []metaTrackIncrementalProducerV650{}
	stateEdges := 0
	for _, dep := range record.StateVersions {
		if dep.Key == "" || dep.RequiredVersion == 0 {
			continue
		}
		access, ok := metaTrackIncrementalAccessForKeyV650(record, dep.Key)
		if !ok || !requiresExactStateValue(access) {
			continue
		}
		producer, ok := state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.RequiredVersion}]
		if !ok || strings.TrimSpace(producer.Shard) == "" {
			continue
		}
		stateEdges++
		if !seenProducer[producer.Ordinal] {
			seenProducer[producer.Ordinal] = true
			producers = append(producers, producer)
		}
	}
	return producers, stateEdges
}

func metaTrackIncrementalPairLocalityV650(keys []string, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	score := 0
	for left := 0; left < len(keys); left++ {
		for right := left + 1; right < len(keys); right++ {
			score += state.PairShardSupport[keyPair(keys[left], keys[right])][shard]
		}
	}
	// Single-key transactions have no pair. Preserve a deterministic historical
	// locality signal without inventing a threshold by using exact key support.
	if len(keys) == 1 {
		score += state.KeyShardSupport[keys[0]][shard]
	}
	return score
}

func metaTrackIncrementalExactCrossV650(record WorkloadRecord, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	cross := 0
	for _, dep := range record.StateVersions {
		if dep.Key == "" || dep.RequiredVersion == 0 {
			continue
		}
		access, ok := metaTrackIncrementalAccessForKeyV650(record, dep.Key)
		if !ok || !requiresExactStateValue(access) {
			continue
		}
		producer, ok := state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.RequiredVersion}]
		if ok && producer.Shard != "" && producer.Shard != shard {
			cross++
		}
	}
	return cross
}

func metaTrackIncrementalReadyRankV651(record WorkloadRecord, shard string, state *metaTrackIncrementalRoutingStateV650) int {
	readyRank := 0
	seenProducer := map[uint64]bool{}
	for _, dep := range record.StateVersions {
		if dep.Key == "" || dep.RequiredVersion == 0 {
			continue
		}
		access, ok := metaTrackIncrementalAccessForKeyV650(record, dep.Key)
		if !ok || !requiresExactStateValue(access) {
			continue
		}
		producer, ok := state.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.RequiredVersion}]
		if !ok || producer.Shard == "" || seenProducer[producer.Ordinal] {
			continue
		}
		seenProducer[producer.Ordinal] = true
		candidateRank := producer.FinishRank
		if producer.Shard != shard {
			// One deterministic transfer stage: this is a structural ordering cost,
			// not a wall-clock estimate or tuned latency weight.
			candidateRank++
		}
		if candidateRank > readyRank {
			readyRank = candidateRank
		}
	}
	return readyRank
}

func metaTrackIncrementalCriterionRankV651(candidates []metaTrackIncrementalCandidateV650, index int, better func(left, right metaTrackIncrementalCandidateV650) bool) int {
	rank := 0
	for other := range candidates {
		if other == index {
			continue
		}
		if better(candidates[other], candidates[index]) {
			rank++
		}
	}
	return rank
}

func metaTrackIncrementalRankCandidatesV651(candidates []metaTrackIncrementalCandidateV650) []metaTrackIncrementalCandidateV650 {
	ranked := append([]metaTrackIncrementalCandidateV650(nil), candidates...)
	for index := range ranked {
		// MBE_METATRACK_MECHPACK_V2_ROUTING
		// Five structural criteria share one threshold-free minimax objective.
		// Exact continuity remains visible, but no one criterion has absolute
		// priority or an empirically tuned weight.
		readyRank := metaTrackIncrementalCriterionRankV651(ranked, index, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.ReadyRank < right.ReadyRank
		})
		exactRank := metaTrackIncrementalCriterionRankV651(ranked, index, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.ExactCross < right.ExactCross
		})
		remoteRank := metaTrackIncrementalCriterionRankV651(ranked, index, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.RemoteCost < right.RemoteCost
		})
		coaccessRank := metaTrackIncrementalCriterionRankV651(ranked, index, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.CoaccessLocality > right.CoaccessLocality
		})
		loadRank := metaTrackIncrementalCriterionRankV651(ranked, index, func(left, right metaTrackIncrementalCandidateV650) bool {
			return left.Load < right.Load
		})
		ranked[index].WorstRank = maxInt(maxInt(maxInt(readyRank, exactRank), remoteRank), maxInt(coaccessRank, loadRank))
		ranked[index].RankSum = readyRank + exactRank + remoteRank + coaccessRank + loadRank
	}
	return ranked
}

// MBE_MT_NOEXACT_ABL_V1: same admissible candidates and full history; only
// remote accesses, coaccess and load participate in the ablation selector.
// ReadyRank/ExactCross remain measured and signed dependencies remain enforced.
func metaTrackRankWithoutExactV1(candidates []metaTrackIncrementalCandidateV650) []metaTrackIncrementalCandidateV650 {
	ranked := append([]metaTrackIncrementalCandidateV650(nil), candidates...)
	for i := range ranked {
		remote := metaTrackIncrementalCriterionRankV651(ranked, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.RemoteCost < b.RemoteCost })
		coaccess := metaTrackIncrementalCriterionRankV651(ranked, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.CoaccessLocality > b.CoaccessLocality })
		load := metaTrackIncrementalCriterionRankV651(ranked, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.Load < b.Load })
		ranked[i].WorstRank = maxInt(remote, maxInt(coaccess, load))
		ranked[i].RankSum = remote + coaccess + load
	}
	return ranked
}

func metaTrackIncrementalCandidateLessV650(left, right metaTrackIncrementalCandidateV650) bool {
	if left.WorstRank != right.WorstRank {
		return left.WorstRank < right.WorstRank
	}
	if left.RankSum != right.RankSum {
		return left.RankSum < right.RankSum
	}
	return left.Shard < right.Shard
}

func metaTrackIncrementalUniqueBestV651(candidates []metaTrackIncrementalCandidateV650, selected metaTrackIncrementalCandidateV650, better func(left, right metaTrackIncrementalCandidateV650) bool) bool {
	best := selected
	for _, candidate := range candidates {
		if better(candidate, best) {
			best = candidate
		}
	}
	if best.Shard != selected.Shard {
		return false
	}
	for _, candidate := range candidates {
		if candidate.Shard == selected.Shard {
			continue
		}
		if !better(selected, candidate) {
			return false
		}
	}
	return len(candidates) > 1
}

func (p *metaTrackRouting) planIncrementalExactContinuityV650(input BatchRoutingInput) BatchRoutingPlan {
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
	// MBE_MT_ONLINE_V1: only candidate ordering changes. No new signed
	// state readiness token and no modification to exact dependency metadata.
	onlineConfiguredV1 := boolFromAny(p.config["online_state_ready_routing_v1"])
	feedbackColdstartV1 := onlineConfiguredV1 && boolFromAny(p.config["online_feedback_coldstart_v2"])
	capacityGuardV1 := onlineConfiguredV1 && boolFromAny(p.config["online_capacity_guard_v2"])
	onlineUsedV1 := false // MBE_MT_ONLINE_FIX_V1 reset per candidate selection
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
	for _, record := range input.Records {
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
				Load:             state.ShardLoad[shard],
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
			QueueLoadBefore:       selected.Load,
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

// Compile-time use of tx documents that v6.5.1 operates on the same signed
// StateVersionDependency contract as existing MetaTrack routing.
var _ = tx.StateVersionDependency{}

// MBE_MT_ONLINE_FIX_V1: evidence is scoped to each actually selected tx.
func mtOnlinePlacementReasonFixedV1(enabled, used, changed bool, c metaTrackIncrementalCandidateV650) string {
	reason := mtOnlinePlacementReasonV1(enabled, used, c)
	if enabled && used {
		if changed {
			return reason + ":legacy_switch=1"
		}
		return reason + ":legacy_switch=0"
	}
	return reason
}
