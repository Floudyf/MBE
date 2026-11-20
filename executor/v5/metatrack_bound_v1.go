package v5

// MT_BOUND_CARD_V1: experimental, router-only batch counterfactual.
// The original Full planner is never invoked on a mutable shared fork.
// Only inputs known before PlanBatch, and the chosen fork, affect the next batch.
import (
	"sort"
	"strconv"
	"strings"
)

const mtBoundMicro10V1 = "bounded_batch_micro10_v1"

func mtBoundCloneStateV1(src *metaTrackIncrementalRoutingStateV650) *metaTrackIncrementalRoutingStateV650 {
	if src == nil {
		return nil
	}
	dst := *src
	dst.ShardLoad = make(map[string]int, len(src.ShardLoad))
	for k, v := range src.ShardLoad {
		dst.ShardLoad[k] = v
	}
	dst.ProducerByVersion = make(map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650, len(src.ProducerByVersion))
	for k, v := range src.ProducerByVersion {
		dst.ProducerByVersion[k] = v
	}
	dst.PairShardSupport = make(map[string]map[string]int, len(src.PairShardSupport))
	for k, v := range src.PairShardSupport {
		cp := make(map[string]int, len(v))
		for sh, n := range v {
			cp[sh] = n
		}
		dst.PairShardSupport[k] = cp
	}
	dst.KeyShardSupport = make(map[string]map[string]int, len(src.KeyShardSupport))
	for k, v := range src.KeyShardSupport {
		cp := make(map[string]int, len(v))
		for sh, n := range v {
			cp[sh] = n
		}
		dst.KeyShardSupport[k] = cp
	}
	dst.HomeShardByKey = make(map[string]string, len(src.HomeShardByKey))
	for k, v := range src.HomeShardByKey {
		dst.HomeShardByKey[k] = v
	}
	return &dst
}

func mtBoundAlternativeRankV1(items []metaTrackIncrementalCandidateV650, mode string) []metaTrackIncrementalCandidateV650 {
	ranked := append([]metaTrackIncrementalCandidateV650(nil), items...)
	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if mode == "remote" {
			if a.RemoteCost != b.RemoteCost {
				return a.RemoteCost < b.RemoteCost
			}
			if a.ReadyRank != b.ReadyRank {
				return a.ReadyRank < b.ReadyRank
			}
			if a.ExactCross != b.ExactCross {
				return a.ExactCross < b.ExactCross
			}
		} else {
			if a.ReadyRank != b.ReadyRank {
				return a.ReadyRank < b.ReadyRank
			}
			if a.ExactCross != b.ExactCross {
				return a.ExactCross < b.ExactCross
			}
			if a.RemoteCost != b.RemoteCost {
				return a.RemoteCost < b.RemoteCost
			}
		}
		if a.CoaccessLocality != b.CoaccessLocality {
			return a.CoaccessLocality > b.CoaccessLocality
		}
		if a.Load != b.Load {
			return a.Load < b.Load
		}
		return a.Shard < b.Shard
	})
	// The common planner sorts by WorstRank then RankSum. Never scale several
	// unbounded counters into a weighted integer (overflow/hidden weights).
	for i := range ranked {
		ranked[i].WorstRank = i
		ranked[i].RankSum = 0
	}
	return ranked
}

type mtBoundTrialV1 struct {
	mode  string
	state *metaTrackIncrementalRoutingStateV650
	plan  BatchRoutingPlan
	ready int
	peak  int
	skew  int
}

func mtBoundReadyDepthV1(plan BatchRoutingPlan) int {
	maxReady := 0
	for _, placement := range plan.TransactionPlacements {
		for _, part := range strings.Split(placement.Reason, ":") {
			if !strings.HasPrefix(part, "ready_rank=") {
				continue
			}
			n, err := strconv.Atoi(strings.TrimPrefix(part, "ready_rank="))
			if err == nil && n > maxReady {
				maxReady = n
			}
		}
	}
	return maxReady
}
func mtBoundPeakV1(state *metaTrackIncrementalRoutingStateV650) int {
	maxLoad := 0
	for _, v := range state.ShardLoad {
		if v > maxLoad {
			maxLoad = v
		}
	}
	return maxLoad
}
func mtBoundSkewV1(state *metaTrackIncrementalRoutingStateV650) int {
	first := true
	minLoad, maxLoad := 0, 0
	for _, v := range state.ShardLoad {
		if first {
			minLoad, maxLoad = v, v
			first = false
			continue
		}
		if v < minLoad {
			minLoad = v
		}
		if v > maxLoad {
			maxLoad = v
		}
	}
	return maxLoad - minLoad
}

// mtBoundRunWithStateV2 runs exactly the existing planner and selector on the
// supplied working state. The caller owns transactional rollback/commit.
func mtBoundRunWithStateV2(p *metaTrackRouting, working *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string) mtBoundTrialV1 {
	config := make(map[string]any, len(p.config)+2)
	for k, v := range p.config {
		config[k] = v
	}
	delete(config, "bounded_batch_routing_v1")
	delete(config, "online_state_ready_routing_v1")
	delete(config, "online_feedback_coldstart_v2")
	delete(config, "online_capacity_guard_v2")
	delete(config, "bound_selector_v1")
	if mode != "full" {
		config["bound_selector_v1"] = mode
	}
	q := &metaTrackRouting{basicPlugin: p.basicPlugin, incrementalV650: working}
	q.basicPlugin.config = config
	plan := q.planIncrementalExactContinuityV650(input)
	// Production candidates never expose the temporarily shared state.
	return mtBoundTrialV1{mode: mode, state: q.incrementalV650, plan: plan, ready: mtBoundReadyDepthV1(plan), peak: mtBoundPeakV1(q.incrementalV650), skew: mtBoundSkewV1(q.incrementalV650)}
}

// Legacy deep-clone oracle remains available to prove exact behavior in tests.
// It is never called from the v2 hot path.
func mtBoundForkV1(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string) mtBoundTrialV1 {
	return mtBoundRunWithStateV2(p, mtBoundCloneStateV1(base), input, mode)
}

func mtBoundBatchSafeV1(candidate, full mtBoundTrialV1) bool {
	c, f := candidate.plan, full.plan
	if c.RemoteAccessEstimate > f.RemoteAccessEstimate || c.IncrementalExactCrossShardStateEdgeCount > f.IncrementalExactCrossShardStateEdgeCount || candidate.ready > full.ready || candidate.peak > full.peak {
		return false
	}
	return c.RemoteAccessEstimate < f.RemoteAccessEstimate || c.IncrementalExactCrossShardStateEdgeCount < f.IncrementalExactCrossShardStateEdgeCount || candidate.peak < full.peak
}
func mtBoundPlanSwitchesV1(candidate, full BatchRoutingPlan) int {
	if len(candidate.TransactionPlacements) != len(full.TransactionPlacements) {
		return int(^uint(0) >> 1)
	}
	switches := 0
	for i, v := range candidate.TransactionPlacements {
		a := full.TransactionPlacements[i]
		if v.TxIndex != a.TxIndex || v.LogicalID != a.LogicalID {
			return int(^uint(0) >> 1)
		}
		if v.ExecutionShard != a.ExecutionShard {
			switches++
		}
	}
	return switches
}
func mtBoundChangesV1(candidate, full mtBoundTrialV1, input BatchRoutingInput) (int, int) {
	keys := map[metaTrackIncrementalVersionSlotV650]bool{}
	pairs := map[string]bool{}
	for _, record := range input.Records {
		for _, dep := range record.StateVersions {
			if dep.Key != "" && dep.ProducedVersion > 0 {
				keys[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.ProducedVersion}] = true
			}
		}
		accesses := normalizedAccessItems(record)
		unique := map[string]bool{}
		list := make([]string, 0, len(accesses))
		for _, access := range accesses {
			if access.Key != "" && !unique[access.Key] {
				unique[access.Key] = true
				list = append(list, access.Key)
			}
		}
		sort.Strings(list)
		for i := 0; i < len(list); i++ {
			for j := i + 1; j < len(list); j++ {
				pairs[keyPair(list[i], list[j])] = true
			}
		}
	}
	moves := 0
	for key := range keys {
		a, okA := candidate.state.ProducerByVersion[key]
		b, okB := full.state.ProducerByVersion[key]
		if okA != okB || a.Shard != b.Shard {
			moves++
		}
	}
	changedPairs := 0
	for pair := range pairs {
		left, right := candidate.state.PairShardSupport[pair], full.state.PairShardSupport[pair]
		if len(left) != len(right) {
			changedPairs++
			continue
		}
		changed := false
		for sh, v := range left {
			if right[sh] != v {
				changed = true
				break
			}
		}
		if changed {
			changedPairs++
		}
	}
	return moves, changedPairs
}
func mtBoundLessV1(a, b mtBoundTrialV1) bool {
	if a.plan.IncrementalExactCrossShardStateEdgeCount != b.plan.IncrementalExactCrossShardStateEdgeCount {
		return a.plan.IncrementalExactCrossShardStateEdgeCount < b.plan.IncrementalExactCrossShardStateEdgeCount
	}
	if a.plan.RemoteAccessEstimate != b.plan.RemoteAccessEstimate {
		return a.plan.RemoteAccessEstimate < b.plan.RemoteAccessEstimate
	}
	if a.ready != b.ready {
		return a.ready < b.ready
	}
	if a.peak != b.peak {
		return a.peak < b.peak
	}
	return a.mode < b.mode
}

// mtBoundTouchedV2 contains the exact map slots the existing planner may write
// while admitting this batch. All keys are derived exclusively from current
// signed inputs, never from any execution-time or future observation.
type mtBoundTouchedV2 struct {
	keys     []string
	pairs    []string
	produced []metaTrackIncrementalVersionSlotV650
}

type mtBoundShardMapV2 struct {
	present bool
	value   map[string]int
}
type mtBoundProducerV2 struct {
	present bool
	value   metaTrackIncrementalProducerV650
}
type mtBoundHomeV2 struct {
	present bool
	value   string
}

type mtBoundDeltaV2 struct {
	shardLoad     map[string]int
	producers     map[metaTrackIncrementalVersionSlotV650]mtBoundProducerV2
	pairSupport   map[string]mtBoundShardMapV2
	keySupport    map[string]mtBoundShardMapV2
	homes         map[string]mtBoundHomeV2
	processed     int
	historyDigest string
	capacity      int
}

func mtBoundTouchedForBatchV2(input BatchRoutingInput) mtBoundTouchedV2 {
	keys, pairs := map[string]bool{}, map[string]bool{}
	producers := map[metaTrackIncrementalVersionSlotV650]bool{}
	for _, record := range input.Records {
		keyList := make([]string, 0)
		local := map[string]bool{}
		for _, access := range normalizedAccessItems(record) {
			if access.Key == "" || local[access.Key] {
				continue
			}
			local[access.Key] = true
			keys[access.Key] = true
			keyList = append(keyList, access.Key)
		}
		sort.Strings(keyList)
		for i := 0; i < len(keyList); i++ {
			for j := i + 1; j < len(keyList); j++ {
				pairs[keyPair(keyList[i], keyList[j])] = true
			}
		}
		for _, dep := range record.StateVersions {
			if dep.Key != "" && dep.ProducedVersion > 0 {
				producers[metaTrackIncrementalVersionSlotV650{Key: dep.Key, Version: dep.ProducedVersion}] = true
			}
		}
	}
	touched := mtBoundTouchedV2{}
	for k := range keys {
		touched.keys = append(touched.keys, k)
	}
	for k := range pairs {
		touched.pairs = append(touched.pairs, k)
	}
	for k := range producers {
		touched.produced = append(touched.produced, k)
	}
	sort.Strings(touched.keys)
	sort.Strings(touched.pairs)
	sort.Slice(touched.produced, func(i, j int) bool {
		if touched.produced[i].Key != touched.produced[j].Key {
			return touched.produced[i].Key < touched.produced[j].Key
		}
		return touched.produced[i].Version < touched.produced[j].Version
	})
	return touched
}

func mtBoundCopyShardMapV2(src map[string]int) map[string]int {
	cp := make(map[string]int, len(src))
	for k, v := range src {
		cp[k] = v
	}
	return cp
}

// Capture is O(current-batch touched entries), rather than O(all-time history).
func mtBoundCaptureV2(s *metaTrackIncrementalRoutingStateV650, touched mtBoundTouchedV2) mtBoundDeltaV2 {
	snap := mtBoundDeltaV2{
		shardLoad:   mtBoundCopyShardMapV2(s.ShardLoad),
		producers:   make(map[metaTrackIncrementalVersionSlotV650]mtBoundProducerV2, len(touched.produced)),
		pairSupport: make(map[string]mtBoundShardMapV2, len(touched.pairs)),
		keySupport:  make(map[string]mtBoundShardMapV2, len(touched.keys)),
		homes:       make(map[string]mtBoundHomeV2, len(touched.keys)),
		processed:   s.Processed, historyDigest: s.HistoryDigest, capacity: s.Capacity,
	}
	for _, slot := range touched.produced {
		value, ok := s.ProducerByVersion[slot]
		snap.producers[slot] = mtBoundProducerV2{present: ok, value: value}
	}
	for _, pair := range touched.pairs {
		src, ok := s.PairShardSupport[pair]
		snap.pairSupport[pair] = mtBoundShardMapV2{present: ok, value: mtBoundCopyShardMapV2(src)}
	}
	for _, key := range touched.keys {
		src, ok := s.KeyShardSupport[key]
		snap.keySupport[key] = mtBoundShardMapV2{present: ok, value: mtBoundCopyShardMapV2(src)}
		home, ok := s.HomeShardByKey[key]
		snap.homes[key] = mtBoundHomeV2{present: ok, value: home}
	}
	return snap
}

// No other history entries are written by the frozen incremental planner.
// The caller holds p.incrementalMu throughout all previews and the final commit.
func mtBoundRestoreV2(s *metaTrackIncrementalRoutingStateV650, delta mtBoundDeltaV2) {
	s.ShardLoad = mtBoundCopyShardMapV2(delta.shardLoad)
	for slot, row := range delta.producers {
		if row.present {
			s.ProducerByVersion[slot] = row.value
		} else {
			delete(s.ProducerByVersion, slot)
		}
	}
	for key, row := range delta.pairSupport {
		if row.present {
			s.PairShardSupport[key] = mtBoundCopyShardMapV2(row.value)
		} else {
			delete(s.PairShardSupport, key)
		}
	}
	for key, row := range delta.keySupport {
		if row.present {
			s.KeyShardSupport[key] = mtBoundCopyShardMapV2(row.value)
		} else {
			delete(s.KeyShardSupport, key)
		}
	}
	for key, row := range delta.homes {
		if row.present {
			s.HomeShardByKey[key] = row.value
		} else {
			delete(s.HomeShardByKey, key)
		}
	}
	s.Processed = delta.processed
	s.HistoryDigest = delta.historyDigest
	s.Capacity = delta.capacity
}

func mtBoundShardMapDifferentV2(a, b mtBoundShardMapV2) bool {
	if len(a.value) != len(b.value) {
		return true
	}
	for k, v := range a.value {
		if b.value[k] != v {
			return true
		}
	}
	return false
}
func mtBoundDeltaChangesV2(candidate, full mtBoundDeltaV2) (int, int) {
	moves, pairs := 0, 0
	for key, a := range candidate.producers {
		b := full.producers[key]
		if a.present != b.present || a.value.Shard != b.value.Shard {
			moves++
		}
	}
	for key, a := range candidate.pairSupport {
		if mtBoundShardMapDifferentV2(a, full.pairSupport[key]) {
			pairs++
		}
	}
	return moves, pairs
}

// COW v3: preserve old map headers and clone only the nested entries touched
// by this batch BEFORE a trial. There is no copy on rollback or final commit.
// All three candidates share the SAME immutable pre-batch prefix.
func mtBoundCOWBeginV3(s *metaTrackIncrementalRoutingStateV650, touched mtBoundTouchedV2) mtBoundDeltaV2 {
	before := mtBoundDeltaV2{
		shardLoad:   s.ShardLoad,
		producers:   make(map[metaTrackIncrementalVersionSlotV650]mtBoundProducerV2, len(touched.produced)),
		pairSupport: make(map[string]mtBoundShardMapV2, len(touched.pairs)),
		keySupport:  make(map[string]mtBoundShardMapV2, len(touched.keys)),
		homes:       make(map[string]mtBoundHomeV2, len(touched.keys)),
		processed:   s.Processed, historyDigest: s.HistoryDigest, capacity: s.Capacity,
	}
	s.ShardLoad = mtBoundCopyShardMapV2(s.ShardLoad)
	for _, slot := range touched.produced {
		value, ok := s.ProducerByVersion[slot]
		before.producers[slot] = mtBoundProducerV2{present: ok, value: value}
	}
	for _, pair := range touched.pairs {
		value, ok := s.PairShardSupport[pair]
		before.pairSupport[pair] = mtBoundShardMapV2{present: ok, value: value}
		if ok {
			s.PairShardSupport[pair] = mtBoundCopyShardMapV2(value)
		}
	}
	for _, key := range touched.keys {
		value, ok := s.KeyShardSupport[key]
		before.keySupport[key] = mtBoundShardMapV2{present: ok, value: value}
		if ok {
			s.KeyShardSupport[key] = mtBoundCopyShardMapV2(value)
		}
		home, ok := s.HomeShardByKey[key]
		before.homes[key] = mtBoundHomeV2{present: ok, value: home}
	}
	return before
}
func mtBoundCOWAfterV3(s *metaTrackIncrementalRoutingStateV650, touched mtBoundTouchedV2) mtBoundDeltaV2 {
	after := mtBoundDeltaV2{
		shardLoad:   s.ShardLoad,
		producers:   make(map[metaTrackIncrementalVersionSlotV650]mtBoundProducerV2, len(touched.produced)),
		pairSupport: make(map[string]mtBoundShardMapV2, len(touched.pairs)),
		keySupport:  make(map[string]mtBoundShardMapV2, len(touched.keys)),
		homes:       make(map[string]mtBoundHomeV2, len(touched.keys)),
		processed:   s.Processed, historyDigest: s.HistoryDigest, capacity: s.Capacity,
	}
	for _, slot := range touched.produced {
		value, ok := s.ProducerByVersion[slot]
		after.producers[slot] = mtBoundProducerV2{present: ok, value: value}
	}
	for _, pair := range touched.pairs {
		value, ok := s.PairShardSupport[pair]
		after.pairSupport[pair] = mtBoundShardMapV2{present: ok, value: value}
	}
	for _, key := range touched.keys {
		value, ok := s.KeyShardSupport[key]
		after.keySupport[key] = mtBoundShardMapV2{present: ok, value: value}
		home, ok := s.HomeShardByKey[key]
		after.homes[key] = mtBoundHomeV2{present: ok, value: home}
	}
	return after
}
func mtBoundCOWApplyV3(s *metaTrackIncrementalRoutingStateV650, delta mtBoundDeltaV2) {
	// The captured map headers are immutable after the trial: the next trial
	// re-clones entries from its own original state. No nested alias is leaked.
	s.ShardLoad = delta.shardLoad
	for slot, row := range delta.producers {
		if row.present {
			s.ProducerByVersion[slot] = row.value
		} else {
			delete(s.ProducerByVersion, slot)
		}
	}
	for key, row := range delta.pairSupport {
		if row.present {
			s.PairShardSupport[key] = row.value
		} else {
			delete(s.PairShardSupport, key)
		}
	}
	for key, row := range delta.keySupport {
		if row.present {
			s.KeyShardSupport[key] = row.value
		} else {
			delete(s.KeyShardSupport, key)
		}
	}
	for key, row := range delta.homes {
		if row.present {
			s.HomeShardByKey[key] = row.value
		} else {
			delete(s.HomeShardByKey, key)
		}
	}
	s.Processed = delta.processed
	s.HistoryDigest = delta.historyDigest
	s.Capacity = delta.capacity
}

type mtBoundSavedTrialV2 struct {
	trial mtBoundTrialV1
	after mtBoundDeltaV2
}

func mtBoundRunRollbackV3(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string, touched mtBoundTouchedV2) (result mtBoundSavedTrialV2) {
	before := mtBoundCOWBeginV3(base, touched)
	defer mtBoundCOWApplyV3(base, before)
	result.trial = mtBoundRunWithStateV2(p, base, input, mode)
	result.after = mtBoundCOWAfterV3(base, touched)
	result.trial.state = nil // never retain a pointer to rolled-back shared history
	return
}

// MT_PRUNE_NATIVE_V1 is a read-only-overlay experiment.  This candidate prefix
// uses the same V669 context, V651 candidate fields and Bound alternative
// selector as the production planner, but does not build signed plan artifacts.
// It is allowed to return TRUE only for monotone impossibility conditions.
// The working history is always reverted before this function returns.
func mtBoundPruneImpossibleV1(p *metaTrackRouting, base *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput, mode string, full mtBoundTrialV1, touched mtBoundTouchedV2) bool {
	if mode != "exact" && mode != "remote" {
		return false
	}
	// Unsupported experimental combinations fall back to the original full
	// candidate planner; never guess whether a different selector is equivalent.
	if boolFromAny(p.config[metaTrackAblationIgnoreCoaccessRoutingV661]) ||
		boolFromAny(p.config["ablation_no_exact_version_routing_v1"]) ||
		boolFromAny(p.config["online_state_ready_routing_v1"]) {
		return false
	}
	before := mtBoundCOWBeginV3(base, touched)
	defer mtBoundCOWApplyV3(base, before)

	remoteTotal, exactCrossTotal, readyMax, switches := 0, 0, 0, 0
	for i, record := range input.Records {
		ctx := metaTrackBuildRoutingRecordContextV669(p, record, input.Sharding, input.ShardIDs, base)
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
			return false // Cannot prove rejection; let the actual planner decide.
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
			return false
		}
		if full.plan.TransactionPlacements[i].ExecutionShard != selected.Shard {
			switches++
		}
		// All five checks are monotone along the remaining batch.  Never use
		// delta pair/version budgets here: their discrepancies can cancel later.
		if switches > 10 || remoteTotal > full.plan.RemoteAccessEstimate ||
			exactCrossTotal > full.plan.IncrementalExactCrossShardStateEdgeCount ||
			readyMax > full.ready || base.ShardLoad[selected.Shard]+1 > full.peak {
			return true
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
	return false
}

func (p *metaTrackRouting) planBoundV1(input BatchRoutingInput) BatchRoutingPlan {
	p.incrementalMu.Lock()
	defer p.incrementalMu.Unlock()
	reset := input.BatchIndex == 0 && p.incrementalV650 != nil && p.incrementalV650.Processed > 0
	if reset || p.incrementalV650 == nil || !p.incrementalV650.compatible(input) {
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(input)
	}
	base := p.incrementalV650
	eligible := len(input.Records) == 100 && len(input.ShardIDs) == 2 && input.ExpectedTransactionCount > 0
	if !eligible {
		// Fast fallback: not a micro10 batch, so invoke Full exactly once.
		only := mtBoundRunWithStateV2(p, base, input, "full")
		plan := only.plan
		plan.IncrementalRoutingPolicy = mtBoundMicro10V1
		plan.TransactionPolicy = "bounded_batch_same_history_pareto_micro10_v1"
		if !input.DeferFinalSeal {
			plan.PlanDigest = routingPlanDigest(plan)
		}
		return plan
	}
	touched := mtBoundTouchedForBatchV2(input)
	full := mtBoundRunRollbackV3(p, base, input, "full", touched)
	selected := full
	for _, mode := range []string{"exact", "remote"} {
		if mtBoundPruneImpossibleV1(p, base, input, mode, full.trial, touched) {
			continue
		}
		alt := mtBoundRunRollbackV3(p, base, input, mode, touched)
		if !mtBoundBatchSafeV1(alt.trial, full.trial) || mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) > 10 {
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
	mtBoundCOWApplyV3(base, selected.after)
	plan := selected.trial.plan
	plan.IncrementalRoutingPolicy = mtBoundMicro10V1
	plan.TransactionPolicy = "bounded_batch_same_history_pareto_micro10_v1"
	if selected.trial.mode != "full" {
		for i := range plan.TransactionPlacements {
			plan.TransactionPlacements[i].Reason += ":bounded_batch_" + selected.trial.mode
		}
	}
	if !input.DeferFinalSeal {
		plan.PlanDigest = routingPlanDigest(plan)
	}
	return plan
}
