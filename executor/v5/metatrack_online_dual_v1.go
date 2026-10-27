package v5

// MBE_MT_ONLINE_DUAL_V1: experimental router-only improvements.
// No change to PBFT, signed exact predecessors, Home or StateReady.
import (
	"math"
	"sort"
)

// Cold start uses a dimensionless one-unit proxy ONLY when all candidate
// shard snapshots are fresh but none contains a valid completed remote RTT.
// The caller labels every such selection as proxy_no_remote_sample.
// Once a real sample exists, delegate unchanged to the original Online v1.
func mtOnlineRankCandidatesColdstartV1(items []metaTrackIncrementalCandidateV650, f *mtOnlineFeedbackV1, nowMS int64) ([]metaTrackIncrementalCandidateV650, bool, bool) {
	if len(items) == 0 || f == nil {
		return items, false, false
	}
	rows := make([]mtOnlineShardFeedbackV1, len(items))
	anyRTT := false
	for i, c := range items {
		v, ok := f.valid(c.Shard, nowMS)
		if !ok {
			return items, false, false
		}
		rows[i] = v
		if v.RemoteLatencyMS > 0 {
			anyRTT = true
		}
	}
	if anyRTT {
		ranked, ok := mtOnlineRankCandidatesV1(items, f, nowMS)
		return ranked, ok, false
	}
	synthetic := &mtOnlineFeedbackV1{SampledMS: f.SampledMS, Shards: make(map[string]mtOnlineShardFeedbackV1, len(items))}
	for i, c := range items {
		v := rows[i]
		v.RemoteLatencyMS = 1 // DIMENSIONLESS proxy, NOT a measured millisecond
		synthetic.Shards[c.Shard] = v
	}
	ranked, ok := mtOnlineRankCandidatesV1(items, synthetic, nowMS)
	return ranked, ok, ok
}

// MBE_MT_ONLINE_BAL_V2: conservative, causal, non-alternating capacity guard.
//  1. Preserve the best predicted completion candidate as the default.
//  2. Only consider load relief beyond one whole RouteBatch (a protocol scale,
//     not a dataset-tuned coefficient).
//  3. Never trade away signed exact predecessor locality or readiness rank just
//     to equalize transaction counts.
//  4. A candidate may cost at most one measured remote service interval more
//     (or one explicitly declared dimensionless interval in proxy mode).
//
// This is a routing performance hint, not a new StateReady safety oracle.
func mtOnlineBalanceChooseV2(items []metaTrackIncrementalCandidateV650, batchSize int, feedback *mtOnlineFeedbackV1) metaTrackIncrementalCandidateV650 {
	if len(items) == 0 {
		return metaTrackIncrementalCandidateV650{}
	}
	baseline := items[0]
	for _, c := range items[1:] {
		if metaTrackIncrementalCandidateLessV650(c, baseline) {
			baseline = c
		}
	}
	if len(items) < 2 {
		return baseline
	}
	if batchSize < 1 {
		batchSize = 1
	}
	// Match the original online scorer's completed remote RTT median.
	// Without RTT the coldstart scorer uses its declared one-unit proxy.
	samples := make([]float64, 0, len(items))
	if feedback != nil {
		for _, c := range items {
			if snapshot, ok := feedback.Shards[c.Shard]; ok && snapshot.RemoteLatencyMS > 0 && !math.IsNaN(snapshot.RemoteLatencyMS) && !math.IsInf(snapshot.RemoteLatencyMS, 0) {
				samples = append(samples, snapshot.RemoteLatencyMS)
			}
		}
	}
	intervalMS := 1.0
	if len(samples) > 0 {
		sort.Float64s(samples)
		intervalMS = samples[len(samples)/2]
		if intervalMS < 1 {
			intervalMS = 1
		}
	}
	maxExtraUS := intervalMS * 1000
	result := baseline
	found := false
	for _, c := range items {
		if c.Shard == baseline.Shard || c.Load >= baseline.Load || baseline.Load-c.Load <= batchSize {
			continue
		}
		if c.ExactCross > baseline.ExactCross || c.ReadyRank > baseline.ReadyRank {
			continue
		}
		// WorstRank is online-predicted microseconds. Compare using float64
		// before adding budget to avoid integer overflow on large estimates.
		if float64(c.WorstRank)-float64(baseline.WorstRank) > maxExtraUS {
			continue
		}
		// Prefer least completion penalty among eligible alternatives; on an
		// equal predicted finish prefer lower load. Never touch score fields:
		// WorstRank remains the actual estimated finish in placement logs.
		if !found || metaTrackIncrementalCandidateLessV650(c, result) ||
			(c.WorstRank == result.WorstRank && c.RankSum == result.RankSum && c.Load < result.Load) {
			result = c
			found = true
		}
	}
	return result
}

func mtOnlineDualReasonV1(reason string, proxy, balance bool) string {
	if proxy {
		reason += ":proxy_no_remote_sample"
	}
	if balance {
		reason += ":load_balance_guard_v2"
	}
	return reason
}

// MBE_MT_ONLINE_CAP_V3: forward-looking capacity envelope. This is an
// experimental ONLINE-only placement bound, not a state/consensus change.
// The envelope permits a square-root-in-RouteBatches excess above even pace;
// unlike strict balancing, it is intentionally slack early in the workload.
// At 10k, batch=100, two shards: reserve=1000, so a 5000-capacity shard
// cannot be exhausted before processed=8000 by envelope-only selection.
// A known same-RouteBatch exact predecessor may receive at most one-batch slack.
func mtOnlineCapacityEnvelopeV3(processed, expected, capacity, batchSize int) int {
	if expected <= 0 || capacity <= 0 {
		return capacity
	}
	if batchSize <= 0 {
		batchSize = 1
	}
	if processed < 0 {
		processed = 0
	}
	batches := (expected + batchSize - 1) / batchSize
	reserveBatches := int(math.Ceil(math.Sqrt(float64(batches))))
	if reserveBatches < 1 {
		reserveBatches = 1
	}
	reserve := int64(reserveBatches) * int64(batchSize)
	progress := int64(processed)
	if progress > int64(expected) {
		progress = int64(expected)
	}
	envelope := (progress*int64(capacity)+int64(expected)-1)/int64(expected) + reserve
	if envelope > int64(capacity) {
		return capacity
	}
	if envelope < 1 {
		return 1
	}
	return int(envelope)
}

// The original Online v2 selection is the default. Only prevent premature
// exhaustion when a still-admissible other shard has capacity under the
// forward-looking envelope. Candidate prediction fields remain untouched.
func mtOnlineCapacityChooseV3(items []metaTrackIncrementalCandidateV650, selected metaTrackIncrementalCandidateV650, processed, expected, capacity, batchSize int) (metaTrackIncrementalCandidateV650, bool) {
	if len(items) < 2 || expected <= 0 || capacity <= 0 {
		return selected, false
	}
	limit := mtOnlineCapacityEnvelopeV3(processed, expected, capacity, batchSize)
	if selected.Load < limit {
		return selected, false
	}
	var next metaTrackIncrementalCandidateV650
	found := false
	for _, candidate := range items {
		if candidate.Shard == selected.Shard || !candidate.Admissible || candidate.Load >= limit {
			continue
		}
		if !found || candidate.ExactCross < next.ExactCross ||
			(candidate.ExactCross == next.ExactCross && (candidate.ReadyRank < next.ReadyRank ||
				(candidate.ReadyRank == next.ReadyRank && metaTrackIncrementalCandidateLessV650(candidate, next)))) {
			next, found = candidate, true
		}
	}
	if found {
		return next, true
	}
	return selected, false
}

// Same-RouteBatch exact predecessors are particularly sensitive to opposite
// shard block progression. If all known exact producers in this same signed
// batch reside on one shard, preserve that locality when physically admissible
// and within one RouteBatch beyond the soft envelope. Never cross hard cap.
// Multiple conflicting producer shards are NOT guessed or reordered.
func mtOnlineSameBatchExactChooseV3(items []metaTrackIncrementalCandidateV650, selected metaTrackIncrementalCandidateV650, deps []metaTrackResolvedExactDependencyV669, batchFirst, ordinal uint64, processed, expected, capacity, batchSize int) (metaTrackIncrementalCandidateV650, bool) {
	if batchFirst == 0 || ordinal <= batchFirst || len(deps) == 0 {
		return selected, false
	}
	target := ""
	for _, dep := range deps {
		if dep.Producer.Ordinal < batchFirst || dep.Producer.Ordinal >= ordinal {
			continue
		}
		if target == "" {
			target = dep.Producer.Shard
		} else if target != dep.Producer.Shard {
			return selected, false
		}
	}
	if target == "" || selected.Shard == target {
		return selected, false
	}
	limit := mtOnlineCapacityEnvelopeV3(processed, expected, capacity, batchSize)
	if batchSize < 1 {
		batchSize = 1
	}
	for _, candidate := range items {
		if candidate.Shard != target || !candidate.Admissible {
			continue
		}
		if capacity > 0 && candidate.Load >= capacity {
			continue
		}
		if expected > 0 && capacity > 0 && int64(candidate.Load) >= int64(limit)+int64(batchSize) {
			continue
		}
		return candidate, true
	}
	return selected, false
}

func mtOnlineCapReasonV3(reason string, envelope, sameBatch bool) string {
	if envelope {
		reason += ":capacity_envelope_v3"
	}
	if sameBatch {
		reason += ":same_batch_exact_v3"
	}
	return reason
}
