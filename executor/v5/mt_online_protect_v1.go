package v5

// MBE_MT_ONLINE_PROTECT_V1: ONLINE joint opt-in only.
// A feasible Full choice is the default. A dynamic substitution cannot break
// a locally known exact predecessor, increase exact cross-shard edges, extend
// the predicted exact critical depth, or increase static remote accesses.
// Real, fresh queue evidence must Pareto-dominate the Full choice. A completed
// StateFetch latency is deliberately NOT treated as per-shard network RTT.
// No consensus, Home, signed StateVersions or StateReady safety gates change.
func mtOnlineProtectedChoiceV1(
	items []metaTrackIncrementalCandidateV650,
	fullShard string,
	deps []metaTrackResolvedExactDependencyV669,
	feedback *mtOnlineFeedbackV1,
	nowMS int64,
) (metaTrackIncrementalCandidateV650, string) {
	if len(items) == 0 {
		return metaTrackIncrementalCandidateV650{}, "protect_empty"
	}
	full := items[0]
	foundFull := false
	for _, candidate := range items {
		if candidate.Shard == fullShard {
			full, foundFull = candidate, true
			break
		}
	}
	// A mismatched Full counterfactual is an invariant violation, never a
	// reason to improvise an Online placement.
	if !foundFull {
		return full, "protect_full_not_found"
	}
	if feedback == nil {
		return full, "protect_full_no_feedback"
	}
	fullStatus, valid := feedback.valid(full.Shard, nowMS)
	if !valid {
		return full, "protect_full_stale_feedback"
	}
	choice := full
	changed := false
	for _, candidate := range items {
		if candidate.Shard == full.Shard || !candidate.Admissible {
			continue
		}
		if candidate.ExactCross > full.ExactCross || candidate.ReadyRank > full.ReadyRank || candidate.RemoteCost > full.RemoteCost {
			continue
		}
		splitLocalExact := false
		for _, dep := range deps {
			if dep.Producer.Shard == full.Shard && dep.Producer.Shard != candidate.Shard {
				splitLocalExact = true
				break
			}
		}
		if splitLocalExact {
			continue
		}
		observed, ok := feedback.valid(candidate.Shard, nowMS)
		if !ok {
			continue
		}
		if !mtOnlineQueueDominatesV1(observed, fullStatus) {
			continue
		}
		// Two-shard experiments have at most one alternative. When more shards
		// are configured, keep a deterministic, lexicographic tie-break.
		if !changed || candidate.Shard < choice.Shard {
			choice, changed = candidate, true
		}
	}
	if changed {
		return choice, "protect_online_queue_dominance"
	}
	return full, "protect_full_structural_or_queue"
}

// All compared fields are aggregate queue/backlog observations from the same
// telemetry schema and freshness window. They are NOT added or weighted.
// ActiveTasks is intentionally excluded: it does not alone measure contention,
// and its producer path needs a separate MetaTrack-specific proof.
func mtOnlineQueueDominatesV1(candidate, baseline mtOnlineShardFeedbackV1) bool {
	left := [...]int{candidate.SchedulerQueue, candidate.RequestQueue, candidate.PendingFetch, candidate.PendingVersion, candidate.MempoolDepth}
	right := [...]int{baseline.SchedulerQueue, baseline.RequestQueue, baseline.PendingFetch, baseline.PendingVersion, baseline.MempoolDepth}
	better := false
	for i := range left {
		if left[i] < 0 || right[i] < 0 || left[i] > right[i] {
			return false
		}
		if left[i] < right[i] {
			better = true
		}
	}
	return better
}

func mtOnlineProtectedReasonV1(reason, protection string) string {
	if protection != "" {
		return reason + ":" + protection
	}
	return reason
}
