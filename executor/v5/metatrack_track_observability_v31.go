package v5

import "sort"

func metaTrackTrackMetricsFromAttemptsV31(attempts []BusinessExecutionAttempt) map[string]any {
	fastInitial := map[string]bool{}
	conservativeInitial := map[string]bool{}
	fastAttemptCount := 0
	conservativeAttemptCount := 0
	fastFinalCount := 0
	conservativeFinalCount := 0
	fastFallbackCount := 0
	fastDiscardedCount := 0
	conservativeReexecutionCount := 0
	fastDurationNS := int64(0)
	conservativeDurationNS := int64(0)
	fastDiscardedDurationNS := int64(0)
	fastSojournNS := int64(0)
	conservativeSojournNS := int64(0)
	fastStateWaitNS := int64(0)
	conservativeStateWaitNS := int64(0)
	fastDependencyWaitNS := int64(0)
	conservativeDependencyWaitNS := int64(0)
	fastQueueWaitNS := int64(0)
	conservativeQueueWaitNS := int64(0)
	fastDurationsNS := make([]int64, 0)
	conservativeDurationsNS := make([]int64, 0)
	fastSojournValuesNS := make([]int64, 0)
	conservativeSojournValuesNS := make([]int64, 0)

	for _, attempt := range attempts {
		track := attempt.Track
		if track != "fast" {
			track = "conservative"
		}
		durationNS := attempt.DurationNS
		if durationNS <= 0 && attempt.DurationUS > 0 {
			durationNS = attempt.DurationUS * 1000
		}
		if durationNS < 0 {
			durationNS = 0
		}
		sojournNS := maxInt64V31(attempt.SojournNS, 0)
		stateWaitNS := maxInt64V31(attempt.StateWaitNS, 0)
		dependencyWaitNS := maxInt64V31(attempt.DependencyWaitNS, 0)
		queueWaitNS := maxInt64V31(attempt.QueueWaitNS, 0)
		if attempt.Attempt == 1 {
			if track == "fast" {
				fastInitial[attempt.TxID] = true
			} else {
				conservativeInitial[attempt.TxID] = true
			}
		}
		if track == "fast" {
			fastAttemptCount++
			fastDurationNS += durationNS
			fastDurationsNS = append(fastDurationsNS, durationNS)
			fastSojournNS += sojournNS
			fastSojournValuesNS = append(fastSojournValuesNS, sojournNS)
			fastStateWaitNS += stateWaitNS
			fastDependencyWaitNS += dependencyWaitNS
			fastQueueWaitNS += queueWaitNS
			if attempt.FinalCompletion {
				fastFinalCount++
			} else {
				fastFallbackCount++
				fastDiscardedCount++
				fastDiscardedDurationNS += durationNS
			}
			continue
		}
		conservativeAttemptCount++
		conservativeDurationNS += durationNS
		conservativeDurationsNS = append(conservativeDurationsNS, durationNS)
		conservativeSojournNS += sojournNS
		conservativeSojournValuesNS = append(conservativeSojournValuesNS, sojournNS)
		conservativeStateWaitNS += stateWaitNS
		conservativeDependencyWaitNS += dependencyWaitNS
		conservativeQueueWaitNS += queueWaitNS
		if attempt.FinalCompletion {
			conservativeFinalCount++
		}
		if attempt.Attempt > 1 {
			conservativeReexecutionCount++
		}
	}

	fastInitialCount := len(fastInitial)
	conservativeInitialCount := len(conservativeInitial)
	fastFallbackRate := 0.0
	if fastInitialCount > 0 {
		fastFallbackRate = float64(fastFallbackCount) / float64(fastInitialCount)
	}
	conservativeReexecutionShare := 0.0
	if conservativeAttemptCount > 0 {
		conservativeReexecutionShare = float64(conservativeReexecutionCount) / float64(conservativeAttemptCount)
	}

	return map[string]any{
		"metatrack_fast_initial_tx_count":                         fastInitialCount,
		"metatrack_conservative_initial_tx_count":                 conservativeInitialCount,
		"metatrack_fast_final_tx_count":                           fastFinalCount,
		"metatrack_conservative_final_tx_count":                   conservativeFinalCount,
		"metatrack_fast_business_execution_attempt_count":         fastAttemptCount,
		"metatrack_conservative_business_execution_attempt_count": conservativeAttemptCount,
		"metatrack_fast_business_execution_sum_ms":                nsToMSV31(fastDurationNS),
		"metatrack_conservative_business_execution_sum_ms":        nsToMSV31(conservativeDurationNS),
		"metatrack_fast_business_execution_mean_ms":               meanNSAsMSV31(fastDurationNS, fastAttemptCount),
		"metatrack_conservative_business_execution_mean_ms":       meanNSAsMSV31(conservativeDurationNS, conservativeAttemptCount),
		"metatrack_fast_business_execution_p95_ms":                durationPercentileNSAsMSV31(fastDurationsNS, 0.95),
		"metatrack_fast_business_execution_p99_ms":                durationPercentileNSAsMSV31(fastDurationsNS, 0.99),
		"metatrack_conservative_business_execution_p95_ms":        durationPercentileNSAsMSV31(conservativeDurationsNS, 0.95),
		"metatrack_conservative_business_execution_p99_ms":        durationPercentileNSAsMSV31(conservativeDurationsNS, 0.99),
		"metatrack_fast_fallback_count":                           fastFallbackCount,
		"metatrack_fast_discarded_tentative_count":                fastDiscardedCount,
		"metatrack_fast_discarded_execution_ms":                   nsToMSV31(fastDiscardedDurationNS),
		"metatrack_fast_fallback_rate":                            fastFallbackRate,
		"metatrack_conservative_reexecution_count":                conservativeReexecutionCount,
		"metatrack_conservative_reexecution_share":                conservativeReexecutionShare,
		"metatrack_fast_track_sojourn_sum_ms":                     nsToMSV31(fastSojournNS),
		"metatrack_conservative_track_sojourn_sum_ms":             nsToMSV31(conservativeSojournNS),
		"metatrack_fast_track_sojourn_mean_ms":                    meanNSAsMSV31(fastSojournNS, fastAttemptCount),
		"metatrack_conservative_track_sojourn_mean_ms":            meanNSAsMSV31(conservativeSojournNS, conservativeAttemptCount),
		"metatrack_fast_track_sojourn_p95_ms":                     durationPercentileNSAsMSV31(fastSojournValuesNS, 0.95),
		"metatrack_fast_track_sojourn_p99_ms":                     durationPercentileNSAsMSV31(fastSojournValuesNS, 0.99),
		"metatrack_conservative_track_sojourn_p95_ms":             durationPercentileNSAsMSV31(conservativeSojournValuesNS, 0.95),
		"metatrack_conservative_track_sojourn_p99_ms":             durationPercentileNSAsMSV31(conservativeSojournValuesNS, 0.99),
		"metatrack_fast_state_wait_sum_ms":                        nsToMSV31(fastStateWaitNS),
		"metatrack_conservative_state_wait_sum_ms":                nsToMSV31(conservativeStateWaitNS),
		"metatrack_fast_dependency_wait_sum_ms":                   nsToMSV31(fastDependencyWaitNS),
		"metatrack_conservative_dependency_wait_sum_ms":           nsToMSV31(conservativeDependencyWaitNS),
		"metatrack_fast_queue_wait_sum_ms":                        nsToMSV31(fastQueueWaitNS),
		"metatrack_conservative_queue_wait_sum_ms":                nsToMSV31(conservativeQueueWaitNS),
		"metatrack_attempt_timing_precision":                      "nanosecond_monotonic",
		"metatrack_track_timing_truth_scope":                      "per_attempt_monotonic_nanosecond_business_and_track_sojourn;state_and_dependency_wait_components_may_overlap;parallel_attempt_sums_are_not_wall_clock",
	}
}

func durationPercentileNSAsMSV31(values []int64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered) == 1 {
		return nsToMSV31(ordered[0])
	}
	position := quantile * float64(len(ordered)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(ordered) {
		upper = lower
	}
	weight := position - float64(lower)
	valueNS := float64(ordered[lower])*(1.0-weight) + float64(ordered[upper])*weight
	return valueNS / 1_000_000.0
}

func nsToMSV31(value int64) float64 {
	return float64(value) / 1_000_000.0
}

func meanNSAsMSV31(total int64, count int) float64 {
	if count <= 0 {
		return 0
	}
	return nsToMSV31(total) / float64(count)
}

func maxInt64V31(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
