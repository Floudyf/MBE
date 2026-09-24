package v5

import "sort"

func metaTrackTrackMetricsFromAttempts(attempts []BusinessExecutionAttempt) map[string]any {
	fastInitial := map[string]bool{}
	conservativeInitial := map[string]bool{}
	fastAttemptCount := 0
	conservativeAttemptCount := 0
	fastFinalCount := 0
	conservativeFinalCount := 0
	fastFallbackCount := 0
	fastDiscardedCount := 0
	conservativeReexecutionCount := 0
	fastDurationUS := int64(0)
	conservativeDurationUS := int64(0)
	fastDiscardedDurationUS := int64(0)
	fastDurationsUS := make([]int64, 0)
	conservativeDurationsUS := make([]int64, 0)

	for _, attempt := range attempts {
		track := attempt.Track
		if track != "fast" {
			track = "conservative"
		}
		durationUS := attempt.DurationUS
		if durationUS < 0 {
			durationUS = 0
		}
		if attempt.Attempt == 1 {
			if track == "fast" {
				fastInitial[attempt.TxID] = true
			} else {
				conservativeInitial[attempt.TxID] = true
			}
		}
		if track == "fast" {
			fastAttemptCount++
			fastDurationUS += durationUS
			fastDurationsUS = append(fastDurationsUS, durationUS)
			if attempt.FinalCompletion {
				fastFinalCount++
			} else {
				fastFallbackCount++
				fastDiscardedCount++
				fastDiscardedDurationUS += durationUS
			}
			continue
		}
		conservativeAttemptCount++
		conservativeDurationUS += durationUS
		conservativeDurationsUS = append(conservativeDurationsUS, durationUS)
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
	fastMeanMS := 0.0
	if fastAttemptCount > 0 {
		fastMeanMS = float64(fastDurationUS) / 1000.0 / float64(fastAttemptCount)
	}
	fastP95MS := durationPercentileMS(fastDurationsUS, 0.95)
	fastP99MS := durationPercentileMS(fastDurationsUS, 0.99)
	conservativeP95MS := durationPercentileMS(conservativeDurationsUS, 0.95)
	conservativeP99MS := durationPercentileMS(conservativeDurationsUS, 0.99)
	conservativeMeanMS := 0.0
	if conservativeAttemptCount > 0 {
		conservativeMeanMS = float64(conservativeDurationUS) / 1000.0 / float64(conservativeAttemptCount)
	}

	return map[string]any{
		"metatrack_fast_initial_tx_count":                         fastInitialCount,
		"metatrack_conservative_initial_tx_count":                 conservativeInitialCount,
		"metatrack_fast_final_tx_count":                           fastFinalCount,
		"metatrack_conservative_final_tx_count":                   conservativeFinalCount,
		"metatrack_fast_business_execution_attempt_count":         fastAttemptCount,
		"metatrack_conservative_business_execution_attempt_count": conservativeAttemptCount,
		"metatrack_fast_business_execution_sum_ms":                float64(fastDurationUS) / 1000.0,
		"metatrack_conservative_business_execution_sum_ms":        float64(conservativeDurationUS) / 1000.0,
		"metatrack_fast_business_execution_mean_ms":               fastMeanMS,
		"metatrack_fast_business_execution_p95_ms":                fastP95MS,
		"metatrack_fast_business_execution_p99_ms":                fastP99MS,
		"metatrack_conservative_business_execution_mean_ms":       conservativeMeanMS,
		"metatrack_conservative_business_execution_p95_ms":        conservativeP95MS,
		"metatrack_conservative_business_execution_p99_ms":        conservativeP99MS,
		"metatrack_fast_fallback_count":                           fastFallbackCount,
		"metatrack_fast_discarded_tentative_count":                fastDiscardedCount,
		"metatrack_fast_discarded_execution_ms":                   float64(fastDiscardedDurationUS) / 1000.0,
		"metatrack_fast_fallback_rate":                            fastFallbackRate,
		"metatrack_conservative_reexecution_count":                conservativeReexecutionCount,
		"metatrack_conservative_reexecution_share":                conservativeReexecutionShare,
		"metatrack_track_timing_truth_scope":                      "sum_of_worker_monotonic_business_attempt_durations;parallel_attempt_sums_are_not_wall_clock",
	}
}

func durationPercentileMS(values []int64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered) == 1 {
		return float64(ordered[0]) / 1000.0
	}
	position := quantile * float64(len(ordered)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(ordered) {
		upper = lower
	}
	weight := position - float64(lower)
	valueUS := float64(ordered[lower])*(1.0-weight) + float64(ordered[upper])*weight
	return valueUS / 1000.0
}
