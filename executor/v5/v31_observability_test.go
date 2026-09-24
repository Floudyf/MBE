package v5

import "testing"

func TestV31MetaTrackNanosecondTrackMetrics(t *testing.T) {
	metrics := metaTrackTrackMetricsFromAttemptsV31([]BusinessExecutionAttempt{
		{TxID: "f1", Track: "fast", Attempt: 1, FinalCompletion: true, DurationNS: 500, SojournNS: 10_000_000, StateWaitNS: 2_000_000, DependencyWaitNS: 3_000_000, QueueWaitNS: 1_000_000},
		{TxID: "f2", Track: "fast", Attempt: 1, FinalCompletion: false, DurationNS: 1_500, SojournNS: 20_000_000, StateWaitNS: 4_000_000, DependencyWaitNS: 5_000_000, QueueWaitNS: 2_000_000},
		{TxID: "f2", Track: "conservative", Attempt: 2, FinalCompletion: true, DurationNS: 2_500, SojournNS: 7_000_000, QueueWaitNS: 1_500_000},
		{TxID: "c1", Track: "conservative", Attempt: 1, FinalCompletion: true, DurationNS: 3_500, SojournNS: 30_000_000, StateWaitNS: 6_000_000, DependencyWaitNS: 7_000_000, QueueWaitNS: 2_500_000},
	})
	if metrics["metatrack_fast_initial_tx_count"] != 2 || metrics["metatrack_conservative_initial_tx_count"] != 1 {
		t.Fatalf("unexpected initial counts: %#v", metrics)
	}
	if metrics["metatrack_fast_fallback_count"] != 1 || metrics["metatrack_conservative_reexecution_count"] != 1 {
		t.Fatalf("unexpected fallback/reexecution metrics: %#v", metrics)
	}
	if got := metrics["metatrack_fast_business_execution_sum_ms"].(float64); got != 0.002 {
		t.Fatalf("fast ns sum conversion=%v", got)
	}
	if got := metrics["metatrack_conservative_business_execution_sum_ms"].(float64); got != 0.006 {
		t.Fatalf("conservative ns sum conversion=%v", got)
	}
	if got := metrics["metatrack_fast_track_sojourn_sum_ms"].(float64); got != 30.0 {
		t.Fatalf("fast sojourn sum=%v", got)
	}
	if got := metrics["metatrack_conservative_track_sojourn_sum_ms"].(float64); got != 37.0 {
		t.Fatalf("conservative sojourn sum=%v", got)
	}
	if got := metrics["metatrack_fast_state_wait_sum_ms"].(float64); got != 6.0 {
		t.Fatalf("fast state wait sum=%v", got)
	}
	if got := metrics["metatrack_conservative_queue_wait_sum_ms"].(float64); got != 4.0 {
		t.Fatalf("conservative queue wait sum=%v", got)
	}
	if got := metrics["metatrack_fast_business_execution_p95_ms"].(float64); got <= 0 || got >= 0.01 {
		t.Fatalf("nanosecond p95 lost precision: %v", got)
	}
	if got := metrics["metatrack_fast_track_sojourn_p99_ms"].(float64); got <= 10.0 {
		t.Fatalf("fast sojourn p99=%v", got)
	}
	if metrics["metatrack_attempt_timing_precision"] != "nanosecond_monotonic" {
		t.Fatalf("timing precision marker missing: %#v", metrics)
	}
}
