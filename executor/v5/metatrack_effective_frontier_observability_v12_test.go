package v5

import "testing"

func TestMetaTrackV12ClassificationEvidenceAggregatesEffectiveFrontierMetrics(t *testing.T) {
	blocks := []map[string]any{
		{
			"metatrack_effective_frontier_policy":                 "inflight_exact_value_transitive_reduction_v1",
			"metatrack_effective_frontier_width_zero_count":       4,
			"metatrack_effective_frontier_width_one_count":        7,
			"metatrack_effective_frontier_width_multi_count":      2,
			"metatrack_effective_frontier_width_max":              2,
			"metatrack_effective_frontier_raw_producer_count":     12,
			"metatrack_effective_frontier_reduced_producer_count": 3,
			"metatrack_effective_frontier_track_demotion_count":   2,
		},
		{
			"metatrack_effective_frontier_policy":                 "inflight_exact_value_transitive_reduction_v1",
			"metatrack_effective_frontier_width_zero_count":       5,
			"metatrack_effective_frontier_width_one_count":        3,
			"metatrack_effective_frontier_width_multi_count":      1,
			"metatrack_effective_frontier_width_max":              3,
			"metatrack_effective_frontier_raw_producer_count":     10,
			"metatrack_effective_frontier_reduced_producer_count": 4,
			"metatrack_effective_frontier_track_demotion_count":   1,
		},
	}
	got := summarizeMetaTrackClassificationEvidence(blocks)
	if got.effectiveFrontierPolicy != "inflight_exact_value_transitive_reduction_v1" {
		t.Fatalf("policy=%q", got.effectiveFrontierPolicy)
	}
	if got.effectiveFrontierWidthZeroCount != 9 ||
		got.effectiveFrontierWidthOneCount != 10 ||
		got.effectiveFrontierWidthMultiCount != 3 ||
		got.effectiveFrontierWidthMax != 3 ||
		got.effectiveFrontierRawProducerCount != 22 ||
		got.effectiveFrontierReducedProducerCount != 7 ||
		got.effectiveFrontierTrackDemotionCount != 3 {
		t.Fatalf("unexpected V12 effective-frontier evidence: %#v", got)
	}
}
