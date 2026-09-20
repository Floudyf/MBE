package main

import (
	"encoding/json"
	"testing"
)

func TestMetaTrackV12NodeSummaryDecodesEffectiveFrontierEvidence(t *testing.T) {
	raw := []byte(`{
		"node_id":"n0",
		"shard_id":"s0",
		"metatrack_effective_frontier_policy":"inflight_exact_value_transitive_reduction_v1",
		"metatrack_effective_frontier_width_zero_count":5,
		"metatrack_effective_frontier_width_one_count":41,
		"metatrack_effective_frontier_width_multi_count":4,
		"metatrack_effective_frontier_width_max":2,
		"metatrack_effective_frontier_raw_producer_count":52,
		"metatrack_effective_frontier_reduced_producer_count":7,
		"metatrack_effective_frontier_track_demotion_count":4
	}`)
	var got v5NodeSummary
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.MetaTrackEffectiveFrontierPolicy != "inflight_exact_value_transitive_reduction_v1" ||
		got.MetaTrackEffectiveFrontierWidthZeroCount != 5 ||
		got.MetaTrackEffectiveFrontierWidthOneCount != 41 ||
		got.MetaTrackEffectiveFrontierWidthMultiCount != 4 ||
		got.MetaTrackEffectiveFrontierWidthMax != 2 ||
		got.MetaTrackEffectiveFrontierRawProducerCount != 52 ||
		got.MetaTrackEffectiveFrontierReducedProducerCount != 7 ||
		got.MetaTrackEffectiveFrontierTrackDemotionCount != 4 {
		t.Fatalf("effective-frontier node evidence lost: %#v", got)
	}
}
