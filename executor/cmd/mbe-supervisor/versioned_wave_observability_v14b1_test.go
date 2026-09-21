package main

import (
	"encoding/json"
	"testing"
)

func TestV14B1NodeSummaryCarriesVersionedWaveEvidence(t *testing.T) {
	raw := []byte(`{
		"node_id":"n0",
		"shard_id":"s0",
		"versioned_wave_execution_policy":"delta_only_v1",
		"versioned_wave_delta_only_count":17,
		"versioned_wave_full_fallback_count":0
	}`)
	var summary v5NodeSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.VersionedWaveExecutionPolicy != "delta_only_v1" {
		t.Fatalf("wave policy dropped: %#v", summary)
	}
	if summary.VersionedWaveDeltaOnlyCount != 17 || summary.VersionedWaveFullFallbackCount != 0 {
		t.Fatalf("wave counts dropped: %#v", summary)
	}
}
