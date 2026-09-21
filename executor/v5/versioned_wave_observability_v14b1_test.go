package v5

import "testing"

func TestV14B1SummarizeVersionedWaveObservability(t *testing.T) {
	summary := summarizeStateReadyEvidence([]map[string]any{
		{
			"versioned_state_ready_wave_count":   4,
			"versioned_wave_execution_policy":    "delta_only_v1",
			"versioned_wave_delta_only_count":    4,
			"versioned_wave_full_fallback_count": 0,
		},
		{
			"versioned_state_ready_wave_count":   3,
			"versioned_wave_execution_policy":    "delta_only_v1",
			"versioned_wave_delta_only_count":    3,
			"versioned_wave_full_fallback_count": 0,
		},
	})
	if summary.versionedWaveCount != 7 {
		t.Fatalf("versioned wave count changed: %d", summary.versionedWaveCount)
	}
	if summary.versionedWaveExecutionPolicy != "delta_only_v1" {
		t.Fatalf("wave execution policy lost: %q", summary.versionedWaveExecutionPolicy)
	}
	if summary.versionedWaveDeltaOnlyCount != 7 || summary.versionedWaveFullFallbackCount != 0 {
		t.Fatalf("wave execution counts lost: delta=%d fallback=%d", summary.versionedWaveDeltaOnlyCount, summary.versionedWaveFullFallbackCount)
	}
}

func TestV14B1SummarizeMixedWavePolicyIsExplicit(t *testing.T) {
	summary := summarizeStateReadyEvidence([]map[string]any{
		{"versioned_wave_execution_policy": "delta_only_v1", "versioned_wave_delta_only_count": 1},
		{"versioned_wave_execution_policy": "full_block_fallback", "versioned_wave_full_fallback_count": 1},
	})
	if summary.versionedWaveExecutionPolicy != "mixed" {
		t.Fatalf("mixed policy was hidden: %q", summary.versionedWaveExecutionPolicy)
	}
	if summary.versionedWaveDeltaOnlyCount != 1 || summary.versionedWaveFullFallbackCount != 1 {
		t.Fatalf("mixed counts changed: %+v", summary)
	}
}
