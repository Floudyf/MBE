package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/v5"
)

func writeV352BlockSummary(t *testing.T, dir string, blocks []map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"blocks": blocks})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "block_execution_summary.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackReadyRoundObservabilityV352CanonicalReplicaAggregation(t *testing.T) {
	root := t.TempDir()
	a0 := filepath.Join(root, "a0")
	b0 := filepath.Join(root, "b0")
	c1 := filepath.Join(root, "c1")
	writeV352BlockSummary(t, a0, []map[string]any{{
		"metatrack_ready_priority_policy":                  "dependency_influence_h_desc_d_desc_canonical_v1",
		"metatrack_dependency_influence_scheduler_enabled": true,
		"metatrack_ready_round_scheduler_enabled":          true,
		"metatrack_ready_round_control_enabled":            false,
		"arbitration_round_count":                          3,
		"multi_candidate_ready_round_count":                2,
		"competition_arbitration_count":                    2,
		"priority_candidate_set_max":                       5,
		"priority_candidate_set_mean":                      4.5,
		"influence_arbitration_decision_count":             3,
		"influence_changed_choice_count":                   2,
		"ready_round_event_drain_skip_count":               1,
		"cross_round_bypass_count":                         0,
	}})
	// Same shard, lexicographically later node: must not amplify replica counts.
	writeV352BlockSummary(t, b0, []map[string]any{{
		"metatrack_ready_priority_policy":         "dependency_influence_h_desc_d_desc_canonical_v1",
		"metatrack_ready_round_scheduler_enabled": true,
		"arbitration_round_count":                 99,
		"multi_candidate_ready_round_count":       99,
		"competition_arbitration_count":           99,
		"priority_candidate_set_max":              99,
		"priority_candidate_set_mean":             99.0,
		"influence_arbitration_decision_count":    99,
		"influence_changed_choice_count":          99,
		"ready_round_event_drain_skip_count":      99,
		"cross_round_bypass_count":                99,
	}})
	writeV352BlockSummary(t, c1, []map[string]any{{
		"metatrack_ready_priority_policy":                  "dependency_influence_h_desc_d_desc_canonical_v1",
		"metatrack_dependency_influence_scheduler_enabled": true,
		"metatrack_ready_round_scheduler_enabled":          true,
		"arbitration_round_count":                          4,
		"multi_candidate_ready_round_count":                1,
		"competition_arbitration_count":                    1,
		"priority_candidate_set_max":                       3,
		"priority_candidate_set_mean":                      3.0,
		"influence_arbitration_decision_count":             1,
		"influence_changed_choice_count":                   0,
		"ready_round_event_drain_skip_count":               2,
		"cross_round_bypass_count":                         0,
	}})

	got, err := aggregateMetaTrackReadyRoundEvidenceV352([]v5.NodePlan{
		{NodeID: "node-b", ShardID: "shard-0", DataDir: b0},
		{NodeID: "node-a", ShardID: "shard-0", DataDir: a0},
		{NodeID: "node-c", ShardID: "shard-1", DataDir: c1},
	})
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]int{
		"arbitration_round_count":              7,
		"multi_candidate_ready_round_count":    3,
		"competition_arbitration_count":        3,
		"priority_candidate_set_max":           5,
		"influence_arbitration_decision_count": 4,
		"influence_changed_choice_count":       2,
		"ready_round_event_drain_skip_count":   3,
		"cross_round_bypass_count":             0,
	}
	for key, want := range checks {
		if value := readyRoundMetricIntV352(got[key]); value != want {
			t.Fatalf("%s=%d want=%d; all=%#v", key, value, want, got)
		}
	}
	if mean := readyRoundMetricFloatV352(got["priority_candidate_set_mean"]); math.Abs(mean-4.0) > 1e-9 {
		t.Fatalf("priority_candidate_set_mean=%v want=4", mean)
	}
	if got["ready_round_metric_truth_scope"] != "canonical_node_per_shard_from_block_execution_summary" {
		t.Fatalf("unexpected truth scope: %#v", got["ready_round_metric_truth_scope"])
	}
	canonical, ok := got["ready_round_canonical_node_by_shard"].(map[string]string)
	if !ok || canonical["shard-0"] != "node-a" || canonical["shard-1"] != "node-c" {
		t.Fatalf("canonical replica selection wrong: %#v", got["ready_round_canonical_node_by_shard"])
	}
}

func TestMetaTrackReadyRoundObservabilityV352MissingBlockSummaryIsOptional(t *testing.T) {
	root := t.TempDir()
	got, err := aggregateMetaTrackReadyRoundEvidenceV352([]v5.NodePlan{
		{NodeID: "node-a", ShardID: "shard-0", DataDir: filepath.Join(root, "missing-node-dir")},
	})
	if err != nil {
		t.Fatalf("missing optional Ready-Round evidence must not fail generic mechanism aggregation: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("missing optional Ready-Round evidence must be non-applicable, got %#v", got)
	}
}
