package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/v5"
)

func writeV38BlockSTMSummary(t *testing.T, dir string, suspend int, scheduler, waitMode string, consistent bool) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"serial_equivalent":               true,
		"scheduler_mode_consistent":       consistent,
		"dependency_wait_mode_consistent": consistent,
		"block_stm_metrics": map[string]any{
			"worker_count":             4,
			"dependency_wait_count":    suspend,
			"dependency_resume_count":  suspend,
			"dependency_suspend_count": suspend,
			"scheduler_mode":           scheduler,
			"dependency_wait_mode":     waitMode,
		},
	}
	raw, _ := json.Marshal(payload)
	if err := os.WriteFile(filepath.Join(dir, "block_stm_summary.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestV38AggregateBlockSTMPreservesSuspendAndModeTruth(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "n0")
	b := filepath.Join(root, "n1")
	writeV38BlockSTMSummary(t, a, 6, "priority_heap_v1", "suspend_same_incarnation_v1", true)
	writeV38BlockSTMSummary(t, b, 6, "priority_heap_v1", "suspend_same_incarnation_v1", true)
	summary, err := aggregateBlockSTM([]v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: a}, {NodeID: "n1", ShardID: "s0", DataDir: b}})
	if err != nil {
		t.Fatal(err)
	}
	if summary["dependency_suspend_count"] != 12 {
		t.Fatalf("suspend count lost: %#v", summary)
	}
	if summary["scheduler_mode"] != "priority_heap_v1" || summary["dependency_wait_mode"] != "suspend_same_incarnation_v1" {
		t.Fatalf("mode truth lost: %#v", summary)
	}
	if summary["scheduler_mode_replica_consistent"] != true || summary["dependency_wait_mode_replica_consistent"] != true {
		t.Fatalf("mode consistency lost: %#v", summary)
	}
	if summary["metric_truth_version"] != "mbe_block_stm_metric_truth_v38_0_3" {
		t.Fatalf("truth version missing: %#v", summary)
	}
}

func TestV38AggregateBlockSTMRejectsNodeInternalModeInconsistency(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "n0")
	b := filepath.Join(root, "n1")
	writeV38BlockSTMSummary(t, a, 2, "priority_heap_v1", "suspend_same_incarnation_v1", false)
	writeV38BlockSTMSummary(t, b, 2, "priority_heap_v1", "suspend_same_incarnation_v1", true)
	summary, err := aggregateBlockSTM([]v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: a}, {NodeID: "n1", ShardID: "s0", DataDir: b}})
	if err != nil {
		t.Fatal(err)
	}
	if summary["scheduler_mode_replica_consistent"] != false || summary["dependency_wait_mode_replica_consistent"] != false {
		t.Fatalf("internal inconsistency was hidden: %#v", summary)
	}
}

// writeV38LegacyBlockSTMSummary models the exact pre-v38 artifact contract:
// scheduler/wait modes are present inside block_stm_metrics, while the newer
// top-level *_consistent booleans do not exist yet.
func writeV38LegacyBlockSTMSummary(t *testing.T, dir string, suspend int, scheduler, waitMode string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"serial_equivalent": true,
		"block_stm_metrics": map[string]any{
			"worker_count":             4,
			"dependency_wait_count":    suspend,
			"dependency_resume_count":  suspend,
			"dependency_suspend_count": suspend,
			"scheduler_mode":           scheduler,
			"dependency_wait_mode":     waitMode,
		},
	}
	raw, _ := json.Marshal(payload)
	if err := os.WriteFile(filepath.Join(dir, "block_stm_summary.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestV38AggregateBlockSTMLegacyV37ModeTruthCompatible(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "n0")
	b := filepath.Join(root, "n1")
	writeV38LegacyBlockSTMSummary(t, a, 5, "priority_heap_v1", "suspend_same_incarnation_v1")
	writeV38LegacyBlockSTMSummary(t, b, 6, "priority_heap_v1", "suspend_same_incarnation_v1")
	summary, err := aggregateBlockSTM([]v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: a}, {NodeID: "n1", ShardID: "s0", DataDir: b}})
	if err != nil {
		t.Fatal(err)
	}
	if summary["scheduler_mode"] != "priority_heap_v1" || summary["dependency_wait_mode"] != "suspend_same_incarnation_v1" {
		t.Fatalf("legacy v37 mode truth lost: %#v", summary)
	}
	if summary["scheduler_mode_replica_consistent"] != true || summary["dependency_wait_mode_replica_consistent"] != true {
		t.Fatalf("legacy v37 same-mode replicas must remain consistent: %#v", summary)
	}
	per, _ := summary["per_validator"].([]map[string]any)
	if len(per) != 2 || per[0]["scheduler_mode_consistent"] != true || per[1]["scheduler_mode_consistent"] != true {
		t.Fatalf("legacy v37 per-validator consistency inference lost: %#v", summary)
	}
}

func TestV38AggregateBlockSTMLegacyMissingModeFailsClosed(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "n0")
	b := filepath.Join(root, "n1")
	writeV38LegacyBlockSTMSummary(t, a, 2, "priority_heap_v1", "suspend_same_incarnation_v1")
	writeV38LegacyBlockSTMSummary(t, b, 2, "", "")
	summary, err := aggregateBlockSTM([]v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: a}, {NodeID: "n1", ShardID: "s0", DataDir: b}})
	if err != nil {
		t.Fatal(err)
	}
	if summary["scheduler_mode_replica_consistent"] != false || summary["dependency_wait_mode_replica_consistent"] != false {
		t.Fatalf("legacy missing mode must fail closed: %#v", summary)
	}
}
