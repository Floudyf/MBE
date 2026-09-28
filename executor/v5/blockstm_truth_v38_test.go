package v5

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/execution"
)

func TestV38BlockSTMNodeSummaryPreservesSuspendAndModes(t *testing.T) {
	dataDir := t.TempDir()
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", DataDir: dataDir}}
	blocks := []map[string]any{
		{"block_hash": "b1", "height": 1, "block_executor_id": execution.BlockSTMExecutorID, "serial_equivalent": true, "block_stm_metrics": map[string]any{
			"worker_count": 4, "dependency_wait_count": 5, "dependency_resume_count": 5, "dependency_suspend_count": 5,
			"scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1",
		}},
		{"block_hash": "b2", "height": 2, "block_executor_id": execution.BlockSTMExecutorID, "serial_equivalent": true, "block_stm_metrics": map[string]any{
			"worker_count": 4, "dependency_wait_count": 7, "dependency_resume_count": 7, "dependency_suspend_count": 7,
			"scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1",
		}},
	}
	if err := runtime.writeBlockSTMArtifacts(blocks); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dataDir, "block_stm_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary map[string]any
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	metrics := summary["block_stm_metrics"].(map[string]any)
	if int(metrics["dependency_suspend_count"].(float64)) != 12 {
		t.Fatalf("suspend count lost: %#v", metrics)
	}
	if metrics["scheduler_mode"] != "priority_heap_v1" || metrics["dependency_wait_mode"] != "suspend_same_incarnation_v1" {
		t.Fatalf("mode truth lost: %#v", metrics)
	}
	if summary["scheduler_mode_consistent"] != true || summary["dependency_wait_mode_consistent"] != true {
		t.Fatalf("mode consistency missing: %#v", summary)
	}

	file, err := os.Open(filepath.Join(dataDir, "block_stm_dependency_trace.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"node_id", "shard_id", "block_hash", "height", "dependency_wait_count", "dependency_resume_count", "dependency_suspend_count", "scheduler_mode", "dependency_wait_mode", "estimate_count"}
	if len(rows) != 3 || !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("dependency trace schema mismatch: %#v", rows)
	}
	if rows[1][6] != "5" || rows[2][6] != "7" {
		t.Fatalf("dependency suspend trace mismatch: %#v", rows)
	}
}

func TestV38BlockSTMNodeSummaryFailsModeConsistencyClosed(t *testing.T) {
	dataDir := t.TempDir()
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", DataDir: dataDir}}
	blocks := []map[string]any{
		{"block_hash": "b1", "height": 1, "block_executor_id": execution.BlockSTMExecutorID, "serial_equivalent": true, "block_stm_metrics": map[string]any{"worker_count": 4, "scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1"}},
		{"block_hash": "b2", "height": 2, "block_executor_id": execution.BlockSTMExecutorID, "serial_equivalent": true, "block_stm_metrics": map[string]any{"worker_count": 4, "scheduler_mode": "legacy_scan_v1", "dependency_wait_mode": "abort_new_incarnation_v1"}},
	}
	if err := runtime.writeBlockSTMArtifacts(blocks); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dataDir, "block_stm_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary map[string]any
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	if summary["scheduler_mode_consistent"] != false || summary["dependency_wait_mode_consistent"] != false {
		t.Fatalf("mixed modes were silently accepted: %#v", summary)
	}
	metrics := summary["block_stm_metrics"].(map[string]any)
	if value, ok := metrics["scheduler_mode"]; ok && value != "" {
		t.Fatalf("mixed scheduler mode must not choose a winner: %#v", metrics)
	}
}
