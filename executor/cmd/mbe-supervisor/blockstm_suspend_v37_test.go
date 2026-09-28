package main

import (
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/v5"
)

func TestV37AggregateBlockSTMSuspensionTruth(t *testing.T) {
	root := t.TempDir()
	nodeA := filepath.Join(root, "nodes", "n0")
	nodeB := filepath.Join(root, "nodes", "n1")
	for _, dir := range []string{nodeA, nodeB} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(dir string, suspend, abort, validation int) {
		writeJSON(t, filepath.Join(dir, "block_stm_summary.json"), map[string]any{
			"serial_equivalent": true,
			"block_stm_metrics": map[string]any{
				"worker_count":                4,
				"maximum_parallel_width":      3,
				"abort_count":                 abort,
				"dependency_abort_count":      0,
				"validation_abort_count":      validation,
				"reexecution_count":           abort,
				"dependency_wait_count":       suspend,
				"dependency_resume_count":     suspend,
				"dependency_suspend_count":    suspend,
				"scheduler_mode":              "priority_heap_v1",
				"dependency_wait_mode":        "suspend_same_incarnation_v1",
				"committed_transaction_count": 10,
			},
		})
	}
	write(nodeA, 5, 2, 2)
	write(nodeB, 6, 3, 3)

	summary, err := aggregateBlockSTM([]v5.NodePlan{
		{NodeID: "n0", ShardID: "s0", DataDir: nodeA},
		{NodeID: "n1", ShardID: "s0", DataDir: nodeB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary["dependency_suspend_count"] != 11 {
		t.Fatalf("suspension total lost: %#v", summary)
	}
	if summary["dependency_abort_count"] != 0 || summary["validation_abort_count"] != 5 || summary["abort_count"] != 5 || summary["abort_decomposition_consistent"] != true {
		t.Fatalf("suspension was incorrectly folded into abort truth: %#v", summary)
	}
	if summary["scheduler_mode"] != "priority_heap_v1" || summary["scheduler_mode_replica_consistent"] != true {
		t.Fatalf("scheduler mode truth lost: %#v", summary)
	}
	if summary["dependency_wait_mode"] != "suspend_same_incarnation_v1" || summary["dependency_wait_mode_replica_consistent"] != true {
		t.Fatalf("dependency wait mode truth lost: %#v", summary)
	}
}
