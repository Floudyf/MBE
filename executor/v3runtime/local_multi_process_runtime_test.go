package v3runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalMultiProcessDryRunWritesArtifactsAndCappedSummary(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.NodeRuntimeMode = NodeRuntimeLocalMultiProcess
	cfg.ShardCount = 2
	cfg.ValidatorsPerShard = 2
	cfg.ExecutorsPerShard = 1
	cfg.StorageNodesPerShard = 1
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, nil, nil)
	preview := RunLocalMultiProcessRuntime(nodeRuntime, ExperimentProfile{
		ProcessRuntimeMode: ProcessRuntimeDryRun,
		MaxLocalProcesses:  3,
	})
	if !preview.Enabled || preview.ProcessRuntimeMode != ProcessRuntimeDryRun {
		t.Fatalf("unexpected local process preview: %+v", preview)
	}
	if !preview.Capped || preview.CappedProcessCount == 0 || len(preview.Plan) != 3 {
		t.Fatalf("max_local_processes guard did not cap: %+v", preview)
	}
	if preview.StartedProcessCount != 0 || preview.RuntimeRealismTruth != RuntimeRealismTruth {
		t.Fatalf("dry run should not start processes and must carry truth: %+v", preview)
	}
	dir := t.TempDir()
	if err := WriteLocalMultiProcessArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"address_table.json", "multi_process_manifest.json", "node_process_log.csv", "node_lifecycle_log.csv", "network_message_log.csv", "node_process_status.json", "local_multi_process_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
}

func TestLogicalSingleProcessSkipsLocalMultiProcessArtifacts(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), nil, nil)
	preview := RunLocalMultiProcessRuntime(nodeRuntime, ExperimentProfile{})
	if preview.Enabled || preview.PlannedProcessCount != 0 {
		t.Fatalf("logical mode must not enable local multi-process runtime: %+v", preview)
	}
	dir := t.TempDir()
	if err := WriteLocalMultiProcessArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "multi_process_manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("logical mode should not write multi-process manifest, err=%v", err)
	}
}
