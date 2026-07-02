package v3runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTestTopology(t *testing.T, dir string) string {
	t.Helper()
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), []Block{{Height: 1, CutTimeMS: 100}}, []ConsensusRecord{{BlockHeight: 1, ConsensusStartTimeMS: 101}})
	launcher := BuildLauncherPreview(nodeRuntime)
	if err := writeLauncherPreviewArtifacts(dir, nodeRuntime, launcher); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "topology.json")
}

func TestNodeProcessPreviewLoadsTopologyAndWritesArtifacts(t *testing.T) {
	dir := t.TempDir()
	topologyFile := writeTestTopology(t, dir)
	result, err := RunNodeProcessPreview(NodeProcessPreviewInput{
		NodeID:       "shard-0-validator-0",
		Role:         "validator",
		ShardID:      0,
		HasShardID:   true,
		TopologyFile: topologyFile,
		OutputDir:    dir,
		PreviewOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Node.LogicalAddress != "logical://shard-0/validator-0" || result.Node.PreviewPort != 9100 {
		t.Fatalf("unexpected node identity: %+v", result.Node)
	}
	for _, name := range []string{"node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	statusHeader := readCSVHeader(t, filepath.Join(dir, "node_process_status.csv"))
	expected := []string{"node_id", "role", "shard_id", "node_index", "logical_address", "preview_host", "preview_port", "topology_file", "preview_only", "status", "message"}
	for i, field := range expected {
		if statusHeader[i] != field {
			t.Fatalf("status header[%d] = %s, want %s", i, statusHeader[i], field)
		}
	}
	manifestBytes, err := os.ReadFile(filepath.Join(dir, "node_process_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest["runtime_truth"] != "localhost_tcp_typed_message_preview_not_real_pbft" || manifest["tcp_preview_only"] != true || manifest["not_production_network"] != true {
		t.Fatalf("manifest missing truth boundary: %+v", manifest)
	}
	logBytes, _ := os.ReadFile(filepath.Join(dir, "node_process_log_sample.log"))
	logText := string(logBytes)
	if !strings.Contains(logText, "node_id=shard-0-validator-0") || !strings.Contains(logText, "not production networking") {
		t.Fatalf("node process log missing identity or preview warning: %s", logText)
	}
}

func TestNodeProcessPreviewRejectsRoleMismatchAndUnknownNode(t *testing.T) {
	dir := t.TempDir()
	topologyFile := writeTestTopology(t, dir)
	_, err := RunNodeProcessPreview(NodeProcessPreviewInput{NodeID: "shard-0-validator-0", Role: "executor", TopologyFile: topologyFile, PreviewOnly: true})
	if err == nil || !strings.Contains(err.Error(), "role mismatch") {
		t.Fatalf("expected role mismatch, got %v", err)
	}
	_, err = RunNodeProcessPreview(NodeProcessPreviewInput{NodeID: "missing-node", Role: "validator", TopologyFile: topologyFile, PreviewOnly: true})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected unknown node error, got %v", err)
	}
}
