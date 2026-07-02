package v3runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLauncherPreviewDefaultTopologyGeneratesAddressTable(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), []Block{{Height: 1, CutTimeMS: 100}}, []ConsensusRecord{{BlockHeight: 1, ConsensusStartTimeMS: 101}})
	launcher := BuildLauncherPreview(nodeRuntime)
	if len(launcher.Addresses) != 25 {
		t.Fatalf("expected 25 launcher entries, got %d", len(launcher.Addresses))
	}
	if launcher.Addresses[0].PreviewHost != "127.0.0.1" || launcher.Addresses[0].PreviewPort != 9100 {
		t.Fatalf("unexpected first preview address: %+v", launcher.Addresses[0])
	}
	if !strings.Contains(launcher.Addresses[0].LaunchCommandWindows, "--preview-only") {
		t.Fatalf("windows command must be preview-only: %s", launcher.Addresses[0].LaunchCommandWindows)
	}
}

func TestLauncherPreviewArtifactsAndTruthBoundary(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), []Block{{Height: 1, CutTimeMS: 100}}, []ConsensusRecord{{BlockHeight: 1, ConsensusStartTimeMS: 101}})
	launcher := BuildLauncherPreview(nodeRuntime)
	dir := t.TempDir()
	if err := writeLauncherPreviewArtifacts(dir, nodeRuntime, launcher); err != nil {
		t.Fatal(err)
	}
	header := readCSVHeader(t, filepath.Join(dir, "node_address_table.csv"))
	expected := []string{"node_id", "shard_id", "node_index", "role", "logical_address", "preview_host", "preview_port", "process_name", "launch_command_windows", "launch_command_linux", "runtime_mode", "network_mode", "network_adapter", "status"}
	for i, field := range expected {
		if header[i] != field {
			t.Fatalf("node_address_table header[%d] = %s, want %s", i, header[i], field)
		}
	}
	topologyBytes, err := os.ReadFile(filepath.Join(dir, "topology.json"))
	if err != nil {
		t.Fatal(err)
	}
	var topology map[string]any
	if err := json.Unmarshal(topologyBytes, &topology); err != nil {
		t.Fatal(err)
	}
	if topology["runtime_truth"] != "localhost_tcp_typed_message_preview_not_real_pbft" {
		t.Fatalf("unexpected topology runtime truth: %v", topology["runtime_truth"])
	}
	windowsBytes, _ := os.ReadFile(filepath.Join(dir, "launch_nodes_windows.bat"))
	if !strings.Contains(string(windowsBytes), "start cmd /k") || !strings.Contains(string(windowsBytes), "Not real PBFT") {
		t.Fatalf("windows launcher missing preview command or warning")
	}
	linuxBytes, _ := os.ReadFile(filepath.Join(dir, "launch_nodes_linux.sh"))
	if !strings.Contains(string(linuxBytes), "go run ./cmd/replay --mode node-preview") || !strings.Contains(string(linuxBytes), "--preview-only") {
		t.Fatalf("linux launcher missing preview command")
	}
	readmeBytes, _ := os.ReadFile(filepath.Join(dir, "launcher_readme.md"))
	if !strings.Contains(string(readmeBytes), "go run ./cmd/replay --mode node-preview") || !strings.Contains(string(readmeBytes), "not BlockEmulator backend") {
		t.Fatalf("launcher readme missing preview-only boundary")
	}
}
