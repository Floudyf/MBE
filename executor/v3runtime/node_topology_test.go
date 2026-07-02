package v3runtime

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultLogicalNodeTopologyGeneratesDeterministicNodes(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	nodes := GenerateLogicalNodes(cfg)
	if len(nodes) != 25 {
		t.Fatalf("expected 25 logical nodes, got %d", len(nodes))
	}
	artifacts := NodeRuntimeArtifacts{Nodes: nodes}
	if artifacts.CountRole("validator") != 16 {
		t.Fatalf("expected 16 validators, got %d", artifacts.CountRole("validator"))
	}
	if artifacts.CountRole("executor") != 4 {
		t.Fatalf("expected 4 executors, got %d", artifacts.CountRole("executor"))
	}
	if artifacts.CountRole("storage") != 4 {
		t.Fatalf("expected 4 storage nodes, got %d", artifacts.CountRole("storage"))
	}
	if artifacts.CountRole("supervisor") != 1 {
		t.Fatalf("expected 1 supervisor, got %d", artifacts.CountRole("supervisor"))
	}
	if nodes[0].LogicalAddress != "logical://shard-0/validator-0" {
		t.Fatalf("unexpected first logical address: %s", nodes[0].LogicalAddress)
	}
}

func TestNodeTopologyArtifactsCanWriteCSV(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	blocks := []Block{{Height: 1, CutTimeMS: 100}}
	consensus := []ConsensusRecord{{BlockHeight: 1, ConsensusStartTimeMS: 101, ViewID: 0, SequenceID: 1}}
	artifacts := BuildLogicalNodeArtifacts(cfg, blocks, consensus)
	dir := t.TempDir()
	if err := writeNodeTopologyCSV(filepath.Join(dir, "node_topology.csv"), artifacts.Nodes); err != nil {
		t.Fatal(err)
	}
	if err := writeNodeLogCSV(filepath.Join(dir, "node_log.csv"), artifacts.NodeEvents); err != nil {
		t.Fatal(err)
	}
	if err := writeNetworkLogCSV(filepath.Join(dir, "network_log.csv"), artifacts.NetworkMessages); err != nil {
		t.Fatal(err)
	}
	if err := writeConsensusMessageLogCSV(filepath.Join(dir, "consensus_message_log.csv"), artifacts.ConsensusMessages); err != nil {
		t.Fatal(err)
	}
	header := readCSVHeader(t, filepath.Join(dir, "node_topology.csv"))
	expected := []string{"node_id", "shard_id", "node_index", "role", "consensus_domain_id", "execution_shard_id", "state_storage_unit_id", "logical_address", "runtime_mode", "network_mode", "status"}
	for i, field := range expected {
		if header[i] != field {
			t.Fatalf("node_topology header[%d] = %s, want %s", i, header[i], field)
		}
	}
	if len(artifacts.NetworkMessages) == 0 || len(artifacts.ConsensusMessages) == 0 {
		t.Fatalf("expected deterministic network and consensus messages")
	}
}

func TestRunWritesNodeTopologySummaryAndArtifacts(t *testing.T) {
	out := filepath.Join(t.TempDir(), "run")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "node_topology_run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.LogicalNodeCount != 25 || result.Summary.ValidatorNodeCount != 16 {
		t.Fatalf("unexpected node metrics: %+v", result.Summary)
	}
	if result.Summary.LaunchableNodeCount != 25 || result.Summary.NodeAddressCount != 25 || !result.Summary.LauncherPreviewOnly {
		t.Fatalf("unexpected launcher metrics: %+v", result.Summary)
	}
	if !result.Summary.NodeProcessEntrypointAvailable || !result.Summary.NodeProcessPreviewOnly {
		t.Fatalf("unexpected node process metrics: %+v", result.Summary)
	}
	if result.Summary.NetworkAdapterSelected != "in_memory_message_bus" || result.Summary.TypedMessageCount == 0 {
		t.Fatalf("unexpected network adapter metrics: %+v", result.Summary)
	}
	if !result.Summary.ConsensusOverNetworkEnabled || result.Summary.ProposalPreviewCount == 0 || result.Summary.VotePreviewCount == 0 {
		t.Fatalf("unexpected consensus network metrics: %+v", result.Summary)
	}
	for _, name := range []string{"node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv", "node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md", "node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log", "tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv", "consensus_network_light_log.csv", "network_consensus_summary.json", "pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
}

func readCSVHeader(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	header, err := csv.NewReader(file).Read()
	if err != nil {
		t.Fatal(err)
	}
	return header
}
