package v3runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPBFTPreviewOverInMemoryNetworkAdapterWritesTypedArtifacts(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), []Block{{Height: 1, Hash: "block_1", CutTimeMS: 10}}, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}})
	launcher := BuildLauncherPreview(nodeRuntime)
	adapter := RunNetworkAdapterPreview(launcher)
	pbft := RunPBFTPreview(nodeRuntime, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}}, ConsensusRuntimeBlockEmulatorPBFTPreview)
	preview := RunPBFTNetworkPreview(nodeRuntime, launcher, adapter, pbft)
	if !preview.Enabled || preview.NetworkPath != PBFTNetworkPathInMemory {
		t.Fatalf("expected enabled in-memory PBFT network preview: %+v", preview)
	}
	if preview.MessageCount != len(pbft.MessageRows) || preview.PrepareNetworkCount != pbft.PrepareCount || preview.CommitNetworkCount != pbft.CommitCount {
		t.Fatalf("PBFT network counts should mirror state machine messages: pbft=%+v network=%+v", pbft, preview)
	}
	seen := map[string]bool{}
	for _, msg := range preview.TypedMessages {
		seen[msg.MessageType] = true
	}
	for _, messageType := range []string{"pbft_preprepare", "pbft_prepare", "pbft_commit", "pbft_finalized"} {
		if !seen[messageType] {
			t.Fatalf("missing typed PBFT message %s in %+v", messageType, seen)
		}
	}
	dir := t.TempDir()
	if err := WritePBFTNetworkArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	assertCSVFields(t, filepath.Join(dir, "consensus_network_log.csv"), []string{"event_id", "time_ms", "consensus_runtime", "network_adapter", "consensus_network_path", "message_id", "message_type", "from_node_id", "to_node_id", "shard_id", "view", "sequence_id", "block_height", "quorum_threshold", "quorum_reached", "status", "error", "details"})
	summaryBytes, err := os.ReadFile(filepath.Join(dir, "pbft_network_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summaryBytes), "blockemulator_aligned_pbft_preview_over_network_not_production_pbft") {
		t.Fatalf("summary missing truth boundary:\n%s", string(summaryBytes))
	}
}

func TestPBFTPreviewOverLocalhostTCPNetworkAdapterUsesTCPPath(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.NetworkMode = NetworkAdapterLocalhostTCPPreview
	cfg.NetworkAdapter = NetworkAdapterLocalhostTCPPreview
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, []Block{{Height: 1, Hash: "block_1", CutTimeMS: 10}}, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}})
	launcher := BuildLauncherPreview(nodeRuntime)
	adapter := RunNetworkAdapterPreview(launcher)
	pbft := RunPBFTPreview(nodeRuntime, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}}, ConsensusRuntimeBlockEmulatorPBFTPreview)
	preview := RunPBFTNetworkPreview(nodeRuntime, launcher, adapter, pbft)
	if preview.NetworkAdapterSelected != NetworkAdapterLocalhostTCPPreview || preview.NetworkPath != PBFTNetworkPathTCP {
		t.Fatalf("expected localhost TCP PBFT network path: %+v", preview)
	}
	if preview.MessageCount == 0 || preview.NetworkQuorumReachedCount == 0 {
		t.Fatalf("expected PBFT network typed messages and quorum summary: %+v", preview)
	}
}

func TestRuntimeSummaryIncludesPBFTNetworkPreviewMetrics(t *testing.T) {
	temp := t.TempDir()
	pluginPath := filepath.Join(temp, "plugin.yaml")
	plugin := `profile_type: plugin_profile
plugin_profile_id: pbft_network_profile
version: v3
stage: V3.7.2
plugins:
  TxPoolPlugin: fifo_pool
  BlockProducer: time_or_count_block_producer
  ConsensusPlugin: pbft_light_model
  ConsensusRuntimePlugin: blockemulator_aligned_pbft_preview
  ShardingPlugin: hash_sharding
  ExecutionSchedulerPlugin: serial_execution
  StateAccessPlugin: direct_fetch
  CommitPlugin: normal_commit
  MetricsPlugin: basic_metrics
`
	if err := os.WriteFile(pluginPath, []byte(plugin), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(temp, "out")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "pbft_network_profile",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "pbft_network_run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Summary.PBFTOverNetworkEnabled || result.Summary.PBFTNetworkMessageCount == 0 || result.Summary.PBFTNetworkQuorumReachedCount == 0 {
		t.Fatalf("missing PBFT over network summary metrics: %+v", result.Summary)
	}
	for _, name := range []string{"consensus_network_log.csv", "pbft_network_summary.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
}
