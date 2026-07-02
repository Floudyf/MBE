package v3runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPBFTPreviewGeneratesStateMachineArtifacts(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), []Block{{Height: 1, Hash: "block_1", CutTimeMS: 10}}, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}})
	preview := RunPBFTPreview(nodeRuntime, []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 10, ConsensusFinalizedTimeMS: 13}}, ConsensusRuntimeBlockEmulatorPBFTPreview)
	if !preview.Enabled {
		t.Fatal("PBFT preview should be enabled")
	}
	if preview.QuorumThreshold != 3 {
		t.Fatalf("expected 2f+1 quorum threshold 3 for 4 validators, got %d", preview.QuorumThreshold)
	}
	if preview.PrePrepareCount != 3 || preview.PrepareCount != 4 || preview.CommitCount != 4 {
		t.Fatalf("unexpected PBFT message counts: %+v", preview)
	}
	if preview.QuorumReachedCount != 2 || preview.FinalizedBlockCount != 1 {
		t.Fatalf("unexpected quorum/finalized counts: %+v", preview)
	}
	stages := map[string]bool{}
	messageTypes := map[string]bool{}
	for _, row := range preview.StateRows {
		stages[row.PBFTStage] = true
	}
	for _, row := range preview.MessageRows {
		messageTypes[row.MessageType] = true
	}
	for _, stage := range []string{"PrePrepare", "Prepare", "Commit", "Finalized"} {
		if !stages[stage] {
			t.Fatalf("missing PBFT stage %s in %+v", stage, stages)
		}
	}
	for _, messageType := range []string{"pbft_preprepare", "pbft_prepare", "pbft_commit", "pbft_finalized"} {
		if !messageTypes[messageType] {
			t.Fatalf("missing PBFT message type %s in %+v", messageType, messageTypes)
		}
	}
	dir := t.TempDir()
	if err := WritePBFTPreviewArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	assertCSVFields(t, filepath.Join(dir, "pbft_message_log.csv"), []string{"message_id", "time_ms", "message_type", "from_node_id", "to_node_id", "shard_id", "view", "sequence_id", "block_height", "payload_digest", "status"})
	assertCSVFields(t, filepath.Join(dir, "quorum_log.csv"), []string{"event_id", "shard_id", "leader_node_id", "view", "sequence_id", "block_height", "quorum_type", "validator_count", "fault_tolerance_f", "quorum_threshold", "confirm_count", "quorum_reached"})
	assertCSVFields(t, filepath.Join(dir, "finalized_block_log.csv"), []string{"block_height", "block_hash", "shard_id", "leader_node_id", "view", "sequence_id", "validator_count", "quorum_threshold", "finalized", "status", "runtime_truth"})
}

func TestPBFTPreviewRuntimeSelectionPopulatesSummary(t *testing.T) {
	temp := t.TempDir()
	pluginPath := filepath.Join(temp, "plugin.yaml")
	plugin := `profile_type: plugin_profile
plugin_profile_id: pbft_preview_profile
version: v3
stage: V3.7.1
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
		PluginProfileID:       "pbft_preview_profile",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "pbft_preview_run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.ConsensusRuntimeSelected != ConsensusRuntimeBlockEmulatorPBFTPreview || !result.Summary.PBFTPreviewEnabled {
		t.Fatalf("unexpected PBFT runtime summary: %+v", result.Summary)
	}
	if result.Summary.PBFTQuorumThreshold != 3 || result.Summary.PBFTFinalizedBlockCount == 0 {
		t.Fatalf("unexpected PBFT quorum summary: %+v", result.Summary)
	}
	logBytes, err := os.ReadFile(filepath.Join(out, "runtime.log"))
	if err != nil {
		t.Fatal(err)
	}
	log := string(logBytes)
	if !strings.Contains(log, "production_pbft=false") || !strings.Contains(log, "pbft_over_network_enabled=true") {
		t.Fatalf("runtime log missing PBFT truth boundary:\n%s", log)
	}
}
