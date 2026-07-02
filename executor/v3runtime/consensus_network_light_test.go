package v3runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsensusLightOverNetworkAdapterProducesProposalVotesAndQuorum(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, []Block{{Height: 1, CutTimeMS: 100}}, []ConsensusRecord{{BlockHeight: 1, SequenceID: 7, ViewID: 0, PluginID: "simple_leader", ConsensusStartTimeMS: 101, ConsensusOrderedTimeMS: 102, Finalized: true}})
	launcher := BuildLauncherPreview(nodeRuntime)
	adapter := RunNetworkAdapterPreview(launcher)

	preview := RunConsensusNetworkLightPreview(nodeRuntime, launcher, adapter, []ConsensusRecord{{BlockHeight: 1, SequenceID: 7, ViewID: 0, PluginID: "simple_leader", ConsensusStartTimeMS: 101, ConsensusOrderedTimeMS: 102, Finalized: true}}, "simple_leader")

	if !preview.Enabled || preview.ConsensusRuntimeSelected != "simple_leader" {
		t.Fatalf("unexpected consensus preview metadata: %+v", preview)
	}
	if preview.NetworkPath != ConsensusNetworkPathInMemory {
		t.Fatalf("unexpected path: %s", preview.NetworkPath)
	}
	if preview.ProposalPreviewCount != 4 || preview.VotePreviewCount != 4 || preview.LightQuorumReachedCount != 1 {
		t.Fatalf("unexpected proposal/vote/quorum metrics: %+v", preview)
	}
	for _, msg := range preview.TypedMessages {
		if strings.HasPrefix(msg.MessageType, "pbft_") || msg.MessageType == "PrePrepare" || msg.MessageType == "Prepare" || msg.MessageType == "Commit" {
			t.Fatalf("V3.6.2 must not emit PBFT stage messages: %+v", msg)
		}
	}
}

func TestConsensusLightUsesLocalhostTCPPreviewPathWhenSelected(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.NetworkMode = NetworkAdapterLocalhostTCPPreview
	cfg.NetworkAdapter = NetworkAdapterLocalhostTCPPreview
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, []Block{{Height: 1, CutTimeMS: 100}}, nil)
	launcher := BuildLauncherPreview(nodeRuntime)
	adapter := RunNetworkAdapterPreview(launcher)

	preview := RunConsensusNetworkLightPreview(nodeRuntime, launcher, adapter, []ConsensusRecord{{BlockHeight: 1, SequenceID: 1, ConsensusStartTimeMS: 10, ConsensusOrderedTimeMS: 11}}, "poa_light")

	if preview.NetworkAdapterSelected != NetworkAdapterLocalhostTCPPreview || preview.NetworkPath != ConsensusNetworkPathTCP {
		t.Fatalf("expected localhost TCP preview path: %+v", preview)
	}
	if preview.ProposalPreviewCount == 0 || preview.VotePreviewCount == 0 {
		t.Fatalf("expected proposal/vote preview messages: %+v", preview)
	}
}

func TestConsensusNetworkLightArtifactsContainTruthBoundary(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, []Block{{Height: 1, CutTimeMS: 100}}, nil)
	launcher := BuildLauncherPreview(nodeRuntime)
	adapter := RunNetworkAdapterPreview(launcher)
	preview := RunConsensusNetworkLightPreview(nodeRuntime, launcher, adapter, []ConsensusRecord{{BlockHeight: 1, SequenceID: 1}}, "pbft_light_model")
	dir := t.TempDir()

	if err := WriteConsensusNetworkLightArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"consensus_network_light_log.csv", "network_consensus_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	header := readCSVHeader(t, filepath.Join(dir, "consensus_network_light_log.csv"))
	expected := []string{"event_id", "time_ms", "network_adapter", "consensus_network_path", "consensus_runtime", "consensus_domain_id", "shard_id", "leader_node_id", "validator_node_id", "message_type", "block_height", "sequence_id", "quorum_target", "vote_count", "light_quorum_reached", "status", "details"}
	for i, field := range expected {
		if header[i] != field {
			t.Fatalf("header[%d] = %s, want %s", i, header[i], field)
		}
	}
	bytes, err := os.ReadFile(filepath.Join(dir, "network_consensus_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary map[string]any
	if err := json.Unmarshal(bytes, &summary); err != nil {
		t.Fatal(err)
	}
	if summary["runtime_truth"] != "network_adapter_consensus_light_preview_not_real_pbft" || summary["not_real_pbft"] != true {
		t.Fatalf("missing truth boundary: %+v", summary)
	}
	if summary["consensus_runtime_selected"] != "pbft_light_model" || summary["not_blockemulator_aligned_pbft"] != true {
		t.Fatalf("pbft_light_model must stay model-based, not BlockEmulator PBFT: %+v", summary)
	}
}
