package v3runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalhostTCPPreviewSendsTypedPingPong(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.NetworkMode = NetworkAdapterLocalhostTCPPreview
	cfg.NetworkAdapter = NetworkAdapterLocalhostTCPPreview
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, []Block{{Height: 1, CutTimeMS: 100}}, nil)
	launcher := BuildLauncherPreview(nodeRuntime)
	launcher.Addresses[0].PreviewPort = 0

	preview := RunNetworkAdapterPreview(launcher)
	if preview.SelectedAdapter != NetworkAdapterLocalhostTCPPreview {
		t.Fatalf("unexpected adapter: %s", preview.SelectedAdapter)
	}
	if !preview.TCPPreview {
		t.Fatalf("expected TCP preview enabled")
	}
	if preview.ListenNodeCount() != 1 {
		t.Fatalf("expected one listening node, got %d", preview.ListenNodeCount())
	}
	if len(preview.SendRows) < 1 || len(preview.ReceiveRows) < 1 || len(preview.TypedMessages) < 2 {
		t.Fatalf("expected ping/pong send/receive rows: %+v", preview)
	}
	if preview.TypedMessages[0].MessageType != "ping" || preview.TypedMessages[1].MessageType != "pong" {
		t.Fatalf("expected ping/pong messages: %+v", preview.TypedMessages)
	}
}

func TestNetworkAdapterArtifactsCanWriteCSV(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.NetworkMode = NetworkAdapterLocalhostTCPPreview
	cfg.NetworkAdapter = NetworkAdapterLocalhostTCPPreview
	preview := RunNetworkAdapterPreview(BuildLauncherPreview(BuildLogicalNodeArtifacts(cfg, nil, nil)))
	dir := t.TempDir()
	if err := WriteNetworkAdapterPreviewArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	header := readCSVHeader(t, filepath.Join(dir, "typed_message_log.csv"))
	expected := []string{"message_id", "message_type", "from_node_id", "to_node_id", "shard_id", "role", "block_height", "sequence_id", "payload_digest", "payload", "timestamp_ms"}
	for i, field := range expected {
		if header[i] != field {
			t.Fatalf("typed_message_log header[%d] = %s, want %s", i, header[i], field)
		}
	}
}
