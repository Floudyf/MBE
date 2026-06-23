package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplayWritesConfigSnapshotAndVirtualLatency(t *testing.T) {
	temp := t.TempDir()
	config := filepath.Join(temp, "config.yaml")
	contents := []byte("state_sharding:\n  shard_count: 4\nnetwork_model:\n  remote_fetch_latency_ms: 5\nconsensus_protocol:\n  block_size: 500\n  block_interval_ms: 100\n  finality_delay_ms: 100\n")
	if err := os.WriteFile(config, contents, 0o644); err != nil {
		t.Fatal(err)
	}

	summary, err := Replay(config, "../../tests/golden/trace_small.jsonl.gz", filepath.Join(temp, "out"))
	if err != nil {
		t.Fatal(err)
	}
	if summary.AvgLatencyMS <= 0 || summary.P95LatencyMS <= 0 || summary.P99LatencyMS <= 0 {
		t.Fatalf("expected non-zero virtual latencies, got %+v", summary)
	}
	snapshot, err := os.ReadFile(filepath.Join(temp, "out", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshot) != string(contents) {
		t.Fatal("config snapshot does not match replay input")
	}
}
