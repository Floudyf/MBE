package core

import (
	"encoding/csv"
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

func TestReplaySummaryCSVIncludesV16V17Metrics(t *testing.T) {
	temp := t.TempDir()
	config := filepath.Join(temp, "config.yaml")
	contents := []byte("state_sharding:\n  shard_count: 4\nexecution_sharding:\n  shard_count: 4\nrouting:\n  policy: co_access\nexecution:\n  dual_track_enabled: true\ncommit:\n  hot_update_aggregation_enabled: true\n")
	if err := os.WriteFile(config, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(temp, "out")
	if _, err := Replay(config, "../../tests/golden/trace_small.jsonl.gz", out); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(out, "summary.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected header and one row, got %d rows", len(rows))
	}
	values := map[string]string{}
	for i, key := range rows[0] {
		values[key] = rows[1][i]
	}
	for _, key := range []string{"dual_track_enabled", "fast_track_tx_count", "conservative_track_tx_count", "fast_track_tx_ratio", "conservative_track_tx_ratio", "scheduler_idle_count", "hot_update_aggregation_enabled", "aggregation_policy", "aggregation_candidate_tx_count", "aggregated_tx_count", "aggregated_commit_count", "conservative_commit_count", "aggregation_saved_commit_count", "aggregation_group_count", "aggregation_hot_key_count", "virtual_time_ms"} {
		if _, ok := values[key]; !ok {
			t.Fatalf("summary.csv missing %s", key)
		}
	}
	if values["dual_track_enabled"] != "true" || values["hot_update_aggregation_enabled"] != "true" {
		t.Fatalf("expected enabled V1.6/V1.7 fields, got %+v", values)
	}
	if values["fast_track_tx_count"] == "" || values["aggregated_commit_count"] == "" {
		t.Fatalf("expected non-empty V1.6/V1.7 metric values, got %+v", values)
	}
}
