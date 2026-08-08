package main

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeIdentityCSV(t *testing.T, path string, header []string, rows [][]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := writer.WriteAll(rows); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeIdentityDrainStatus(t *testing.T, root string, submitted int, finishedAt int64) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"submitted":         submitted,
		"terminal":          submitted,
		"incomplete":        0,
		"phase":             "COMPLETED",
		"completion_reason": "drain_quiescent",
		"drain_started_at":  int64(1000),
		"drain_finished_at": finishedAt,
		"last_progress_at":  finishedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "drain_status.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionClassificationUsesExplicitLogicalIdentity(t *testing.T) {
	root := t.TempDir()
	writeIdentityCSV(t,
		filepath.Join(root, "client", "client_submission_log.csv"),
		[]string{"timestamp", "tx_id", "logical_tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"},
		[][]string{{"1", "physical-1", "logical-1", "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}},
	)
	classification, err := loadSubmissionClassification(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := classification["physical-1"]; exists {
		t.Fatalf("physical id leaked into logical classification: %#v", classification)
	}
	if isCross, exists := classification["logical-1"]; !exists || isCross {
		t.Fatalf("logical classification missing or incorrect: %#v", classification)
	}
}

func TestSubmissionClassificationMapsPhysicalIdentityThroughClientLifecycle(t *testing.T) {
	root := t.TempDir()
	writeIdentityCSV(t,
		filepath.Join(root, "client", "client_submission_log.csv"),
		[]string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"},
		[][]string{{"1", "physical-1", "sender", "n0", "s0", "", "true", "s0", "s1", "true", "1", ""}},
	)
	writeIdentityCSV(t,
		filepath.Join(root, "client", "client_lifecycle.csv"),
		[]string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"},
		[][]string{{"1", "physical-1", "logical-1", "submitted", "mbe-client", "s0", "s0", "s1", "0", "true", ""}},
	)
	classification, err := loadSubmissionClassification(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	if isCross, exists := classification["logical-1"]; !exists || !isCross {
		t.Fatalf("legacy physical id was not mapped to logical identity: %#v", classification)
	}
	terminal, _, err := deriveLiveTerminal(classification, []map[string]any{{
		"source_finalized_logical_tx_ids": []any{"logical-1"},
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal["logical-1"] {
		t.Fatalf("logical terminal event was not matched to submitted transaction: %#v", terminal)
	}
}

func TestFinalityArtifactsUseSameLogicalSubmissionClassificationAsDrain(t *testing.T) {
	root := t.TempDir()
	writeIdentityCSV(t,
		filepath.Join(root, "client", "client_submission_log.csv"),
		[]string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"},
		[][]string{{"1000", "physical-1", "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}},
	)
	writeIdentityCSV(t,
		filepath.Join(root, "client", "client_lifecycle.csv"),
		[]string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"},
		[][]string{
			{"1000", "physical-1", "logical-1", "submitted", "mbe-client", "s0", "", "", "0", "true", ""},
			{"2000", "physical-1", "logical-1", "durable_committed", "n0", "s0", "", "", "1", "true", ""},
		},
	)
	writeIdentityDrainStatus(t, root, 1, 2000)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["submitted_unique_tx_count"] != 1 ||
		summary["terminal_unique_tx_count"] != 1 ||
		summary["incomplete_unique_tx_count"] != 0 {
		t.Fatalf("logical identity join produced incorrect finality summary: %#v", summary)
	}
}
