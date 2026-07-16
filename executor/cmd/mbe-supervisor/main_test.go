package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestProgressSnapshotChangesOnlyOnRealProgress(t *testing.T) {
	initial := progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}
	if progressChanged(initial, initial) {
		t.Fatal("identical runtime snapshot was reported as progress")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 3, MinHeight: 4, MaxHeight: 5, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("terminal progress was not detected")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 6, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("height progress was not detected")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 7, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("mempool progress was not detected")
	}
	if progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 9, Reserved: 2, Pending: 4, ProposalInFlight: false}) {
		t.Fatal("queue growth or proposal jitter was reported as progress")
	}
	raw, err := json.Marshal(initial)
	if err != nil || string(raw) == "{}" {
		t.Fatalf("progress snapshot did not serialize: %s", raw)
	}
}

func TestFinalityDoesNotDrainBeforeSourceFinalize(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeRows := func(path string, header []string, rows [][]string) {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		_ = writer.Write(header)
		_ = writer.WriteAll(rows)
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	submissionRows := [][]string{}
	lifecycleRows := [][]string{}
	for i := 0; i < 1000; i++ {
		id := "tx-" + strconv.Itoa(i)
		cross := i >= 750
		crossText := "false"
		if cross {
			crossText = "true"
		}
		submissionRows = append(submissionRows, []string{strconv.Itoa(i + 1), id, "sender", "n0", "s0", "", crossText, "s0", "s1", "true", "1", ""})
		lifecycleRows = append(lifecycleRows, []string{strconv.Itoa(i + 1), id, id, "submitted", "n0", "s0", "", "", "1", "true"})
		stage := "durable_committed"
		if cross {
			lifecycleRows = append(lifecycleRows,
				[]string{strconv.Itoa(2000 + i), id, id, "sourcelock", "n0", "s0", "s0", "s1", "2", "true"},
				[]string{strconv.Itoa(3000 + i), id, id, "targetcommit", "n1", "s1", "s0", "s1", "3", "true"})
		} else {
			lifecycleRows = append(lifecycleRows, []string{strconv.Itoa(2000 + i), id, id, stage, "n0", "s0", "", "", "2", "true"})
		}
	}
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, submissionRows)
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, lifecycleRows)

	assertSummary := func(wantTerminal, wantIncomplete int) {
		summary, err := deriveFinalityArtifacts(root, nil)
		if err != nil {
			t.Fatal(err)
		}
		if summary["terminal_unique_tx_count"] != wantTerminal || summary["incomplete_unique_tx_count"] != wantIncomplete {
			t.Fatalf("unexpected finality summary: %#v", summary)
		}
	}
	assertSummary(750, 250)

	file, err := os.OpenFile(filepath.Join(root, "client", "client_lifecycle.csv"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	for i := 750; i < 1000; i++ {
		id := "tx-" + strconv.Itoa(i)
		_ = writer.Write([]string{strconv.Itoa(4000 + i), id, id, "sourcefinalize", "n0", "s0", "s0", "s1", "4", "true"})
	}
	writer.Flush()
	_ = file.Close()
	assertSummary(1000, 0)
}

func TestDeriveLiveTerminalUsesSubmittedClassification(t *testing.T) {
	classification := map[string]bool{"intra": false, "cross": true}
	statuses := []map[string]any{
		{"durable_committed_logical_tx_ids": []any{"intra", "cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"intra", "cross"}},
		{"durable_committed_logical_tx_ids": []any{"cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"cross"}},
	}
	terminal, counts, err := deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 1 || !terminal["intra"] || counts["incomplete"] != 1 {
		t.Fatalf("cross-shard durable commit was incorrectly terminal: terminal=%v counts=%v", terminal, counts)
	}
	statuses[0]["source_finalized_logical_tx_ids"] = []any{"cross"}
	statuses[1]["source_finalized_logical_tx_ids"] = []any{"cross"}
	terminal, counts, err = deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 2 || counts["incomplete"] != 0 {
		t.Fatalf("source finalize did not close cross-shard transaction: terminal=%v counts=%v", terminal, counts)
	}
}

func TestDeriveLiveTerminalHandlesTargetCommitAndDuplicates(t *testing.T) {
	classification := map[string]bool{}
	for i := 0; i < 750; i++ {
		classification[fmt.Sprintf("intra-%d", i)] = false
	}
	for i := 0; i < 250; i++ {
		classification[fmt.Sprintf("cross-%d", i)] = true
	}
	statuses := []map[string]any{{
		"durable_committed_logical_tx_ids": []any{},
		"source_finalized_logical_tx_ids":  []any{},
		"refunded_logical_tx_ids":          []any{},
		"failed_logical_tx_ids":            []any{},
		"target_commit_logical_tx_ids":     []any{},
	}}
	for i := 0; i < 750; i++ {
		statuses[0]["durable_committed_logical_tx_ids"] = append(statuses[0]["durable_committed_logical_tx_ids"].([]any), fmt.Sprintf("intra-%d", i))
	}
	for i := 0; i < 250; i++ {
		statuses[0]["durable_committed_logical_tx_ids"] = append(statuses[0]["durable_committed_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i), fmt.Sprintf("cross-%d", i))
		if i < 248 {
			statuses[0]["source_finalized_logical_tx_ids"] = append(statuses[0]["source_finalized_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i))
		}
	}
	terminal, counts, err := deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 998 || counts["incomplete"] != 2 {
		t.Fatalf("expected 998 terminal and 2 incomplete: terminal=%d counts=%v", len(terminal), counts)
	}
	for i := 248; i < 250; i++ {
		statuses[0]["source_finalized_logical_tx_ids"] = append(statuses[0]["source_finalized_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i))
	}
	terminal, counts, err = deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 1000 || counts["incomplete"] != 0 {
		t.Fatalf("expected all transactions terminal after finalize: terminal=%d counts=%v", len(terminal), counts)
	}
}

func TestSubmissionClassificationMustMatchExpectedCount(t *testing.T) {
	if err := validateSubmissionClassification(map[string]bool{"only": false}, 2); err == nil {
		t.Fatal("missing submitted classification was accepted")
	}
}

func TestHasNonTerminalMempoolIgnoresTerminalResidue(t *testing.T) {
	terminal := map[string]bool{"done": true}
	statuses := []map[string]any{
		{"mempool_depth": float64(1), "mempool_logical_tx_ids": []any{"done"}},
	}
	if hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("terminal mempool residue should not block drain")
	}
	statuses = []map[string]any{
		{"mempool_depth": float64(1), "mempool_logical_tx_ids": []any{"waiting"}},
	}
	if !hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("non-terminal mempool item should block drain")
	}
	statuses = []map[string]any{
		{"mempool_depth": float64(1)},
	}
	if !hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("legacy status without mempool IDs should remain conservative")
	}
}

func TestFinalityCountsDurableCommitIndependentlyOfTerminalStageOrder(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(path string, header []string, rows [][]string) {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		_ = writer.Write(header)
		_ = writer.WriteAll(rows)
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	id := "intra-same-timestamp"
	write(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}
	write(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"100", id, id, "submitted", "n0", "s0", "", "", "1", "true"},
		{"200", id, id, "refund", "n0", "s0", "", "", "2", "true"},
		{"200", id, id, "durable_committed", "n0", "s0", "", "", "2", "true"},
	})
	summary, err := deriveFinalityArtifacts(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary["intra_shard_committed_unique_count"] != 1 || summary["intra_shard_terminal_unique_count"] != 1 || summary["terminal_unique_tx_count"] != 1 {
		t.Fatalf("same-timestamp stages caused metric drift: %#v", summary)
	}
}
