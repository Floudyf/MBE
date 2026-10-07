package v5

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTxAlloHistoryV213(t *testing.T, path string, rows []txalloHistorySidecarRow) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := gz.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestTxAlloHistoryResolverV213ClientAndNodeLayouts(t *testing.T) {
	runDir := t.TempDir()
	historyPath := filepath.Join(runDir, "workload", "txallo_history.jsonl.gz")
	if err := os.MkdirAll(filepath.Dir(historyPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(historyPath, []byte("fixture"), 0o644); err != nil {
		t.Fatal(err)
	}
	clientDir := filepath.Join(runDir, "client")
	nodeDir := filepath.Join(runDir, "nodes", "n0")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dataDir := range []string{clientDir, nodeDir} {
		got, err := txalloRunHistoryPath(dataDir, "workload/txallo_history.jsonl.gz")
		if err != nil {
			t.Fatalf("resolver failed for %s: %v", dataDir, err)
		}
		if filepath.Clean(got) != filepath.Clean(historyPath) {
			t.Fatalf("resolver got=%s want=%s", got, historyPath)
		}
	}
	for _, bad := range []string{"../escape", "workload/../../escape"} {
		if _, err := txalloRunHistoryPath(nodeDir, bad); err == nil {
			t.Fatalf("unsafe history traversal was accepted: %s", bad)
		}
	}
}

func TestTxAlloNodeBootstrapV213VerifiesHistorySHA(t *testing.T) {
	runDir := t.TempDir()
	nodeDir := filepath.Join(runDir, "nodes", "n0")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	historyPath := filepath.Join(runDir, "workload", "txallo_history.jsonl.gz")
	rows := []txalloHistorySidecarRow{
		{SchemaVersion: "mbe_txallo_history_record_v1", SourceOrder: 96998, TransactionID: "tx-a", SenderID: "alice", ReceiverID: "m.federation", Accounts: []string{"alice", "m.federation"}},
		{SchemaVersion: "mbe_txallo_history_record_v1", SourceOrder: 96999, TransactionID: "tx-b", SenderID: "bob", ReceiverID: "m.federation", Accounts: []string{"bob", "m.federation"}},
	}
	goodSHA := writeTxAlloHistoryV213(t, historyPath, rows)
	planFor := func(historySHA string) WorkloadPlan {
		return WorkloadPlan{AuditMetadata: map[string]any{"txallo_history": map[string]any{
			"selection_policy":                    "preceding_ratio_v1",
			"future_evaluation_transactions_used": 0,
			"history_relative_path":               "workload/txallo_history.jsonl.gz",
			"selected_history_count":              len(rows),
			"selected_history_sha256":             historySHA,
			"history_ratio":                       0.10,
			"history_pool_count":                  97000,
			"history_window_start_source_order":   96998,
			"history_window_end_source_order":     96999,
		}}}
	}
	newPlugin := func() *txalloAccountSharding {
		return &txalloAccountSharding{
			basicPlugin: makeBasic("sharding", txalloShardingID, map[string]any{"allocation_mode": "paper_g_ratio_snapshot", "eta": 2.0}),
			aliases:     map[string]string{},
			evidence:    map[string]any{},
		}
	}
	bad := newPlugin()
	err := bad.BootstrapHistoricalAllocation(context.Background(), HistoricalAllocationBootstrapInput{Plan: planFor(strings.Repeat("0", 64)), DataDir: nodeDir, ShardIDs: []string{"s0", "s1"}})
	if err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("tampered/mismatched history was not rejected: %v", err)
	}
	good := newPlugin()
	if err := good.BootstrapHistoricalAllocation(context.Background(), HistoricalAllocationBootstrapInput{Plan: planFor(goodSHA), DataDir: nodeDir, ShardIDs: []string{"s0", "s1"}}); err != nil {
		t.Fatalf("node-layout bootstrap failed with attested history: %v", err)
	}
	ev := good.HistoricalAllocationEvidence()
	if intValue(ev["g_txallo_run_count"]) != 1 || intValue(ev["a_txallo_run_count"]) != 0 {
		t.Fatalf("unexpected G/A lifecycle evidence: %#v", ev)
	}
	if verified, _ := ev["history_sidecar_sha256_verified"].(bool); !verified {
		t.Fatalf("history SHA verification evidence missing: %#v", ev)
	}
	if intValue(ev["history_transaction_count"]) != len(rows) {
		t.Fatalf("history count mismatch: %#v", ev)
	}
}
