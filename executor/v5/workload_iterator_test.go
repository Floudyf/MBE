package v5

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func writeCanonicalFixture(t *testing.T, root string, records []map[string]any) (string, string) {
	t.Helper()
	relative := filepath.ToSlash(filepath.Join("materialized", "fixture", "workload.jsonl.gz"))
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	for _, record := range records {
		raw, _ := json.Marshal(record)
		if _, err := gz.Write(append(raw, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	sum := sha256.Sum256(raw)
	return relative, hex.EncodeToString(sum[:])
}

func canonicalPlan(relative, hash string, count int) WorkloadPlan {
	return WorkloadPlan{PluginID: "canonical_trace_replay", SourceType: "dataset", DatasetID: "dcl_sales_polygon_271868", VariantID: "original", VariantMode: "original_window", MaterializedRelativePath: relative, SourceSHA256: "sourcehash", MaterializedSHA256: hash, TruthLabel: "real_observed", ReplayMode: "max_throughput", IdentityMappingVersion: "mbe_dataset_identity_v1", NoFallback: true, TxCount: count, ActualTxCount: count, Seed: 7}
}

func canonicalRecord(index int, buyer, contract string) map[string]any {
	return map[string]any{"schema_version": "mbe_workload_record_v1", "dataset_id": "dcl_sales_polygon_271868", "source_row_index": index, "source_event_id": "sale", "timestamp_ms": int64(1700000000000 + index), "category": "wearable", "buyer_address": buyer, "seller_address": "0x00000000000000000000000000000000000000ff", "contract_address": contract, "runtime_value": 1, "state_keys": []string{"account:buyer:" + buyer, "account:seller:0x00000000000000000000000000000000000000ff", "contract:" + contract}, "materialized_index": index, "logical_event_id": "logical"}
}

func crossShardPair(shards int) (string, string) {
	buyer := "0x0000000000000000000000000000000000000001"
	for index := 2; index < 100; index++ {
		contract := "0x" + strings.Repeat("0", 40-len(fmt.Sprint(index))) + fmt.Sprint(index)
		if stableShard([]string{"account:buyer:" + buyer}, shards) != stableShard([]string{"contract:" + contract}, shards) {
			return buyer, contract
		}
	}
	return buyer, "0x0000000000000000000000000000000000000002"
}

func TestCanonicalTraceIteratorSignsStableIdentityAndContinuousNonce(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	buyer, contract := crossShardPair(2)
	relative, hash := writeCanonicalFixture(t, root, []map[string]any{canonicalRecord(0, buyer, contract), canonicalRecord(1, buyer, contract)})
	iter, err := NewCanonicalTraceIterator(canonicalPlan(relative, hash, 2), 2, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	first, err := iter.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceShard == "" || first.TargetShard == "" || first.SourceShard == first.TargetShard {
		t.Fatalf("dataset cross-shard record did not expose distinct source/target shards: %#v", first)
	}
	firstTx, err := iter.SignedTransaction(first)
	if err != nil || tx.Verify(firstTx) != nil {
		t.Fatalf("first signature failed: %v", err)
	}
	second, err := iter.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secondTx, err := iter.SignedTransaction(second)
	if err != nil || tx.Verify(secondTx) != nil {
		t.Fatalf("second signature failed: %v", err)
	}
	if firstTx.Sender != secondTx.Sender || firstTx.Nonce != 0 || secondTx.Nonce != 1 {
		t.Fatalf("identity/nonce continuity failed: %#v %#v", firstTx, secondTx)
	}
	if _, err := iter.Next(context.Background()); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	summary := iter.Summary()
	if summary.IdentityCount != 1 || !summary.NonceContinuity || summary.SignaturePassCount != 2 || !summary.NoFallback {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if summary.ActualCrossShardCount != 2 || summary.ExpectedCrossShardCount != summary.ActualCrossShardCount || summary.ExpectedCrossShardRatio != summary.ActualCrossShardRatio {
		t.Fatalf("dataset expected cross-shard summary did not match modeled actuals: %#v", summary)
	}
}

func TestCanonicalTraceIteratorRejectsHashMismatchAndEarlyEOF(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	relative, hash := writeCanonicalFixture(t, root, []map[string]any{canonicalRecord(0, "0x0000000000000000000000000000000000000001", "0x0000000000000000000000000000000000000002")})
	if _, err := NewCanonicalTraceIterator(canonicalPlan(relative, "bad"+hash, 1), 2, dataDir); err == nil {
		t.Fatal("hash mismatch was not rejected")
	}
	iter, err := NewCanonicalTraceIterator(canonicalPlan(relative, hash, 2), 2, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := iter.Next(context.Background()); err == nil {
		t.Fatal("early EOF was not rejected")
	}
}

func TestCanonicalTraceIteratorRejectsMalformedRecordAndSyntheticRegression(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	relative, hash := writeCanonicalFixture(t, root, []map[string]any{{"schema_version": "wrong"}})
	iter, err := NewCanonicalTraceIterator(canonicalPlan(relative, hash, 1), 2, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if _, err := iter.Next(context.Background()); err == nil {
		t.Fatal("malformed schema was not rejected")
	}
	synthetic := NewSyntheticIterator(builtinWorkload{}, WorkloadPlan{TxCount: 3, Seed: 1, CrossShardRatio: 1}, 2)
	for i := 0; i < 3; i++ {
		record, err := synthetic.Next(context.Background())
		if err != nil || !record.CrossShard {
			t.Fatalf("synthetic regression failed: %#v %v", record, err)
		}
	}
}
