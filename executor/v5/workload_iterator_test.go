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

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/mempool"
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
	return WorkloadPlan{PluginID: "canonical_trace_replay", SourceType: "dataset", DatasetID: "generic_fixture_dataset", VariantID: "original", VariantMode: "original_window", MaterializedRelativePath: relative, SourceSHA256: "sourcehash", MaterializedSHA256: hash, TruthLabel: "real_observed", ReplayMode: "max_throughput", IdentityMappingVersion: "mbe_dataset_identity_v1", NoFallback: true, TxCount: count, ActualTxCount: count, Seed: 7}
}

func canonicalRecord(index int, sender, targetKey string) map[string]any {
	receiver := "user:receiver:ff"
	return map[string]any{"schema_version": "mbe_workload_record_v1", "dataset_id": "generic_fixture_dataset", "source_row_index": index, "source_event_id": "sale", "timestamp_ms": int64(1700000000000 + index), "sender_id": sender, "receiver_id": receiver, "operation_type": "asset_sale", "runtime_value": 1, "state_keys": []string{"account:sender:" + sender, "account:receiver:" + receiver, targetKey}, "routing_source_key": "account:sender:" + sender, "routing_target_key": targetKey, "skew_keys": map[string]any{"contract": targetKey}, "provenance": map[string]any{"adapter_id": "test_generic"}, "metadata": map[string]any{}, "materialized_index": index, "logical_event_id": "logical"}
}

func crossShardPair(shards int) (string, string) {
	sender := "user:sender:1"
	plan := canonicalPlan("unused", "unused", 1)
	for index := 2; index < 100; index++ {
		targetKey := "contract:" + strings.Repeat("0", 4-len(fmt.Sprint(index))) + fmt.Sprint(index)
		if canonicalRuntimeSourceShard(plan, sender, shards) != stableShard([]string{targetKey}, shards) {
			return sender, targetKey
		}
	}
	return sender, "contract:2"
}

func TestCanonicalTraceIteratorSignsStableIdentityAndContinuousNonce(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	sender, targetKey := crossShardPair(2)
	relative, hash := writeCanonicalFixture(t, root, []map[string]any{canonicalRecord(0, sender, targetKey), canonicalRecord(1, sender, targetKey)})
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
	assertAccessItem(t, first.AccessList, "account:sender:"+sender, tx.AccessReadWrite, "routing_source_state")
	assertAccessItem(t, first.AccessList, targetKey, tx.AccessReadWrite, "routing_target_state")
	firstTx, err := iter.SignedTransaction(first)
	if err != nil || tx.Verify(firstTx) != nil {
		t.Fatalf("first signature failed: %v", err)
	}
	assertAccessItem(t, firstTx.AccessList, targetKey, tx.AccessReadWrite, "routing_target_state")
	planningAccess := canonicalRuntimeAccessList(iter.plan, first)
	assertAccessItem(t, planningAccess, "balance:"+firstTx.Sender, tx.AccessReadWrite, "set")
	assertAccessItem(t, planningAccess, "nonce:"+firstTx.Sender, tx.AccessReadWrite, "set")
	if fmt.Sprint(planningAccess) != fmt.Sprint(firstTx.AccessList) {
		t.Fatalf("dataset routing planner must see the same runtime access list as signed execution: %#v != %#v", planningAccess, firstTx.AccessList)
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
	expectedSourceShard := fmt.Sprintf("s%d", canonicalRuntimeSourceShard(iter.plan, sender, 2))
	if first.SourceShard != expectedSourceShard || second.SourceShard != expectedSourceShard {
		t.Fatalf("same runtime sender must use one nonce source shard: %s %s want %s", first.SourceShard, second.SourceShard, expectedSourceShard)
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

func TestCanonicalTraceIteratorKeepsRuntimeSenderOnOneSourceShardAcrossTargets(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	sender := "user:sender:shared"
	records := []map[string]any{
		canonicalRecord(0, sender, "contract:0001"),
		canonicalRecord(1, sender, "contract:0002"),
		canonicalRecord(2, sender, "contract:0003"),
	}
	relative, hash := writeCanonicalFixture(t, root, records)
	iter, err := NewCanonicalTraceIterator(canonicalPlan(relative, hash, len(records)), 4, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	expected := fmt.Sprintf("s%d", canonicalRuntimeSourceShard(iter.plan, sender, 4))
	for index := range records {
		record, err := iter.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if record.SourceShard != expected {
			t.Fatalf("record %d source shard drifted with target key: got %s want %s", index, record.SourceShard, expected)
		}
		item, err := iter.SignedTransaction(record)
		if err != nil {
			t.Fatal(err)
		}
		if item.Nonce != uint64(index) {
			t.Fatalf("record %d nonce = %d, want %d", index, item.Nonce, index)
		}
	}
}

func TestCanonicalTraceIteratorDatasetSenderNonceStreamAdmitsOnOneSourceShard(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	sender := "user:sender:shared"
	records := []map[string]any{
		canonicalRecord(0, sender, "contract:0001"),
		canonicalRecord(1, sender, "contract:0002"),
		canonicalRecord(2, sender, "contract:0003"),
	}
	relative, hash := writeCanonicalFixture(t, root, records)
	iter, err := NewCanonicalTraceIterator(canonicalPlan(relative, hash, len(records)), 4, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	pools := map[string]*mempool.Mempool{}
	sourceShards := map[string]bool{}
	for index := range records {
		record, err := iter.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		item, err := iter.SignedTransaction(record)
		if err != nil {
			t.Fatal(err)
		}
		if item.Nonce != uint64(index) {
			t.Fatalf("record %d nonce = %d, want %d", index, item.Nonce, index)
		}
		sourceShards[record.SourceShard] = true
		pool := pools[record.SourceShard]
		if pool == nil {
			pool = mempool.New("node-"+record.SourceShard, record.SourceShard, mempool.DefaultPolicy(), account.NewNonceManager())
			pools[record.SourceShard] = pool
		}
		if result := pool.Admit(item); !result.Accepted {
			t.Fatalf("dataset sender nonce stream should admit on %s at nonce %d: %s", record.SourceShard, item.Nonce, result.RejectReason)
		}
	}
	if len(sourceShards) != 1 {
		t.Fatalf("single runtime sender nonce stream split across source shards: %#v", sourceShards)
	}
}

func TestWorkloadIngressShardPreservesCrossShardSourceProtocol(t *testing.T) {
	record := WorkloadRecord{CrossShard: true, SourceShard: "s1", TargetShard: "s0", Payload: "v5_cross:s0:dataset_event:wearable"}
	route := RoutingDecision{ShardID: "s0", Reason: "metatrack_batch_affinity"}
	if got := workloadIngressShard(record, route); got != "s1" {
		t.Fatalf("cross-shard source transaction must enter source shard before relay, got %s", got)
	}
	local := WorkloadRecord{SourceShard: "s1", Payload: "dataset_event:wearable"}
	if got := workloadIngressShard(local, route); got != "s0" {
		t.Fatalf("non-cross-shard transaction should use routing decision, got %s", got)
	}
}

func assertAccessItem(t *testing.T, items []tx.AccessItem, key string, mode tx.AccessMode, semantics string) {
	t.Helper()
	for _, item := range items {
		if item.Key == key {
			if item.Mode != mode || item.UpdateSemantics != semantics {
				t.Fatalf("unexpected access item for %s: %#v", key, item)
			}
			return
		}
	}
	t.Fatalf("missing access item for %s in %#v", key, items)
}

func TestCanonicalTraceIteratorRejectsHashMismatchAndEarlyEOF(t *testing.T) {
	dataDir := t.TempDir()
	root := filepath.Join(dataDir, ".cache", "workloads")
	relative, hash := writeCanonicalFixture(t, root, []map[string]any{canonicalRecord(0, "user:sender:1", "contract:2")})
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
