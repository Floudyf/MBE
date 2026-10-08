package v5

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testDynamicShardingV222(t *testing.T, config map[string]any) *txalloAccountSharding {
	t.Helper()
	initial := []txalloHistoryTx{{Accounts: []string{"alice", "bob"}}, {Accounts: []string{"alice", "carol"}}}
	alloc := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	alloc.RunG(initial)
	return &txalloAccountSharding{
		basicPlugin:         makeBasic("sharding", txalloShardingID, config),
		allocator:           alloc,
		shards:              []string{"s0", "s1"},
		aliases:             map[string]string{},
		evidence:            map[string]any{"history_transaction_count": 2, "initial_g_history_transaction_count": 2, "g_txallo_run_count": 1, "a_txallo_run_count": 0, "closed_source_epoch_count": 0},
		provisionalAccounts: map[string]bool{},
		dynamicHistory:      append([]txalloHistoryTx(nil), initial...),
		mappingSnapshotPath: filepath.Join(t.TempDir(), txalloDynamicMappingSnapshotName),
	}
}

func TestTxAlloDynamicV222SourceBlockBucket(t *testing.T) {
	anchor := int64(1000)
	cases := []struct {
		block int64
		want  int64
	}{{1000, 0}, {1014, 0}, {1015, 1}, {1029, 1}, {1030, 2}}
	for _, tc := range cases {
		got, err := txalloSourceBlockEpoch(tc.block, anchor, 15)
		if err != nil || got != tc.want {
			t.Fatalf("block=%d got=%d want=%d err=%v", tc.block, got, tc.want, err)
		}
	}
}

func TestTxAlloDynamicV222CommittedEpochRunsAAndPublishesSnapshot(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 20})
	records := []WorkloadRecord{{LogicalID: "e0", SenderID: "alice", ReceiverID: "dave"}, {LogicalID: "e1", SenderID: "dave", ReceiverID: "bob"}}
	if err := p.ApplyCommittedTxAlloEpoch(records, 0, true); err != nil {
		t.Fatal(err)
	}
	if p.TxAlloMappingEpoch() != 1 {
		t.Fatalf("mapping epoch=%d want=1", p.TxAlloMappingEpoch())
	}
	ev := p.HistoricalAllocationEvidence()
	if intValue(ev["a_txallo_run_count"]) != 1 || intValue(ev["committed_dynamic_transaction_count"]) != 2 || intValue(ev["closed_source_epoch_count"]) != 1 {
		t.Fatalf("unexpected A evidence: %#v", ev)
	}
	raw, err := os.ReadFile(p.mappingSnapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	var snap txalloMappingSnapshotV22
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Epoch != 1 || snap.StateDigest != txalloStateDigest(snap.Mapping, snap.Aliases) {
		t.Fatalf("bad snapshot: %#v", snap)
	}
}

func TestTxAlloDynamicV222EmptyEpochAdvancesClockWithoutFakeA(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 20})
	if err := p.ApplyCommittedTxAlloEpoch(nil, 0, true); err != nil {
		t.Fatal(err)
	}
	ev := p.HistoricalAllocationEvidence()
	if intValue(ev["closed_source_epoch_count"]) != 1 || intValue(ev["a_txallo_run_count"]) != 0 || p.TxAlloMappingEpoch() != 0 {
		t.Fatalf("empty epoch fabricated mapping update: %#v", ev)
	}
}

func TestTxAlloDynamicV222PeriodicGUsesCompleteCommittedHistory(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 2})
	if err := p.ApplyCommittedTxAlloEpoch([]WorkloadRecord{{LogicalID: "h1", SenderID: "alice", ReceiverID: "dave"}}, 0, true); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyCommittedTxAlloEpoch([]WorkloadRecord{{LogicalID: "h2", SenderID: "dave", ReceiverID: "erin"}}, 1, true); err != nil {
		t.Fatal(err)
	}
	ev := p.HistoricalAllocationEvidence()
	if intValue(ev["g_txallo_run_count"]) != 2 || intValue(ev["periodic_g_txallo_run_count"]) != 1 || intValue(ev["a_txallo_run_count"]) != 1 {
		t.Fatalf("unexpected cadence: %#v", ev)
	}
	if intValue(ev["total_allocator_history_transaction_count"]) != 4 {
		t.Fatalf("periodic G lost committed history: %#v", ev)
	}
}

func TestTxAlloDynamicV222StatefulMigrationFailsClosed(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 20})
	// Existing learned accounts that move require migration.
	before := map[string]string{"alice": "s0", "bob": "s0"}
	// A newly observed account has already committed under its deterministic
	// fallback shard during this epoch. Learning it onto the opposite shard is
	// also a state-home move and must not bypass the Stateful guard.
	fallback := p.fallbackShard("new", p.shards)
	opposite := "s0"
	if fallback == "s0" {
		opposite = "s1"
	}
	after := map[string]string{"alice": "s1", "bob": "s0", "new": opposite}
	moved := txalloStatefulMigrationAccounts(p, before, after, []WorkloadRecord{{SenderID: "new", ReceiverID: "bob"}})
	if len(moved) != 2 || moved[0] != "alice" || moved[1] != "new" {
		t.Fatalf("moved=%v fallback=%s opposite=%s", moved, fallback, opposite)
	}
}

func TestTxAlloDynamicV2297RejectsImmutableNextEpochDigestConflict(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, txalloDynamicMappingSnapshotName)
	bad := txalloMappingSnapshotV22{
		SchemaVersion: txalloDynamicMappingSchema,
		Epoch:         1,
		Mapping:       map[string]string{"alice": "s1"},
		Aliases:       map[string]string{},
	}
	bad.MappingDigest = stableJSONDigest(bad.Mapping)
	bad.AliasesDigest = stableJSONDigest(bad.Aliases)
	bad.StateDigest = "not-the-real-state-digest"
	if err := txalloPublishImmutableJSON(txalloMappingSnapshotEpochPath(base, 1), bad); err != nil {
		t.Fatal(err)
	}
	p := &txalloAccountSharding{
		basicPlugin:         makeBasic("sharding", txalloShardingID, map[string]any{"dynamic_a_txallo_runtime": true}),
		allocator:           newTxAlloAllocator([]string{"s0", "s1"}, 2, 1, 0.1),
		shards:              []string{"s0", "s1"},
		aliases:             map[string]string{},
		evidence:            map[string]any{},
		provisionalAccounts: map[string]bool{},
		mappingSnapshotPath: base,
	}
	p.allocator.Mapping = map[string]string{"alice": "s0"}
	if err := p.RefreshTxAlloMapping(); err == nil {
		t.Fatal("invalid immutable next-epoch digest was accepted")
	}
}

func TestTxAlloDynamicV222CommittedOnlyBarrier(t *testing.T) {
	root := t.TempDir()
	n0 := filepath.Join(root, "nodes", "n0")
	if err := os.MkdirAll(n0, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := []txalloEpochLifecycleRowV22{
		{SchemaVersion: txalloDynamicEpochFeedSchema, LogicalTxID: "intra", Stage: "durable_committed", Success: true},
		{SchemaVersion: txalloDynamicEpochFeedSchema, LogicalTxID: "cross", Stage: "durable_committed", Success: true},
		{SchemaVersion: txalloDynamicEpochFeedSchema, LogicalTxID: "cross", Stage: "sourcefinalize", Success: true},
	}
	f, err := os.Create(filepath.Join(n0, txalloDynamicEpochFeedName))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		b, _ := json.Marshal(row)
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	records := []WorkloadRecord{{LogicalID: "intra"}, {LogicalID: "cross"}}
	cross := map[string]bool{"cross": true}
	reader := newTxAlloEpochFeedReaderV22()
	if err := txalloWaitCommittedEpoch(context.Background(), []NodePlan{{DataDir: n0}}, records, cross, false, reader); err != nil {
		t.Fatal(err)
	}
}

func TestTxAlloDynamicV2297MappingAckBarrierUsesImmutableEpochFiles(t *testing.T) {
	root := t.TempDir()
	nodes := []NodePlan{{NodeID: "n0", DataDir: filepath.Join(root, "n0")}, {NodeID: "n1", DataDir: filepath.Join(root, "n1")}}
	for _, node := range nodes {
		if err := os.MkdirAll(node.DataDir, 0o755); err != nil {
			t.Fatal(err)
		}
		// Deliberately publish a conflicting mutable latest alias. The barrier
		// must ignore it and consume only the exact immutable epoch ACK.
		latest := txalloMappingAckV222{SchemaVersion: txalloDynamicMappingAckSchema, NodeID: node.NodeID, Epoch: 99, StateDigest: "wrong", Status: "ok", TimestampMS: 123}
		if err := txalloWriteAtomicJSON(filepath.Join(node.DataDir, txalloDynamicMappingAckName), latest); err != nil {
			t.Fatal(err)
		}
		ack := txalloMappingAckV222{SchemaVersion: txalloDynamicMappingAckSchema, NodeID: node.NodeID, Epoch: 2, StateDigest: "abc", Status: "ok", TimestampMS: 0}
		if err := txalloPublishImmutableJSON(txalloMappingAckEpochPath(node.DataDir, 2), ack); err != nil {
			t.Fatal(err)
		}
	}
	if err := txalloWaitMappingAcks(context.Background(), nodes, 2, "abc"); err != nil {
		t.Fatal(err)
	}
}

func TestTxAlloDynamicV227AtomicSnapshotReplacesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), txalloDynamicMappingSnapshotName)
	first := txalloMappingSnapshotV22{
		SchemaVersion: txalloDynamicMappingSchema,
		Epoch:         0,
		Mapping:       map[string]string{"alice": "s0"},
		Aliases:       map[string]string{},
	}
	first.MappingDigest = stableJSONDigest(first.Mapping)
	first.AliasesDigest = stableJSONDigest(first.Aliases)
	first.StateDigest = txalloStateDigest(first.Mapping, first.Aliases)
	if err := txalloWriteAtomicJSON(path, first); err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}

	second := txalloMappingSnapshotV22{
		SchemaVersion: txalloDynamicMappingSchema,
		Epoch:         1,
		Mapping:       map[string]string{"alice": "s1", "bob": "s0"},
		Aliases:       map[string]string{},
	}
	second.MappingDigest = stableJSONDigest(second.Mapping)
	second.AliasesDigest = stableJSONDigest(second.Aliases)
	second.StateDigest = txalloStateDigest(second.Mapping, second.Aliases)
	if err := txalloWriteAtomicJSON(path, second); err != nil {
		t.Fatalf("replace existing snapshot: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got txalloMappingSnapshotV22
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 1 || got.StateDigest != second.StateDigest || len(got.Mapping) != 2 {
		t.Fatalf("snapshot replacement did not publish epoch 1: %#v", got)
	}
}

func TestTxAlloDynamicV229StatefulRequiresReplicaConvergenceProof(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{
		"dynamic_a_txallo_runtime":        true,
		"a_epoch_blocks":                  15,
		"g_epoch_multiple":                20,
		"stateful_paper_replicated_state": true,
	})
	records := []WorkloadRecord{{Index: 0, LogicalID: "e0", SenderID: "alice", ReceiverID: "dave"}}
	if err := p.ApplyCommittedTxAlloEpoch(records, 0, false); err == nil {
		t.Fatal("Stateful mapping update passed without durable replica convergence proof")
	}
	tokens := txalloExpectedReplicaTokensV229(records)
	if err := p.ConfirmTxAlloReplicaConvergence(0, stableJSONDigest(tokens), len(tokens)); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyCommittedTxAlloEpoch(records, 0, false); err != nil {
		t.Fatal(err)
	}
	if p.TxAlloMappingEpoch() != 1 {
		t.Fatalf("mapping epoch=%d want=1", p.TxAlloMappingEpoch())
	}
}

func TestTxAlloDynamicV229MappingEpochHistoryBindsSnapshot(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 20})
	if err := p.writeTxAlloMappingSnapshot(); err != nil {
		t.Fatal(err)
	}
	historyPath := filepath.Join(filepath.Dir(p.mappingSnapshotPath), txalloMappingEpochHistoryName)
	raw, err := os.ReadFile(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("initial mapping history rows=%d want=1", len(lines))
	}
	var first txalloMappingEpochRowV229
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Epoch != 0 || first.SourceEpoch != -1 || first.StateDigest != p.TxAlloMappingStateDigest() {
		t.Fatalf("unexpected initial mapping epoch row: %#v", first)
	}
	if err := p.ApplyCommittedTxAlloEpoch([]WorkloadRecord{{Index: 0, LogicalID: "e0", SenderID: "alice", ReceiverID: "dave"}}, 0, true); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	lines = strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("mapping history rows=%d want=2", len(lines))
	}
	var second txalloMappingEpochRowV229
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if second.Epoch != 1 || second.SourceEpoch != 0 || second.UpdateAlgorithm != "A-TxAllo" || second.StateDigest != p.TxAlloMappingStateDigest() {
		t.Fatalf("unexpected adaptive mapping epoch row: %#v", second)
	}
}

func TestTxAlloDynamicV2297ImmutablePublicationIdempotentAndConflictFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epoch.json")
	first := txalloMappingSnapshotV22{SchemaVersion: txalloDynamicMappingSchema, Epoch: 1, Mapping: map[string]string{"alice": "s0"}, Aliases: map[string]string{}}
	first.MappingDigest = stableJSONDigest(first.Mapping)
	first.AliasesDigest = stableJSONDigest(first.Aliases)
	first.StateDigest = txalloStateDigest(first.Mapping, first.Aliases)
	if err := txalloPublishImmutableJSON(path, first); err != nil {
		t.Fatal(err)
	}
	if err := txalloPublishImmutableJSON(path, first); err != nil {
		t.Fatalf("identical immutable retry failed: %v", err)
	}
	second := first
	second.Mapping = map[string]string{"alice": "s1"}
	second.MappingDigest = stableJSONDigest(second.Mapping)
	second.StateDigest = txalloStateDigest(second.Mapping, second.Aliases)
	if err := txalloPublishImmutableJSON(path, second); err == nil {
		t.Fatal("conflicting immutable rewrite was accepted")
	}
}

func TestTxAlloDynamicV2297RefreshIgnoresMutableLatestAlias(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, txalloDynamicMappingSnapshotName)
	p := &txalloAccountSharding{
		basicPlugin:         makeBasic("sharding", txalloShardingID, map[string]any{"dynamic_a_txallo_runtime": true}),
		allocator:           newTxAlloAllocator([]string{"s0", "s1"}, 2, 1, 0.1),
		shards:              []string{"s0", "s1"},
		aliases:             map[string]string{},
		evidence:            map[string]any{},
		provisionalAccounts: map[string]bool{},
		mappingSnapshotPath: base,
	}
	p.allocator.Mapping = map[string]string{"alice": "s0"}

	wrongLatest := txalloMappingSnapshotV22{SchemaVersion: txalloDynamicMappingSchema, Epoch: 77, Mapping: map[string]string{"alice": "s0"}, Aliases: map[string]string{}}
	wrongLatest.MappingDigest = stableJSONDigest(wrongLatest.Mapping)
	wrongLatest.AliasesDigest = stableJSONDigest(wrongLatest.Aliases)
	wrongLatest.StateDigest = txalloStateDigest(wrongLatest.Mapping, wrongLatest.Aliases)
	if err := txalloWriteAtomicJSON(base, wrongLatest); err != nil {
		t.Fatal(err)
	}

	next := txalloMappingSnapshotV22{SchemaVersion: txalloDynamicMappingSchema, Epoch: 1, Mapping: map[string]string{"alice": "s1"}, Aliases: map[string]string{}}
	next.MappingDigest = stableJSONDigest(next.Mapping)
	next.AliasesDigest = stableJSONDigest(next.Aliases)
	next.StateDigest = txalloStateDigest(next.Mapping, next.Aliases)
	if err := txalloPublishImmutableJSON(txalloMappingSnapshotEpochPath(base, 1), next); err != nil {
		t.Fatal(err)
	}
	if err := p.RefreshTxAlloMapping(); err != nil {
		t.Fatal(err)
	}
	if p.TxAlloMappingEpoch() != 1 || p.TxAlloMappingStateDigest() != next.StateDigest {
		t.Fatalf("immutable refresh failed epoch=%d digest=%s", p.TxAlloMappingEpoch(), p.TxAlloMappingStateDigest())
	}
}

func TestTxAlloDynamicV2297InitialAndAdaptiveEpochFilesExist(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{"dynamic_a_txallo_runtime": true, "a_epoch_blocks": 15, "g_epoch_multiple": 20})
	if err := p.writeTxAlloMappingSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(txalloMappingSnapshotEpochPath(p.mappingSnapshotPath, 0)); err != nil {
		t.Fatalf("missing immutable initial mapping: %v", err)
	}
	if err := p.ApplyCommittedTxAlloEpoch([]WorkloadRecord{{Index: 0, LogicalID: "e0", SenderID: "alice", ReceiverID: "dave"}}, 0, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(txalloMappingSnapshotEpochPath(p.mappingSnapshotPath, 1)); err != nil {
		t.Fatalf("missing immutable adaptive mapping: %v", err)
	}
}
