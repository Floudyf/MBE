package state

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDBRootProofAndRecovery(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	db.SetBalance("alice", 10)
	db.SetNonce("alice", 1)
	root := db.Root()
	proof := db.GenerateProof("balance:alice")
	if !VerifyProof("balance:alice", "10", proof, root) {
		t.Fatalf("proof did not verify")
	}
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Root() != root || loaded.Balance("alice") != 10 {
		t.Fatalf("state did not recover")
	}
}

func TestDBSaveUsesAtomicSnapshotWithoutTempFiles(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	db.Set("value", "first")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(dir, "state_snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	db.Set("value", "second")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, err := Open(dir, "s0")
	if err != nil || loaded.Get("value") != "second" {
		t.Fatalf("new snapshot was not reloadable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state_snapshot.json.bak")); !os.IsNotExist(err) {
		t.Fatal("snapshot backup was not removed")
	}
	if matches, err := filepath.Glob(filepath.Join(dir, "state_snapshot-*.tmp")); err != nil || len(matches) != 0 {
		t.Fatal("snapshot temp file remained")
	}
	if len(old) == 0 {
		t.Fatal("initial snapshot was empty")
	}
}

func TestDBPersistsAndRecoversWALDeltaWithoutPerBlockSnapshot(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	before := db.Root()
	updates := []StateKV{{Key: "balance:alice", Value: "10", TxIDs: []string{"tx1"}}}
	if err := db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	after := db.Root()
	metrics, err := db.PersistDelta(DeltaMetadata{BlockHeight: 1, BlockHash: "b1", ParentHash: "genesis", DeltaID: "d1", StateRootBefore: before, StateRootAfter: after}, updates, DefaultPersistenceOptions())
	if err != nil {
		t.Fatal(err)
	}
	if metrics.WALRecordCount != 1 || metrics.SnapshotCount != 0 || metrics.WrittenBytes == 0 {
		t.Fatalf("unexpected wal metrics: %+v", metrics)
	}
	if _, err := os.Stat(filepath.Join(dir, "state_snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("normal wal commit should not write full snapshot, err=%v", err)
	}
	writeTestCommitMarker(t, dir, "b1", 1)
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Get("balance:alice"); got != "10" {
		t.Fatalf("wal recovery value=%q", got)
	}
	if loaded.Root() != after {
		t.Fatalf("wal recovery root=%s want=%s", loaded.Root(), after)
	}
}

func TestDBWALSnapshotCadenceTruncatesReplayLog(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	opts := DefaultPersistenceOptions()
	opts.SnapshotCadence = 2
	for height := uint64(1); height <= 2; height++ {
		before := db.Root()
		blockHash := "b" + string(rune('0'+height))
		updates := []StateKV{{Key: "counter", Value: string(rune('0' + height))}}
		if err := db.ApplyDeterministicBatch(updates); err != nil {
			t.Fatal(err)
		}
		after := db.Root()
		metrics, err := db.PersistDelta(DeltaMetadata{BlockHeight: height, BlockHash: blockHash, ParentHash: "p", DeltaID: "d" + string(rune('0'+height)), StateRootBefore: before, StateRootAfter: after}, updates, opts)
		if err != nil {
			t.Fatal(err)
		}
		writeTestCommitMarker(t, dir, blockHash, height)
		snapshotMetrics, err := db.SnapshotIfDue(height, opts)
		if err != nil {
			t.Fatal(err)
		}
		if height == 2 && metrics.SnapshotCount != 0 {
			t.Fatalf("wal append should not snapshot before durable commit, got %+v", metrics)
		}
		if height == 2 && snapshotMetrics.SnapshotCount != 1 {
			t.Fatalf("expected cadence snapshot at height 2, got %+v", snapshotMetrics)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "state_delta.2.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("wal should be truncated after snapshot, got %q", string(data))
	}
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Get("counter"); got != "2" {
		t.Fatalf("snapshot recovery value=%q", got)
	}
}

func TestDBWALIgnoresTornTailWithoutMarker(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	before := db.Root()
	updates := []StateKV{{Key: "balance:alice", Value: "10"}}
	if err := db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	after := db.Root()
	if _, err := db.PersistDelta(DeltaMetadata{BlockHeight: 1, BlockHash: "b1", ParentHash: "genesis", DeltaID: "d1", StateRootBefore: before, StateRootAfter: after}, updates, DefaultPersistenceOptions()); err != nil {
		t.Fatal(err)
	}
	writeTestCommitMarker(t, dir, "b1", 1)
	f, err := os.OpenFile(filepath.Join(dir, "state_delta.1.wal"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"version":"state_delta_wal_v1"`); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Get("balance:alice"); got != "10" {
		t.Fatalf("committed wal record lost after torn tail: %q", got)
	}
}

func TestDBWALSkipsRecordsWithoutDurableBlockEvidence(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	before := db.Root()
	updates := []StateKV{{Key: "balance:alice", Value: "10"}}
	if err := db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PersistDelta(DeltaMetadata{BlockHeight: 1, BlockHash: "uncommitted", ParentHash: "genesis", DeltaID: "d1", StateRootBefore: before, StateRootAfter: db.Root()}, updates, DefaultPersistenceOptions()); err != nil {
		t.Fatal(err)
	}
	writeTestCommitMarker(t, dir, "other", 1)
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Get("balance:alice"); got != "" {
		t.Fatalf("uncommitted wal record was replayed: %q", got)
	}
}

func TestDBWALRejectsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state_delta.wal"), []byte(`{"version":"state_delta_wal_v1","namespace":"s0","checksum":"bad"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeTestCommitMarker(t, dir, "corrupt", 1)
	if _, err := Open(dir, "s0"); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum recovery failure, got %v", err)
	}
}

func writeTestCommitMarker(t *testing.T, dir, blockHash string, height uint64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "commit_markers.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"version":"durable_commit_marker_v1","block_hash":"` + blockHash + `","height":` + strconv.FormatUint(height, 10) + `,"committed":true}` + "\n"); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBRollbackRestoresWALCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	cp, err := db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	before := db.Root()
	updates := []StateKV{{Key: "balance:alice", Value: "10"}}
	if err := db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PersistDelta(DeltaMetadata{BlockHeight: 1, BlockHash: "b1", ParentHash: "genesis", DeltaID: "d1", StateRootBefore: before, StateRootAfter: db.Root()}, updates, DefaultPersistenceOptions()); err != nil {
		t.Fatal(err)
	}
	db.Restore(map[string]string{})
	if err := db.Rollback(cp); err != nil {
		t.Fatal(err)
	}
	loaded, err := Open(dir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Get("balance:alice"); got != "" {
		t.Fatalf("rollback left wal state %q", got)
	}
}

func TestCommutativeDeltaInitialValueRequiresExplicitMetadata(t *testing.T) {
	db := NewDB(t.TempDir(), "s0")
	if err := db.ApplyDeterministicBatch([]StateKV{{Key: "contract::balance:looks-like-account", UpdateSemantics: "commutative_delta", Delta: -1}}); err != nil {
		t.Fatal(err)
	}
	if got := db.Get("contract::balance:looks-like-account"); got != "-1" {
		t.Fatalf("ordinary balance-like key should not receive account default, got %q", got)
	}

	db = NewDB(t.TempDir(), "s0")
	if err := db.ApplyDeterministicBatch([]StateKV{{Key: "balance:alice", UpdateSemantics: "commutative_delta", Delta: -1}}); err != nil {
		t.Fatal(err)
	}
	if got := db.Get("balance:alice"); got != "-1" {
		t.Fatalf("ordinary balance key without explicit metadata should not receive account default, got %q", got)
	}

	db = NewDB(t.TempDir(), "s0")
	if err := db.ApplyDeterministicBatch([]StateKV{{Key: "balance:alice", UpdateSemantics: "commutative_delta", Delta: -1, ApplyOrigin: "metatrack_remote_state", DeltaKind: "account_balance_delta", HasInitialValue: true, InitialValue: 1_000_000}}); err != nil {
		t.Fatal(err)
	}
	if got := db.Get("balance:alice"); got != "999999" {
		t.Fatalf("explicit MetaTrack remote account delta should use its initial value, got %q", got)
	}

	db = NewDB(t.TempDir(), "s0")
	if err := db.ApplyDeterministicBatch([]StateKV{{Key: "balance:bob", UpdateSemantics: "commutative_delta", Delta: 1, ApplyOrigin: "metatrack_remote_state", DeltaKind: "account_balance_delta", HasInitialValue: true, InitialValue: 0}}); err != nil {
		t.Fatal(err)
	}
	if got := db.Get("balance:bob"); got != "1" {
		t.Fatalf("missing receiver account should advance from explicit zero initial value, got %q", got)
	}
	if err := db.ApplyDeterministicBatch([]StateKV{{Key: "balance:bob", UpdateSemantics: "commutative_delta", Delta: 1, ApplyOrigin: "metatrack_remote_state", DeltaKind: "account_balance_delta", HasInitialValue: true, InitialValue: 1_000_000}}); err != nil {
		t.Fatal(err)
	}
	if got := db.Get("balance:bob"); got != "2" {
		t.Fatalf("existing receiver account must ignore repeated initial value and apply delta only, got %q", got)
	}
}

func TestCommutativeDeltaWALIncludesZeroInitialValue(t *testing.T) {
	dir := t.TempDir()
	db := NewDB(dir, "s0")
	before := db.Root()
	updates := []StateKV{{Key: "balance:bob", Value: "1", UpdateSemantics: "commutative_delta", Delta: 1, ApplyOrigin: "metatrack_remote_state", DeltaKind: "account_balance_delta", HasInitialValue: true, InitialValue: 0}}
	if err := db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PersistDelta(DeltaMetadata{BlockHeight: 1, BlockHash: "b1", ParentHash: "genesis", DeltaID: "d1", StateRootBefore: before, StateRootAfter: db.Root()}, updates, DefaultPersistenceOptions()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "state_delta.1.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"initial_value":0`) {
		t.Fatalf("wal json should make zero initial value explicit: %s", data)
	}
}
