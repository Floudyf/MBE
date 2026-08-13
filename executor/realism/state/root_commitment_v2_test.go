package state

import (
	"fmt"
	"testing"
)

func TestCommitmentMatchesFullRootAfterIncrementalUpdates(t *testing.T) {
	working := map[string]string{"s0::a": "1", "s0::b": "2", "s0::c": "3"}
	commitment := NewCommitment(working)
	if got, want := commitment.Root(), Root(working); got != want {
		t.Fatalf("initial root mismatch: got %s want %s", got, want)
	}
	updates := []struct{ key, value string }{{"s0::b", "20"}, {"s0::d", "4"}, {"s0::a", "10"}}
	for _, update := range updates {
		working[update.key] = update.value
		commitment.Set(update.key, update.value)
		if got, want := commitment.Root(), Root(working); got != want {
			t.Fatalf("root mismatch after %s=%s: got %s want %s", update.key, update.value, got, want)
		}
	}
}

func TestCommitmentIndependentOfInsertionOrder(t *testing.T) {
	ascending := NewCommitment(nil)
	descending := NewCommitment(nil)
	for i := 0; i < 100; i++ {
		ascending.Set(fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i))
	}
	for i := 99; i >= 0; i-- {
		descending.Set(fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%03d", i))
	}
	if got, want := ascending.Root(), descending.Root(); got != want {
		t.Fatalf("deterministic root mismatch: got %s want %s", got, want)
	}
}

func TestLegacyRootVersionRemainsReadable(t *testing.T) {
	snapshot := map[string]string{"b": "2", "a": "1"}
	if got, want := RootForVersion(snapshot, LegacyCommitmentVersion), LegacyRoot(snapshot); got != want {
		t.Fatalf("legacy version mismatch: got %s want %s", got, want)
	}
	if got, want := RootForVersion(snapshot, CommitmentVersion), Root(snapshot); got != want {
		t.Fatalf("v2 version mismatch: got %s want %s", got, want)
	}
}

func TestDBRootTracksIncrementalWrites(t *testing.T) {
	db := NewDB(t.TempDir(), "s0")
	db.Set("a", "1")
	db.Set("b", "2")
	if got, want := db.Root(), Root(db.Snapshot()); got != want {
		t.Fatalf("db root after Set mismatch: got %s want %s", got, want)
	}
	db.ApplyBatch(map[string]string{"a": "10", "c": "3"})
	if got, want := db.Root(), Root(db.Snapshot()); got != want {
		t.Fatalf("db root after ApplyBatch mismatch: got %s want %s", got, want)
	}
}

func TestCommitmentCloneIsolatedAndEquivalent(t *testing.T) {
	base := map[string]string{"s0::a": "1", "s0::b": "2", "s0::c": "3"}
	original := NewCommitment(base)
	originalRoot := original.Root()
	clone := original.Clone()
	clone.Set("s0::b", "20")
	clone.Set("s0::d", "4")
	if original.Root() != originalRoot {
		t.Fatalf("mutating clone changed original root: before=%s after=%s", originalRoot, original.Root())
	}
	want := map[string]string{"s0::a": "1", "s0::b": "20", "s0::c": "3", "s0::d": "4"}
	if clone.Root() != Root(want) {
		t.Fatalf("clone root mismatch: got=%s want=%s", clone.Root(), Root(want))
	}
}

func TestDBCommitmentSnapshotTracksStateWithoutAliasing(t *testing.T) {
	db := NewDB(t.TempDir(), "s0")
	db.Set("a", "1")
	snapshot := db.CommitmentSnapshot()
	rootBefore := snapshot.Root()
	db.Set("a", "2")
	if snapshot.Root() != rootBefore {
		t.Fatalf("DB mutation changed prior commitment snapshot: before=%s after=%s", rootBefore, snapshot.Root())
	}
	if db.Root() == rootBefore {
		t.Fatal("DB root did not change after state mutation")
	}
}

func TestCommitmentCloneRepeatedBlockUpdatesMatchFullRebuild(t *testing.T) {
	working := map[string]string{}
	for i := 0; i < 256; i++ {
		working[fmt.Sprintf("s0::key:%03d", i)] = fmt.Sprintf("value:%03d", i)
	}
	commitment := NewCommitment(working)
	for blockIndex := 0; blockIndex < 20; blockIndex++ {
		blockCommitment := commitment.Clone()
		before := commitment.Root()
		for updateIndex := 0; updateIndex < 25; updateIndex++ {
			keyIndex := (blockIndex*31 + updateIndex*17) % 256
			key := fmt.Sprintf("s0::key:%03d", keyIndex)
			value := fmt.Sprintf("block:%02d:update:%02d", blockIndex, updateIndex)
			working[key] = value
			blockCommitment.Set(key, value)
		}
		if commitment.Root() != before {
			t.Fatalf("base commitment mutated while block clone was updated")
		}
		want := RootOfSnapshot(working)
		if got := blockCommitment.Root(); got != want {
			t.Fatalf("block %d root mismatch: got %s want %s", blockIndex, got, want)
		}
		commitment = blockCommitment
	}
}

func TestDBSnapshotWithCommitmentIsAtomicAndEquivalent(t *testing.T) {
	db := NewDB(t.TempDir(), "s0")
	for i := 0; i < 128; i++ {
		db.Set(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
	}
	snapshot, commitment := db.SnapshotWithCommitment()
	if got, want := commitment.Root(), Root(snapshot); got != want {
		t.Fatalf("atomic snapshot commitment mismatch: got=%s want=%s", got, want)
	}
	before := commitment.Root()
	db.Set("k000", "changed-after-snapshot")
	if commitment.Root() != before {
		t.Fatal("captured commitment changed after subsequent DB mutation")
	}
	if snapshot[db.key("k000")] == "changed-after-snapshot" {
		t.Fatal("captured KV snapshot aliased later DB mutation")
	}
}
