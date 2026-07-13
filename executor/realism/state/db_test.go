package state

import (
	"os"
	"path/filepath"
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
