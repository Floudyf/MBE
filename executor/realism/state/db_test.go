package state

import "testing"

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
