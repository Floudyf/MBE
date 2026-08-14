package pbft

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestPBFTAuthenticationBindsSenderAndPayload(t *testing.T) {
	pub0, priv0, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub1, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	prepare := Prepare{View: 2, Sequence: 7, Height: 7, NodeID: "n0", BlockHash: "block-a"}
	if err := SignPrepare(&prepare, priv0); err != nil {
		t.Fatal(err)
	}
	if prepare.Signature == "" {
		t.Fatal("signed PREPARE must carry a signature")
	}
	if err := VerifyPrepare(prepare, pub0); err != nil {
		t.Fatalf("valid PREPARE signature rejected: %v", err)
	}
	if err := VerifyPrepare(prepare, pub1); err == nil {
		t.Fatal("PREPARE signed by n0 must not verify as n1")
	}

	tampered := prepare
	tampered.BlockHash = "block-b"
	if err := VerifyPrepare(tampered, pub0); err == nil {
		t.Fatal("tampered PREPARE payload must invalidate its signature")
	}
}

func TestPBFTAuthenticationUsesMessageTypeDomainSeparation(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	prepare := Prepare{View: 1, Sequence: 3, Height: 3, NodeID: "n0", BlockHash: "block-a"}
	if err := SignPrepare(&prepare, priv); err != nil {
		t.Fatal(err)
	}
	commit := Commit{View: 1, Sequence: 3, Height: 3, NodeID: "n0", BlockHash: "block-a", Signature: prepare.Signature}
	if err := VerifyCommit(commit, pub); err == nil {
		t.Fatal("PREPARE signature must not authenticate a COMMIT")
	}
}

func TestPBFTAuthenticationRoundTripsAllPersistentEvidenceMessages(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	pre := PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: "block-a"}
	if err := SignPrePrepare(&pre, priv); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPrePrepare(pre, pub); err != nil {
		t.Fatal(err)
	}

	commit := Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n0", BlockHash: "block-a"}
	if err := SignCommit(&commit, priv); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCommit(commit, pub); err != nil {
		t.Fatal(err)
	}

	checkpoint := Checkpoint{Height: 20, NodeID: "n0", BlockHash: "block-20", StateRoot: "state-20"}
	if err := SignCheckpoint(&checkpoint, priv); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(checkpoint, pub); err != nil {
		t.Fatal(err)
	}

	viewChange := ViewChange{View: 0, NewView: 1, NodeID: "n0", Height: 2, LeaderID: "n1"}
	if err := SignViewChange(&viewChange, priv); err != nil {
		t.Fatal(err)
	}
	if err := VerifyViewChange(viewChange, pub); err != nil {
		t.Fatal(err)
	}

	newView := NewView{View: 1, LeaderID: "n0", Height: 2}
	if err := SignNewView(&newView, priv); err != nil {
		t.Fatal(err)
	}
	if err := VerifyNewView(newView, pub); err != nil {
		t.Fatal(err)
	}
}
