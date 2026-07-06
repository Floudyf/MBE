package pbft

import (
	"testing"

	"metaverse-chainlab/executor/realism/block"
)

func testBlock() block.Block {
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxIDs: []string{"tx1"}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	block.AssignHash(&b)
	return b
}

func TestQuorumCalculation(t *testing.T) {
	if got := Quorum(4); got != 3 {
		t.Fatalf("expected quorum 3, got %d", got)
	}
}

func TestPrePrepareValidationAndVoteCollection(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n1", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "bad", BlockHash: b.BlockHash, Block: b}); err == nil {
		t.Fatalf("expected wrong leader reject")
	}
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: "bad", Block: b}); err == nil {
		t.Fatalf("expected wrong digest reject")
	}
	prepare, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b})
	if err != nil {
		t.Fatal(err)
	}
	if prepare.BlockHash != b.BlockHash {
		t.Fatalf("unexpected prepare: %+v", prepare)
	}
	reached, _ := s.OnPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n0", BlockHash: b.BlockHash})
	if reached {
		t.Fatalf("quorum too early")
	}
	s.OnPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n2", BlockHash: b.BlockHash})
	reached, votes := s.OnPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n3", BlockHash: b.BlockHash})
	if !reached || votes != 3 {
		t.Fatalf("expected prepare quorum, got reached=%t votes=%d", reached, votes)
	}
	reached, votes = s.OnPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n3", BlockHash: b.BlockHash})
	if !reached || votes != 3 {
		t.Fatalf("duplicate vote should be ignored")
	}
	s.OnCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n0", BlockHash: b.BlockHash})
	s.OnCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n2", BlockHash: b.BlockHash})
	committed, votes, committedBlock := s.OnCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n3", BlockHash: b.BlockHash})
	if !committed || votes != 3 || committedBlock.BlockHash != b.BlockHash {
		t.Fatalf("expected commit quorum")
	}
}

func TestBasicViewChangeUpdatesView(t *testing.T) {
	s := NewState("n1", "s0", "n0", []string{"n0", "n1", "n2", "n3"})
	s.OnViewChange(ViewChange{View: 0, NewView: 1, NodeID: "n0", LeaderID: "n1"})
	s.OnViewChange(ViewChange{View: 0, NewView: 1, NodeID: "n2", LeaderID: "n1"})
	reached, votes := s.OnViewChange(ViewChange{View: 0, NewView: 1, NodeID: "n3", LeaderID: "n1"})
	if !reached || votes != 3 {
		t.Fatalf("expected view-change quorum")
	}
	s.OnNewView(NewView{View: 1, LeaderID: "n1"})
	if s.ViewID != 1 || s.LeaderID != "n1" {
		t.Fatalf("new view not applied")
	}
}
