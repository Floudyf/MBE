package pbft

import (
	"sync"
	"testing"
)

func TestStateConcurrentPrepareAndCommitDoesNotPanic(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		for _, nodeID := range validators {
			wg.Add(2)
			go func(id string) {
				defer wg.Done()
				s.OnPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: b.BlockHash})
			}(nodeID)
			go func(id string) {
				defer wg.Done()
				s.OnCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: b.BlockHash})
			}(nodeID)
		}
	}
	wg.Wait()

	committed, votes, committedBlock := s.OnCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n3", BlockHash: b.BlockHash})
	if !committed || votes != 4 || committedBlock.BlockHash != b.BlockHash {
		t.Fatalf("expected committed block after concurrent votes, committed=%t votes=%d block=%s", committed, votes, committedBlock.BlockHash)
	}
}
