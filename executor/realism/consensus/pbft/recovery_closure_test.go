package pbft

import (
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

func TestCommitCertificateRetainedAfterDurableCommit(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"n0", "n1", "n2"} {
		if _, _, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: b.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"n0", "n1", "n2"} {
		if _, _, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: b.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, ok := s.CommitCertificate(1)
	if !ok || len(cert.Commits) != 3 || cert.BlockHash != b.BlockHash {
		t.Fatalf("missing commit certificate: %+v", cert)
	}
	s.MarkDurableCommit(b)
	cert, ok = s.CommitCertificate(1)
	if !ok || len(cert.Commits) != 3 {
		t.Fatal("durable commit discarded catch-up certificate")
	}
	if got := s.CommitVoteCount(b.BlockHash); got != 0 {
		t.Fatalf("raw commit votes should be pruned, got %d", got)
	}
}

func TestExternalCommitCertificateRequiresQuorumAndMatchingBlock(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n3", "s0", "n0", validators)
	b := testBlock()
	cert := CommitCertificate{View: 0, Sequence: 1, Height: 1, BlockHash: b.BlockHash}
	for _, id := range []string{"n0", "n1", "n2"} {
		cert.Commits = append(cert.Commits, Commit{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: b.BlockHash})
	}
	if err := s.AcceptCommitCertificate(cert, b); err != nil {
		t.Fatal(err)
	}
	bad := cert
	bad.Commits = bad.Commits[:2]
	if err := s.ValidateCommitCertificate(bad); err == nil {
		t.Fatal("insufficient commit proof accepted")
	}
	tampered := b
	tampered.Timestamp++
	if err := s.AcceptCommitCertificate(cert, tampered); err == nil {
		t.Fatal("tampered certified block accepted")
	}
}

func TestViewChangeMessageDoesNotResetRequestProgressTimer(t *testing.T) {
	s := NewState("n1", "s0", "n0", []string{"n0", "n1", "n2", "n3"})
	before := time.Now().Add(-time.Minute)
	s.mu.Lock()
	s.LastProgressTime = before
	s.mu.Unlock()
	if _, _, err := s.AcceptViewChange(ViewChange{View: 0, NewView: 1, NodeID: "n0", Height: 1, LeaderID: "n1"}); err != nil {
		t.Fatal(err)
	}
	after := s.LastProgress()
	if !after.Equal(before) {
		t.Fatalf("isolated VIEW-CHANGE reset request progress: before=%v after=%v", before, after)
	}
}

func TestRemoteCheckpointQuorumDoesNotAdvanceAheadOfLocalDurableState(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n3", "s0", "n0", validators)
	b := testBlock()
	b.Height = 20
	block.AssignHash(&b)

	for _, nodeID := range []string{"n0", "n1", "n2"} {
		stable, count, cert, err := s.AcceptCheckpoint(Checkpoint{
			Height: 20, NodeID: nodeID, BlockHash: b.BlockHash, StateRoot: "state-root-20",
		})
		if err != nil {
			t.Fatal(err)
		}
		if stable {
			t.Fatalf("remote checkpoint quorum advanced local watermarks before durable state: count=%d cert=%+v", count, cert)
		}
	}
	before := s.Snapshot()
	if before.LowWatermark != 0 || before.StableCheckpointHeight != 0 {
		t.Fatalf("remote checkpoint quorum changed local watermarks: %+v", before)
	}

	s.MarkDurableCommit(b)
	stable, count, cert, err := s.AcceptCheckpoint(Checkpoint{
		Height: 20, NodeID: "n3", BlockHash: b.BlockHash, StateRoot: "state-root-20",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !stable || count != 4 || cert.Height != 20 {
		t.Fatalf("local durable checkpoint did not install buffered quorum: stable=%t count=%d cert=%+v", stable, count, cert)
	}
	after := s.Snapshot()
	if after.LowWatermark != 20 || after.StableCheckpointHeight != 20 {
		t.Fatalf("local checkpoint did not advance watermarks: %+v", after)
	}
}

func TestStableCheckpointRetainsOldCertifiedCatchupProofs(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)

	old := testBlock()
	cert := CommitCertificate{View: 0, Sequence: 1, Height: 1, BlockHash: old.BlockHash}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		cert.Commits = append(cert.Commits, Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: old.BlockHash})
	}
	if err := s.AcceptCommitCertificate(cert, old); err != nil {
		t.Fatal(err)
	}

	checkpointBlock := testBlock()
	checkpointBlock.Height = 220
	checkpointBlock.PreviousHash = "height-219"
	block.AssignHash(&checkpointBlock)
	s.MarkDurableCommit(checkpointBlock)
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		stable, _, _, err := s.AcceptCheckpoint(Checkpoint{
			Height: 220, NodeID: nodeID, BlockHash: checkpointBlock.BlockHash, StateRoot: "state-root-220",
		})
		if err != nil {
			t.Fatal(err)
		}
		if nodeID == "n2" && !stable {
			t.Fatal("checkpoint did not become stable")
		}
	}
	if _, ok := s.CommitCertificate(1); !ok {
		t.Fatal("stable checkpoint pruned the old certified catch-up proof")
	}
}
