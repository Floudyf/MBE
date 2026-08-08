package pbft

import (
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

func TestQuorumWithExtraReplicaUsesStrictTwoThirds(t *testing.T) {
	if got := Quorum(8); got != 6 {
		t.Fatalf("expected strict >2/3 quorum 6 for 8 validators, got %d", got)
	}
}

func TestConflictingPrePrepareSameViewSequenceRejected(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n1", "s0", "n0", validators)
	first := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{
		View: 0, Sequence: 1, Height: 1, LeaderID: "n0",
		BlockHash: first.BlockHash, Block: first,
	}); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Timestamp++
	block.AssignHash(&second)
	if second.BlockHash == first.BlockHash {
		t.Fatal("test setup did not create conflicting digest")
	}
	if _, err := s.OnPrePrepare(PrePrepare{
		View: 0, Sequence: 1, Height: 1, LeaderID: "n0",
		BlockHash: second.BlockHash, Block: second,
	}); err == nil {
		t.Fatal("conflicting digest for same view/sequence was accepted")
	}
}

func TestPreparedAndCommitCertificatesRequireMatchingValidatorQuorums(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{
		View: 0, Sequence: 1, Height: 1, LeaderID: "n0",
		BlockHash: b.BlockHash, Block: b,
	}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1"} {
		reached, _, err := s.AcceptPrepare(Prepare{
			View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash,
		})
		if err != nil {
			t.Fatal(err)
		}
		if reached {
			t.Fatalf("prepare quorum reached too early at %s", nodeID)
		}
	}
	reached, count, err := s.AcceptPrepare(Prepare{
		View: 0, Sequence: 1, Height: 1, NodeID: "n2", BlockHash: b.BlockHash,
	})
	if err != nil || !reached || count != 3 {
		t.Fatalf("expected prepared certificate: reached=%t count=%d err=%v", reached, count, err)
	}
	cert, preparedBlock, ok := s.PreparedCertificate(1)
	if !ok || cert.BlockHash != b.BlockHash || preparedBlock.BlockHash != b.BlockHash || len(cert.Prepares) != 3 {
		t.Fatalf("unexpected prepared certificate: %+v block=%+v ok=%t", cert, preparedBlock, ok)
	}
	for _, nodeID := range []string{"n0", "n1"} {
		committed, _, _, err := s.AcceptCommit(Commit{
			View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash,
		})
		if err != nil {
			t.Fatal(err)
		}
		if committed {
			t.Fatalf("commit quorum reached too early at %s", nodeID)
		}
	}
	committed, commitCount, committedBlock, err := s.AcceptCommit(Commit{
		View: 0, Sequence: 1, Height: 1, NodeID: "n2", BlockHash: b.BlockHash,
	})
	if err != nil || !committed || commitCount != 3 || committedBlock.BlockHash != b.BlockHash {
		t.Fatalf("expected commit certificate: committed=%t count=%d block=%s err=%v", committed, commitCount, committedBlock.BlockHash, err)
	}
}

func TestPrepareAndCommitEquivocationRejected(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n9", BlockHash: b.BlockHash}); err == nil {
		t.Fatal("unknown validator prepare was accepted")
	}
	if _, _, err := s.AcceptPrepare(Prepare{View: 1, Sequence: 1, Height: 1, NodeID: "n1", BlockHash: b.BlockHash}); err == nil {
		t.Fatal("wrong-view prepare was accepted")
	}
	if _, _, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n9", BlockHash: b.BlockHash}); err == nil {
		t.Fatal("unknown validator commit was accepted")
	}
}

func TestViewChangeSelectsHighestPreparedCertificate(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	source := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := source.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, err := source.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, preparedBlock, ok := source.PreparedCertificate(1)
	if !ok {
		t.Fatal("test setup did not prepare block")
	}
	newPrimary := NewState("n1", "s0", "n0", validators)
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		certCopy := copyPreparedCertificate(cert)
		blockCopy := preparedBlock
		vc := ViewChange{
			View: 0, NewView: 1, NodeID: nodeID, Height: 1, LeaderID: "n1",
			Prepared: &certCopy, PreparedBlock: &blockCopy,
		}
		if _, _, err := newPrimary.AcceptViewChange(vc); err != nil {
			t.Fatalf("accept view change from %s: %v", nodeID, err)
		}
	}
	nv, err := newPrimary.BuildNewView(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if nv.SelectedPrepared == nil || nv.SelectedBlock == nil || nv.SelectedPrepared.BlockHash != b.BlockHash || nv.SelectedBlock.BlockHash != b.BlockHash {
		t.Fatalf("new view did not select prepared block: %+v", nv)
	}
	selected, hasSelected, err := newPrimary.AcceptNewView(nv)
	if err != nil || !hasSelected || selected.BlockHash != b.BlockHash {
		t.Fatalf("new view acceptance failed: selected=%s has=%t err=%v", selected.BlockHash, hasSelected, err)
	}
	if newPrimary.View() != 1 || newPrimary.Leader() != "n1" {
		t.Fatalf("view/leader not advanced: view=%d leader=%s", newPrimary.View(), newPrimary.Leader())
	}
}

func TestNewViewCannotOmitPreparedCertificate(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	source := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := source.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, err := source.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, preparedBlock, _ := source.PreparedCertificate(1)
	viewChanges := make([]ViewChange, 0, 3)
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		certCopy := copyPreparedCertificate(cert)
		blockCopy := preparedBlock
		viewChanges = append(viewChanges, ViewChange{
			View: 0, NewView: 1, NodeID: nodeID, Height: 1, LeaderID: "n1",
			Prepared: &certCopy, PreparedBlock: &blockCopy,
		})
	}
	target := NewState("n2", "s0", "n0", validators)
	if _, _, err := target.AcceptNewView(NewView{
		View: 1, LeaderID: "n1", Height: 1, ViewChanges: viewChanges,
	}); err == nil {
		t.Fatal("NEW-VIEW omitted highest prepared proof but was accepted")
	}
}

func TestStableCheckpointAdvancesWatermarks(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	b.Height = 20
	block.AssignHash(&b)
	s.MarkDurableCommit(b)
	var stable bool
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		reached, _, cert, err := s.AcceptCheckpoint(Checkpoint{
			Height: 20, NodeID: nodeID, BlockHash: b.BlockHash, StateRoot: "state-root-20",
		})
		if err != nil {
			t.Fatal(err)
		}
		if reached {
			stable = true
			if cert.Height != 20 || len(cert.Checkpoints) != 3 {
				t.Fatalf("unexpected checkpoint certificate: %+v", cert)
			}
		}
	}
	if !stable {
		t.Fatal("checkpoint quorum did not become stable")
	}
	snapshot := s.Snapshot()
	if snapshot.LowWatermark != 20 || snapshot.HighWatermark <= snapshot.LowWatermark || snapshot.StableCheckpointHeight != 20 {
		t.Fatalf("watermarks did not advance: %+v", snapshot)
	}
}

func TestPrepareAndCommitArrivingBeforePrePrepareAreReplayed(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n3", "s0", "n0", validators)
	b := testBlock()
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if reached, _, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash}); err != nil || reached {
			t.Fatalf("early prepare %s: reached=%t err=%v", nodeID, reached, err)
		}
		if committed, _, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash}); err != nil || committed {
			t.Fatalf("early commit %s: committed=%t err=%v", nodeID, committed, err)
		}
	}
	prepare, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b})
	if err != nil {
		t.Fatal(err)
	}
	reached, count, err := s.AcceptPrepare(prepare)
	if err != nil || !reached || count != 4 {
		t.Fatalf("buffered prepares were not replayed: reached=%t count=%d err=%v", reached, count, err)
	}
	committed, commitCount, blockOut, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: "n3", BlockHash: b.BlockHash})
	if err != nil || !committed || commitCount != 4 || blockOut.BlockHash != b.BlockHash {
		t.Fatalf("buffered commits were not replayed: committed=%t count=%d hash=%s err=%v", committed, commitCount, blockOut.BlockHash, err)
	}
}

func TestCommitBroadcastCanBeRetriedAfterInterval(t *testing.T) {
	s := NewState("n0", "s0", "n0", []string{"n0", "n1", "n2", "n3"})
	if !s.ShouldBroadcastCommit(0, 1, "hash", time.Hour) {
		t.Fatal("first commit broadcast should be allowed")
	}
	if s.ShouldBroadcastCommit(0, 1, "hash", time.Hour) {
		t.Fatal("immediate duplicate commit broadcast should be rate limited")
	}
	if !s.ShouldBroadcastCommit(0, 1, "hash", 0) {
		t.Fatal("explicit retransmission should be allowed with zero interval")
	}
}

func TestDuplicateNewViewStillRequiresValidCertificate(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n2", "s0", "n0", validators)
	viewChanges := make([]ViewChange, 0, 3)
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		viewChanges = append(viewChanges, ViewChange{View: 0, NewView: 1, NodeID: nodeID, Height: 1, LeaderID: "n1"})
	}
	valid := NewView{View: 1, LeaderID: "n1", Height: 1, ViewChanges: viewChanges}
	if _, _, err := s.AcceptNewView(valid); err != nil {
		t.Fatalf("valid new view rejected: %v", err)
	}
	// A duplicate for the already-installed view must still carry the full
	// certificate. Merely matching the current view/leader is insufficient.
	if _, _, err := s.AcceptNewView(NewView{View: 1, LeaderID: "n1", Height: 1}); err == nil {
		t.Fatal("duplicate NEW-VIEW without certificate was accepted")
	}
}

func TestStableCheckpointPrunesEarlyVotesBelowWatermark(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	for _, nodeID := range []string{"n1", "n2"} {
		if _, _, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 10, Height: 10, NodeID: nodeID, BlockHash: "early-hash"}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 10, Height: 10, NodeID: nodeID, BlockHash: "early-hash"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.EarlyPrepareVotes) == 0 || len(s.EarlyCommitVotes) == 0 {
		t.Fatal("test setup did not buffer early votes")
	}
	b := testBlock()
	b.Height = 20
	block.AssignHash(&b)
	s.MarkDurableCommit(b)
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, _, err := s.AcceptCheckpoint(Checkpoint{Height: 20, NodeID: nodeID, BlockHash: b.BlockHash, StateRoot: "state-root-20"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.EarlyPrepareVotes) != 0 || len(s.EarlyCommitVotes) != 0 {
		t.Fatalf("stable checkpoint retained early votes: prepare=%d commit=%d", len(s.EarlyPrepareVotes), len(s.EarlyCommitVotes))
	}
}

func TestViewChangeRejectsTamperedPreparedBlockBody(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	source := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := source.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, err := source.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, preparedBlock, ok := source.PreparedCertificate(1)
	if !ok {
		t.Fatal("test setup did not prepare block")
	}
	preparedBlock.PreviousHash = "tampered-parent"
	target := NewState("n1", "s0", "n0", validators)
	if _, _, err := target.AcceptViewChange(ViewChange{
		View: 0, NewView: 1, NodeID: "n0", Height: 1, LeaderID: "n1",
		Prepared: &cert, PreparedBlock: &preparedBlock,
	}); err == nil {
		t.Fatal("tampered prepared block body was accepted")
	}
}

func TestCheckpointReportsStableOnlyOnFirstQuorumTransition(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	b.Height = 20
	block.AssignHash(&b)
	s.MarkDurableCommit(b)
	for index, nodeID := range validators {
		stable, _, _, err := s.AcceptCheckpoint(Checkpoint{Height: 20, NodeID: nodeID, BlockHash: b.BlockHash, StateRoot: "root"})
		if err != nil {
			t.Fatal(err)
		}
		if index < 2 && stable {
			t.Fatalf("checkpoint stable too early at vote %d", index+1)
		}
		if index == 2 && !stable {
			t.Fatal("checkpoint quorum transition was not reported")
		}
		if index == 3 && stable {
			t.Fatal("additional checkpoint vote re-reported the stable transition")
		}
	}
}

func TestEarlyVoteEquivocationIsRejected(t *testing.T) {
	s := NewState("n0", "s0", "n0", []string{"n0", "n1", "n2", "n3"})
	firstPrepare := Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n1", BlockHash: "hash-a"}
	if _, _, err := s.AcceptPrepare(firstPrepare); err != nil {
		t.Fatal(err)
	}
	conflictPrepare := firstPrepare
	conflictPrepare.BlockHash = "hash-b"
	if _, _, err := s.AcceptPrepare(conflictPrepare); err == nil {
		t.Fatal("conflicting early PREPARE was accepted")
	}
	firstCommit := Commit{View: 0, Sequence: 2, Height: 2, NodeID: "n2", BlockHash: "hash-a"}
	if _, _, _, err := s.AcceptCommit(firstCommit); err != nil {
		t.Fatal(err)
	}
	conflictCommit := firstCommit
	conflictCommit.BlockHash = "hash-b"
	if _, _, _, err := s.AcceptCommit(conflictCommit); err == nil {
		t.Fatal("conflicting early COMMIT was accepted")
	}
}

func TestEightValidatorPrepareAndCommitRequireSixVotes(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7"}
	s := NewState("n0", "s0", "n0", validators)
	b := testBlock()
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}); err != nil {
		t.Fatal(err)
	}
	for index, nodeID := range validators[:6] {
		reached, count, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash})
		if err != nil {
			t.Fatal(err)
		}
		if index < 5 && reached {
			t.Fatalf("8-node prepare quorum reached at %d votes", count)
		}
		if index == 5 && (!reached || count != 6) {
			t.Fatalf("8-node prepare quorum mismatch: reached=%t count=%d", reached, count)
		}
	}
	for index, nodeID := range validators[:6] {
		committed, count, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: b.BlockHash})
		if err != nil {
			t.Fatal(err)
		}
		if index < 5 && committed {
			t.Fatalf("8-node commit quorum reached at %d votes", count)
		}
		if index == 5 && (!committed || count != 6) {
			t.Fatalf("8-node commit quorum mismatch: committed=%t count=%d", committed, count)
		}
	}
}

func TestNewViewDoesNotReuseOldViewPrepareOrCommitVotes(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	block0 := testBlock()
	s := NewState("n1", "s0", "n0", validators)
	if _, err := s.OnPrePrepare(PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: block0.BlockHash, Block: block0}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, err := s.AcceptPrepare(Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block0.BlockHash}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := s.AcceptCommit(Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block0.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, preparedBlock, ok := s.PreparedCertificate(1)
	if !ok {
		t.Fatal("setup did not create prepared certificate")
	}
	viewChanges := make([]ViewChange, 0, 3)
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		certCopy := copyPreparedCertificate(cert)
		blockCopy := preparedBlock
		viewChanges = append(viewChanges, ViewChange{View: 0, NewView: 1, NodeID: nodeID, Height: 1, LeaderID: "n1", Prepared: &certCopy, PreparedBlock: &blockCopy})
	}
	nv := NewView{View: 1, LeaderID: "n1", Height: 1, ViewChanges: viewChanges, SelectedPrepared: &cert, SelectedBlock: &preparedBlock}
	if _, _, err := s.AcceptNewView(nv); err != nil {
		t.Fatal(err)
	}
	if got := s.PrepareVoteCount(block0.BlockHash); got != 0 {
		t.Fatalf("old-view PREPARE votes survived view transition: %d", got)
	}
	if got := s.CommitVoteCount(block0.BlockHash); got != 0 {
		t.Fatalf("old-view COMMIT votes survived view transition: %d", got)
	}
	prepare, err := s.OnPrePrepare(PrePrepare{View: 1, Sequence: 1, Height: 1, LeaderID: "n1", BlockHash: block0.BlockHash, Block: block0})
	if err != nil {
		t.Fatal(err)
	}
	reached, count, err := s.AcceptPrepare(prepare)
	if err != nil {
		t.Fatal(err)
	}
	if reached || count != 1 {
		t.Fatalf("new view reused old PREPARE votes: reached=%t count=%d", reached, count)
	}
}
