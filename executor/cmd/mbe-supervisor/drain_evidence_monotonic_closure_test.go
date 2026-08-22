package main

import "testing"

func TestMergeTerminalEvidenceNeverRegressesOnTransientStatusLoss(t *testing.T) {
	seen := map[string]bool{"tx-1": true, "tx-2": true}
	seen = mergeTerminalEvidence(seen, map[string]bool{})
	if len(seen) != 2 {
		t.Fatalf("terminal evidence regressed after empty snapshot: %v", seen)
	}
	seen = mergeTerminalEvidence(seen, map[string]bool{"tx-3": true})
	if len(seen) != 3 || !seen["tx-1"] || !seen["tx-2"] || !seen["tx-3"] {
		t.Fatalf("terminal evidence merge failed: %v", seen)
	}
}

func TestIncompleteStatusCannotInventInfrastructureProgress(t *testing.T) {
	previous := progressSnapshot{
		Terminal: 100, MinHeight: 2900, MaxHeight: 3024,
		Mempool: 80, Reserved: 8, Pending: 4, ProposalInFlight: true,
	}
	partial := progressSnapshot{
		Terminal: 101, MinHeight: 3024, MaxHeight: 3024,
		Mempool: 0, Reserved: 0, Pending: 0, ProposalInFlight: false,
	}
	got := observeDrainProgress(previous, true, partial, false)
	if !got.AnyProgress || !got.TerminalProgress {
		t.Fatalf("new terminal evidence was not retained: %+v", got)
	}
	if got.HeightProgress || got.MempoolProgress || got.PendingProgress {
		t.Fatalf("partial node snapshot invented infrastructure progress: %+v", got)
	}
	if got.Snapshot.Terminal != 101 || got.Snapshot.MinHeight != previous.MinHeight ||
		got.Snapshot.MaxHeight != previous.MaxHeight || got.Snapshot.Mempool != previous.Mempool ||
		got.Snapshot.Reserved != previous.Reserved || got.Snapshot.Pending != previous.Pending ||
		got.Snapshot.ProposalInFlight != previous.ProposalInFlight {
		t.Fatalf("partial snapshot overwrote trusted infrastructure state: got=%+v previous=%+v", got.Snapshot, previous)
	}
}

func TestIncompleteStatusWithoutTerminalAdvanceDoesNotResetNoProgress(t *testing.T) {
	previous := progressSnapshot{
		Terminal: 100, MinHeight: 2900, MaxHeight: 3024,
		Mempool: 80, Reserved: 8, Pending: 4, ProposalInFlight: true,
	}
	partial := progressSnapshot{
		Terminal: 100, MinHeight: 3024, MaxHeight: 3024,
		Mempool: 0, Reserved: 0, Pending: 0, ProposalInFlight: false,
	}
	got := observeDrainProgress(previous, true, partial, false)
	if got.AnyProgress || got.TerminalProgress || got.HeightProgress || got.MempoolProgress || got.PendingProgress {
		t.Fatalf("partial snapshot incorrectly reset progress accounting: %+v", got)
	}
	if got.Snapshot != previous {
		t.Fatalf("partial snapshot replaced trusted state: got=%+v previous=%+v", got.Snapshot, previous)
	}
}

func TestCompleteStatusCanAdvanceInfrastructureProgress(t *testing.T) {
	previous := progressSnapshot{Terminal: 100, MinHeight: 2900, MaxHeight: 3024, Mempool: 80, Reserved: 8, Pending: 4, ProposalInFlight: true}
	complete := progressSnapshot{Terminal: 100, MinHeight: 2920, MaxHeight: 3024, Mempool: 60, Reserved: 4, Pending: 2, ProposalInFlight: false}
	got := observeDrainProgress(previous, true, complete, true)
	if !got.AnyProgress || !got.HeightProgress || !got.MempoolProgress || !got.PendingProgress {
		t.Fatalf("complete snapshot progress was not recognized: %+v", got)
	}
	if got.Snapshot != complete {
		t.Fatalf("complete snapshot was not accepted: got=%+v want=%+v", got.Snapshot, complete)
	}
}
