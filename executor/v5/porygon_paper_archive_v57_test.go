package v5

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestPorygonV57ArchivePrunedTerminalIdentities(t *testing.T) {
	r := &NodeRuntime{node: NodePlan{DataDir: t.TempDir()}}
	state := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"early":   {TxID: "early", OriginHeight: 1, Status: porygonPaperTxCommitted, WitnessRound: 1, OrderingRound: 2, PreExecutionRound: 3, CommitProposalHeight: 3, CommitRound: 4},
		"pending": {TxID: "pending", OriginHeight: 2, Status: porygonPaperTxUpdatePending, CrossShard: true},
		"late":    {TxID: "late", OriginHeight: 15, Status: porygonPaperTxCommitted},
	}, proposalUpdates: map[uint64][]PorygonProposalUpdate{}}
	porygonPaperRuntimeStates.Store(r, state)
	defer porygonPaperRuntimeStates.Delete(r)
	r.porygonPruneProtocolCaches(18) // cutoff = 10
	state.mu.Lock()
	_, oldKept := state.txs["early"]
	_, pendingKept := state.txs["pending"]
	_, lateKept := state.txs["late"]
	state.mu.Unlock()
	if oldKept || !pendingKept || !lateKept {
		t.Fatalf("incorrect prune: old=%v pending=%v late=%v", oldKept, pendingKept, lateKept)
	}
	r.porygonPruneProtocolCaches(18) // idempotent
	path := filepath.Join(r.node.DataDir, "paper_all.csv")
	if err := r.porygonV56WritePaperIdentity(path); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("want header plus 3 identities, got %d: %v", len(rows), rows)
	}
	if rows[1][0] != "early" || rows[1][1] != "committed" || rows[2][0] != "late" || rows[3][0] != "pending" {
		t.Fatalf("lost or falsified paper status: %v", rows)
	}
}

func TestPorygonV57ArchiveFailureDoesNotPruneTruth(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(badPath, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	r := &NodeRuntime{node: NodePlan{DataDir: badPath}}
	state := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"terminal": {TxID: "terminal", OriginHeight: 1, Status: porygonPaperTxCommitted},
	}, proposalUpdates: map[uint64][]PorygonProposalUpdate{}}
	porygonPaperRuntimeStates.Store(r, state)
	defer porygonPaperRuntimeStates.Delete(r)
	r.porygonPruneProtocolCaches(18)
	if _, ok := state.txs["terminal"]; !ok {
		t.Fatal("terminal row deleted despite archive write error")
	}
}

func TestPorygonV57OrderedFrontierRespectsActualConsensusProgress(t *testing.T) {
	r := &NodeRuntime{}
	if got := r.porygonV57LifecycleOrderedHeight(10); got != 10 {
		t.Fatalf("no pipeline: %d", got)
	}
	pipeline := &porygonPipelineRuntime{orderedHeight: 12}
	porygonPipelineRuntimes.Store(r, pipeline)
	defer porygonPipelineRuntimes.Delete(r)
	if got := r.porygonV57LifecycleOrderedHeight(10); got != 12 {
		t.Fatalf("missed earlier ordered frontier: %d", got)
	}
	if got := r.porygonV57LifecycleOrderedHeight(13); got != 13 {
		t.Fatalf("invented ordered frontier: %d", got)
	}
	paper := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"itx": {TxID: "itx", OriginHeight: 10, Status: porygonPaperTxITxExecuted, WitnessRound: 10, OrderingRound: 11, PreExecutionRound: 12, CommitProposalHeight: 12, CommitRound: 13},
	}}
	r.porygonPaperCommitReadyLocked(paper, r.porygonV57LifecycleOrderedHeight(10))
	if paper.txs["itx"].Status != porygonPaperTxCommitted {
		t.Fatalf("late execution never caught up to certified ordering: %s", paper.txs["itx"].Status)
	}
}
