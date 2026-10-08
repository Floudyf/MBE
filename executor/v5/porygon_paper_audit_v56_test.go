package v5

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestPorygonV56PaperIdentitySnapshotTruth(t *testing.T) {
	r := &NodeRuntime{}
	state := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"later":   {TxID: "later", Status: porygonPaperTxUpdatePending, OriginHeight: 7, CrossShard: true, CommitProposalHeight: 10},
		"earlier": {TxID: "earlier", Status: porygonPaperTxCommitted, OriginHeight: 3, CrossShard: false, CommitRound: 6},
	}}
	porygonPaperRuntimeStates.Store(r, state)
	defer porygonPaperRuntimeStates.Delete(r)
	path := filepath.Join(t.TempDir(), "paper.csv")
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
	if len(rows) != 3 || rows[1][0] != "earlier" || rows[1][1] != "committed" || rows[2][0] != "later" || rows[2][1] != "update_pending" {
		t.Fatalf("non-deterministic/false paper snapshot: %v", rows)
	}
	if rows[2][9] != "10" {
		t.Fatalf("missing protocol commit evidence: %v", rows[2])
	}
}
func TestPorygonV56NoSyntheticPaperState(t *testing.T) {
	r := &NodeRuntime{}
	path := filepath.Join(t.TempDir(), "absent.csv")
	if err := r.porygonV56WritePaperIdentity(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unexpected manufactured snapshot: %v", err)
	}
}
