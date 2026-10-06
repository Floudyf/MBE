package v5

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestTxAlloHistoryRatioV21SidecarReader(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "h.gz")
	f, e := os.Create(p)
	if e != nil {
		t.Fatal(e)
	}
	gz := gzip.NewWriter(f)
	for i := 0; i < 3; i++ {
		r := txalloHistorySidecarRow{SchemaVersion: "mbe_txallo_history_record_v1", SourceOrder: i, TransactionID: "tx", SenderID: "alice", ReceiverID: "bob", Accounts: []string{"alice", "bob"}}
		b, _ := json.Marshal(r)
		if _, e = gz.Write(append(b, '\n')); e != nil {
			t.Fatal(e)
		}
	}
	if e = gz.Close(); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	h, rows, e := readTxAlloHistorySidecar(p, 3)
	if e != nil {
		t.Fatal(e)
	}
	if len(h) != 3 || len(rows) != 3 {
		t.Fatalf("h=%d rows=%d", len(h), len(rows))
	}
}

func TestTxAlloHistoryRatioV21RunLocalResolver(t *testing.T) {
	runDir := t.TempDir()
	clientDir := filepath.Join(runDir, "client")
	workDir := filepath.Join(runDir, "workload")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(workDir, "txallo_history.jsonl.gz")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := txalloRunHistoryPath(clientDir, "workload/txallo_history.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Clean(got) != filepath.Clean(target) {
		t.Fatalf("got=%s want=%s", got, target)
	}
	if _, err := txalloRunHistoryPath(clientDir, "../escape"); err == nil {
		t.Fatal("unsafe path accepted")
	}
}
