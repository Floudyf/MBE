package bridge

import (
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestBlockEmulatorSelectedTxsCSVParseSample(t *testing.T) {
	path := writeSampleCSV(t, "from,to,amount,time\nalice,bob,2,10\n")
	summary, imported, err := ImportSelectedTxsCSV(ImportOptions{Input: path, OutDir: t.TempDir(), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ImportedTxCount != 1 || len(imported) != 1 {
		t.Fatalf("unexpected import count: %+v len=%d", summary, len(imported))
	}
}

func TestBlockEmulatorCSVToSignedTxJSONL(t *testing.T) {
	outDir := t.TempDir()
	path := writeSampleCSV(t, "sender,receiver,value\nalice,bob,1\ncarol,dave,3\n")
	summary, _, err := ImportSelectedTxsCSV(ImportOptions{Input: path, OutDir: outDir, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if summary.SignedTxJSONL == "" {
		t.Fatal("missing signed tx jsonl path")
	}
	if info, err := os.Stat(filepath.Join(outDir, "blockemulator_signed_txs.jsonl")); err != nil || info.Size() == 0 {
		t.Fatalf("missing jsonl: %v size=%d", err, sizeOf(info))
	}
}

func TestBlockEmulatorSignedTxsVerify(t *testing.T) {
	path := writeSampleCSV(t, "account_from,account_to,value\nalice,bob,1\n")
	_, imported, err := ImportSelectedTxsCSV(ImportOptions{Input: path, OutDir: t.TempDir(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Verify(imported[0]); err != nil {
		t.Fatalf("imported tx should verify: %v", err)
	}
}

func TestBlockEmulatorMalformedCSVClearError(t *testing.T) {
	path := writeSampleCSV(t, "who,receiver,value\nalice,bob,1\n")
	if _, _, err := ImportSelectedTxsCSV(ImportOptions{Input: path, OutDir: t.TempDir(), Limit: 1}); err == nil {
		t.Fatal("expected malformed csv error")
	}
}

func writeSampleCSV(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "selectedTxs.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func sizeOf(info os.FileInfo) int64 {
	if info == nil {
		return 0
	}
	return info.Size()
}
