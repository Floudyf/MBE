package trace

import (
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestImportCSVProducesSignedTxJSONL(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "trace.csv")
	if err := os.WriteFile(input, []byte("from,to,value,state_keys\nalice,bob,3,acct:alice|acct:bob\nalice,carol,4,\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "signed.jsonl")
	summary, err := ImportCSV(ImportOptions{InputCSV: input, OutputJSONL: output, LogCSV: filepath.Join(dir, "trace_import_log.csv"), SummaryJSON: filepath.Join(dir, "summary.json"), Seed: "11"})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ImportedCount != 2 || summary.RejectedCount != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	f, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	count := 0
	if err := tx.ReadJSONL(f, func(item tx.SignedTransaction) error {
		count++
		return tx.Verify(item)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 txs, got %d", count)
	}
}
