package core

import "testing"

func TestStreamTransactionsReadsGoldenTrace(t *testing.T) {
	txs, errs := StreamTransactions("../../tests/golden/trace_small.jsonl.gz")
	count := 0
	for tx := range txs {
		count++
		if tx.TxID == "" {
			t.Fatal("empty transaction ID")
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 2 {
		t.Fatalf("read %d transactions, want 2", count)
	}
}
