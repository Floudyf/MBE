package core

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

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

func TestStreamTransactionsAcceptsV1OptionalFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.jsonl.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	_, err = writer.Write([]byte(`{"tx_id":"v1-1","tx_type":"reward_pool_delta","timestamp":0,"chain_id":"mockchain-0","contract":"reward","function":"delta","args":{},"read_set":["reward_pool:0"],"write_set":["reward_pool:0"],"access_list":["reward_pool:0"],"commutative":true,"update_type":"delta","status":"pending","chain_latency_ms":0,"primary_key":"reward_pool:0","access_size":1,"is_cross_shard":false,"hot_key_tag":"reward_hotspot:0","conflict_group":"reward_conflict:0","dependency_hint":"reward_conflict:0","delta_value":2.5}` + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	txs, errs := StreamTransactions(path)
	tx := <-txs
	if tx.PrimaryKey == nil || *tx.PrimaryKey != "reward_pool:0" || tx.DeltaValue == nil || *tx.DeltaValue != 2.5 {
		t.Fatalf("optional V1 fields were not decoded: %+v", tx)
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
