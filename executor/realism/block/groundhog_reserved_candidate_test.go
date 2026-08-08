package block

import (
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

func TestGroundhogBuildFromReservedDoesNotRequireMempool(t *testing.T) {
	proposer := NewProposer("n0", "s0")
	items := []tx.SignedTransaction{{TxID: "tx-1"}, {TxID: "tx-2"}}
	now := time.UnixMilli(1234)
	block, err := proposer.BuildFromReserved(items, now)
	if err != nil {
		t.Fatal(err)
	}
	if block.Height != 1 || block.Timestamp != 1234 || len(block.TxList) != 2 {
		t.Fatalf("unexpected block: %+v", block)
	}
	if block.TxIDs[0] != "tx-1" || block.TxIDs[1] != "tx-2" {
		t.Fatalf("reserved order changed: %v", block.TxIDs)
	}
	if block.BlockHash == "" || block.TxRoot == "" {
		t.Fatalf("candidate hashes missing: %+v", block)
	}
}

func TestGroundhogBuildFromReservedRejectsEmptyInput(t *testing.T) {
	proposer := NewProposer("n0", "s0")
	if _, err := proposer.BuildFromReserved(nil, time.Now()); err == nil || err.Error() != "empty_mempool" {
		t.Fatalf("expected empty_mempool, got %v", err)
	}
}
