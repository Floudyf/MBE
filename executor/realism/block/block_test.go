package block

import (
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBlockHashAndTxRootDeterministic(t *testing.T) {
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "alice", Receiver: "bob", Value: 1, Seed: "1"})
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{txs[0].TxID, txs[1].TxID}
	rootA := TxRoot(ids)
	rootB := TxRoot(ids)
	if rootA != rootB {
		t.Fatalf("tx root not deterministic")
	}
	b := Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxIDs: ids, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	AssignHash(&b)
	h := b.BlockHash
	AssignHash(&b)
	if b.BlockHash != h {
		t.Fatalf("block hash not deterministic")
	}
}

func TestProposerBuildsFromMempoolAndRejectsEmpty(t *testing.T) {
	pool := mempool.New("n0", "s0", mempool.Policy{Capacity: 10, TTL: time.Minute}, account.NewNonceManager())
	proposer := NewProposer("n0", "s0")
	if _, err := proposer.Build(pool, 10, time.UnixMilli(1)); err == nil {
		t.Fatalf("expected empty mempool error")
	}
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "alice", Receiver: "bob", Value: 1, Seed: "1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range txs {
		if result := pool.Admit(item); !result.Accepted {
			t.Fatalf("admit failed: %+v", result)
		}
	}
	b, err := proposer.Build(pool, 10, time.UnixMilli(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(b.TxIDs) != 2 || b.BlockHash == "" || b.StateCommit {
		t.Fatalf("unexpected block: %+v", b)
	}
}
