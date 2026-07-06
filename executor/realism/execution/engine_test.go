package execution

import (
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestEngineDeterministicExecution(t *testing.T) {
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "alice", Receiver: "bob", Value: 1, Seed: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	block.AssignHash(&b)
	db1 := state.NewDB(t.TempDir(), "s0")
	db2 := state.NewDB(t.TempDir(), "s0")
	r1 := NewEngine().ExecuteBlock(b, db1)
	r2 := NewEngine().ExecuteBlock(b, db2)
	if r1.StateRootAfter != r2.StateRootAfter || r1.ReceiptRoot != r2.ReceiptRoot {
		t.Fatalf("execution not deterministic")
	}
	if r1.SuccessfulTxs != 2 || r1.FailedTxs != 0 {
		t.Fatalf("unexpected receipt counts: %+v", r1)
	}
}
