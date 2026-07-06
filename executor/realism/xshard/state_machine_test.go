package xshard

import (
	"testing"

	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestCrossShardSuccessAndRefund(t *testing.T) {
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "alice", Receiver: "bob", Value: 1, Seed: "xs"})
	if err != nil {
		t.Fatal(err)
	}
	source := state.NewDB(t.TempDir(), "s0")
	target := state.NewDB(t.TempDir(), "s1")
	result, err := RunSuccess(txs[0], "s0", "s1", "block", source, target, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !result.RealCrossShardStateMachine || len(result.Certificates) != 1 || target.Get("xshard_credit:bob:"+txs[0].TxID) == "" {
		t.Fatalf("unexpected success result: %+v", result)
	}
	refund, err := RunRefund(txs[1], "s0", "s1", source, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if refund.RefundEvents != 1 || source.Get("xshard_refund:"+txs[1].TxID) == "" {
		t.Fatalf("unexpected refund result: %+v", refund)
	}
}
