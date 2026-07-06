package node

import (
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestRunOnceOutputsSummary(t *testing.T) {
	dir := t.TempDir()
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 10, Sender: "alice", Receiver: "bob", Value: 1, Seed: "7"})
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, "signed.jsonl")
	f, err := os.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.WriteJSONL(f, txs); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := RunOnce(Config{NodeID: "n0", ShardID: "s0", DataDir: dir, InputJSONL: input, RunMode: "once", MempoolCapacity: 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.AcceptedTxs != 10 || result.Summary.MempoolSize != 10 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
	if result.Summary.RealP2P || result.Summary.RealPBFT || result.Summary.StateCommit {
		t.Fatalf("V4.0 summary must not claim V4.1/V4.2 capabilities")
	}
	if _, err := os.Stat(filepath.Join(dir, "node_mempool_log.csv")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "v4_0_real_node_summary.json")); err != nil {
		t.Fatal(err)
	}
}
