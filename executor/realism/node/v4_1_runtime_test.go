package node

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestV41FourNodeTCPGossipAndPBFTCommitSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	validators := []string{"n0", "n1", "n2", "n3"}
	runtimes := []*RuntimeV41{}
	for i, id := range validators {
		role := "validator"
		if i == 0 {
			role = "leader"
		}
		rt := NewRuntimeV41(Config{NodeID: id, ShardID: "s0", DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0", Role: role, LeaderID: "n0", Validators: validators, BlockSize: 10})
		if err := rt.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer rt.Stop()
		runtimes = append(runtimes, rt)
	}
	peers := []p2p.Peer{}
	for i, rt := range runtimes {
		role := "validator"
		leader := false
		if i == 0 {
			role = "leader"
			leader = true
		}
		peers = append(peers, p2p.Peer{NodeID: validators[i], ShardID: "s0", ListenAddr: rt.ListenAddr(), Role: role, Leader: leader})
	}
	for _, rt := range runtimes {
		rt.SetPeers(peers)
	}
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 10, Sender: "alice", Receiver: "bob", Value: 1, Seed: "v41"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range txs {
		if result := runtimes[0].node.Mempool.Admit(item); !result.Accepted {
			t.Fatalf("leader admit failed: %+v", result)
		}
		if err := runtimes[0].GossipTx(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return runtimes[1].node.Mempool.Len() == 10 })
	block, err := runtimes[0].ProposeBlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		committed := 0
		for _, rt := range runtimes {
			if rt.Summary().CommittedBlockHash == block.BlockHash {
				committed++
			}
		}
		return committed >= 3
	})
	for _, rt := range runtimes {
		if err := rt.WriteArtifacts(rt.cfg.DataDir); err != nil {
			t.Fatal(err)
		}
	}
	summary := runtimes[0].Summary()
	if !summary.RealP2P || !summary.PBFTStyleConsensus || !summary.RealPBFTMessages || summary.ProductionPBFT || summary.StateCommit || summary.CrossShardProtocol {
		t.Fatalf("summary truth labels wrong: %+v", summary)
	}
}

func waitFor(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
