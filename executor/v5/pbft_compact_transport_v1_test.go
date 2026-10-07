package v5

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func compactV1Transactions(t *testing.T, count int) []tx.SignedTransaction {
	t.Helper()
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: count, Sender: "alice", Receiver: "bob", StartNonce: 0,
		Value: 1, Seed: "pbft-compact-v1", StartTimeMS: 1, SourceKind: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func compactV1Block(t *testing.T, items []tx.SignedTransaction) realblock.Block {
	t.Helper()
	block := pbftUnitBlock()
	block.TxList = append([]tx.SignedTransaction(nil), items...)
	block.TxIDs = make([]string, 0, len(items))
	for _, item := range items {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	realblock.AssignHash(&block)
	return block
}

func compactV1Pool(t *testing.T, nodeID string, items []tx.SignedTransaction) *mempool.Mempool {
	t.Helper()
	pool := mempool.New(nodeID, "s0", mempool.Policy{Capacity: 100, TTL: time.Minute}, account.NewNonceManager())
	for _, item := range items {
		if result := pool.Admit(item); !result.Accepted {
			t.Fatalf("admit %s: %+v", item.TxID, result)
		}
	}
	return pool
}

func TestPBFTCompactV1PrePrepareElidesTxBodiesOnWire(t *testing.T) {
	leader := pbftUnitRuntime("n0")
	items := compactV1Transactions(t, 2)
	block := compactV1Block(t, items)
	var wire pbft.PrePrepare
	leader.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == p2p.MessagePBFTPrePrepare && wire.BlockHash == "" {
			decoded, err := p2p.DecodePayload[pbft.PrePrepare](msg)
			if err != nil {
				t.Fatal(err)
			}
			wire = decoded
		}
		return nil
	}
	if err := leader.beginPBFTProposal(context.Background(), block, len(items)); err != nil {
		t.Fatal(err)
	}
	if wire.BlockHash != block.BlockHash {
		t.Fatalf("wire hash changed: got=%s want=%s", wire.BlockHash, block.BlockHash)
	}
	if len(wire.Block.TxList) != 0 {
		t.Fatalf("compact PRE-PREPARE leaked %d transaction bodies", len(wire.Block.TxList))
	}
	if len(wire.Block.TxIDs) != len(items) || wire.Block.TxRoot != block.TxRoot {
		t.Fatal("compact PRE-PREPARE lost ordered TxIDs/TxRoot")
	}
	if realblock.Hash(wire.Block) != block.BlockHash {
		t.Fatal("clearing TxList changed consensus block hash")
	}
}

func TestPBFTCompactV1FollowerHydratesFromLocalMempool(t *testing.T) {
	ctx := context.Background()
	leader := pbftUnitRuntime("n0")
	follower := pbftUnitRuntime("n1")
	items := compactV1Transactions(t, 2)
	block := compactV1Block(t, items)
	follower.pool = compactV1Pool(t, "n1", items)
	var wire p2p.MessageEnvelope
	leader.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == p2p.MessagePBFTPrePrepare && wire.MessageType == "" {
			wire = msg
		}
		return nil
	}
	prepareCount, repairRequestCount := 0, 0
	follower.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		switch msg.MessageType {
		case p2p.MessagePBFTPrepare:
			prepareCount++
		case pbftCompactTxRequestMessage:
			repairRequestCount++
		}
		return nil
	}
	if err := leader.beginPBFTProposal(ctx, block, len(items)); err != nil {
		t.Fatal(err)
	}
	if err := follower.handlePBFTPrePrepare(ctx, wire); err != nil {
		t.Fatal(err)
	}
	if prepareCount == 0 {
		t.Fatal("follower did not PREPARE after local compact reconstruction")
	}
	if repairRequestCount != 0 {
		t.Fatalf("fully gossiped proposal unexpectedly requested %d body repairs", repairRequestCount)
	}
	if got := follower.proposals[block.BlockHash]; len(got.TxList) != len(items) {
		t.Fatalf("remembered proposal was not fully reconstructed: %d", len(got.TxList))
	}
}

func TestPBFTCompactV1MissingBodyRepairRestoresProposal(t *testing.T) {
	ctx := context.Background()
	leader := pbftUnitRuntime("n0")
	follower := pbftUnitRuntime("n1")
	items := compactV1Transactions(t, 2)
	block := compactV1Block(t, items)
	follower.pool = compactV1Pool(t, "n1", items[:1])
	var wire, requestEnv, responseEnv p2p.MessageEnvelope
	leader.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == p2p.MessagePBFTPrePrepare && wire.MessageType == "" {
			wire = msg
		}
		if msg.MessageType == pbftCompactTxResponseMessage && to == "n1" {
			responseEnv = msg
		}
		return nil
	}
	prepareCount := 0
	follower.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == pbftCompactTxRequestMessage && to == "n0" {
			requestEnv = msg
		}
		if msg.MessageType == p2p.MessagePBFTPrepare {
			prepareCount++
		}
		return nil
	}
	if err := leader.beginPBFTProposal(ctx, block, len(items)); err != nil {
		t.Fatal(err)
	}
	if err := follower.handlePBFTPrePrepare(ctx, wire); err != nil {
		t.Fatal(err)
	}
	if requestEnv.MessageType == "" || prepareCount != 0 {
		t.Fatalf("missing body did not defer PREPARE/request repair: request=%s prepare=%d", requestEnv.MessageType, prepareCount)
	}
	if err := leader.handlePBFTCompactTxRequest(ctx, requestEnv); err != nil {
		t.Fatal(err)
	}
	if responseEnv.MessageType == "" {
		t.Fatal("leader did not answer compact body repair request")
	}
	if err := follower.handlePBFTCompactTxResponse(ctx, responseEnv); err != nil {
		t.Fatal(err)
	}
	if prepareCount == 0 {
		t.Fatal("repaired compact proposal did not resume PBFT PREPARE")
	}
	if got := follower.proposals[block.BlockHash]; len(got.TxList) != len(items) {
		t.Fatal("repair did not restore exact full proposal")
	}
}

func TestPBFTCompactV1RejectsTamperedRepairBody(t *testing.T) {
	ctx := context.Background()
	follower := pbftUnitRuntime("n1")
	items := compactV1Transactions(t, 1)
	block := compactV1Block(t, items)
	follower.pool = compactV1Pool(t, "n1", nil)
	wirePre := pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: block.BlockHash, Block: block}
	wirePre.Block.TxList = nil
	wireEnv, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, "n0", "n1", "s0", 1, 0, 1, wirePre)
	if err != nil {
		t.Fatal(err)
	}
	var requestEnv p2p.MessageEnvelope
	prepareCount := 0
	follower.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == pbftCompactTxRequestMessage {
			requestEnv = msg
		}
		if msg.MessageType == p2p.MessagePBFTPrepare {
			prepareCount++
		}
		return nil
	}
	if err := follower.handlePBFTPrePrepare(ctx, wireEnv); err != nil {
		t.Fatal(err)
	}
	if requestEnv.MessageType == "" {
		t.Fatal("test setup did not create missing-body request")
	}
	bad := items[0]
	bad.Signature = "tampered"
	response := pbftCompactTxResponse{BlockHash: block.BlockHash, Height: 1, View: 0, Sequence: 1, LeaderNodeID: "n0", Transactions: []tx.SignedTransaction{bad}}
	responseEnv, err := p2p.NewEnvelope(pbftCompactTxResponseMessage, "n0", "n1", "s0", 1, 0, 1, response)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.handlePBFTCompactTxResponse(ctx, responseEnv); err == nil {
		t.Fatal("tampered missing-body repair was accepted")
	}
	if prepareCount != 0 {
		t.Fatal("tampered repair advanced PBFT")
	}
}
