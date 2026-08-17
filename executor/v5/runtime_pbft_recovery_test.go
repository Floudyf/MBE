package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

func recoveryCertificate(blockHash string) pbft.CommitCertificate {
	cert := pbft.CommitCertificate{View: 0, Sequence: 1, Height: 1, BlockHash: blockHash}
	for _, id := range []string{"n0", "n1", "n2"} {
		cert.Commits = append(cert.Commits, pbft.Commit{View: 0, Sequence: 1, Height: 1, NodeID: id, BlockHash: blockHash})
	}
	return cert
}

func TestPBFTCertifiedCatchupAcceptsQuorumProofAndQueuesBlock(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	item := CatchupBlock{Block: b, SourceNode: "n1", Certificate: recoveryCertificate(b.BlockHash)}
	if err := r.handleCertifiedCatchupBlock(context.Background(), "n1", item); err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-r.commitTasks:
		if task.kind != commitTaskCatchUp || task.block.BlockHash != b.BlockHash {
			t.Fatalf("unexpected catch-up task: %+v", task)
		}
	default:
		t.Fatal("certified block did not queue catch-up execution")
	}
}

func TestPBFTCertifiedCatchupRejectsInsufficientProof(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	cert := recoveryCertificate(b.BlockHash)
	cert.Commits = cert.Commits[:2]
	if err := r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{Block: b, SourceNode: "n1", Certificate: cert}); err == nil {
		t.Fatal("insufficient proof accepted")
	}
}

func TestPBFTCommittedBackupCanReturnCertificateToLaggingPrimary(t *testing.T) {
	r := pbftUnitRuntime("n1")
	b := pbftUnitBlock()
	r.store = storage.NewBlockStore(t.TempDir(), "n1", "s0")
	if err := r.store.AppendCommitted(b, storage.CommitRecord{NodeID: "n1", ShardID: "s0", Height: 1, BlockHash: b.BlockHash, Committed: true}); err != nil {
		t.Fatal(err)
	}
	cert := recoveryCertificate(b.BlockHash)
	if err := r.pbftState().AcceptCommitCertificate(cert, b); err != nil {
		t.Fatal(err)
	}
	r.committedHeight = 1
	r.committedHash = b.BlockHash
	sent := false
	r.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if to == "n0" && msg.MessageType == catchupBlockMessage {
			payload, err := p2p.DecodePayload[CatchupBlock](msg)
			if err != nil {
				t.Fatal(err)
			}
			if payload.Certificate.BlockHash != b.BlockHash {
				t.Fatal("wrong certificate returned")
			}
			sent = true
		}
		return nil
	}
	pre := pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: b.BlockHash, Block: b}
	handled, err := r.maybeRespondWithCertifiedCommit(context.Background(), "n0", pre)
	if err != nil || !handled || !sent {
		t.Fatalf("certified stale response failed handled=%t sent=%t err=%v", handled, sent, err)
	}
}

func TestPBFTCatchupRequestCanBeServedByBackup(t *testing.T) {
	r := pbftUnitRuntime("n1")
	b := pbftUnitBlock()
	r.store = storage.NewBlockStore(t.TempDir(), "n1", "s0")
	if err := r.store.AppendCommitted(b, storage.CommitRecord{NodeID: "n1", ShardID: "s0", Height: 1, BlockHash: b.BlockHash, Committed: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.pbftState().AcceptCommitCertificate(recoveryCertificate(b.BlockHash), b); err != nil {
		t.Fatal(err)
	}
	r.committedHeight = 1
	r.committedHash = b.BlockHash
	sent := false
	r.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if to == "n0" && msg.MessageType == catchupBlockMessage {
			sent = true
		}
		return nil
	}
	if err := r.handleCertifiedCatchupRequest(context.Background(), "n0", CatchupRequest{ShardID: "s0", FromHeight: 1, ToHeight: 1}); err != nil {
		t.Fatal(err)
	}
	if !sent {
		t.Fatal("backup did not serve certified catch-up")
	}
}

func TestPBFTCatchupReplaySuppressesCrossShardSideEffects(t *testing.T) {
	r := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true}, crossEventSeen: map[string]bool{}}
	item := tx.SignedTransaction{TxID: "logical-1", Payload: "v5_cross:s1"}
	r.onCommittedTxWithOrigin(context.Background(), item, Relay{}, CommitOriginCatchUp)
	if len(r.events) != 0 || len(r.lifecycle) != 0 {
		t.Fatal("certified catch-up replay emitted cross-shard side effects")
	}
}

func recoveryCertificateAt(height uint64, blockHash string) pbft.CommitCertificate {
	cert := pbft.CommitCertificate{View: 0, Sequence: height, Height: height, BlockHash: blockHash}
	for _, id := range []string{"n0", "n1", "n2"} {
		cert.Commits = append(cert.Commits, pbft.Commit{View: 0, Sequence: height, Height: height, NodeID: id, BlockHash: blockHash})
	}
	return cert
}

func TestPBFTUrgentCatchupUsesThirtyTwoBlockBatchToKnownTarget(t *testing.T) {
	r := pbftUnitRuntime("n3")
	r.committedHeight = 319
	r.committedHash = "height-319"
	r.noteCatchupTarget(1500)

	var got CatchupRequest
	r.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if to != "n0" || msg.MessageType != catchupRequestMessage {
			t.Fatalf("unexpected catch-up send: to=%s type=%s", to, msg.MessageType)
		}
		request, err := p2p.DecodePayload[CatchupRequest](msg)
		if err != nil {
			t.Fatal(err)
		}
		got = request
		return nil
	}

	r.requestCatchup(context.Background())
	if got.FromHeight != 320 || got.ToHeight != 351 {
		t.Fatalf("urgent catch-up range=%d..%d, want 320..351", got.FromHeight, got.ToHeight)
	}
	if r.runtimeMetricCounts["pbft_catchup_urgent_request_count"] != 1 {
		t.Fatalf("urgent request metric=%d", r.runtimeMetricCounts["pbft_catchup_urgent_request_count"])
	}
}

func TestPBFTNormalCatchupKeepsSmallProbeRange(t *testing.T) {
	r := pbftUnitRuntime("n3")
	var got CatchupRequest
	r.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		request, err := p2p.DecodePayload[CatchupRequest](msg)
		if err != nil {
			t.Fatal(err)
		}
		got = request
		return nil
	}
	r.requestCatchup(context.Background())
	if got.FromHeight != 1 || got.ToHeight != 9 {
		t.Fatalf("normal catch-up range=%d..%d, want 1..9", got.FromHeight, got.ToHeight)
	}
}

func TestPBFTCatchupRequestServesMultiBlockBatch(t *testing.T) {
	r := pbftUnitRuntime("n1")
	r.store = storage.NewBlockStore(t.TempDir(), "n1", "s0")

	b1 := pbftUnitBlock()
	b2 := pbftUnitBlock()
	b2.Height = 2
	b2.PreviousHash = b1.BlockHash
	realblock.AssignHash(&b2)
	for _, b := range []realblock.Block{b1, b2} {
		if err := r.store.AppendCommitted(b, storage.CommitRecord{NodeID: "n1", ShardID: "s0", Height: b.Height, BlockHash: b.BlockHash, Committed: true}); err != nil {
			t.Fatal(err)
		}
		if err := r.pbftState().AcceptCommitCertificate(recoveryCertificateAt(b.Height, b.BlockHash), b); err != nil {
			t.Fatal(err)
		}
	}
	r.committedHeight = 2
	r.committedHash = b2.BlockHash

	heights := []uint64{}
	r.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == catchupBlockMessage {
			payload, err := p2p.DecodePayload[CatchupBlock](msg)
			if err != nil {
				t.Fatal(err)
			}
			heights = append(heights, payload.Block.Height)
		}
		return nil
	}
	if err := r.handleCertifiedCatchupRequest(context.Background(), "n0", CatchupRequest{ShardID: "s0", FromHeight: 1, ToHeight: 2}); err != nil {
		t.Fatal(err)
	}
	if len(heights) != 2 || heights[0] != 1 || heights[1] != 2 {
		t.Fatalf("served heights=%v", heights)
	}
	if r.runtimeMetricCounts["pbft_catchup_blocks_served_count"] != 2 {
		t.Fatalf("served metric=%d", r.runtimeMetricCounts["pbft_catchup_blocks_served_count"])
	}
	if r.runtimeMetricCounts["pbft_catchup_indexed_range_read_count"] != 1 {
		t.Fatalf("indexed range reads=%d, want 1", r.runtimeMetricCounts["pbft_catchup_indexed_range_read_count"])
	}
}

func TestPBFTCatchupRequestRejectsOversizedBatch(t *testing.T) {
	r := pbftUnitRuntime("n1")
	err := r.handleCertifiedCatchupRequest(context.Background(), "n0", CatchupRequest{ShardID: "s0", FromHeight: 1, ToHeight: certifiedCatchupBatchSize + 1})
	if err == nil {
		t.Fatal("oversized certified catch-up batch was accepted")
	}
}
