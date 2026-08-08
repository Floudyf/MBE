package v5

import (
	"context"
	"fmt"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
)

func pbftUnitRuntime(nodeID string) *NodeRuntime {
	validators := []string{"n0", "n1", "n2", "n3"}
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: validators},
		{NodeID: "n1", ShardID: "s0", Validators: validators},
		{NodeID: "n2", ShardID: "s0", Validators: validators},
		{NodeID: "n3", ShardID: "s0", Validators: validators},
		{NodeID: "other", ShardID: "s1", Leader: true, Validators: []string{"other"}},
	}}
	var node NodePlan
	for _, candidate := range plan.NodeConfigs {
		if candidate.NodeID == nodeID {
			node = candidate
			break
		}
	}
	return &NodeRuntime{
		plan:                plan,
		node:                node,
		consensus:           pbft.NewState(nodeID, "s0", "n0", validators),
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		committed:           map[string]bool{},
		committing:          map[string]bool{},
		queuedCommitTasks:   map[string]bool{},
		pendingCommits:      map[uint64]realblock.Block{},
		pendingCommitErrors: map[uint64]string{},
		runtimeMetricCounts: map[string]int64{},
		committedHash:       "genesis",
		lastProgressAt:      time.Now().UnixMilli(),
		plugins: RuntimePlugins{
			BlockProducer: builtinBlockProducer{makeBasic("block_producer", "fixed_interval_block_producer", map[string]any{"block_size": 10, "interval_ms": 75})},
			Consensus:     builtinConsensus{makeBasic("consensus", "pbft_style_consensus", map[string]any{"timeout_ms": 2000})},
		},
	}
}

func pbftUnitBlock() realblock.Block {
	block := realblock.Block{
		ShardID:      "s0",
		Height:       1,
		PreviousHash: "genesis",
		ProposerID:   "n0",
		Timestamp:    1,
	}
	realblock.AssignHash(&block)
	return block
}

func TestPBFTNormalCaseBroadcastsPrepareAndCommitWithinValidatorSet(t *testing.T) {
	runtime := pbftUnitRuntime("n0")
	block := pbftUnitBlock()
	type sentMessage struct {
		to  string
		msg p2p.MessageEnvelope
	}
	messages := []sentMessage{}
	runtime.sendToNodeHook = func(_ context.Context, nodeID string, msg p2p.MessageEnvelope) error {
		messages = append(messages, sentMessage{to: nodeID, msg: msg})
		return nil
	}

	if err := runtime.beginPBFTProposal(context.Background(), block, 10); err != nil {
		t.Fatal(err)
	}
	countType := func(messageType string) int {
		count := 0
		for _, item := range messages {
			if item.msg.MessageType == messageType {
				count++
				if item.to == "other" {
					t.Fatalf("PBFT %s crossed shard boundary", messageType)
				}
			}
		}
		return count
	}
	if got := countType(p2p.MessagePBFTPrePrepare); got != 3 {
		t.Fatalf("expected PRE-PREPARE to 3 peer validators, got %d", got)
	}
	if got := countType(p2p.MessagePBFTPrepare); got != 3 {
		t.Fatalf("expected local PREPARE to 3 peer validators, got %d", got)
	}

	for _, nodeID := range []string{"n1", "n2"} {
		prepare := pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block.BlockHash}
		envelope, err := p2p.NewEnvelope(p2p.MessagePBFTPrepare, nodeID, "n0", "s0", 1, 0, 1, prepare)
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.handlePBFTPrepare(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	if got := countType(p2p.MessagePBFTCommit); got != 3 {
		t.Fatalf("expected prepared replica to multicast COMMIT, got %d", got)
	}
}

func TestPBFTCommitCertificateQueuesExecutionOnlyAfterCommitQuorum(t *testing.T) {
	ctx := context.Background()
	runtime := pbftUnitRuntime("n0")
	block := pbftUnitBlock()
	runtime.sendToNodeHook = func(context.Context, string, p2p.MessageEnvelope) error { return nil }
	runtime.commitWorkerContext = ctx
	runtime.commitTasks = make(chan commitTask, 4)

	if err := runtime.beginPBFTProposal(ctx, block, 10); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n1", "n2"} {
		prepare := pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block.BlockHash}
		envelope, _ := p2p.NewEnvelope(p2p.MessagePBFTPrepare, nodeID, "n0", "s0", 1, 0, 1, prepare)
		if err := runtime.handlePBFTPrepare(ctx, envelope); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case task := <-runtime.commitTasks:
		t.Fatalf("execution queued before COMMIT quorum: %+v", task)
	default:
	}

	for index, nodeID := range []string{"n1", "n2"} {
		commit := pbft.Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block.BlockHash}
		envelope, _ := p2p.NewEnvelope(p2p.MessagePBFTCommit, nodeID, "n0", "s0", 1, 0, 1, commit)
		if err := runtime.handlePBFTCommit(ctx, envelope); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			select {
			case task := <-runtime.commitTasks:
				t.Fatalf("execution queued at only 2 COMMIT votes: %+v", task)
			default:
			}
		}
	}
	select {
	case task := <-runtime.commitTasks:
		if task.kind != commitTaskConsensus || task.block.BlockHash != block.BlockHash {
			t.Fatalf("unexpected commit-certificate task: %+v", task)
		}
	default:
		t.Fatal("COMMIT certificate did not queue deterministic execution")
	}
}

func TestPBFTDelayedPrepareAfterSameDigestTimeoutStillCounts(t *testing.T) {
	runtime := pbftUnitRuntime("n0")
	block := pbftUnitBlock()
	runtime.proposals[block.BlockHash] = block
	runtime.proposalInFlight = true
	runtime.proposalInFlightHash = block.BlockHash
	runtime.proposalStartedAt = time.Now().Add(-time.Minute)
	pre := pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: block.BlockHash, Block: block}
	localPrepare, err := runtime.pbftState().OnPrePrepare(pre)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.pbftState().AcceptPrepare(localPrepare); err != nil {
		t.Fatal(err)
	}
	runtime.mirrorPrepareVote(localPrepare)

	runtime.expireStaleProposal(time.Second)
	if runtime.proposals[block.BlockHash].BlockHash != block.BlockHash {
		t.Fatal("timeout removed original proposal hash")
	}
	for _, nodeID := range []string{"n1", "n2"} {
		reached, _, err := runtime.pbftState().AcceptPrepare(pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block.BlockHash})
		if err != nil {
			t.Fatal(err)
		}
		if nodeID == "n2" && !reached {
			t.Fatal("delayed PREPARE for preserved digest did not reach quorum")
		}
	}
}

func TestPBFTViewChangeCarriesPreparedBlockWithoutRehashing(t *testing.T) {
	validators := []string{"n0", "n1", "n2", "n3"}
	block := pbftUnitBlock()
	source := pbft.NewState("n0", "s0", "n0", validators)
	if _, err := source.OnPrePrepare(pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: block.BlockHash, Block: block}); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n1", "n2"} {
		if _, _, err := source.AcceptPrepare(pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: block.BlockHash}); err != nil {
			t.Fatal(err)
		}
	}
	cert, preparedBlock, ok := source.PreparedCertificate(1)
	if !ok {
		t.Fatal("setup did not create prepared certificate")
	}
	newPrimary := pbft.NewState("n1", "s0", "n0", validators)
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		certCopy := cert
		certCopy.Prepares = append([]pbft.Prepare(nil), cert.Prepares...)
		blockCopy := preparedBlock
		vc := pbft.ViewChange{View: 0, NewView: 1, NodeID: nodeID, Height: 1, LeaderID: "n1", Prepared: &certCopy, PreparedBlock: &blockCopy}
		if _, _, err := newPrimary.AcceptViewChange(vc); err != nil {
			t.Fatal(err)
		}
	}
	nv, err := newPrimary.BuildNewView(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	selected, hasSelected, err := newPrimary.AcceptNewView(nv)
	if err != nil || !hasSelected {
		t.Fatalf("accept NEW-VIEW: selected=%t err=%v", hasSelected, err)
	}
	if selected.BlockHash != block.BlockHash || selected.ProposerID != "n0" {
		t.Fatalf("prepared block was rehashed or proposer rewritten: before=%s/%s after=%s/%s", block.BlockHash, block.ProposerID, selected.BlockHash, selected.ProposerID)
	}
	if newPrimary.Leader() != "n1" || newPrimary.View() != 1 {
		t.Fatalf("new primary/view not installed: leader=%s view=%d", newPrimary.Leader(), newPrimary.View())
	}
}

func TestPBFTEnvelopeSenderAndSlotMismatchRejectedAtRuntimeBoundary(t *testing.T) {
	runtime := pbftUnitRuntime("n0")
	block := pbftUnitBlock()
	if _, err := runtime.pbftState().OnPrePrepare(pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: block.BlockHash, Block: block}); err != nil {
		t.Fatal(err)
	}
	prepare := pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n1", BlockHash: block.BlockHash}
	spoofed, _ := p2p.NewEnvelope(p2p.MessagePBFTPrepare, "n2", "n0", "s0", 1, 0, 1, prepare)
	if err := runtime.handlePBFTPrepare(context.Background(), spoofed); err == nil {
		t.Fatal("spoofed PREPARE envelope sender was accepted")
	}
	wrongSlot, _ := p2p.NewEnvelope(p2p.MessagePBFTPrepare, "n1", "n0", "s0", 2, 0, 2, prepare)
	if err := runtime.handlePBFTPrepare(context.Background(), wrongSlot); err == nil {
		t.Fatal("PREPARE envelope/payload slot mismatch was accepted")
	}
}

func TestPBFTBackupCannotBeginProposal(t *testing.T) {
	runtime := pbftUnitRuntime("n1")
	if err := runtime.beginPBFTProposal(context.Background(), pbftUnitBlock(), 10); err == nil {
		t.Fatal("backup began a PBFT proposal")
	}
}

func TestPBFTPrePrepareMinoritySendFailureKeepsProposalAlive(t *testing.T) {
	runtime := pbftUnitRuntime("n0")
	block := pbftUnitBlock()
	runtime.sendToNodeHook = func(_ context.Context, nodeID string, msg p2p.MessageEnvelope) error {
		if msg.MessageType == p2p.MessagePBFTPrePrepare && nodeID == "n3" {
			return fmt.Errorf("injected peer send failure")
		}
		return nil
	}
	if err := runtime.beginPBFTProposal(context.Background(), block, 10); err != nil {
		t.Fatalf("minority send failure aborted proposal: %v", err)
	}
	if !runtime.proposalInFlight || runtime.proposalInFlightHash != block.BlockHash {
		t.Fatal("minority PRE-PREPARE send failure discarded the proposal")
	}
	if runtime.proposals[block.BlockHash].BlockHash != block.BlockHash {
		t.Fatal("proposal disappeared after minority send failure")
	}
}
