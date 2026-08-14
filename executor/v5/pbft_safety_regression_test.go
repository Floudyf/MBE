package v5

import (
	"context"
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func pbftSafetySignedTx(t *testing.T, seed string, nonce uint64, value int64) tx.SignedTransaction {
	t.Helper()
	publicKey, privateKey := tx.DeterministicKeyPair(seed)
	item := tx.SignedTransaction{
		Sender:    tx.AddressFromPublicKey(publicKey),
		Receiver:  "receiver-" + seed,
		Nonce:     nonce,
		Value:     value,
		StateKeys: []string{"state:" + seed},
		AccessList: []tx.AccessItem{{
			Key:             "state:" + seed,
			Mode:            tx.AccessReadWrite,
			UpdateSemantics: "set",
		}},
		Payload:   "pbft-safety-test",
		Timestamp: 1,
	}
	if err := tx.Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := tx.Verify(item); err != nil {
		t.Fatal(err)
	}
	return item
}

func pbftSafetyBlock(items ...tx.SignedTransaction) realblock.Block {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.TxID)
	}
	block := realblock.Block{
		ShardID:         "s0",
		Height:          1,
		PreviousHash:    "genesis",
		ProposerID:      "n0",
		Timestamp:       1,
		TxIDs:           ids,
		TxList:          append([]tx.SignedTransaction(nil), items...),
		StateRootBefore: "empty",
		StateRootAfter:  "pending_not_executed",
		ReceiptRoot:     "pending_not_executed",
	}
	realblock.AssignHash(&block)
	return block
}

func pbftSafetyStrictRuntime(t *testing.T, nodeID string) (*NodeRuntime, map[string]string) {
	t.Helper()
	nodes := []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1"}},
	}
	publicKeys, privateKeys, err := GeneratePBFTIdentityMaterial(nodes)
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		PlanID:             "pbft-safety-strict-test",
		PBFTIdentityScheme: PBFTIdentitySchemeEd25519V1,
		PBFTPublicKeys:     publicKeys,
		NodeConfigs:        nodes,
	}
	if err := ValidatePBFTIdentityPlan(plan); err != nil {
		t.Fatal(err)
	}
	var selected NodePlan
	for _, node := range nodes {
		if node.NodeID == nodeID {
			selected = node
		}
	}
	return &NodeRuntime{
		plan:                plan,
		node:                selected,
		consensus:           pbft.NewState(nodeID, "s0", "n0", []string{"n0", "n1"}),
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		committed:           map[string]bool{},
		committing:          map[string]bool{},
		pendingCommits:      map[uint64]realblock.Block{},
		pendingCommitErrors: map[uint64]string{},
		runtimeMetricCounts: map[string]int64{},
	}, privateKeys
}

func TestConsensusBlockBodyRejectsBodyMutationWithUnchangedDigestVote(t *testing.T) {
	runtime, _ := pbftSafetyStrictRuntime(t, "n1")
	item := pbftSafetySignedTx(t, "body-a", 0, 7)
	block := pbftSafetyBlock(item)
	if err := runtime.validateConsensusBlockBody(block); err != nil {
		t.Fatalf("valid body rejected: %v", err)
	}

	tampered := block
	tampered.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	tampered.TxList[0].Value = 99
	if realblock.Hash(tampered) != block.BlockHash {
		t.Fatal("regression fixture requires TxList body mutation to preserve the legacy block digest")
	}
	if err := runtime.validateConsensusBlockBody(tampered); err == nil {
		t.Fatal("modified transaction body with unchanged TxID/TxRoot/BlockHash must be rejected")
	}
}

func TestConsensusBlockBodyRejectsLengthOrderDuplicateAndSignatureMismatch(t *testing.T) {
	runtime, _ := pbftSafetyStrictRuntime(t, "n1")
	first := pbftSafetySignedTx(t, "body-first", 0, 1)
	second := pbftSafetySignedTx(t, "body-second", 0, 2)

	lengthMismatch := pbftSafetyBlock(first)
	lengthMismatch.TxList = append(lengthMismatch.TxList, second)
	if err := runtime.validateConsensusBlockBody(lengthMismatch); err == nil || !strings.Contains(err.Error(), "count mismatch") {
		t.Fatalf("expected count mismatch rejection, got %v", err)
	}

	orderMismatch := pbftSafetyBlock(first, second)
	orderMismatch.TxIDs[0], orderMismatch.TxIDs[1] = orderMismatch.TxIDs[1], orderMismatch.TxIDs[0]
	realblock.AssignHash(&orderMismatch)
	if err := runtime.validateConsensusBlockBody(orderMismatch); err == nil || !strings.Contains(err.Error(), "id/body mismatch") {
		t.Fatalf("expected id/body ordering rejection, got %v", err)
	}

	duplicate := pbftSafetyBlock(first, first)
	if err := runtime.validateConsensusBlockBody(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate transaction") {
		t.Fatalf("expected duplicate transaction rejection, got %v", err)
	}

	badSignature := pbftSafetyBlock(first)
	badSignature.TxList = append([]tx.SignedTransaction(nil), badSignature.TxList...)
	badSignature.TxList[0].Signature = "not-a-signature"
	if err := runtime.validateConsensusBlockBody(badSignature); err == nil || !strings.Contains(err.Error(), "signature/body verification") {
		t.Fatalf("expected transaction signature rejection, got %v", err)
	}
}

func TestPBFTPrepareAuthenticationRejectsForgedValidatorIdentity(t *testing.T) {
	runtime, privateKeys := pbftSafetyStrictRuntime(t, "n1")
	private0, err := pbft.DecodePrivateKey(privateKeys["n0"])
	if err != nil {
		t.Fatal(err)
	}
	prepare := pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n0", BlockHash: "block-a"}
	if err := pbft.SignPrepare(&prepare, private0); err != nil {
		t.Fatal(err)
	}
	if err := runtime.verifyPBFTPrepareAuthentication(prepare); err != nil {
		t.Fatalf("valid n0 PREPARE rejected: %v", err)
	}
	forged := prepare
	forged.NodeID = "n1"
	if err := runtime.verifyPBFTPrepareAuthentication(forged); err == nil {
		t.Fatal("one validator key must not authenticate another validator identity")
	}
}

func TestCommitCertificateAuthenticationRejectsForgedEmbeddedVote(t *testing.T) {
	runtime, privateKeys := pbftSafetyStrictRuntime(t, "n1")
	commits := make([]pbft.Commit, 0, 2)
	for _, nodeID := range []string{"n0", "n1"} {
		privateKey, err := pbft.DecodePrivateKey(privateKeys[nodeID])
		if err != nil {
			t.Fatal(err)
		}
		commit := pbft.Commit{View: 0, Sequence: 1, Height: 1, NodeID: nodeID, BlockHash: "block-a"}
		if err := pbft.SignCommit(&commit, privateKey); err != nil {
			t.Fatal(err)
		}
		commits = append(commits, commit)
	}
	certificate := pbft.CommitCertificate{View: 0, Sequence: 1, Height: 1, BlockHash: "block-a", Commits: commits}
	if err := runtime.verifyCommitCertificateAuthentication(certificate); err != nil {
		t.Fatalf("valid signed commit certificate rejected: %v", err)
	}
	certificate.Commits[1].Signature = certificate.Commits[0].Signature
	if err := runtime.verifyCommitCertificateAuthentication(certificate); err == nil {
		t.Fatal("commit certificate with a forged embedded validator vote must be rejected")
	}
}

func TestNewViewAuthenticationRejectsResignedTamperedPreparedBody(t *testing.T) {
	runtime, privateKeys := pbftSafetyStrictRuntime(t, "n1")
	item := pbftSafetySignedTx(t, "new-view-body", 0, 3)
	block := pbftSafetyBlock(item)

	private0, err := pbft.DecodePrivateKey(privateKeys["n0"])
	if err != nil {
		t.Fatal(err)
	}
	prepare := pbft.Prepare{View: 0, Sequence: 1, Height: 1, NodeID: "n0", BlockHash: block.BlockHash}
	if err := pbft.SignPrepare(&prepare, private0); err != nil {
		t.Fatal(err)
	}
	prepared := pbft.PreparedCertificate{View: 0, Sequence: 1, Height: 1, BlockHash: block.BlockHash, Prepares: []pbft.Prepare{prepare}}

	private1, err := pbft.DecodePrivateKey(privateKeys["n1"])
	if err != nil {
		t.Fatal(err)
	}
	newView := pbft.NewView{View: 1, LeaderID: "n1", Height: 1, SelectedPrepared: &prepared, SelectedBlock: &block}
	if err := pbft.SignNewView(&newView, private1); err != nil {
		t.Fatal(err)
	}
	if err := runtime.verifyPBFTNewViewAuthentication(newView); err != nil {
		t.Fatalf("valid signed NEW-VIEW evidence rejected: %v", err)
	}

	tamperedBlock := block
	tamperedBlock.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	tamperedBlock.TxList[0].Value++
	if realblock.Hash(tamperedBlock) != block.BlockHash {
		t.Fatal("regression fixture requires prepared block body mutation to preserve legacy block hash")
	}
	newView.SelectedBlock = &tamperedBlock
	if err := pbft.SignNewView(&newView, private1); err != nil {
		t.Fatal(err)
	}
	if err := runtime.verifyPBFTNewViewAuthentication(newView); err == nil {
		t.Fatal("even a valid new-primary signature must not authorize a prepared body inconsistent with its transaction ID")
	}
}

func TestCertifiedCatchupRejectsTamperedExecutionBodyBeforeCertificateImport(t *testing.T) {
	runtime, _ := pbftSafetyStrictRuntime(t, "n1")
	item := pbftSafetySignedTx(t, "catchup-body", 0, 5)
	block := pbftSafetyBlock(item)
	tampered := block
	tampered.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	tampered.TxList[0].Value++
	itemWire := CatchupBlock{Block: tampered, SourceNode: "n0"}
	if err := runtime.handleCertifiedCatchupBlock(context.Background(), "n0", itemWire); err == nil || !strings.Contains(err.Error(), "block body") {
		t.Fatalf("expected catch-up body rejection before certificate import, got %v", err)
	}
}

func TestDuplicatePrePrepareDoesNotOverwriteLockedBody(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1"}},
	}}
	runtime := &NodeRuntime{
		plan:                plan,
		node:                plan.NodeConfigs[1],
		consensus:           pbft.NewState("n1", "s0", "n0", []string{"n0", "n1"}),
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		committed:           map[string]bool{},
		committing:          map[string]bool{},
		pendingCommits:      map[uint64]realblock.Block{},
		pendingCommitErrors: map[uint64]string{},
		runtimeMetricCounts: map[string]int64{},
		sendToNodeHook: func(context.Context, string, p2p.MessageEnvelope) error {
			return nil
		},
	}
	bodyA := tx.SignedTransaction{TxID: "fixture-tx", Sender: "a", Receiver: "b", Value: 1, StateKeys: []string{"k"}}
	blockA := pbftSafetyBlock(bodyA)
	preA := pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: blockA.BlockHash, Block: blockA}
	envelopeA, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, "n0", "n1", "s0", 1, 0, 1, preA)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handlePBFTPrePrepare(context.Background(), envelopeA); err != nil {
		t.Fatalf("first PRE-PREPARE rejected: %v", err)
	}

	blockB := blockA
	blockB.TxList = append([]tx.SignedTransaction(nil), blockA.TxList...)
	blockB.TxList[0].Value = 999
	if realblock.Hash(blockB) != blockA.BlockHash {
		t.Fatal("fixture requires same digest for different legacy TxList body")
	}
	preB := pbft.PrePrepare{View: 0, Sequence: 1, Height: 1, LeaderID: "n0", BlockHash: blockB.BlockHash, Block: blockB}
	envelopeB, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, "n0", "n1", "s0", 1, 0, 1, preB)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handlePBFTPrePrepare(context.Background(), envelopeB); err != nil {
		t.Fatalf("same-digest retransmission rejected: %v", err)
	}
	locked := runtime.pbftState().LockedBlocks[blockA.BlockHash]
	if len(locked.TxList) != 1 || locked.TxList[0].Value != 1 {
		t.Fatalf("duplicate PRE-PREPARE overwrote locked execution body: %#v", locked.TxList)
	}
}

func TestPBFTIdentityPlanRejectsCrossShardValidatorMembership(t *testing.T) {
	nodes := []NodePlan{
		{NodeID: "n0", ShardID: "s0", Validators: []string{"n0", "n1"}},
		{NodeID: "n1", ShardID: "s1", Validators: []string{"n0", "n1"}},
	}
	publicKeys, _, err := GeneratePBFTIdentityMaterial(nodes)
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		PlanID:             "cross-shard-validator-invalid",
		PBFTIdentityScheme: PBFTIdentitySchemeEd25519V1,
		PBFTPublicKeys:     publicKeys,
		NodeConfigs:        nodes,
	}
	if err := ValidatePBFTIdentityPlan(plan); err == nil || !strings.Contains(err.Error(), "belongs to shard") {
		t.Fatalf("cross-shard validator membership must be rejected, got %v", err)
	}
}

func TestAuthenticatedRuntimeRejectsLegacySingletonCommitWireShape(t *testing.T) {
	nodes := []NodePlan{{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0"}}}
	publicKeys, _, err := GeneratePBFTIdentityMaterial(nodes)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &NodeRuntime{
		plan: Plan{
			PlanID:             "strict-singleton",
			PBFTIdentityScheme: PBFTIdentitySchemeEd25519V1,
			PBFTPublicKeys:     publicKeys,
			NodeConfigs:        nodes,
		},
		node:                nodes[0],
		consensus:           pbft.NewState("n0", "s0", "n0", []string{"n0"}),
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		committed:           map[string]bool{},
		committing:          map[string]bool{},
		pendingCommits:      map[uint64]realblock.Block{},
		pendingCommitErrors: map[uint64]string{},
		runtimeMetricCounts: map[string]int64{},
	}
	envelope, err := p2p.NewEnvelope(
		p2p.MessagePBFTCommit,
		"n0",
		"n0",
		"s0",
		1,
		0,
		1,
		Proposal{Block: realblock.Block{BlockHash: "legacy", Height: 1, ShardID: "s0"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handlePBFTCommit(context.Background(), envelope); err == nil || !strings.Contains(err.Error(), "legacy singleton") {
		t.Fatalf("authenticated runtime must reject unsigned legacy singleton commit wire shape, got %v", err)
	}
}

func TestPBFTIdentityPlanRejectsKeyReuseAndLocalPrivateKeyMismatch(t *testing.T) {
	runtime, privateKeys := pbftSafetyStrictRuntime(t, "n0")
	badPlan := runtime.plan
	badPlan.PBFTPublicKeys = map[string]string{
		"n0": runtime.plan.PBFTPublicKeys["n0"],
		"n1": runtime.plan.PBFTPublicKeys["n0"],
	}
	if err := ValidatePBFTIdentityPlan(badPlan); err == nil {
		t.Fatal("two PBFT NodeIDs must not share one validator public key")
	}

	t.Setenv(PBFTPrivateKeyEnv, privateKeys["n1"])
	if _, err := runtime.pbftSigningPrivateKey(); err == nil {
		t.Fatal("node n0 must reject a child-process private key registered to n1")
	}
}
