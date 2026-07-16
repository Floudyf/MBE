package v5

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

func TestCommitDurableFailureDoesNotAdvanceRuntimeState(t *testing.T) {
	for _, failpoint := range []string{"after_block_append", "after_receipt_append"} {
		t.Run(failpoint, func(t *testing.T) {
			testCommitDurableFailure(t, failpoint)
		})
	}
}

func TestCommitRollbackFailureFreezesRuntime(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	plugins, err := InstantiatePlugins(profile)
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), account.NewNonceManager())
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: "fatal-client", Receiver: "receiver", Value: 1, StateKeys: []string{"key"}, Seed: "fatal"})
	if err != nil || !pool.Admit(items[0]).Accepted {
		t.Fatalf("failed to prepare transaction: %v", err)
	}
	proposer := realblock.NewProposer("n0", "s0")
	b, err := proposer.Build(pool, 1, nowForTest())
	if err != nil {
		t.Fatal(err)
	}
	db := state.NewDB(root, "s0")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(storeDir, "n0", "s0")
	store.SetFailpointForTest("after_block_append")
	store.SetRollbackFailpointForTest(true)
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root}, pool: pool, proposer: proposer, db: db, store: store, engine: execution.NewEngine(), proposals: map[string]realblock.Block{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, committedHash: "genesis", pluginSnapshot: profile, plugins: plugins}
	if err := runtime.commit(context.Background(), b); err == nil {
		t.Fatal("expected injected rollback failure")
	}
	if runtime.fatalPersistenceError == "" {
		t.Fatal("rollback failure did not freeze runtime")
	}
	if runtime.committedHeight != 0 || runtime.committedHash != "genesis" || !pool.Has(items[0].TxID) {
		t.Fatal("fatal rollback failure advanced or cleaned runtime state")
	}
	store.SetFailpointForTest("")
	store.SetRollbackFailpointForTest(false)
	if err := runtime.commit(context.Background(), b); err == nil {
		t.Fatal("fatal persistence freeze allowed a later commit")
	}
}

func testCommitDurableFailure(t *testing.T, failpoint string) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	plugins, err := InstantiatePlugins(profile)
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), account.NewNonceManager())
	generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: "client", Receiver: "receiver", Value: 1, StateKeys: []string{"key"}, Seed: "atomicity"})
	if err != nil {
		t.Fatal(err)
	}
	if result := pool.Admit(generated[0]); !result.Accepted {
		t.Fatal(result)
	}
	proposer := realblock.NewProposer("n0", "s0")
	block, err := proposer.Build(pool, 1, nowForTest())
	if err != nil {
		t.Fatal(err)
	}
	db := state.NewDB(root, "s0")
	db.Set("before", "stable")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	stateBefore := db.Snapshot()
	snapshotBefore, err := os.ReadFile(filepath.Join(root, "state_snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(storeDir, "n0", "s0")
	store.SetFailpointForTest(failpoint)
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root}, pool: pool, proposer: proposer, db: db, store: store, engine: execution.NewEngine(), proposals: map[string]realblock.Block{block.BlockHash: block}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, committedHash: "genesis", pluginSnapshot: profile, plugins: plugins}
	if err := runtime.commit(context.Background(), block); err == nil {
		t.Fatal("expected durable commit failure")
	}
	if runtime.committedHeight != 0 || runtime.committedHash != "genesis" {
		t.Fatalf("runtime advanced after failure: height=%d hash=%s", runtime.committedHeight, runtime.committedHash)
	}
	if runtime.committed[block.BlockHash] || runtime.blockCount != 0 {
		t.Fatalf("block marked committed after failure")
	}
	if proposer.NextHeight != 1 || proposer.PreviousHash != "genesis" {
		t.Fatalf("proposer advanced after failure")
	}
	if !pool.Has(generated[0].TxID) {
		t.Fatal("reserved transaction was removed after failure")
	}
	if _, err := os.Stat(filepath.Join(storeDir, "blocks.jsonl")); err == nil {
		t.Fatal("unexpected durable block evidence")
	}
	if !reflect.DeepEqual(runtime.db.Snapshot(), stateBefore) {
		t.Fatal("in-memory state changed after durable failure")
	}
	snapshotAfter, err := os.ReadFile(filepath.Join(root, "state_snapshot.json"))
	if err != nil || !reflect.DeepEqual(snapshotAfter, snapshotBefore) {
		t.Fatal("state snapshot changed after durable failure")
	}
	store.SetFailpointForTest("")
	if err := runtime.commit(context.Background(), block); err != nil {
		t.Fatalf("same block was not retryable: %v", err)
	}
	if !runtime.committed[block.BlockHash] || runtime.committedHeight != 1 {
		t.Fatal("retry did not durably commit block")
	}
}

func TestFinalizeClearsSourceRelayAfterTargetCommit(t *testing.T) {
	root := t.TempDir()
	runtime := &NodeRuntime{
		node:                   NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root},
		relaySource:            map[string]Relay{"tx-1": {Tx: tx.SignedTransaction{TxID: "tx-1"}, SourceShard: "s0", TargetShard: "s1"}},
		relayAdmissionFailures: map[string]string{"tx-1": "stale_nonce"},
	}
	envelope, err := p2p.NewEnvelope(finalizeMessage, "s1-leader", "n0", "s1", 0, 0, 0, Finalize{TxID: "tx-1", SourceShard: "s0", TargetShard: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handle(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if _, ok := runtime.relaySource["tx-1"]; ok {
		t.Fatal("source relay remained pending after finalize")
	}
	if _, ok := runtime.relayAdmissionFailures["tx-1"]; ok {
		t.Fatal("stale relay failure remained after finalize")
	}
}

func TestExpireStaleProposalReleasesReservedTransactions(t *testing.T) {
	runtime, block, generated := proposalRuntimeForTest(t, "proposal-timeout")
	runtime.votes[block.BlockHash] = map[string]bool{"n0": true}
	if runtime.pool.ReservedCount() != 1 {
		t.Fatal("test setup did not reserve transaction")
	}
	runtime.expireStaleProposal(5 * time.Second)
	if runtime.proposalInFlight || runtime.proposalInFlightHash != "" {
		t.Fatal("stale proposal remained in flight")
	}
	if runtime.pool.ReservedCount() != 0 {
		t.Fatal("stale proposal did not release reserved transaction")
	}
	if !runtime.pool.Has(generated[0].TxID) {
		t.Fatal("transaction was dropped instead of being made available for reproposal")
	}
	if _, ok := runtime.proposals[block.BlockHash]; ok {
		t.Fatal("stale proposal was not removed")
	}
}

func TestExpireStaleProposalDoesNotReleaseQuorumOrCommittingProposal(t *testing.T) {
	runtime, block, generated := proposalRuntimeForTest(t, "proposal-commit-race")
	runtime.votes[block.BlockHash] = map[string]bool{"n0": true, "n1": true, "n2": true}
	runtime.committing[block.BlockHash] = true
	runtime.expireStaleProposal(5 * time.Second)
	if !runtime.proposalInFlight || runtime.proposalInFlightHash != block.BlockHash {
		t.Fatal("quorum/committing proposal was cleared by timeout")
	}
	if runtime.pool.ReservedCount() != 1 {
		t.Fatal("timeout released a committing proposal reservation")
	}
	if _, ok := runtime.proposals[block.BlockHash]; !ok {
		t.Fatal("timeout removed a committing proposal")
	}
	delete(runtime.committing, block.BlockHash)
	if _, err := runtime.commitWithOrigin(context.Background(), block, CommitOriginConsensus); err != nil {
		t.Fatalf("commit after protected timeout failed: %v", err)
	}
	if runtime.pool.Has(generated[0].TxID) || runtime.pool.ReservedCount() != 0 {
		t.Fatal("committed transaction remained in mempool reservation state")
	}
	if runtime.blockCount != 1 || runtime.committedHeight != 1 {
		t.Fatalf("unexpected commit count/height: count=%d height=%d", runtime.blockCount, runtime.committedHeight)
	}
	if _, err := runtime.commitWithOrigin(context.Background(), block, CommitOriginConsensus); err != nil {
		t.Fatalf("idempotent duplicate commit returned error: %v", err)
	}
	if runtime.blockCount != 1 {
		t.Fatal("duplicate durable commit advanced block count")
	}
}

func proposalRuntimeForTest(t *testing.T, seed string) (*NodeRuntime, realblock.Block, []tx.SignedTransaction) {
	t.Helper()
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	plugins, err := InstantiatePlugins(profile)
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), account.NewNonceManager())
	generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: seed + "-client", Receiver: "receiver", Value: 1, StateKeys: []string{"key"}, Seed: seed})
	if err != nil {
		t.Fatal(err)
	}
	if result := pool.Admit(generated[0]); !result.Accepted {
		t.Fatal(result)
	}
	proposer := realblock.NewProposer("n0", "s0")
	block, err := proposer.Build(pool, 1, nowForTest())
	if err != nil {
		t.Fatal(err)
	}
	db := state.NewDB(root, "s0")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	runtime := &NodeRuntime{
		node:                 NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root, Validators: []string{"n0", "n1", "n2", "n3"}},
		pool:                 pool,
		proposer:             proposer,
		db:                   db,
		store:                storage.NewBlockStore(storeDir, "n0", "s0"),
		engine:               execution.NewEngine(),
		proposals:            map[string]realblock.Block{block.BlockHash: block},
		votes:                map[string]map[string]bool{block.BlockHash: {"n0": true}},
		committed:            map[string]bool{},
		committing:           map[string]bool{},
		pendingCommits:       map[uint64]realblock.Block{},
		committedHash:        "genesis",
		pluginSnapshot:       profile,
		plugins:              plugins,
		proposalInFlight:     true,
		proposalInFlightHash: block.BlockHash,
		proposalStartedAt:    time.Now().Add(-10 * time.Second),
	}
	return runtime, block, generated
}

func nowForTest() (t time.Time) { return time.Unix(100, 0) }
