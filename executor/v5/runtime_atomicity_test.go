package v5

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
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
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root}, pool: pool, proposer: proposer, db: db, store: store, proposals: map[string]realblock.Block{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, committedHash: "genesis", pluginSnapshot: profile, plugins: plugins}
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

func TestCommitRejectsStaleRemoteSetWithoutDurableSuccess(t *testing.T) {
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
	db := state.NewDB(root, "s0")
	db.Set("object:parcel-1", "new-owner")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	beforeRoot := db.Root()
	store := storage.NewBlockStore(storeDir, "n0", "s0")
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: nowForTest().UnixMilli(), TxIDs: []string{}, TxList: []tx.SignedTransaction{}, StateRootBefore: beforeRoot, StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed", SystemStateDeltas: []realblock.SystemStateDelta{{DeltaID: "stale-delta", Key: "object:parcel-1", Value: "stale-owner", TxID: "tx-stale", TxIDs: []string{"tx-stale"}, BaseValue: "old-owner", BaseValueDigest: stateValueDigest("old-owner"), HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::object:parcel-1", SourceHeight: 1, SourceBlockHash: "source-block"}}}
	realblock.AssignHash(&block)
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root}, pool: pool, proposer: realblock.NewProposer("n0", "s0"), db: db, store: store, proposals: map[string]realblock.Block{block.BlockHash: block}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, pendingStateDeltaKeys: map[string]bool{}, appliedStateDeltaKeys: map[string]bool{}, committedHash: "genesis", pluginSnapshot: profile, plugins: plugins}
	if _, err := runtime.commitWithOrigin(context.Background(), block, CommitOriginConsensus); err == nil {
		t.Fatal("expected stale remote set CAS mismatch")
	}
	if got := db.Get("object:parcel-1"); got != "new-owner" {
		t.Fatalf("stale CAS failure changed DB state: %q", got)
	}
	if runtime.committedHeight != 0 || runtime.committedHash != "genesis" || runtime.blockCount != 0 || runtime.committed[block.BlockHash] {
		t.Fatalf("stale CAS failure advanced runtime: height=%d hash=%s count=%d committed=%t", runtime.committedHeight, runtime.committedHash, runtime.blockCount, runtime.committed[block.BlockHash])
	}
	if _, err := os.Stat(filepath.Join(root, "state_delta.1.wal")); err == nil {
		t.Fatal("stale CAS failure wrote a WAL record")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(storeDir, "blocks.jsonl")); err == nil {
		t.Fatal("stale CAS failure wrote durable block")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for _, event := range runtime.lifecycle {
		if event.Stage == "durable_committed" && event.Success {
			t.Fatalf("stale CAS failure produced durable success lifecycle: %#v", event)
		}
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
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root}, pool: pool, proposer: proposer, db: db, store: store, proposals: map[string]realblock.Block{block.BlockHash: block}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, committedHash: "genesis", pluginSnapshot: profile, plugins: plugins}
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

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "tx-1",
		},
		LogicalTxID: "tx-1",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	ackSent := false

	runtime := &NodeRuntime{
		plan: Plan{
			NodeConfigs: []NodePlan{
				{
					NodeID:  "n0",
					ShardID: "s0",
					Leader:  true,
				},
				{
					NodeID:  "s1-leader",
					ShardID: "s1",
					Leader:  true,
				},
			},
		},
		node: NodePlan{
			NodeID:  "n0",
			ShardID: "s0",
			Leader:  true,
			DataDir: root,
		},
		relaySource: map[string]Relay{
			"tx-1": relay,
		},
		pendingOutboundRelays: map[string]Relay{
			"tx-1": relay,
		},
		outboundRelaySendErrors: map[string]string{
			"tx-1": "previous_send_failure",
		},
		relayAdmissionFailures: map[string]string{
			"tx-1": "stale_nonce",
		},
		crossEventSeen: map[string]bool{},
	}

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		if nodeID == "s1-leader" &&
			envelope.MessageType == finalizeAckMessage {
			ackSent = true
		}
		return nil
	}

	envelope, err := p2p.NewEnvelope(
		finalizeMessage,
		"s1-leader",
		"n0",
		"s1",
		0,
		0,
		0,
		Finalize{
			TxID:        "tx-1",
			LogicalTxID: "tx-1",
			SourceShard: "s0",
			TargetShard: "s1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.handle(
		context.Background(),
		envelope,
	); err != nil {
		t.Fatal(err)
	}

	if _, ok := runtime.relaySource["tx-1"]; ok {
		t.Fatal(
			"source relay remained pending after finalize",
		)
	}

	if _, ok :=
		runtime.pendingOutboundRelays["tx-1"]; ok {
		t.Fatal(
			"source outbound relay remained pending after finalize",
		)
	}

	if _, ok :=
		runtime.outboundRelaySendErrors["tx-1"]; ok {
		t.Fatal(
			"outbound relay send error remained after finalize",
		)
	}

	if _, ok :=
		runtime.relayAdmissionFailures["tx-1"]; ok {
		t.Fatal(
			"stale relay failure remained after finalize",
		)
	}

	if !ackSent {
		t.Fatal(
			"source leader did not send FinalizeAck",
		)
	}
}

func TestExpireStaleProposalPreservesReservationForSameDigestRetransmit(t *testing.T) {
	runtime, block, generated := proposalRuntimeForTest(t, "proposal-timeout")
	runtime.votes[block.BlockHash] = map[string]bool{"n0": true}
	if runtime.pool.ReservedCount() != 1 {
		t.Fatal("test setup did not reserve transaction")
	}
	runtime.expireStaleProposal(5 * time.Second)
	if !runtime.proposalInFlight || runtime.proposalInFlightHash != block.BlockHash {
		t.Fatal("same-view timeout replaced the proposal")
	}
	if runtime.pool.ReservedCount() != 1 {
		t.Fatal("same-view timeout released the proposal reservation")
	}
	if !runtime.pool.Has(generated[0].TxID) {
		t.Fatal("reserved transaction disappeared from the mempool")
	}
	if _, ok := runtime.proposals[block.BlockHash]; !ok {
		t.Fatal("same-view timeout removed the proposal")
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

func TestRetryPendingRelaysDoesNotReadmitDurablyCommittedTransaction(t *testing.T) {
	runtime, block, generated := proposalRuntimeForTest(t, "committed-relay-retry")
	if _, err := runtime.commitWithOrigin(context.Background(), block, CommitOriginConsensus); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	item := generated[0]
	runtime.node.Leader = false
	runtime.relaySource = map[string]Relay{
		item.TxID: {
			Tx:          item,
			LogicalTxID: item.TxID,
			SourceShard: "s1",
			TargetShard: "s0",
		},
	}
	runtime.retryPendingRelays()
	if runtime.pool.Has(item.TxID) || runtime.pool.ReservedCount() != 0 {
		t.Fatal("durably committed relay was re-admitted to the target mempool")
	}
}

func TestProposalTimeoutScalesWithBlockProducerConfig(t *testing.T) {
	runtime, _, _ := proposalRuntimeForTest(t, "proposal-timeout-budget")
	runtime.node.PluginProfile["block_producer"] = PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 100, "interval_ms": 75}}
	plugins, err := InstantiatePlugins(runtime.node.PluginProfile)
	if err != nil {
		t.Fatal(err)
	}
	runtime.plugins = plugins
	if got := runtime.proposalTimeout(); got < 15*time.Second || got > 16*time.Second {
		t.Fatalf("unexpected proposal timeout for 100 tx block: %s", got)
	}
	runtime.node.PluginProfile["block_producer"] = PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 1000, "interval_ms": 1000}}
	plugins, err = InstantiatePlugins(runtime.node.PluginProfile)
	if err != nil {
		t.Fatal(err)
	}
	runtime.plugins = plugins
	if got := runtime.proposalTimeout(); got != 60*time.Second {
		t.Fatalf("proposal timeout hard cap changed: %s", got)
	}
}

func TestCatchupRequestIntervalTracksProposalTimeout(t *testing.T) {
	runtime, _, _ := proposalRuntimeForTest(t, "catchup-budget")
	runtime.node.PluginProfile["block_producer"] = PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 10, "interval_ms": 75}}
	plugins, err := InstantiatePlugins(runtime.node.PluginProfile)
	if err != nil {
		t.Fatal(err)
	}
	runtime.plugins = plugins
	if got := runtime.catchupRequestInterval(); got != 10*time.Second {
		t.Fatalf("small-block catch-up interval should keep the minimum guard: %s", got)
	}
	runtime.node.PluginProfile["block_producer"] = PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 100, "interval_ms": 75}}
	plugins, err = InstantiatePlugins(runtime.node.PluginProfile)
	if err != nil {
		t.Fatal(err)
	}
	runtime.plugins = plugins
	if got := runtime.catchupRequestInterval(); got < 15*time.Second || got > 16*time.Second {
		t.Fatalf("catch-up interval did not track proposal timeout: %s", got)
	}
}

func TestRuntimeStatusWriteIntervalIsThrottled(t *testing.T) {
	t.Setenv("MBE_RUNTIME_STATUS_INTERVAL_MS", "")
	if got := runtimeStatusWriteInterval(75 * time.Millisecond); got != 5*time.Second {
		t.Fatalf("fast block intervals should throttle large status rewrites: %s", got)
	}
	if got := runtimeStatusWriteInterval(2 * time.Second); got != 2*time.Second {
		t.Fatalf("slow block intervals should preserve their cadence: %s", got)
	}

	t.Setenv("MBE_RUNTIME_STATUS_INTERVAL_MS", "7500")
	if got := runtimeStatusWriteInterval(75 * time.Millisecond); got != 7500*time.Millisecond {
		t.Fatalf("runtime status interval override was ignored: %s", got)
	}
	// MBE_META_TRACK_RAPID_FIX_V3
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
		node:                 NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: root, Validators: []string{"n0", "n1", "n2", "n3"}, PluginProfile: profile},
		pool:                 pool,
		proposer:             proposer,
		db:                   db,
		store:                storage.NewBlockStore(storeDir, "n0", "s0"),
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

func TestRuntimeStatusExposesInFlightProposalWork(t *testing.T) {
	runtime, block, generated := proposalRuntimeForTest(t, "proposal-status")
	block.SystemStateDeltas = []realblock.SystemStateDelta{{DeltaID: "delta-1", Key: "key", Value: "1"}}
	runtime.proposals[block.BlockHash] = block
	runtime.lastProgressAt = 123456

	if err := runtime.writeRuntimeStatus(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(runtime.node.DataDir, "node_runtime_status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}
	if status["proposal_in_flight"] != true || status["proposal_work_details_available"] != true {
		t.Fatalf("proposal work status missing: %#v", status)
	}
	if status["proposal_in_flight_hash"] != block.BlockHash {
		t.Fatalf("proposal hash mismatch: %#v", status["proposal_in_flight_hash"])
	}
	ids, ok := status["proposal_logical_tx_ids"].([]any)
	if !ok || len(ids) != 1 || ids[0] != generated[0].TxID {
		t.Fatalf("proposal transaction IDs mismatch: %#v", status["proposal_logical_tx_ids"])
	}
	if status["proposal_system_state_delta_count"] != float64(1) {
		t.Fatalf("proposal system delta count mismatch: %#v", status["proposal_system_state_delta_count"])
	}
}

func nowForTest() (t time.Time) { return time.Unix(100, 0) }

func TestLifecycleStatusSeparatesAdmissionAndExecutionFailure(t *testing.T) {
	events := []LifecycleEvent{
		{TimestampMS: 100, TxID: "tx-admission", LogicalTxID: "tx-admission", Stage: "failed", ShardID: "s0", Success: false, Error: "future_nonce_not_supported"},
		{TimestampMS: 200, TxID: "tx-execution", LogicalTxID: "tx-execution", Stage: "failed", ShardID: "s0", BlockHeight: 7, Success: false, Error: "execution_failed"},
		{TimestampMS: 300, TxID: "tx-admission", LogicalTxID: "tx-admission", Stage: "durable_committed", ShardID: "s0", BlockHeight: 8, Success: true},
	}
	sets := classifyLifecycleStatus(events)
	if !sets.admissionRejected["tx-admission"] || sets.executionFailed["tx-admission"] {
		t.Fatalf("admission rejection classification drifted: %#v", sets)
	}
	if !sets.executionFailed["tx-execution"] || sets.admissionRejected["tx-execution"] {
		t.Fatalf("execution failure classification drifted: %#v", sets)
	}
	if !sets.terminal["tx-admission"] {
		t.Fatal("later durable commit did not make admission-rejected transaction terminal")
	}
	if !sets.terminal["tx-execution"] {
		t.Fatal("committed-block execution failure was not terminal")
	}
}

func TestLifecycleStatusAdmissionRejectionAloneIsNotNodeTerminal(t *testing.T) {
	sets := classifyLifecycleStatus([]LifecycleEvent{{
		TimestampMS: 100,
		TxID:        "tx-1",
		LogicalTxID: "tx-1",
		Stage:       "failed",
		ShardID:     "s0",
		Success:     false,
		Error:       "future_nonce_not_supported",
	}})
	if !sets.admissionRejected["tx-1"] || !sets.failed["tx-1"] {
		t.Fatalf("admission rejection evidence missing: %#v", sets)
	}
	if sets.terminal["tx-1"] {
		t.Fatal("replica-local admission rejection became node-terminal")
	}
}

func TestLifecycleStatusLegacyCrossExecutionFailureWaitsForProtocolOutcome(t *testing.T) {
	events := []LifecycleEvent{
		{TimestampMS: 100, TxID: "cross-1", LogicalTxID: "cross-1", Stage: "sourcelock", ShardID: "s0", BlockHeight: 1, Success: true},
		{TimestampMS: 200, TxID: "cross-1", LogicalTxID: "cross-1", Stage: "failed", ShardID: "s1", BlockHeight: 2, Success: false, Error: "execution_failed"},
	}
	sets := classifyLifecycleStatus(events)
	if !sets.executionFailed["cross-1"] {
		t.Fatal("cross-shard execution failure evidence was lost")
	}
	if sets.terminal["cross-1"] {
		t.Fatal("legacy cross-shard execution failure closed before finalize/refund")
	}

	events = append(events, LifecycleEvent{TimestampMS: 300, TxID: "cross-1", LogicalTxID: "cross-1", Stage: "refund", ShardID: "s0", BlockHeight: 3, Success: false})
	sets = classifyLifecycleStatus(events)
	if !sets.terminal["cross-1"] || !sets.refunded["cross-1"] {
		t.Fatal("refund did not close legacy cross-shard lifecycle")
	}
}
