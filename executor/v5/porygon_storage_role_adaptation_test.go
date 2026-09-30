package v5

import (
	"context"
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonUnitTestStateFetch(base map[string]string, orderingDomain string, executionShardCount int) PorygonStateFetchFunc {
	return func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		homeShard := fmt.Sprintf("s%d", porygonStateShard(access.Key, executionShardCount))
		value := ""
		if base != nil {
			if candidate, ok := base[qualifyStateKey(homeShard, access.Key)]; ok {
				value = candidate
			} else if candidate, ok := base[qualifyStateKey(orderingDomain, access.Key)]; ok {
				value = candidate
			} else if candidate, ok := base[access.Key]; ok {
				value = candidate
			}
		}
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, HomeShard: homeShard, Value: value, ReadinessToken: "unit-test-state-projection"}, nil
	}
}

func TestPorygonPartitionStateStoreUsesExecutionShardIdentity(t *testing.T) {
	store := porygonPartitionStateStore{builtinStateStorage{makeBasic("state_storage", porygonStateStorageID, nil)}}
	var candidate any = store
	capability, ok := candidate.(ExecutionShardStorageIdentityCapability)
	if !ok {
		t.Fatal("Porygon state store must expose execution-shard storage identity capability")
	}
	if !capability.UseExecutionShardStorageIdentity() {
		t.Fatal("Porygon state store must separate porygon-global consensus identity from execution-shard storage identity")
	}
}

func TestPorygonTransactionSnapshotFetchesRemoteProjectionOnly(t *testing.T) {
	remoteKey := ""
	localKey := ""
	for i := 0; i < 1000 && (remoteKey == "" || localKey == ""); i++ {
		key := "porygon-key-" + string(rune('a'+(i%26))) + "-" + string(rune('A'+((i/26)%26)))
		home := porygonStateShard(key, 2)
		if home == 0 && localKey == "" {
			localKey = key
		}
		if home == 1 && remoteKey == "" {
			remoteKey = key
		}
	}
	if localKey == "" || remoteKey == "" {
		t.Fatal("failed to construct local/remote Porygon state keys")
	}

	item := tx.SignedTransaction{
		TxID: "porygon-projection",
		AccessList: []tx.AccessItem{
			{Key: localKey, Mode: tx.AccessRead},
			{Key: remoteKey, Mode: tx.AccessRead},
		},
	}
	localBase := map[string]string{qualifyStateKey("s0", localKey): "local-value"}
	fetchCalls := 0
	fetch := func(ctx context.Context, got tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		fetchCalls++
		if got.TxID != item.TxID || access.Key != remoteKey {
			t.Fatalf("unexpected Porygon fetch: tx=%s key=%s", got.TxID, access.Key)
		}
		return RemoteStateReadyEvent{TxID: got.TxID, Key: access.Key, HomeShard: "s1", Value: "remote-value", ReadinessToken: "proof"}, nil
	}

	snapshot, err := porygonTransactionSnapshotWithFetch(context.Background(), localBase, "porygon-global", "s0", 2, item, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if fetchCalls != 1 {
		t.Fatalf("expected exactly one remote projection fetch, got %d", fetchCalls)
	}
	if snapshot[qualifyStateKey("porygon-global", localKey)] != "local-value" {
		t.Fatalf("local projection missing: %#v", snapshot)
	}
	if snapshot[qualifyStateKey("porygon-global", remoteKey)] != "remote-value" {
		t.Fatalf("remote projection missing: %#v", snapshot)
	}
}

func TestPorygonRemoteProjectionFailsClosedWithoutFetcher(t *testing.T) {
	remoteKey := ""
	for i := 0; i < 1000; i++ {
		key := "remote-only-" + string(rune('a'+(i%26))) + "-" + string(rune('A'+((i/26)%26)))
		if porygonStateShard(key, 2) == 1 {
			remoteKey = key
			break
		}
	}
	if remoteKey == "" {
		t.Fatal("failed to construct remote key")
	}
	item := tx.SignedTransaction{
		TxID:       "missing-fetcher",
		AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead}},
	}
	if _, err := porygonTransactionSnapshotWithFetch(context.Background(), nil, "porygon-global", "s0", 2, item, nil); err == nil {
		t.Fatal("Porygon must fail closed when a remote state projection is required but no fetcher is wired")
	}
}

func TestPorygonDirectExecutorCompatibilityPreservesLegacyGlobalStateRoot(t *testing.T) {
	left, right := findDisjointCrossESCPair(t)
	items := []tx.SignedTransaction{left, right}
	block := porygonFixtureBlock(t, items...)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)

	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	result, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             planned.Block,
		BaseStateSnapshot: map[string]string{},
		WorkerCount:       4,
	})
	if err != nil {
		t.Fatal(err)
	}

	serial := execution.NewSerialExecutor().ExecuteBlock(planned.Block, map[string]string{})
	if result.ExecutionResult.StateRootAfter != serial.StateRootAfter {
		t.Fatalf("legacy direct-executor root changed: porygon=%s serial=%s", result.ExecutionResult.StateRootAfter, serial.StateRootAfter)
	}
	if got := result.ActualMetrics["porygon_state_root_scope"]; got != "legacy_direct_executor_global_snapshot" {
		t.Fatalf("legacy direct-executor root scope=%v", got)
	}
}

func TestPorygonLocalStorageRoleProjectionDoesNotSendToSelfPeer(t *testing.T) {
	localKey := ""
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("porygon-local-self-%d", i)
		if porygonStateShard(key, 2) == 0 {
			localKey = key
			break
		}
	}
	if localKey == "" {
		t.Fatal("failed to construct local Porygon state key")
	}

	storePlugin := porygonPartitionStateStore{builtinStateStorage{makeBasic("state_storage", porygonStateStorageID, nil)}}
	db, blockStore, err := storePlugin.Open(StateStorageInput{DataDir: t.TempDir(), NodeID: "n0", ShardID: "s0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := storePlugin.ApplyBatch(db, []state.StateKV{{Key: qualifyStateKey("s0", localKey), Value: "local-storage-role-value"}}); err != nil {
		t.Fatal(err)
	}

	sendCalls := 0
	runtime := &NodeRuntime{
		node: NodePlan{NodeID: "n0", ShardID: "porygon-global", ExecutionShardID: "s0", ConsensusDomainID: "porygon-global"},
		db:   db, store: blockStore, plugins: RuntimePlugins{StateStorage: storePlugin},
		stateFetchSnapshots: map[string]map[string]string{}, stateFetchSnapshotRoots: map[string]string{},
		sendToNodeHook: func(context.Context, string, p2p.MessageEnvelope) error {
			sendCalls++
			return fmt.Errorf("self peer transport must not be used")
		},
	}
	block := realblock.Block{Height: 1, BlockHash: "porygon-local-storage-role-block"}
	item := tx.SignedTransaction{TxID: "porygon-local-storage-role-tx"}
	access := tx.AccessItem{Key: localKey, Mode: tx.AccessRead}
	event, err := runtime.porygonStateProjectionFetch(context.Background(), block, item, access, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if sendCalls != 0 {
		t.Fatalf("local Storage Role projection unexpectedly used network self-send: calls=%d", sendCalls)
	}
	if event.HomeShard != "s0" {
		t.Fatalf("home shard=%q want s0", event.HomeShard)
	}
	if event.Value != "local-storage-role-value" {
		t.Fatalf("local Storage Role value=%q", event.Value)
	}
	if event.ReadinessToken == "" {
		t.Fatal("local Storage Role projection must retain witness digest semantics")
	}
	if runtime.runtimeMetricCounts["porygon_storage_role_local_projection_count"] != 1 {
		t.Fatalf("local projection metric=%d", runtime.runtimeMetricCounts["porygon_storage_role_local_projection_count"])
	}
}
