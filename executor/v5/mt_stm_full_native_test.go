package v5

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

// MT_STM_SINGLE_CONTEXT_V1: real execution and version publication, no mock
// scheduler and no TCP peers. Reproduce the historical wave-order problem with
// three all-successful direct-access transactions, matching S0/8477's scope.
func TestMTSTMFullBlockStateReadyOrderIndependence(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "ready_before_execution"
		if late {
			name = "ready_during_execution"
		}
		t.Run(name, func(t *testing.T) {
			profile := testMetaTrackProfile()
			profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
			profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
			profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
			profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
			profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{
				"worker_count": 3, "execution_mode": "performance", "oracle_mode": "off",
				"maximum_incarnations": 0, "incarnation_limit_action": "fail",
				"scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1",
				"versioned_wave_policy": "maximal_compatible_exact_version_v2",
			}}
			plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
				{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
				{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
			}}
			runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
			if err != nil {
				t.Fatal(err)
			}
			localKey := func(label string) string {
				for i := 0; i < 4096; i++ {
					k := fmt.Sprintf("mt-stm-%s-%d", label, i)
					if runtime.stateHomeShardForKey(k, runtime.shardIDs()) == "s0" {
						return k
					}
				}
				t.Fatalf("no local home for %s", label)
				return ""
			}
			gate := localKey("gate")
			shared := localKey("shared")
			runtime.db.Set(gate, "seed")
			runtime.db.Set(shared, "base")
			runtime.stateVersionInitial[gate] = "seed"
			if !late {
				runtime.publishStateVersion(gate, 7, "seed")
			}
			a := tx.SignedTransaction{TxID: "mt-full-t0", Sender: "alice", Receiver: "bob", AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_single_context", AccessList: []tx.AccessItem{{Key: localKey("dummy"), Mode: tx.AccessWrite, UpdateSemantics: "set"}}}
			b := tx.SignedTransaction{TxID: "mt-full-t1", Sender: "bob", Receiver: "carol", AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_single_context", AccessList: []tx.AccessItem{{Key: gate, Mode: tx.AccessRead, UpdateSemantics: "category_metadata"}, {Key: shared, Mode: tx.AccessReadWrite, UpdateSemantics: "update"}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 8, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: gate, RequiredVersion: 7}}}}
			c := tx.SignedTransaction{TxID: "mt-full-t2", Sender: "carol", Receiver: "dave", AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_single_context", AccessList: []tx.AccessItem{{Key: shared, Mode: tx.AccessReadWrite, UpdateSemantics: "update"}}}
			items := []tx.SignedTransaction{a, b, c}
			block := realblock.Block{BlockHash: "mt-stm-full-native", Height: 1, ShardID: "s0", ProposerID: "n-s0", TxList: items, TxIDs: []string{a.TxID, b.TxID, c.TxID}}
			base := runtime.db.Snapshot()
			expected := execution.NewSerialExecutor().ExecuteBlock(block, base)
			if len(expected.Receipts) != 3 || !expected.Receipts[0].Success || !expected.Receipts[1].Success || !expected.Receipts[2].Success {
				t.Fatalf("invalid all-success fixture: %#v", expected.Receipts)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			// Delayed release must be causally after an *observed* blocked
			// StateReady probe, not an arbitrary wall-clock sleep. Otherwise
			// slow Windows startup can make the supposed late case all-ready.
			gateReleased := make(chan struct{})
			if late {
				go func() {
					tick := time.NewTicker(5 * time.Millisecond)
					defer tick.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-tick.C:
							runtime.mu.Lock()
							observed := runtime.runtimeMetricCounts["block_stm_tx_state_ready_wait_observed"]
							runtime.mu.Unlock()
							if observed > 0 {
								runtime.publishStateVersion(gate, 7, "seed")
								close(gateReleased)
								return
							}
						}
					}
				}()
			}
			actual, err := runtime.executeVersionedRemoteBlock(ctx, block, base)
			if err != nil {
				t.Fatalf("native full-block execution error: %v", err)
			}
			got := actual.ExecutionResult
			if !reflect.DeepEqual(expected.StateDelta, got.StateDelta) || expected.StateRootAfter != got.StateRootAfter || expected.ReceiptRoot != got.ReceiptRoot || !reflect.DeepEqual(expected.Receipts, got.Receipts) {
				t.Fatalf("MT_STM_RED_DIVERGENCE: late=%t serial=%s/%s actual=%s/%s serial_deltas=%v actual_deltas=%v", late, expected.StateRootAfter, expected.ReceiptRoot, got.StateRootAfter, got.ReceiptRoot, expected.StateDelta, got.StateDelta)
			}
			if policy, _ := actual.ActualMetrics["versioned_state_ready_wave_policy"].(string); policy != "shared_blockstm_single_context_v1" {
				t.Fatalf("not using the whole-block shared MVMemory adapter: %q", policy)
			}
			if late {
				select {
				case <-gateReleased:
				default:
					t.Fatal("late gate was never released after observing a blocked probe")
				}
			}
			if late && int64FromAny(actual.ActualMetrics["versioned_state_ready_wait_observation_count"]) < 1 {
				t.Fatal("late gate did not test per-transaction readiness suspension")
			}
			runtime.mu.Lock()
			observedWaitEvents := runtime.runtimeMetricCounts["block_stm_tx_state_ready_wait_observed"]
			runtime.mu.Unlock()
			if late && observedWaitEvents != 1 {
				t.Fatalf("expected exactly one real late-StateReady admission event; got %d", observedWaitEvents)
			}
			if !late && observedWaitEvents != 0 {
				t.Fatalf("unexpected StateReady wait when all versions are ready: %d", observedWaitEvents)
			}
		})
	}
}

// A signed in-block predecessor is not always an MV write: a successful
// read-only producer can publish a logical ordering alias. The consumer must
// see the published exact value without waiting for an unrelated wave.
func TestMTSTMFullBlockInternalNoOpAlias(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
	profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 2, "execution_mode": "performance", "oracle_mode": "off", "scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1"}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	r, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	gate := keyWithHomeShard(t, "s0", r.shardIDs())
	r.db.Set(gate, "current")
	r.stateVersionInitial[gate] = "historical"
	first := tx.SignedTransaction{TxID: "noop-producer", Sender: "a", Receiver: "b", AccessListSchema: "mbe_workload_record_v3", AccessList: []tx.AccessItem{{Key: gate, Mode: tx.AccessRead, UpdateSemantics: "category_metadata"}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 1, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: gate, RequiredVersion: 0, ProducedVersion: 1}}}}
	second := tx.SignedTransaction{TxID: "noop-consumer", Sender: "c", Receiver: "d", AccessListSchema: "mbe_workload_record_v3", AccessList: []tx.AccessItem{{Key: gate, Mode: tx.AccessReadWrite, UpdateSemantics: "historical_update"}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 2, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: gate, RequiredVersion: 1}}}}
	b := realblock.Block{BlockHash: "mt-stm-noop", ShardID: "s0", Height: 1, ProposerID: "n-s0", TxList: []tx.SignedTransaction{first, second}, TxIDs: []string{first.TxID, second.TxID}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := r.executeVersionedRemoteBlock(ctx, b, r.db.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := r.stateVersionValue(gate, 1)
	if !ok || v != "historical" {
		t.Fatalf("missing published exact no-op alias: %q %t", v, ok)
	}
	if len(out.ExecutionResult.TxDeltas) != 2 || len(out.ExecutionResult.TxDeltas[1].ReadSet) == 0 {
		t.Fatal("consumer has no validated exact read")
	}
	if got := out.ExecutionResult.TxDeltas[1].ReadSet[0].Value; got != "historical" {
		t.Fatalf("no-op alias read incorrect: %q", got)
	}
}

// Two consumers of the same logical key may request two different exact
// historical values while an earlier transaction publishes an MV write.
// The signed exact value must not be replaced with the latest MV write.
func TestMTSTMFullBlockTwoHistoricalVersions(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
	profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{
		"worker_count": 2, "execution_mode": "performance", "oracle_mode": "off",
		"scheduler_mode": "priority_heap_v1", "dependency_wait_mode": "suspend_same_incarnation_v1",
	}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	r, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	key := keyWithHomeShard(t, "s0", r.shardIDs())
	r.db.Set(key, "current")
	r.stateVersionInitial[key] = "current"
	r.publishStateVersion(key, 5, "historical-five")
	r.publishStateVersion(key, 7, "historical-seven")
	makeTx := func(id string, ordinal, required uint64) tx.SignedTransaction {
		return tx.SignedTransaction{
			TxID: id, Sender: "a-" + id, Receiver: "b-" + id,
			AccessListSchema: "mbe_workload_record_v3", AccessListSource: "exact_history_test",
			AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "update"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{
				RoutingOrdinal: ordinal, ExecutionShard: "s0",
				StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: required}},
			},
		}
	}
	a := makeTx("history-five", 10, 5)
	b := makeTx("history-seven", 11, 7)
	block := realblock.Block{BlockHash: "mt-stm-two-histories", Height: 1, ShardID: "s0", ProposerID: "n-s0",
		TxList: []tx.SignedTransaction{a, b}, TxIDs: []string{a.TxID, b.TxID}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := r.executeVersionedRemoteBlock(ctx, block, r.db.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	got := out.ExecutionResult
	if len(got.TxDeltas) != 2 || !got.TxDeltas[0].Success || !got.TxDeltas[1].Success {
		t.Fatalf("incomplete exact-version execution: %+v", got.TxDeltas)
	}
	if got.TxDeltas[0].ReadSet[0].Value != "historical-five" || got.TxDeltas[1].ReadSet[0].Value != "historical-seven" {
		t.Fatalf("exact history conflated across transactions: first=%+v second=%+v", got.TxDeltas[0].ReadSet, got.TxDeltas[1].ReadSet)
	}
	if got.TxDeltas[0].WriteSet[key] == got.TxDeltas[1].WriteSet[key] {
		t.Fatal("different historical versions produced identical RMW results")
	}
}
