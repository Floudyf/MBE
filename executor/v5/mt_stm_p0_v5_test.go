package v5

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// Exact-version reads may come from different speculative MVMemory sources
// under different worker schedules. Compare signed read values/digests, full
// write effects and receipts, not internal provenance telemetry.
func mtP0V5ComparableDeltas(input []execution.TxDelta) []execution.TxDelta {
	out := make([]execution.TxDelta, len(input))
	for i, d := range input {
		out[i] = d
		out[i].ReadSet = append([]execution.ReadObservation(nil), d.ReadSet...)
		for j := range out[i].ReadSet {
			out[i].ReadSet[j].Source = ""
		}
		sort.Slice(out[i].ReadSet, func(a, b int) bool {
			x, y := out[i].ReadSet[a], out[i].ReadSet[b]
			if x.Key != y.Key {
				return x.Key < y.Key
			}
			if x.Value != y.Value {
				return x.Value < y.Value
			}
			return x.ValueDigest < y.ValueDigest
		})
	}
	return out
}

// The actual stateless adapter must preserve exact historical reads and
// durable output when the authenticated block-start commitment is supplied.
func TestMTP0VersionedBlockCommitmentOutputEquivalent(t *testing.T) {
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
			TxID: id, Sender: "sender-" + id, Receiver: "receiver-" + id,
			AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_p0",
			AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "update"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: ordinal, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: required}}},
		}
	}
	a, b := makeTx("historical-five", 10, 5), makeTx("historical-seven", 11, 7)
	block := realblock.Block{BlockHash: "mt-stm-p0-exact", Height: 1, ShardID: "s0", ProposerID: "n-s0", TxList: []tx.SignedTransaction{a, b}, TxIDs: []string{a.TxID, b.TxID}}
	base, commitment := r.db.SnapshotWithCommitment()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	legacy, err := r.executeVersionedRemoteBlockWithCommitment(ctx, block, base, nil)
	if err != nil {
		t.Fatalf("legacy result: %v", err)
	}
	got, err := r.executeVersionedRemoteBlockWithCommitment(ctx, block, base, commitment)
	if err != nil {
		t.Fatalf("certified result: %v", err)
	}
	old, now := legacy.ExecutionResult, got.ExecutionResult
	if old.StateRootBefore != now.StateRootBefore || old.StateRootAfter != now.StateRootAfter || old.ReceiptRoot != now.ReceiptRoot || !reflect.DeepEqual(old.Receipts, now.Receipts) || !reflect.DeepEqual(mtP0V5ComparableDeltas(old.TxDeltas), mtP0V5ComparableDeltas(now.TxDeltas)) || !reflect.DeepEqual(old.StateDelta, now.StateDelta) || !reflect.DeepEqual(old.StateUpdates, now.StateUpdates) || !reflect.DeepEqual(legacy.StateDelta, got.StateDelta) {
		t.Fatalf("versioned materialization changed block outputs: old=%s/%s new=%s/%s", old.StateRootAfter, old.ReceiptRoot, now.StateRootAfter, now.ReceiptRoot)
	}
	if state.RootOfSnapshot(now.StateUpdates) != now.StateRootAfter {
		t.Fatal("incremental root differs from independent full reconstruction")
	}
	if len(now.TxDeltas) != 2 || len(now.TxDeltas[0].ReadSet) == 0 || len(now.TxDeltas[1].ReadSet) == 0 || now.TxDeltas[0].ReadSet[0].Value != "historical-five" || now.TxDeltas[1].ReadSet[0].Value != "historical-seven" {
		t.Fatalf("transaction-bound exact-version reads changed: %+v", now.TxDeltas)
	}
}
