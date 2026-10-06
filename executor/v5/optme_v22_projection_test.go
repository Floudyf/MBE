package v5

import (
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestOptMEV22GlobalRoutingFinality(t *testing.T) {
	r := optmeGlobalRouting{basicPlugin: makeBasic("routing", optmeStatefulRoutingID, nil)}
	if got := r.CrossShardFinalityMode(); got != CrossShardFinalityOptMEGlobalCommit {
		t.Fatalf("finality mode=%q", got)
	}
	s := statelessOptmeRouting{basicPlugin: makeBasic("routing", optmeStatelessRoutingID, nil)}
	if got := s.CrossShardFinalityMode(); got != CrossShardFinalityOptMEGlobalCommit {
		t.Fatalf("stateless finality mode=%q", got)
	}
	if s.StatelessVersionAdmission() {
		t.Fatal("Stateless-OptME v22 must not use transaction-level version admission")
	}
}

func TestOptMEV22ProjectionTasksDeduplicateKeys(t *testing.T) {
	block := realblock.Block{TxList: []tx.SignedTransaction{
		{TxID: "t1", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessReadWrite}, {Key: "b", Mode: tx.AccessRead}}},
		{TxID: "t2", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "c", Mode: tx.AccessWrite}}},
	}}
	tasks, err := optmeProjectionTasks(block, []string{"s0", "s1"}, func(key string, _ []string) string {
		if key == "b" {
			return "s1"
		}
		return "s0"
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("unique tasks=%d", len(tasks))
	}
	if tasks[0].key != "a" || tasks[1].key != "b" || tasks[2].key != "c" {
		t.Fatalf("unexpected key order: %#v", tasks)
	}
}

func TestOptMEV22PartitionMaterializationKeepsOnlyOwnedHome(t *testing.T) {
	r := &NodeRuntime{
		node: NodePlan{NodeID: "n0", ShardID: optmeGlobalOrderingDomainID, ExecutionShardID: "s0"},
		plan: Plan{NodeConfigs: []NodePlan{
			{NodeID: "n0", ShardID: optmeGlobalOrderingDomainID, ExecutionShardID: "s0"},
			{NodeID: "n1", ShardID: optmeGlobalOrderingDomainID, ExecutionShardID: "s1"},
		}},
		plugins: RuntimePlugins{
			Routing:       statelessOptmeRouting{basicPlugin: makeBasic("routing", optmeStatelessRoutingID, nil)},
			BlockExecutor: optmeBlockExecutor{basicPlugin: makeBasic("block_executor", optmeStatelessExecutorID, nil), mode: optmeStatelessMode},
			StateStorage:  optmePartitionStateStore{builtinStateStorage{makeBasic("state_storage", optmePartitionStateStorageID, nil)}},
			Sharding:      builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)},
		},
		runtimeMetricCounts: map[string]int64{},
	}
	shards := []string{"s0", "s1"}
	localKey := ""
	remoteKey := ""
	for i := 0; i < 10000 && (localKey == "" || remoteKey == ""); i++ {
		key := "k" + fmt.Sprint(i)
		home := r.stateHomeShardForKey(key, shards)
		if home == "s0" && localKey == "" {
			localKey = key
		}
		if home == "s1" && remoteKey == "" {
			remoteKey = key
		}
	}
	if localKey == "" || remoteKey == "" {
		t.Fatal("failed to find fixture keys")
	}
	block := realblock.Block{ShardID: optmeGlobalOrderingDomainID, Height: 7}
	items := []state.StateKV{
		{Key: qualifyStateKey(optmeGlobalOrderingDomainID, localKey), Value: "L", TxIDs: []string{"t1"}},
		{Key: qualifyStateKey(optmeGlobalOrderingDomainID, remoteKey), Value: "R", TxIDs: []string{"t2"}},
	}
	out, err := r.optmePartitionOwnedStateDelta(block, items)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Key != qualifyStateKey("s0", localKey) || out[0].Value != "L" {
		t.Fatalf("unexpected owned delta: %#v", out)
	}
	if out[0].ProducedVersion != 0 || out[0].PreviousVersion != 0 || out[0].ApplyOrigin != optmePartitionMaterializeOrigin {
		t.Fatalf("legacy version transport leaked into v22 materialization: %#v", out[0])
	}
}

func TestOptMEV22PartitionStoreUsesExecutionShardIdentity(t *testing.T) {
	plugin := optmePartitionStateStore{builtinStateStorage{makeBasic("state_storage", optmePartitionStateStorageID, nil)}}
	capability, ok := any(plugin).(ExecutionShardStorageIdentityCapability)
	if !ok || !capability.UseExecutionShardStorageIdentity() {
		t.Fatal("OptME v22 partition store must persist by execution-shard identity")
	}
}

func TestOptMEV22RejectsLegacyTransactionStateVersions(t *testing.T) {
	block := realblock.Block{TxList: []tx.SignedTransaction{{
		TxID:             "legacy",
		ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "a", RequiredVersion: 1, ProducedVersion: 2}}},
	}}}
	if err := validateOptMEV22NoTransactionStateVersions(block); err == nil {
		t.Fatal("v22 must fail closed when legacy transaction-level StateVersions reappear")
	}
}
