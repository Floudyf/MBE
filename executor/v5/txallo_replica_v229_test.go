package v5

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestTxAlloReplicaV229DependencyChainIncludesAccountState(t *testing.T) {
	last := map[string]uint64{}
	record := WorkloadRecord{Index: 4, AccessList: []tx.AccessItem{
		{Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		{Key: "m.federation/miners/alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		{Key: "hot-counter", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "commutative_delta", Delta: 1},
	}}
	deps := txalloStatefulReplicaDependenciesForRecord(record, 5, last)
	if len(deps) != 2 {
		t.Fatalf("deps=%v want two non-commutative keys", deps)
	}
	for _, dep := range deps {
		if dep.ProducedVersion != 5 || dep.RequiredVersion != 0 {
			t.Fatalf("unexpected dependency: %#v", dep)
		}
	}
	if last["nonce:alice"] != 5 || last["m.federation/miners/alice"] != 5 {
		t.Fatalf("last-writer frontier not advanced: %#v", last)
	}
	if _, ok := last["hot-counter"]; ok {
		t.Fatal("commutative delta was incorrectly serialized into exact-version chain")
	}
}

func TestTxAlloReplicaV229TargetRelayIsProtocolOnly(t *testing.T) {
	item := tx.SignedTransaction{Payload: "v5_cross:s1:alien_worlds_event"}
	if !txalloReplicaTargetCommitV229(item, "s1") {
		t.Fatal("target relay was not recognized")
	}
	if txalloReplicaTargetCommitV229(item, "s0") {
		t.Fatal("source execution shard was misclassified as relay target")
	}
}

func TestTxAlloReplicaV229ConvergenceRequiresEveryNode(t *testing.T) {
	root := t.TempDir()
	nodes := []NodePlan{{NodeID: "n0", DataDir: filepath.Join(root, "n0")}, {NodeID: "n1", DataDir: filepath.Join(root, "n1")}}
	token := txalloReplicaTokenV229("asset", 7)
	write := func(node NodePlan) {
		t.Helper()
		if err := os.MkdirAll(node.DataDir, 0o755); err != nil {
			t.Fatal(err)
		}
		row := txalloReplicaCommitRowV229{SchemaVersion: txalloReplicaCommitFeedSchema, Token: token, Key: "asset", RoutingOrdinal: 7, Value: "value", ValueDigest: stateValueDigest("value"), SourceTxID: "t", SourceShard: "s0", MaterializedShard: "s0"}
		b, _ := json.Marshal(row)
		if err := os.WriteFile(filepath.Join(node.DataDir, txalloReplicaCommitFeedName), append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(nodes[0])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := txalloWaitReplicaConvergenceV229(ctx, nodes, []string{token}, newTxAlloReplicaFeedReaderV229()); err == nil {
		t.Fatal("convergence passed while one node lacked the durable token")
	}
	write(nodes[1])
	if err := txalloWaitReplicaConvergenceV229(context.Background(), nodes, []string{token}, newTxAlloReplicaFeedReaderV229()); err != nil {
		t.Fatal(err)
	}
}

func TestTxAlloReplicaV229ConfirmationIsEpochBound(t *testing.T) {
	p := testDynamicShardingV222(t, map[string]any{
		"dynamic_a_txallo_runtime":        true,
		"stateful_paper_replicated_state": true,
	})
	if err := p.ConfirmTxAlloReplicaConvergence(1, "digest", 2); err == nil {
		t.Fatal("future source epoch convergence was accepted")
	}
	if err := p.ConfirmTxAlloReplicaConvergence(0, "digest", 2); err != nil {
		t.Fatal(err)
	}
	ev := p.HistoricalAllocationEvidence()
	if !boolFromAny(ev["stateful_replica_convergence_confirmed"]) || intValue(ev["stateful_replica_convergence_token_count"]) != 2 {
		t.Fatalf("confirmation evidence missing: %#v", ev)
	}
}

func TestTxAlloReplicaV229ConvergenceRejectsExactValueMismatch(t *testing.T) {
	root := t.TempDir()
	nodes := []NodePlan{{NodeID: "n0", DataDir: filepath.Join(root, "n0")}, {NodeID: "n1", DataDir: filepath.Join(root, "n1")}}
	token := txalloReplicaTokenV229("asset", 7)
	for index, node := range nodes {
		if err := os.MkdirAll(node.DataDir, 0o755); err != nil {
			t.Fatal(err)
		}
		value := "v0"
		if index == 1 {
			value = "v1"
		}
		row := txalloReplicaCommitRowV229{SchemaVersion: txalloReplicaCommitFeedSchema, Token: token, Key: "asset", RoutingOrdinal: 7, Value: value, ValueDigest: stateValueDigest(value), SourceTxID: "t", SourceShard: "s0", MaterializedShard: node.NodeID, Commutative: false}
		b, _ := json.Marshal(row)
		if err := os.WriteFile(filepath.Join(node.DataDir, txalloReplicaCommitFeedName), append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := txalloWaitReplicaConvergenceV229(context.Background(), nodes, []string{token}, newTxAlloReplicaFeedReaderV229()); err == nil {
		t.Fatal("convergence accepted conflicting exact replica values")
	}
}

func TestTxAlloReplicaV229SupersededReadUsesLocalExactHistory(t *testing.T) {
	r := &NodeRuntime{
		node:                     NodePlan{ShardID: "s0", DataDir: t.TempDir()},
		stateVersionValues:       map[string]map[uint64]string{"asset": {2: "old-value", 5: "new-value"}},
		stateVersionMaterialized: map[string]uint64{"asset": 5},
	}
	stateV := r.txalloReplicaStateV229()
	stateV.loaded = true
	stateV.rows[txalloReplicaTokenV229("asset", 2)] = txalloReplicaCommitRowV229{Key: "asset", RoutingOrdinal: 2, Value: "old-value", ValueDigest: stateValueDigest("old-value"), SourceTxID: "t2", SourceShard: "s1"}
	ready, err := r.txalloReplicaExternalDependencyReadyV229(tx.StateVersionDependency{Key: "asset", RequiredVersion: 2})
	if err != nil || !ready {
		t.Fatalf("superseded read-only exact version not ready: ready=%v err=%v", ready, err)
	}
	if _, err := r.txalloReplicaExternalDependencyReadyV229(tx.StateVersionDependency{Key: "asset", RequiredVersion: 2, ProducedVersion: 6}); err == nil {
		t.Fatal("superseded writer predecessor was incorrectly accepted")
	}
}

func TestTxAlloReplicaV2296StatefulDisablesGenericVersionedWave(t *testing.T) {
	plugins, err := InstantiatePlugins(txalloNodeRatioModeProfileV21(false))
	if err != nil {
		t.Fatal(err)
	}
	r := &NodeRuntime{
		plugins: plugins,
		plan:    Plan{NodeConfigs: []NodePlan{{NodeID: "n0", ShardID: "s0"}, {NodeID: "n1", ShardID: "s1"}}},
	}
	block := realblock.Block{TxList: []tx.SignedTransaction{{
		ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1}}},
	}}}
	if r.versionedRemoteWaveExecutionEnabled(block) {
		t.Fatal("Stateful-TxAllo paper replica mode leaked into generic versioned remote-wave execution")
	}
}
