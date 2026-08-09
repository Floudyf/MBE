package v5

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func statelessVersionAdmissionTestRuntimes(t *testing.T) (*NodeRuntime, *NodeRuntime, string) {
	t.Helper()
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
	profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 2, "execution_mode": "performance", "oracle_mode": "off"}}
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "s0"), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	s0, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	s1, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	runtimes := map[string]*NodeRuntime{"n-s0": s0, "n-s1": s1}
	wire := func(from *NodeRuntime) func(context.Context, string, p2p.MessageEnvelope) error {
		return func(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
			target := runtimes[nodeID]
			if target == nil {
				return fmt.Errorf("unknown test node %s", nodeID)
			}
			if envelope.MessageType != stateFetchRequestMessage {
				return fmt.Errorf("unexpected message %s", envelope.MessageType)
			}
			request, err := p2p.DecodePayload[StateFetchRequest](envelope)
			if err != nil {
				return err
			}
			if request.AccessKind != statelessVersionAdmissionProbeAccessKind {
				return fmt.Errorf("unexpected access kind %s", request.AccessKind)
			}
			value, ready := target.stateVersionValue(request.Key, request.RequiredVersion)
			response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: request.HomeShard + "::" + request.Key, Value: value, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: target.plugins.StateStorage.Root(target.db), StateVersion: request.RequiredVersion, Versioned: true, Success: ready}
			if !ready {
				response.Error = "state_version_not_ready"
			}
			response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
			from.handleStateFetchResponse(response)
			return nil
		}
	}
	s0.sendToNodeHook = wire(s0)
	s1.sendToNodeHook = wire(s1)
	key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	s0.db.Set(key, "seed")
	s0.stateVersionInitial[key] = "seed"
	return s0, s1, key
}

func statelessVersionAdmissionItem(id, execShard, key string, ordinal, required uint64) tx.SignedTransaction {
	item := tx.SignedTransaction{TxID: id, Sender: "sender-" + id, Receiver: "receiver-" + id, AccessListSchema: "mbe_workload_record_v3", AccessListSource: "stateless_version_admission_test", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}, StateKeys: []string{key}}
	item.ExecutionRouting = &tx.ExecutionRoutingMetadata{RoutingOrdinal: ordinal, ExecutionShard: execShard, StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: required, ProducedVersion: ordinal}}}
	return item
}

func TestStatelessVersionAdmissionKeepsSameCandidateDependencyChain(t *testing.T) {
	_, s1, key := statelessVersionAdmissionTestRuntimes(t)
	first := statelessVersionAdmissionItem("tx1", "s1", key, 1, 0)
	second := statelessVersionAdmissionItem("tx2", "s1", key, 2, 1)
	block := realblock.Block{BlockHash: "candidate-chain", Height: 1, ShardID: "s1", Timestamp: time.Now().UnixMilli(), TxIDs: []string{first.TxID, second.TxID}, TxList: []tx.SignedTransaction{first, second}}
	admitted, deferred, err := s1.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(deferred) != 0 || len(admitted.TxList) != 2 {
		t.Fatalf("same-candidate chain was split: admitted=%d deferred=%d", len(admitted.TxList), len(deferred))
	}
	if admitted.TxList[0].TxID != first.TxID || admitted.TxList[1].TxID != second.TxID {
		t.Fatalf("candidate order changed: %#v", admitted.TxIDs)
	}
}

func TestStatelessVersionAdmissionDefersExternalFutureVersionAndResumes(t *testing.T) {
	s0, s1, key := statelessVersionAdmissionTestRuntimes(t)
	consumer := statelessVersionAdmissionItem("tx2", "s1", key, 2, 1)
	block := realblock.Block{BlockHash: "candidate-external", Height: 1, ShardID: "s1", Timestamp: time.Now().UnixMilli(), TxIDs: []string{consumer.TxID}, TxList: []tx.SignedTransaction{consumer}}
	admitted, deferred, err := s1.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.TxList) != 0 || len(deferred) != 1 || deferred[0].TxID != consumer.TxID {
		t.Fatalf("future exact version was admitted: admitted=%d deferred=%#v", len(admitted.TxList), deferred)
	}
	s0.publishStateVersion(key, 1, "v1")
	admitted, deferred, err = s1.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(deferred) != 0 || len(admitted.TxList) != 1 || admitted.TxList[0].TxID != consumer.TxID {
		t.Fatalf("published exact version did not resume transaction: admitted=%#v deferred=%#v", admitted.TxIDs, deferred)
	}
}

func TestStatelessVersionAdmissionAlternatingCrossShardChainProgresses(t *testing.T) {
	s0, s1, key := statelessVersionAdmissionTestRuntimes(t)
	chain := []struct {
		runtime *NodeRuntime
		item    tx.SignedTransaction
	}{
		{s1, statelessVersionAdmissionItem("tx1", "s1", key, 1, 0)},
		{s0, statelessVersionAdmissionItem("tx2", "s0", key, 2, 1)},
		{s1, statelessVersionAdmissionItem("tx3", "s1", key, 3, 2)},
		{s0, statelessVersionAdmissionItem("tx4", "s0", key, 4, 3)},
	}
	for index, step := range chain {
		block := realblock.Block{BlockHash: fmt.Sprintf("alternating-%d", index+1), Height: uint64(index + 1), ShardID: step.runtime.node.ShardID, Timestamp: time.Now().UnixMilli(), TxIDs: []string{step.item.TxID}, TxList: []tx.SignedTransaction{step.item}}
		admitted, deferred, err := step.runtime.admitStatelessVersionCandidate(context.Background(), block)
		if err != nil {
			t.Fatalf("step %d admission error: %v", index+1, err)
		}
		if len(deferred) != 0 || len(admitted.TxList) != 1 {
			t.Fatalf("step %d stalled: admitted=%d deferred=%d", index+1, len(admitted.TxList), len(deferred))
		}
		s0.publishStateVersion(key, uint64(index+1), fmt.Sprintf("v%d", index+1))
	}
	if _, ok := s0.stateVersionValue(key, 4); !ok {
		t.Fatal("final exact version was not published")
	}
}

func TestStateVersionAdmissionProbeReturnsImmediateNotReadyWithoutSubscription(t *testing.T) {
	s0, _, key := statelessVersionAdmissionTestRuntimes(t)
	s0.mu.Lock()
	s0.stateFetchResponseTasks = make(chan stateFetchResponseTask, 1)
	s0.mu.Unlock()
	request := StateFetchRequest{RequestID: "admission-probe", TxID: "tx", BlockHash: "b", Key: key, HomeShard: "s0", ExecutionShard: "s1", AccessKind: statelessVersionAdmissionProbeAccessKind, RequiredVersion: 99, Versioned: true}
	if err := s0.handleStateFetchRequest(context.Background(), "n-s1", request); err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-s0.stateFetchResponseTasks:
		if task.response.Success || task.response.Error != "state_version_not_ready" {
			t.Fatalf("unexpected probe response: %+v", task.response)
		}
	default:
		t.Fatal("admission probe did not receive immediate response")
	}
	s0.mu.Lock()
	defer s0.mu.Unlock()
	if len(s0.stateVersionRemoteSubscriptions) != 0 {
		t.Fatalf("admission probe registered a long-lived subscription: %#v", s0.stateVersionRemoteSubscriptions)
	}
}
