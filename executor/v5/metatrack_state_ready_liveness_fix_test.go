package v5

import (
	"context"
	"fmt"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackVersionedProbeUsesImmediateNotReadyAccessKind(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	item := tx.SignedTransaction{TxID: "probe", AccessListSchema: "mbe_workload_record_v3", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	item.ExecutionRouting = &tx.ExecutionRoutingMetadata{RoutingOrdinal: 2, ExecutionShard: "s1", StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: 1, ProducedVersion: 2}}}
	seenKind := ""
	runtime.sendToNodeHook = func(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
		request, err := p2p.DecodePayload[StateFetchRequest](envelope)
		if err != nil {
			return err
		}
		seenKind = request.AccessKind
		response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: request.HomeShard + "::" + request.Key, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateVersion: request.RequiredVersion, Versioned: true, Success: false, Error: "state_version_not_ready"}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		runtime.handleStateFetchResponse(response)
		return nil
	}
	block := realblock.Block{BlockHash: "probe-block", Height: 1, ShardID: "s1"}
	_, ready, _, err := runtime.probeStateAccessOnce(context.Background(), block, item, item.AccessList[0], "s0")
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("not-ready exact version was reported ready")
	}
	if seenKind != statelessVersionAdmissionProbeAccessKind {
		t.Fatalf("versioned polling probe used subscription access kind %q", seenKind)
	}
}

func TestMetaTrackVersionedWaveUsesLogicalStateUnitPhysicalHome(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "metatrack_coaccess_routing", Config: map[string]any{"state_storage_unit_count": 17}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	shards := []string{"s0", "s1"}
	key := ""
	for i := 0; i < 10000; i++ {
		candidate := "logical-state-key-" + fmt.Sprint(i)
		if runtime.stateHomeShardForKey(candidate, shards) != runtime.homeShardFor([]string{candidate}, shards) {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("failed to find state-unit/home-shard separation key")
	}
	item := tx.SignedTransaction{TxID: "logical-home", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	item.ExecutionRouting = &tx.ExecutionRoutingMetadata{RoutingOrdinal: 2, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: 1, ProducedVersion: 2}}}
	probes := versionedStateAccesses(item, shards, runtime.stateHomeShardForKey, "s0")
	if len(probes) != 1 {
		t.Fatalf("expected one versioned probe, got %#v", probes)
	}
	if probes[0].homeShard != runtime.stateHomeShardForKey(key, shards) {
		t.Fatalf("probe home %q does not follow logical state-unit serving shard %q", probes[0].homeShard, runtime.stateHomeShardForKey(key, shards))
	}
}

func TestVersionedStateReadyTimeoutTracksNoProgressNotBlockAge(t *testing.T) {
	now := time.Unix(1000, 0)
	if versionedStateReadyNoProgressExceeded(now.Add(-versionedStateReadyNoProgressTimeout+time.Millisecond), now) {
		t.Fatal("watchdog expired before the no-progress interval")
	}
	if !versionedStateReadyNoProgressExceeded(now.Add(-versionedStateReadyNoProgressTimeout), now) {
		t.Fatal("watchdog did not expire at the no-progress interval")
	}
}

func TestMetaTrackRoutingMicroBatchSizeIsIndependentFromBlockSize(t *testing.T) {
	routing := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"micro_batch_size": 100})}
	if got := routing.RoutingBatchSize(1000); got != 100 {
		t.Fatalf("micro batch size=%d, want 100", got)
	}
	inherited := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"micro_batch_size": 0})}
	if got := inherited.RoutingBatchSize(1000); got != 1000 {
		t.Fatalf("compatibility micro batch size=%d, want inherited 1000", got)
	}
}

func TestMetaTrackAccessSizeDiagnosticsDoNotGateExecutionStability(t *testing.T) {
	execution := dualTrackExecution{basicPlugin: makeBasic("execution", "dual_track_execution", nil)}
	makeTx := func(id string, count int) tx.SignedTransaction {
		accesses := make([]tx.AccessItem, 0, count)
		for i := 0; i < count; i++ {
			accesses = append(accesses, tx.AccessItem{Key: id + "-k-" + fmt.Sprint(i), Mode: tx.AccessRead})
		}
		return tx.SignedTransaction{TxID: id, AccessList: accesses}
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{makeTx("a", 11), makeTx("b", 16)}})
	if result.Decisions["a"].Track != "fast" || result.Decisions["b"].Track != "fast" {
		t.Fatalf("stable wide accesses were incorrectly gated by cardinality: %#v", result.Decisions)
	}
	if result.AccessSizeMin != 11 || result.AccessSizeMax != 16 || result.AccessSizeP95 != 16 || result.AccessSizeTotal != 27 {
		t.Fatalf("unexpected access-size diagnostics: %#v", result)
	}
}
