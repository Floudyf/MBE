package v5

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBuiltinRegistryCoversEveryCategory(t *testing.T) {
	r := BuiltinRegistry()
	for _, category := range Categories {
		if _, err := r.Create(category, firstPlugin(category), map[string]any{}); err != nil {
			t.Fatalf("%s: %v", category, err)
		}
	}
}

func TestRegistryRejectsDuplicateAndUnknown(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("routing", "test", func(map[string]any) (Plugin, error) { return basicPlugin{category: "routing", id: "test"}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("routing", "test", func(map[string]any) (Plugin, error) { return nil, nil }); err == nil {
		t.Fatal("expected duplicate rejection")
	}
	if _, err := r.Create("routing", "missing", nil); err == nil {
		t.Fatal("expected unknown rejection")
	}
}

func TestBehaviorPluginsProduceDifferentRealDecisions(t *testing.T) {
	r := BuiltinRegistry()
	hash, err := r.Create("routing", "hash_routing_baseline", nil)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := r.Create("routing", "metatrack_coaccess_routing", nil)
	if err != nil {
		t.Fatal(err)
	}
	input := RoutingInput{Index: 2, StateKeys: []string{"asset:2"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}, ShardIDs: []string{"s0", "s1", "s2", "s3"}}
	hashRoute := hash.(RoutingPlugin).Route(input)
	metaRoute := meta.(RoutingPlugin).Route(input)
	if hashRoute.Reason == metaRoute.Reason || !strings.HasPrefix(metaRoute.Reason, "majority_place") {
		t.Fatalf("routing factories must expose distinct semantics: hash=%#v meta=%#v", hashRoute, metaRoute)
	}
	serial, _ := r.Create("execution", "serial_execution_baseline", nil)
	dual, _ := r.Create("execution", "dual_track_execution", nil)
	item := tx.SignedTransaction{AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	if serial.(ExecutionPlugin).Classify(item).Track == dual.(ExecutionPlugin).Classify(item).Track {
		t.Fatal("execution factories did not change track")
	}
	normal, _ := r.Create("commit", "normal_commit", nil)
	aggregate, _ := r.Create("commit", "commutative_hot_update_aggregation", nil)
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", StateKeys: []string{"coaccess:hot-update"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-2", StateKeys: []string{"coaccess:hot-update"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	stateDelta := []state.StateKV{{Key: "s0::coaccess:hot-update", Value: "2"}}
	normalDecision := normal.(CommitPlugin).DecideCommit(CommitInput{Transactions: transactions, StateDelta: stateDelta})
	aggregateDecision := aggregate.(CommitPlugin).DecideCommit(CommitInput{ShardID: "s0", Height: 1, Transactions: transactions, StateDelta: stateDelta, BaseStateSnapshot: map[string]string{"s0::coaccess:hot-update": "0"}})
	if normalDecision.AggregatedKeyCount != 0 || aggregateDecision.AggregatedKeyCount != 1 || aggregateDecision.AggregatedLogicalDeltaCount != 2 {
		t.Fatal("commit factories did not change aggregation materialization evidence")
	}
}

func TestBuiltinRuntimePluginsCreateRuntimeObjects(t *testing.T) {
	poolPlugin := builtinTxPool{makeBasic("txpool", "fifo_per_node_mempool", map[string]any{"capacity": 3})}
	pool := poolPlugin.CreatePool(TxPoolInput{NodeID: "n0", ShardID: "s0", Policy: mempool.Policy{Capacity: 3}})
	if pool == nil || pool.Capacity() != 3 || pool.Len() != 0 {
		t.Fatalf("txpool plugin did not create a usable mempool: %#v", pool)
	}

	producer := builtinBlockProducer{makeBasic("block_producer", "time_or_count_block_producer", map[string]any{"interval_ms": 25, "block_size": 2})}
	if producer.Interval() != 25*time.Millisecond || producer.BlockSize() != 2 || producer.ShouldProduce(BlockProductionInput{Pool: pool}) {
		t.Fatalf("block producer plugin did not expose runtime thresholds")
	}
	defaultProducer := builtinBlockProducer{makeBasic("block_producer", "time_or_count_block_producer", nil)}
	if defaultProducer.Interval() != 75*time.Millisecond || defaultProducer.BlockSize() != 100 {
		t.Fatalf("block producer fallback defaults drifted: size=%d interval=%s", defaultProducer.BlockSize(), defaultProducer.Interval())
	}
	if _, err := producer.BuildCandidate(BlockProductionInput{Pool: pool}); err == nil {
		t.Fatal("block producer should delegate candidate construction and reject missing proposer")
	}

	consensus := builtinConsensus{makeBasic("consensus", "pbft_style_consensus", map[string]any{"timeout_ms": 75})}
	if consensus.Quorum(4) != 3 || consensus.Timeout() != 75*time.Millisecond || consensus.ProposalPolicy() == "" || consensus.VotePolicy() == "" {
		t.Fatalf("consensus plugin did not expose runtime policy")
	}

	network := builtinNetwork{makeBasic("network", "localhost_tcp_typed_network", nil)}
	transport := network.CreateTransport(NetworkInput{NodeID: "n0", ListenAddr: "127.0.0.1:0", Peers: []p2p.Peer{{NodeID: "n1", ListenAddr: "127.0.0.1:1"}}})
	if transport == nil || transport.NodeID != "n0" || transport.ListenAddr != "127.0.0.1:0" || len(transport.Peers) != 1 {
		t.Fatalf("network plugin did not create transport: %#v", transport)
	}

	storagePlugin := builtinStateStorage{makeBasic("state_storage", "persistent_local_state_store", nil)}
	db, store, err := storagePlugin.Open(StateStorageInput{DataDir: t.TempDir(), NodeID: "n0", ShardID: "s0"})
	if err != nil || db == nil || store == nil {
		t.Fatalf("state storage plugin did not open db/store: db=%#v store=%#v err=%v", db, store, err)
	}
	checkpoint, err := storagePlugin.Checkpoint(db)
	if err != nil {
		t.Fatalf("state storage plugin did not checkpoint: %v", err)
	}
	if err := storagePlugin.ApplyBatch(db, []state.StateKV{{Key: "asset:1", Value: "7"}}); err != nil {
		t.Fatal(err)
	}
	if storagePlugin.Snapshot(db)["s0::asset:1"] != "7" || storagePlugin.Root(db) == "" {
		t.Fatalf("state storage plugin did not apply/snapshot/root through adapter")
	}
	if err := storagePlugin.Rollback(db, checkpoint); err != nil {
		t.Fatalf("state storage plugin did not rollback: %v", err)
	}

	access := builtinStateAccess{makeBasic("state_access", "direct_state_access", nil)}
	fetch := access.BuildFetchRequest(StateFetchInput{RequestID: "req", TxID: "tx", Key: "k", HomeShard: "s0", ExecutionShard: "s1", AccessKind: "read", RequiredVersion: 17, Versioned: true})
	if fetch.RequestID != "req" || fetch.Key != "k" || fetch.HomeShard != "s0" || fetch.ExecutionShard != "s1" || fetch.RequiredVersion != 17 || !fetch.Versioned {
		t.Fatalf("state access plugin did not build fetch request: %#v", fetch)
	}
	apply := access.BuildDeltaApplyRequest(StateDeltaApplyInput{RequestID: "apply", TxID: "tx", TxIDs: []string{"tx"}, Key: "k", Value: "7", HomeShard: "s0", ExecutionShard: "s1", PreviousVersion: 17, ProducedVersion: 23, OrderingNoop: true})
	if apply.RequestID != "apply" || apply.Key != "k" || apply.Value != "7" || apply.PreviousVersion != 17 || apply.ProducedVersion != 23 || !apply.OrderingNoop {
		t.Fatalf("state access plugin did not build delta apply request: %#v", apply)
	}

	cross := builtinCrossShard{makeBasic("cross_shard", "relay_certificate_protocol", nil)}
	relay := cross.BuildRelay(CrossShardRelayInput{Tx: tx.SignedTransaction{TxID: "tx"}, LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"})
	finalize := cross.BuildFinalize(CrossShardFinalizeInput{TxID: "tx", LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"})
	if relay.Tx.TxID != "tx" || relay.TargetShard != "s1" || finalize.TxID != "tx" || finalize.TargetShard != "s1" {
		t.Fatalf("cross-shard plugin did not build protocol messages: relay=%#v finalize=%#v", relay, finalize)
	}
	if cross.SourceLock(CrossShardRelayInput{Tx: tx.SignedTransaction{TxID: "tx"}, LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"}).Stage != "SourceLock" {
		t.Fatal("cross-shard plugin did not expose SourceLock entry")
	}
	if cross.TargetCommit(CrossShardFinalizeInput{TxID: "tx", LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"}).Stage != "TargetCommit" {
		t.Fatal("cross-shard plugin did not expose TargetCommit entry")
	}
	if cross.HandleFinalize(CrossShardFinalizeInput{TxID: "tx", LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"}).Stage != "SourceFinalize" {
		t.Fatal("cross-shard plugin did not expose SourceFinalize entry")
	}
	if cross.TimeoutRefund(CrossShardFinalizeInput{TxID: "tx", LogicalTxID: "logical", SourceShard: "s0", TargetShard: "s1"}, "timeout").Stage != "Refund" {
		t.Fatal("cross-shard plugin did not expose Refund entry")
	}

	metrics := builtinMetrics{makeBasic("metrics", "core_metrics_collector", nil)}
	if got := metrics.Consume(RuntimeEvent{Type: "TxAdmitted"}); len(got) != 1 || got[0].Key != "tx_admitted_count" {
		t.Fatalf("metrics plugin did not consume runtime event: %#v", got)
	}
	observer := builtinObserver{makeBasic("observability", "jsonl_trace_observer", nil)}
	if row := observer.Observe(RuntimeEvent{Type: "TxAdmitted", NodeID: "n0", ShardID: "s0", TxID: "tx", Success: true}); len(row) != 10 || row[1] != "TxAdmitted" {
		t.Fatalf("observability plugin did not emit artifact row: %#v", row)
	}
}

func TestMetaTrackPluginsUseStructuredAccessLists(t *testing.T) {
	r := BuiltinRegistry()
	meta, err := r.Create("routing", "metatrack_coaccess_routing", nil)
	if err != nil {
		t.Fatal(err)
	}
	inputA := RoutingInput{Index: 0, StateKeys: []string{"asset:2"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}, ShardIDs: []string{"s0", "s1", "s2", "s3"}}
	inputB := inputA
	inputB.Index = 99
	routeA := meta.(RoutingPlugin).Route(inputA)
	routeB := meta.(RoutingPlugin).Route(inputB)
	if routeA.ShardID != routeB.ShardID {
		t.Fatalf("metatrack routing must not depend on transaction index: %s != %s", routeA.ShardID, routeB.ShardID)
	}
	if routeA.ShardID == "" || !strings.HasPrefix(routeA.Reason, "majority_place") {
		t.Fatalf("unexpected metatrack routing: %#v", routeA)
	}
	hash, err := r.Create("routing", "hash_routing_baseline", nil)
	if err != nil {
		t.Fatal(err)
	}
	sourceInput := inputA
	sourceInput.SourceShard = "s3"
	if got := hash.(RoutingPlugin).Route(sourceInput); got.ShardID != "s3" || got.Reason != "source_shard_home" {
		t.Fatalf("hash routing should preserve dataset source shard: %#v", got)
	}
	if got := meta.(RoutingPlugin).Route(sourceInput); got.ShardID == "s3" || got.Reason == "source_shard_home" {
		t.Fatalf("metatrack routing should not be blindly overwritten by source shard: %#v", got)
	}

	dual, err := r.Create("execution", "dual_track_execution", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}); got.Track != "fast" {
		t.Fatalf("commutative access should be fast: %#v", got)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{Payload: "v5_cross:s1", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}); got.Track != "conservative" || got.Reason != "legacy_cross_shard_protocol_boundary" {
		t.Fatalf("cross-shard transaction must be conservative: %#v", got)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{SourceKind: "cross_shard_relay", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}); got.Track != "conservative" || got.Reason != "legacy_cross_shard_protocol_boundary" {
		t.Fatalf("relay transaction must be conservative: %#v", got)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{Payload: "v5_safe"}); got.Track != "conservative" {
		t.Fatalf("payload-only transaction should be conservative: %#v", got)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{AccessList: []tx.AccessItem{{Key: "balance:a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}); got.Track != "conservative" {
		t.Fatalf("noncommutative write should be conservative: %#v", got)
	}
}

func TestFastFirstSchedulerOrdersByExecutionTrack(t *testing.T) {
	scheduler := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}
	execution := dualTrackExecution{}
	conservative := tx.SignedTransaction{TxID: "conservative"}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}

	ordered := scheduler.Order([]tx.SignedTransaction{conservative, fast}, execution)

	if ordered[0].TxID != "fast" || ordered[1].TxID != "conservative" {
		t.Fatalf("fast_first_scheduler did not move fast track first: %#v", ordered)
	}
}

func TestDualTrackBatchClassificationKeepsIndependentNonCommutativeWritesConservative(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})}
	first := tx.SignedTransaction{TxID: "first", Sender: "alice", AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:bob", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	second := tx.SignedTransaction{TxID: "second", Sender: "carol", AccessList: []tx.AccessItem{{Key: "balance:carol", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:dave", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:carol", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{first, second}})
	for _, id := range []string{"first", "second"} {
		if result.Decisions[id].Track != "conservative" || !strings.Contains(result.Decisions[id].Reason, "non_commutative_write:") {
			t.Fatalf("paper non-commutative write must remain conservative even when independent: id=%s decision=%#v", id, result.Decisions[id])
		}
	}
}

func TestDualTrackAccessSizeThresholdUsesPaperTau(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})}
	item := tx.SignedTransaction{TxID: "wide", AccessList: []tx.AccessItem{
		{Key: "k1", Mode: tx.AccessRead}, {Key: "k2", Mode: tx.AccessRead}, {Key: "k3", Mode: tx.AccessRead},
		{Key: "k4", Mode: tx.AccessRead}, {Key: "k5", Mode: tx.AccessRead},
	}}
	decision := execution.Classify(item)
	if decision.Track != "conservative" || decision.Reason != "access_size_exceeds_threshold:5>4" {
		t.Fatalf("access list wider than tau must be conservative: %#v", decision)
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{item}})
	if result.Decisions[item.TxID].Track != "conservative" || !strings.Contains(result.Decisions[item.TxID].Reason, "access_size_exceeds_threshold:5>4") {
		t.Fatalf("batch classifier lost tau gate: %#v", result.Decisions[item.TxID])
	}
}

func TestDualTrackBatchClassificationKeepsTopoSafeEligibleDependenciesOnFastTrack(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{"access_size_threshold": 4})}
	items := []tx.SignedTransaction{
		{TxID: "writer", AccessList: []tx.AccessItem{{Key: "counter:shared", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "reader", AccessList: []tx.AccessItem{{Key: "counter:shared", Mode: tx.AccessRead}}},
		{TxID: "missing"},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if result.Decisions["writer"].Track != "fast" {
		t.Fatalf("eligible commutative writer should remain fast: %#v", result.Decisions["writer"])
	}
	if result.Decisions["reader"].Track != "fast" || !strings.Contains(result.Decisions["reader"].Reason, "topo_safe_dependency") || !strings.Contains(result.Decisions["reader"].Reason, "raw_dependency") {
		t.Fatalf("topologically ordered eligible RAW reader should remain fast: %#v", result.Decisions["reader"])
	}
	if result.Decisions["missing"].Track != "conservative" {
		t.Fatalf("missing access list should be conservative: %#v", result.Decisions["missing"])
	}
	if result.ConflictEdgeCount == 0 || result.DeduplicatedEdgeCount == 0 {
		t.Fatalf("dependency edges were not recorded: %#v", result)
	}
}

func TestDualTrackStateReadinessSuspendsWithoutChangingTrackAdmission(t *testing.T) {
	execution := dualTrackExecution{}
	item := tx.SignedTransaction{TxID: "remote-wait", AccessList: []tx.AccessItem{{Key: "asset:remote", Mode: tx.AccessRead}}}
	result := execution.ClassifyBatch(BatchClassificationInput{
		Transactions:         []tx.SignedTransaction{item},
		RemoteStateReadiness: map[string]bool{"k:asset:remote": false},
	})
	if result.Decisions[item.TxID].Track != "fast" {
		t.Fatalf("state readiness must not change channel admission: %#v", result.Decisions[item.TxID])
	}
	if !reflect.DeepEqual(result.StateWaitKeys[item.TxID], []string{"k:asset:remote"}) {
		t.Fatalf("state wait evidence missing: %#v", result.StateWaitKeys)
	}
}

func TestDualTrackBatchClassificationPreservesNonceDependencyAcrossCrossShardBoundary(t *testing.T) {
	execution := dualTrackExecution{}

	transferAccessList := func(sender, receiver string) []tx.AccessItem {
		return []tx.AccessItem{
			{
				Key:             "balance:" + sender,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
			{
				Key:             "balance:" + receiver,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
			{
				Key:             "nonce:" + sender,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
		}
	}

	first := tx.SignedTransaction{
		TxID:       "intra-0",
		Sender:     "alice",
		Receiver:   "bob",
		Nonce:      0,
		Value:      10,
		Payload:    "v5_safe",
		AccessList: transferAccessList("alice", "bob"),
	}

	cross := tx.SignedTransaction{
		TxID:       "cross-1",
		Sender:     "alice",
		Receiver:   "carol",
		Nonce:      1,
		Value:      10,
		Payload:    "v5_cross:s1",
		AccessList: transferAccessList("alice", "carol"),
	}

	result := execution.ClassifyBatch(
		BatchClassificationInput{
			Transactions: []tx.SignedTransaction{
				first,
				cross,
			},
		},
	)

	hasDependency := false
	for _, dependency := range result.Dependencies[cross.TxID] {
		if dependency == first.TxID {
			hasDependency = true
			break
		}
	}

	if !hasDependency {
		t.Fatalf(
			"cross-shard transaction must depend on the previous sender nonce transaction: dependencies=%#v",
			result.Dependencies,
		)
	}

	decision := result.Decisions[cross.TxID]

	if decision.Track != "conservative" {
		t.Fatalf(
			"cross-shard sender-follow-up transaction must remain conservative: %#v",
			decision,
		)
	}

	if !strings.Contains(decision.Reason, "nonce_order_dependency") {
		t.Fatalf(
			"cross-shard transaction lost nonce dependency reason: %#v",
			decision,
		)
	}

	if !strings.Contains(decision.Reason, "legacy_cross_shard_protocol_boundary") {
		t.Fatalf(
			"cross-shard transaction lost remote-boundary reason: %#v",
			decision,
		)
	}
}

func TestDualTrackBatchClassificationChainsFollowingTransactionAfterCrossShardTx(t *testing.T) {
	execution := dualTrackExecution{}

	transferAccessList := func(sender, receiver string) []tx.AccessItem {
		return []tx.AccessItem{
			{
				Key:             "balance:" + sender,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
			{
				Key:             "balance:" + receiver,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
			{
				Key:             "nonce:" + sender,
				Mode:            tx.AccessReadWrite,
				UpdateSemantics: "set",
			},
		}
	}

	first := tx.SignedTransaction{
		TxID:       "intra-0",
		Sender:     "alice",
		Receiver:   "bob",
		Nonce:      0,
		Value:      10,
		Payload:    "v5_safe",
		AccessList: transferAccessList("alice", "bob"),
	}

	cross := tx.SignedTransaction{
		TxID:       "cross-1",
		Sender:     "alice",
		Receiver:   "carol",
		Nonce:      1,
		Value:      10,
		Payload:    "v5_cross:s1",
		AccessList: transferAccessList("alice", "carol"),
	}

	following := tx.SignedTransaction{
		TxID:       "intra-2",
		Sender:     "alice",
		Receiver:   "dave",
		Nonce:      2,
		Value:      10,
		Payload:    "v5_safe",
		AccessList: transferAccessList("alice", "dave"),
	}

	items := []tx.SignedTransaction{
		first,
		cross,
		following,
	}

	result := execution.ClassifyBatch(
		BatchClassificationInput{
			Transactions: items,
		},
	)

	containsDependency := func(txID, expected string) bool {
		for _, dependency := range result.Dependencies[txID] {
			if dependency == expected {
				return true
			}
		}
		return false
	}

	if !containsDependency(cross.TxID, first.TxID) {
		t.Fatalf(
			"cross-shard transaction must depend on its sender predecessor: dependencies=%#v",
			result.Dependencies,
		)
	}

	if !containsDependency(following.TxID, cross.TxID) {
		t.Fatalf(
			"transaction after a cross-shard transaction must depend on that cross-shard predecessor: dependencies=%#v",
			result.Dependencies,
		)
	}

	scheduler := builtinScheduler{
		makeBasic(
			"scheduler",
			"fast_first_scheduler",
			nil,
		),
	}

	schedule := scheduler.Schedule(items, execution)

	position := map[string]int{}
	for index, item := range schedule.Ordered {
		position[item.TxID] = index
	}

	if position[first.TxID] >= position[cross.TxID] {
		t.Fatalf(
			"first nonce transaction must dispatch before cross-shard successor: order=%#v",
			schedule.Ordered,
		)
	}

	if position[cross.TxID] >= position[following.TxID] {
		t.Fatalf(
			"cross-shard nonce transaction must dispatch before following intra-shard transaction: order=%#v",
			schedule.Ordered,
		)
	}
}

func TestFastFirstSchedulerEmitsQueueWaitAndWakeupEvidence(t *testing.T) {
	scheduler := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}
	execution := dualTrackExecution{}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	first := tx.SignedTransaction{TxID: "first", AccessList: []tx.AccessItem{{Key: "nonce:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	second := tx.SignedTransaction{TxID: "second", AccessList: []tx.AccessItem{{Key: "nonce:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}

	schedule := scheduler.Schedule([]tx.SignedTransaction{first, second, fast}, execution)

	if schedule.Ordered[len(schedule.Ordered)-1].TxID != "second" {
		t.Fatalf("dependent work should dispatch after dependency release, got %#v", schedule.Ordered)
	}
	if !sawScheduleEvent(schedule.Events, "fast", "fast_queue", false, false) {
		t.Fatalf("missing fast queue evidence: %#v", schedule.Events)
	}
	if !sawScheduleEvent(schedule.Events, "second", "blocked_waiting", true, false) {
		t.Fatalf("missing dependency wait evidence: %#v", schedule.Events)
	}
	if !sawScheduleEvent(schedule.Events, "second", "conservative_queue", false, true) {
		t.Fatalf("missing dependency wakeup evidence: %#v", schedule.Events)
	}
	if !sawScheduleDepth(schedule.Events, 3, 1, 2) {
		t.Fatalf("missing ready/fast/conservative queue depth evidence: %#v", schedule.Events)
	}
	if !sawScheduleWaitAndIdle(schedule.Events) {
		t.Fatalf("missing dependency wait or idle evidence: %#v", schedule.Events)
	}
}

func TestMetaTrackBlockExecutorWorkerCountOneMeasuresSingleInflightBusinessExecution(t *testing.T) {
	result := runMetaTrackExecutorTestBlock(t, 1, independentTransferTxs(3), nil)
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("worker_count=1 should cap measured business concurrency at 1, got %d", got)
	}
	if got := intMetric(t, result.ActualMetrics, "configured_worker_count"); got != 1 {
		t.Fatalf("configured worker count mismatch: %d", got)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, independentTransferTxs(3))
}

func TestMetaTrackBlockExecutorWorkerCountTwoMeasuresTwoInflightBusinessExecutions(t *testing.T) {
	result := runMetaTrackExecutorTestBlock(t, 2, independentTransferTxs(2), map[string]any{"business_execution_delay_ms": 15})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 2 {
		t.Fatalf("worker_count=2 should reach measured business concurrency 2 for independent txs, got %d", got)
	}
	if got := intMetric(t, result.ActualMetrics, "business_execute_invocation_count"); got != 2 {
		t.Fatalf("business executions should equal tx count without retry, got %d", got)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, independentTransferTxs(2))
}

func TestMetaTrackBlockExecutorWorkerCountFourDoesNotExceedConfiguredWorkers(t *testing.T) {
	result := runMetaTrackExecutorTestBlock(t, 4, independentTransferTxs(4), map[string]any{"business_execution_delay_ms": 15})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got > 4 {
		t.Fatalf("measured business concurrency exceeded configured workers: %d", got)
	}
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got < 2 {
		t.Fatalf("independent txs should exercise real worker concurrency, got %d", got)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, independentTransferTxs(4))
}

func TestMetaTrackBlockExecutorBalancesInitialWorkerAssignmentsAndKeepsWorkStealingAvailable(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlock(t, 2, txs, map[string]any{"business_execution_delay_ms": 20})
	counts, ok := result.ActualMetrics["worker_execution_count"].([]int)
	if !ok || len(counts) != 2 {
		t.Fatalf("missing worker execution counts: %#v", result.ActualMetrics["worker_execution_count"])
	}
	if counts[0] == 0 || counts[1] == 0 {
		t.Fatalf("round-robin assignment should use both workers, counts=%#v", counts)
	}
	if delta := counts[0] - counts[1]; delta > 1 || delta < -1 {
		t.Fatalf("initial worker assignment should remain balanced, counts=%#v", counts)
	}
	if got := intMetric(t, result.ActualMetrics, "steal_attempt_count"); got < 0 {
		t.Fatalf("work-stealing metric must remain available, metrics=%#v", result.ActualMetrics)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackBlockExecutorReleasesDependentTransactionAfterCompletion(t *testing.T) {
	first := tx.SignedTransaction{TxID: "alice-0", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 10, StateKeys: []string{"balance:alice", "balance:bob", "nonce:alice", "nonce:bob"}, AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:bob", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:bob", Mode: tx.AccessRead, UpdateSemantics: "seller_nonce_state"}}}
	second := first
	second.TxID = "alice-1"
	second.Nonce = 1
	result := runMetaTrackExecutorTestBlock(t, 4, []tx.SignedTransaction{first, second}, map[string]any{"business_execution_delay_ms": 10})
	if got := intMetric(t, result.ActualMetrics, "wakeup_event_count"); got == 0 {
		t.Fatalf("dependent tx should be woken after predecessor completion: metrics=%#v", result.ActualMetrics)
	}
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("dependency chain should serialize business execution, got inflight %d", got)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, []tx.SignedTransaction{first, second})
}

func TestMetaTrackBlockExecutorFallsBackFastAccessViolationToConservative(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:       "declared-read-only-transfer",
		Sender:     "alice",
		Receiver:   "bob",
		Nonce:      0,
		Value:      1,
		StateKeys:  []string{"asset:declared"},
		AccessList: []tx.AccessItem{{Key: "asset:declared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
	}
	result := runMetaTrackExecutorTestBlockWithExecution(t, 1, []tx.SignedTransaction{item}, nil, forcedFastExecution{makeBasic("execution", "forced_fast_test_execution", nil)})
	if got := intMetric(t, result.ActualMetrics, "fast_fallback_count"); got != 1 {
		t.Fatalf("expected one fast fallback, metrics=%#v", result.ActualMetrics)
	}
	if got := intMetric(t, result.ActualMetrics, "discarded_tentative_result_count"); got != 1 {
		t.Fatalf("expected discarded tentative result, metrics=%#v", result.ActualMetrics)
	}
	if len(result.BusinessAttempts) != 2 || result.BusinessAttempts[0].FinalCompletion || !result.BusinessAttempts[1].FinalCompletion {
		t.Fatalf("fallback should have one discarded attempt and one final completion: %#v", result.BusinessAttempts)
	}
	sawFallbackEvent := false
	for _, event := range result.ScheduleEvents {
		if strings.HasPrefix(event.DecisionReason, "fast_fallback:") && event.Track == "conservative" {
			sawFallbackEvent = true
			break
		}
	}
	if !sawFallbackEvent {
		t.Fatalf("missing fast fallback schedule event: %#v", result.ScheduleEvents)
	}
	if result.ExecutionResult.SuccessfulTxs != 1 || len(result.ExecutionResult.TxDeltas) != 1 {
		t.Fatalf("fallback transaction should still finalize once: %+v", result.ExecutionResult)
	}
}

type forcedFastExecution struct{ basicPlugin }

func (forcedFastExecution) Classify(item tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "fast", Reason: "forced_fast_test_only"}
}

func (forcedFastExecution) ClassifyBatch(input BatchClassificationInput) BatchClassificationResult {
	decisions := make(map[string]ExecutionDecision, len(input.Transactions))
	for _, item := range input.Transactions {
		decisions[item.TxID] = ExecutionDecision{Track: "fast", Reason: "forced_fast_test_only"}
	}
	return BatchClassificationResult{Decisions: decisions}
}

func runMetaTrackExecutorTestBlock(t *testing.T, workerCount int, txs []tx.SignedTransaction, config map[string]any) BlockExecutionResult {
	t.Helper()
	return runMetaTrackExecutorTestBlockWithExecution(t, workerCount, txs, config, dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)})
}

func runMetaTrackExecutorTestBlockWithExecution(t *testing.T, workerCount int, txs []tx.SignedTransaction, config map[string]any, executionPlugin ExecutionPlugin) BlockExecutionResult {
	t.Helper()
	if config == nil {
		config = map[string]any{}
	}
	config["worker_count"] = workerCount
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	executorPlugin := metaTrackBlockExecutor{makeBasic("block_executor", "metatrack_block_executor", config)}
	result, err := executorPlugin.ExecuteBlock(context.Background(), BlockExecutionInput{Block: block, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: workerCount, Execution: executionPlugin, Scheduler: builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}})
	if err != nil {
		t.Fatal(err)
	}
	expectedAttempts := len(txs) + intMetric(t, result.ActualMetrics, "conservative_reexecution_count")
	if len(result.BusinessAttempts) != expectedAttempts {
		t.Fatalf("unexpected business attempt count: got %d want %d for %d txs", len(result.BusinessAttempts), expectedAttempts, len(txs))
	}
	finalAttempts := 0
	for _, attempt := range result.BusinessAttempts {
		if attempt.FinalCompletion {
			finalAttempts++
		}
		if attempt.Attempt < 1 {
			t.Fatalf("unexpected retry/final attempt evidence: %#v", attempt)
		}
	}
	if finalAttempts != len(txs) {
		t.Fatalf("expected exactly one final attempt per tx, got %d for %d txs: %#v", finalAttempts, len(txs), result.BusinessAttempts)
	}
	return result
}

func independentTransferTxs(count int) []tx.SignedTransaction {
	items := make([]tx.SignedTransaction, 0, count)
	for index := 0; index < count; index++ {
		sender := fmt.Sprintf("sender-%d", index)
		receiver := fmt.Sprintf("receiver-%d", index)
		items = append(items, tx.SignedTransaction{TxID: fmt.Sprintf("tx-%d", index), Sender: sender, Receiver: receiver, Nonce: 0, Value: 10, StateKeys: []string{"balance:" + sender, "balance:" + receiver, "nonce:" + sender, "nonce:" + receiver}, AccessList: []tx.AccessItem{{Key: "balance:" + sender, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:" + receiver, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:" + sender, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:" + receiver, Mode: tx.AccessRead, UpdateSemantics: "seller_nonce_state"}}})
	}
	return items
}

func assertMetaTrackSerialEquivalent(t *testing.T, got execution.Result, txs []tx.SignedTransaction) {
	t.Helper()
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	want := execution.NewSerialExecutor().ExecuteBlock(block, map[string]string{})
	if got.StateRootAfter != want.StateRootAfter {
		t.Fatalf("state root mismatch: got %s want %s", got.StateRootAfter, want.StateRootAfter)
	}
	if got.ReceiptRoot != want.ReceiptRoot {
		t.Fatalf("receipt root mismatch: got %s want %s", got.ReceiptRoot, want.ReceiptRoot)
	}
}

func intMetric(t *testing.T, metrics map[string]any, key string) int {
	t.Helper()
	switch value := metrics[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		t.Fatalf("missing or non-int metric %s: %#v", key, metrics[key])
	}
	return 0
}

func sawScheduleEvent(events []ScheduleEvent, txID, queue string, blocked, wakeup bool) bool {
	for _, event := range events {
		if event.TxID == txID && event.QueueName == queue && event.Blocked == blocked && event.Wakeup == wakeup {
			return true
		}
	}
	return false
}

func sawScheduleDepth(events []ScheduleEvent, ready, fast, conservative int) bool {
	for _, event := range events {
		if event.ReadyQueueDepth == ready && event.FastQueueDepth == fast && event.ConservativeQueueDepth == conservative {
			return true
		}
	}
	return false
}

func sawScheduleWaitAndIdle(events []ScheduleEvent) bool {
	sawWait := false
	sawIdle := false
	for _, event := range events {
		if event.DependencyWaitMS > 0 {
			sawWait = true
		}
		if event.SchedulerIdleMS > 0 {
			sawIdle = true
		}
	}
	return sawWait && sawIdle
}

func TestSyntheticIteratorCarriesStructuredAccessList(t *testing.T) {
	iterator := NewSyntheticIterator(builtinWorkload{}, WorkloadPlan{TxCount: 4, Seed: 7}, 2)
	first, err := iterator.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.AccessList) == 0 {
		t.Fatal("synthetic workload record must carry structured access list")
	}
	var sawCommutative bool
	for i := 1; i < 4; i++ {
		record, err := iterator.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, access := range record.AccessList {
			if access.Mode == tx.AccessCommutativeDelta && access.Key == "coaccess:hot-update" {
				sawCommutative = true
			}
		}
	}
	if !sawCommutative {
		t.Fatal("synthetic hotspot records should declare commutative_delta access")
	}
}

func TestAggregationCommitUsesPrePersistenceStateDelta(t *testing.T) {
	aggregate := aggregationCommit{}
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-3", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	originalDelta := []state.StateKV{{Key: "s0::coaccess:hot-update", Value: "3"}}
	decision := aggregate.DecideCommit(CommitInput{ShardID: "s0", Height: 7, Transactions: transactions, StateDelta: originalDelta, BaseStateSnapshot: map[string]string{"s0::coaccess:hot-update": "0"}})
	if !decision.Applied || decision.LogicalUpdates != 3 || decision.PhysicalUpdates != 1 || len(decision.PhysicalStateDelta) != 1 {
		t.Fatalf("unexpected aggregation decision: %#v", decision)
	}
	if decision.PreAggregationPhysicalOps != 3 || decision.PostAggregationPhysicalOps != 1 || decision.AggregatedKeyCount != 1 || decision.AggregatedLogicalDeltaCount != 3 {
		t.Fatalf("unexpected same-unit aggregation counters: %#v", decision)
	}
	physical := decision.PhysicalStateDelta[0]
	if physical.Key != originalDelta[0].Key || physical.Value != originalDelta[0].Value || physical.UpdateSemantics != "commutative_delta" || physical.Delta != 3 || !reflect.DeepEqual(physical.TxIDs, []string{"tx-1", "tx-2", "tx-3"}) {
		t.Fatalf("aggregation physical delta must carry a single committed update with contributing tx ids: %#v", physical)
	}
	serialDB := state.NewDB(t.TempDir(), "s0")
	serialDB.Set("coaccess:hot-update", "0")
	if err := serialDB.ApplyDeterministicBatch([]state.StateKV{{Key: "s0::coaccess:hot-update", Value: "3"}}); err != nil {
		t.Fatal(err)
	}
	aggregatedDB := state.NewDB(t.TempDir(), "s0")
	aggregatedDB.Set("coaccess:hot-update", "0")
	if err := aggregatedDB.ApplyDeterministicBatch(decision.PhysicalStateDelta); err != nil {
		t.Fatal(err)
	}
	if serialDB.Root() != aggregatedDB.Root() {
		t.Fatalf("aggregated physical delta changed state root: %s != %s", serialDB.Root(), aggregatedDB.Root())
	}
}

func TestAggregationCommitAggregatesMarketSaleCounter(t *testing.T) {
	aggregate := aggregationCommit{}
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "market:0xabc", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "market_sale_counter", Delta: 1}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "market:0xabc", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "market_sale_counter", Delta: 1}}},
	}
	decision := aggregate.DecideCommit(CommitInput{ShardID: "s0", Height: 8, Transactions: transactions, StateDelta: []state.StateKV{
		{Key: "s0::market:0xabc", Value: "1"},
		{Key: "s0::market:0xabc", Value: "2"},
	}, BaseStateSnapshot: map[string]string{"s0::market:0xabc": "0"}})
	if !decision.Applied || decision.PhysicalUpdates != 1 || decision.AggregatedKeyCount != 1 || decision.AggregatedLogicalDeltaCount != 2 {
		t.Fatalf("market sale counter should aggregate into one physical update: %#v", decision)
	}
	physical := decision.PhysicalStateDelta[0]
	if physical.UpdateSemantics != "commutative_delta" || physical.Delta != 2 || !reflect.DeepEqual(physical.TxIDs, []string{"tx-1", "tx-2"}) {
		t.Fatalf("unexpected aggregated market delta: %#v", physical)
	}
}

func TestAggregationCommitFallsBackWhenLowerBoundWouldBeViolated(t *testing.T) {
	aggregate := aggregationCommit{}
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: -4}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: -4}}},
	}
	decision := aggregate.DecideCommit(CommitInput{
		ShardID: "s0", Height: 9, Transactions: transactions,
		StateDelta:        []state.StateKV{{Key: "s0::balance:alice", Value: "-3"}},
		BaseStateSnapshot: map[string]string{"s0::balance:alice": "5"},
	})
	if decision.Applied || decision.ConstraintFallbackCount != 1 || decision.AtomicReservationCount != 0 {
		t.Fatalf("lower-bound violation must keep ordinary commit path: %#v", decision)
	}
	if len(decision.ConstraintFallbackReasons) != 1 || !strings.Contains(decision.ConstraintFallbackReasons[0], "lower_bound_violation") {
		t.Fatalf("missing lower-bound fallback evidence: %#v", decision.ConstraintFallbackReasons)
	}
}

func TestAggregationCommitFallsBackWhenBaseDeltaDoesNotMatchFinalState(t *testing.T) {
	aggregate := aggregationCommit{}
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "counter:sales", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "counter:sales", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	decision := aggregate.DecideCommit(CommitInput{
		ShardID: "s0", Height: 10, Transactions: transactions,
		StateDelta:        []state.StateKV{{Key: "s0::counter:sales", Value: "12"}},
		BaseStateSnapshot: map[string]string{"s0::counter:sales": "5"},
	})
	if decision.Applied || decision.ConstraintFallbackCount != 1 {
		t.Fatalf("mismatched final value must fall back: %#v", decision)
	}
	if len(decision.ConstraintFallbackReasons) != 1 || !strings.Contains(decision.ConstraintFallbackReasons[0], "base_delta_final_mismatch") {
		t.Fatalf("missing base/delta/final mismatch evidence: %#v", decision.ConstraintFallbackReasons)
	}
}

func TestAggregationCommitDoesNotAggregateMixedWriteSemantics(t *testing.T) {
	aggregate := aggregationCommit{}
	transactions := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "tx-3", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	decision := aggregate.DecideCommit(CommitInput{ShardID: "s0", Height: 8, Transactions: transactions, StateDelta: []state.StateKV{{Key: "s0::coaccess:hot-update", Value: "9"}}})
	if decision.Applied || decision.PhysicalStateDelta[0].UpdateSemantics == "commutative_delta" {
		t.Fatalf("mixed commutative/non-commutative writes must not aggregate: %#v", decision)
	}
}

func TestSyntheticSignedAccessListDoesNotPollutePureCommutativeUpdates(t *testing.T) {
	declared := []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}
	accesses := syntheticSignedAccessList("sender", "receiver", declared)
	if len(accesses) != 1 || accesses[0].Mode != tx.AccessCommutativeDelta {
		t.Fatalf("pure commutative synthetic workload should remain pure, got %#v", accesses)
	}
	transfer := syntheticSignedAccessList("sender", "receiver", []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}})
	if len(transfer) <= len(declared) {
		t.Fatalf("ordinary synthetic workload should retain transfer access declaration, got %#v", transfer)
	}
}

func TestRuntimeWorkerCountComesFromBlockExecutorProfile(t *testing.T) {
	profile := map[string]PluginConfig{
		"block_executor": {PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 4}},
	}
	if got := blockExecutorWorkerCountFromProfile(profile); got != 4 {
		t.Fatalf("expected worker_count from block_executor profile, got %d", got)
	}
	if got := blockExecutorWorkerCountFromProfile(map[string]PluginConfig{}); got != 1 {
		t.Fatalf("missing profile should conservatively fall back to one worker, got %d", got)
	}
}

func TestMetaTrackBatchPlanBuildsAccessMatricesAndStablePlacements(t *testing.T) {
	routing := metaTrackRouting{}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "a", StateKeys: []string{"asset:a"}, SourceShard: "s0", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}, {Key: "balance:a", Mode: tx.AccessRead, UpdateSemantics: "validate"}}},
		{Index: 1, LogicalID: "b", StateKeys: []string{"asset:b"}, SourceShard: "s0", AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}, {Key: "balance:b", Mode: tx.AccessRead, UpdateSemantics: "validate"}}},
		{Index: 2, LogicalID: "c", StateKeys: []string{"asset:c"}, SourceShard: "s1", AccessList: []tx.AccessItem{{Key: "coaccess:cold", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
	}
	first := routing.PlanBatch(BatchRoutingInput{BatchIndex: 7, Records: records, ShardIDs: []string{"s0", "s1", "s2", "s3"}})
	second := routing.PlanBatch(BatchRoutingInput{BatchIndex: 7, Records: records, ShardIDs: []string{"s0", "s1", "s2", "s3"}})
	if first.PlanDigest == "" || first.PlanDigest != second.PlanDigest {
		t.Fatalf("metatrack batch plan digest must be stable: %q %q", first.PlanDigest, second.PlanDigest)
	}
	if first.PlacementPolicy != "frequency_coaccess_admissible_v2" || first.TransactionPolicy != "majority_place_queue_tie_v1" || first.PlacementBudget < 1 || first.PlacementCapacity < 1 || first.PlacementTotalFrequency != 5 {
		t.Fatalf("metatrack formal policy/budget evidence missing: %#v", first)
	}
	if len(first.AccessMatrix) != 5 {
		t.Fatalf("unexpected access matrix size: %d", len(first.AccessMatrix))
	}
	if len(first.CoaccessEdges) == 0 {
		t.Fatal("expected coaccess edge evidence")
	}
	hotPlacement := findStatePlacement(t, first, "coaccess:hot-update")
	if hotPlacement.Frequency != 2 {
		t.Fatalf("unexpected hot placement: %#v", hotPlacement)
	}
	if len(first.PlacementScores) == 0 {
		t.Fatal("frequency-coaccess placement must record candidate score evidence")
	}
	if !hasCandidateScoreComponents(first.PlacementScores, "balance:b") {
		t.Fatalf("placement scores must expose real candidate components: %#v", first.PlacementScores)
	}
	if first.TransactionPlacements[0].ExecutionShard == "" || first.TransactionPlacements[0].CoaccessGroup == "" {
		t.Fatalf("missing transaction placement: %#v", first.TransactionPlacements[0])
	}
}

func hasCandidateScoreComponents(scores []PlacementScore, key string) bool {
	for _, score := range scores {
		if score.Key == key && (score.CoaccessLocalityGain != 0 || score.ShardStateLoadPenalty != 0) {
			return true
		}
	}
	return false
}

func TestMetaTrackColdStateUsesDeterministicAdmissibleLeastLoadPlacement(t *testing.T) {
	routing := metaTrackRouting{}
	shards := []string{"s0", "s1", "s2"}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "cold", StateKeys: []string{"asset:cold"}, AccessList: []tx.AccessItem{{Key: "asset:cold", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 12, Records: records, ShardIDs: shards})
	placement := findStatePlacement(t, plan, "asset:cold")
	if placement.ExecutionShard != "s0" || placement.Reason != "frequency_coaccess_seed" || plan.PlacementFallbackCount != 0 {
		t.Fatalf("cold state should use deterministic admissible least-load placement: %#v fallback=%d", placement, plan.PlacementFallbackCount)
	}
	if len(plan.PlacementScores) != len(shards) {
		t.Fatalf("expected one score per candidate shard, got %#v", plan.PlacementScores)
	}
	for _, score := range plan.PlacementScores {
		if !score.Admissible || score.Capacity != plan.PlacementCapacity {
			t.Fatalf("admissibility evidence missing: %#v", score)
		}
	}
}

func TestMetaTrackPlacementScoresCandidateSpecificCoaccessGain(t *testing.T) {
	row := StateFrequencyRow{Key: "state:target", Frequency: 3, ReadCount: 1, WriteCount: 1}
	placed := map[string]StatePlacement{
		"state:left":  {Key: "state:left", ExecutionShard: "s1"},
		"state:right": {Key: "state:right", ExecutionShard: "s2"},
	}
	edges := []CoaccessEdge{
		{LeftKey: "state:target", RightKey: "state:left", Weight: 2},
		{LeftKey: "state:target", RightKey: "state:right", Weight: 5},
	}
	_, scores := chooseAdmissibleStatePlacement(row, []string{"s0", "s1", "s2"}, map[string]int{}, 100, edges, placed)
	gains := map[string]int{}
	for _, score := range scores {
		gains[score.CandidateShard] = score.CoaccessLocalityGain
	}
	if gains["s0"] != 0 || gains["s1"] != 2 || gains["s2"] != 5 {
		t.Fatalf("candidate coaccess gains must use only neighbors placed on that candidate: %#v", gains)
	}
}

func TestMetaTrackSenderGroupRouteRemainsStableAcrossBatchesWithinRoutingEpoch(t *testing.T) {
	routing := &metaTrackRouting{basicPlugin: basicPlugin{config: map[string]any{"routing_epoch": 7, "remote_write_weight": 2}}}
	shards := []string{"s0", "s1"}
	first := routing.PlanBatch(BatchRoutingInput{
		BatchIndex: 1,
		ShardIDs:   shards,
		Records: []WorkloadRecord{{
			Index:     0,
			LogicalID: "first",
			SenderID:  "0xsender",
			AccessList: []tx.AccessItem{
				{Key: keyWithShard(t, "first-hot", "s1", shards), Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
			},
		}},
	})
	if len(first.TransactionPlacements) != 1 {
		t.Fatalf("missing first sender-group placement: %#v", first.TransactionPlacements)
	}
	firstShard := first.TransactionPlacements[0].ExecutionShard
	if first.TransactionPlacements[0].RoutingEpoch != 7 {
		t.Fatalf("routing epoch was not propagated: %#v", first.TransactionPlacements[0])
	}

	second := routing.PlanBatch(BatchRoutingInput{
		BatchIndex: 2,
		ShardIDs:   shards,
		Records: []WorkloadRecord{{
			Index:     1,
			LogicalID: "second",
			SenderID:  "0xsender",
			AccessList: []tx.AccessItem{
				{Key: keyWithShard(t, "second-hot", oppositeShard(firstShard, shards), shards), Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
			},
		}},
	})
	if len(second.TransactionPlacements) != 1 {
		t.Fatalf("missing second sender-group placement: %#v", second.TransactionPlacements)
	}
	if got := second.TransactionPlacements[0].ExecutionShard; got != firstShard {
		t.Fatalf("same sender migrated inside one routing epoch: first=%s second=%s", firstShard, got)
	}
	if second.TransactionPlacements[0].SenderGroupID != "sender:0xsender" {
		t.Fatalf("unexpected sender group identity: %#v", second.TransactionPlacements[0])
	}
}

func keyWithShard(t *testing.T, prefix, desired string, shards []string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("%s:%d", prefix, index)
		if shardFor(nil, []string{key}, shards) == desired {
			return key
		}
	}
	t.Fatalf("could not find key for shard %s", desired)
	return ""
}

func oppositeShard(current string, shards []string) string {
	for _, shard := range shards {
		if shard != current {
			return shard
		}
	}
	return current
}

func TestMetaTrackPaperDefaultDoesNotHideNonceOrSenderStickyRouting(t *testing.T) {
	routing := metaTrackRouting{}
	shards := []string{"s0", "s1", "s2", "s3"}
	nonceKey := "nonce:0xhot"
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "a", SenderID: "0xhot", SourceShard: "s0", AccessList: []tx.AccessItem{{Key: nonceKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{Index: 1, LogicalID: "b", SenderID: "0xhot", SourceShard: "s1", AccessList: []tx.AccessItem{{Key: nonceKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 11, Records: records, ShardIDs: shards})
	placement := findStatePlacement(t, plan, nonceKey)
	if placement.Reason == "nonce_home_constraint" {
		t.Fatalf("paper-default MetaTrack must not hide nonce-home extension: %#v", placement)
	}
	for _, txPlacement := range plan.TransactionPlacements {
		if strings.Contains(txPlacement.Reason, "sender_group") || strings.Contains(txPlacement.Reason, "nonce_home") {
			t.Fatalf("paper-default transaction placement contains hidden sticky extension: %#v", txPlacement)
		}
	}
}

func TestRoutingAndMetaTrackUseInjectedShardingPlugin(t *testing.T) {
	sharding := lastShardSharding{id: "test_last_shard"}
	shards := []string{"s0", "s1", "s2"}
	hash := hashRouting{}
	if got := hash.Route(RoutingInput{StateKeys: []string{"asset:a"}, ShardIDs: shards, Sharding: sharding}); got.ShardID != "s2" {
		t.Fatalf("hash routing did not use injected sharding plugin: %#v", got)
	}

	routing := metaTrackRouting{}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "a", StateKeys: []string{"asset:a"}, AccessList: []tx.AccessItem{{Key: "asset:a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{Index: 1, LogicalID: "b", StateKeys: []string{"nonce:alice"}, AccessList: []tx.AccessItem{{Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
	}
	injected := routing.PlanBatch(BatchRoutingInput{BatchIndex: 2, Records: records, ShardIDs: shards, Sharding: sharding})
	for _, placement := range injected.StatePlacements {
		if placement.HomeShard != "s2" {
			t.Fatalf("metatrack state placement bypassed sharding plugin: %#v", injected.StatePlacements)
		}
	}
	for _, placement := range injected.TransactionPlacements {
		if placement.HomeShard != "s2" {
			t.Fatalf("metatrack transaction placement bypassed sharding plugin: %#v", injected.TransactionPlacements)
		}
	}

	builtin := routing.PlanBatch(BatchRoutingInput{BatchIndex: 2, Records: records, ShardIDs: shards})
	if injected.PlanDigest == builtin.PlanDigest {
		t.Fatalf("routing plan digest must change when sharding ownership changes: %s", injected.PlanDigest)
	}
}

func findStatePlacement(t *testing.T, plan BatchRoutingPlan, key string) StatePlacement {
	t.Helper()
	for _, placement := range plan.StatePlacements {
		if placement.Key == key {
			return placement
		}
	}
	t.Fatalf("missing placement for %s", key)
	return StatePlacement{}
}

type lastShardSharding struct{ id string }

func (p lastShardSharding) ID() string {
	if p.id != "" {
		return p.id
	}
	return "last_shard"
}
func (lastShardSharding) Category() string              { return "sharding" }
func (lastShardSharding) Validate(map[string]any) error { return nil }
func (lastShardSharding) ShardFor(_ []string, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	return shards[len(shards)-1]
}

type testRoutingPlugin struct{}

func (testRoutingPlugin) ID() string                    { return "test_routing" }
func (testRoutingPlugin) Category() string              { return "routing" }
func (testRoutingPlugin) Validate(map[string]any) error { return nil }
func (testRoutingPlugin) Route(input RoutingInput) RoutingDecision {
	return RoutingDecision{ShardID: input.ShardIDs[len(input.ShardIDs)-1], Reason: "test_factory_route"}
}

type testExecutionPlugin struct{}

func (testExecutionPlugin) ID() string                    { return "test_execution" }
func (testExecutionPlugin) Category() string              { return "execution" }
func (testExecutionPlugin) Validate(map[string]any) error { return nil }
func (testExecutionPlugin) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "test_track", Reason: "test_factory_execution"}
}

type testCommitPlugin struct{}

func (testCommitPlugin) ID() string                    { return "test_commit" }
func (testCommitPlugin) Category() string              { return "commit" }
func (testCommitPlugin) Validate(map[string]any) error { return nil }
func (testCommitPlugin) DecideCommit(input CommitInput) CommitDecision {
	return CommitDecision{AggregationGroupID: "test_group", LogicalUpdates: len(input.Transactions), PhysicalUpdates: 1, Applied: true}
}

func TestRegisteredTestPluginFactoriesChangeCategoryBehavior(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("routing", "test_routing", func(map[string]any) (Plugin, error) { return testRoutingPlugin{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("execution", "test_execution", func(map[string]any) (Plugin, error) { return testExecutionPlugin{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("commit", "test_commit", func(map[string]any) (Plugin, error) { return testCommitPlugin{}, nil }); err != nil {
		t.Fatal(err)
	}
	routing, _ := r.Create("routing", "test_routing", nil)
	if got := routing.(RoutingPlugin).Route(RoutingInput{ShardIDs: []string{"s0", "s1"}}); got.ShardID != "s1" || got.Reason != "test_factory_route" {
		t.Fatalf("unexpected routing behavior: %#v", got)
	}
	execution, _ := r.Create("execution", "test_execution", nil)
	if got := execution.(ExecutionPlugin).Classify(tx.SignedTransaction{}); got.Track != "test_track" {
		t.Fatalf("unexpected execution behavior: %#v", got)
	}
	commit, _ := r.Create("commit", "test_commit", nil)
	if got := commit.(CommitPlugin).DecideCommit(CommitInput{Transactions: []tx.SignedTransaction{{}, {}}}); !got.Applied || got.PhysicalUpdates != 1 {
		t.Fatalf("unexpected commit behavior: %#v", got)
	}
}

func firstPlugin(category string) string {
	for _, candidate := range BuiltinRegistry().factories {
		_ = candidate
	}
	defaults := map[string]string{"workload": "deterministic_signed_synthetic", "transaction_admission": "signature_nonce_admission", "txpool": "fifo_per_node_mempool", "sharding": "deterministic_state_key_sharding", "routing": "hash_routing_baseline", "block_producer": "time_or_count_block_producer", "consensus": "pbft_style_consensus", "network": "localhost_tcp_typed_network", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "block_executor": "serial_block_executor", "state_access": "direct_state_access", "state_storage": "persistent_local_state_store", "cross_shard": "relay_certificate_protocol", "commit": "normal_commit", "fault_injection": "faults_disabled", "metrics": "runtime_core_metrics", "observability": "node_network_consensus_observer"}
	return defaults[category]
}

func TestStatelessHashBatchPlanKeepsSourceNonceStreamAndRemoteHomeEvidence(t *testing.T) {
	routing := statelessHashRouting{}
	shards := []string{"s0", "s1", "s2"}
	records := []WorkloadRecord{{
		Index:       7,
		LogicalID:   "stateless-7",
		SourceShard: "s0",
		TargetShard: "s1",
		CrossShard:  true,
		StateKeys:   []string{"asset:a", "account:b"},
		AccessList: []tx.AccessItem{
			{Key: "asset:a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
			{Key: "account:b", Mode: tx.AccessRead, UpdateSemantics: "read"},
		},
	}}
	first := routing.PlanBatch(BatchRoutingInput{BatchIndex: 1, Records: records, ShardIDs: shards})
	second := routing.PlanBatch(BatchRoutingInput{BatchIndex: 1, Records: records, ShardIDs: shards})
	if first.PlanDigest == "" || first.PlanDigest != second.PlanDigest {
		t.Fatalf("stateless hash plan is not deterministic: %q/%q", first.PlanDigest, second.PlanDigest)
	}
	if len(first.TransactionPlacements) != 1 {
		t.Fatalf("missing stateless transaction placement: %#v", first.TransactionPlacements)
	}
	wantExecution := records[0].SourceShard
	placement := first.TransactionPlacements[0]
	if placement.ExecutionShard != wantExecution || placement.Reason != "stateless_source_hash_execution" {
		t.Fatalf("stateless hash split the source nonce stream: got=%#v want=%s", placement, wantExecution)
	}
	if placement.HomeShard != records[0].SourceShard || placement.TargetShard != records[0].TargetShard {
		t.Fatalf("logical source/target evidence was lost: %#v", placement)
	}
	if len(first.StatePlacements) != 2 {
		t.Fatalf("state-home evidence missing: %#v", first.StatePlacements)
	}
	for _, statePlacement := range first.StatePlacements {
		wantHome := shardFor(nil, []string{statePlacement.Key}, shards)
		if statePlacement.HomeShard != wantHome || statePlacement.ExecutionShard != wantHome {
			t.Fatalf("state home mapping changed: %#v want=%s", statePlacement, wantHome)
		}
	}
}

func TestStatelessHashBatchPlanDoesNotSplitOneSourceNonceStream(t *testing.T) {
	routing := statelessHashRouting{}
	shards := []string{"s0", "s1"}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "tx-0", SourceShard: "s1", StateKeys: []string{"asset:a"}},
		{Index: 1, LogicalID: "tx-1", SourceShard: "s1", StateKeys: []string{"asset:b"}},
		{Index: 2, LogicalID: "tx-2", SourceShard: "s1", StateKeys: []string{"asset:c"}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{Records: records, ShardIDs: shards})
	if len(plan.TransactionPlacements) != len(records) {
		t.Fatalf("placement count mismatch: got=%d want=%d", len(plan.TransactionPlacements), len(records))
	}
	for _, placement := range plan.TransactionPlacements {
		if placement.ExecutionShard != "s1" || placement.Reason != "stateless_source_hash_execution" {
			t.Fatalf("one source nonce stream was split: %#v", placement)
		}
	}
}

func TestStatelessHashBatchPlanFallsBackToStateHashWithoutSourceProvenance(t *testing.T) {
	routing := statelessHashRouting{}
	shards := []string{"s0", "s1", "s2"}
	record := WorkloadRecord{Index: 0, LogicalID: "tx", StateKeys: []string{"asset:fallback"}}
	plan := routing.PlanBatch(BatchRoutingInput{Records: []WorkloadRecord{record}, ShardIDs: shards})
	if len(plan.TransactionPlacements) != 1 {
		t.Fatalf("placement count mismatch: %#v", plan.TransactionPlacements)
	}
	want := shardFor(nil, record.StateKeys, shards)
	got := plan.TransactionPlacements[0]
	if got.ExecutionShard != want || got.Reason != "stateless_state_hash_execution" {
		t.Fatalf("state-hash fallback changed: got=%#v want=%s", got, want)
	}
}

func TestMetaTrackScheduleExecutesReadyWorkWhileRemoteStateIsPending(t *testing.T) {
	remote := tx.SignedTransaction{TxID: "remote", AccessList: []tx.AccessItem{{Key: "asset:remote", Mode: tx.AccessRead}}}
	local := tx.SignedTransaction{TxID: "local", AccessList: []tx.AccessItem{{Key: "asset:local", Mode: tx.AccessRead}}}
	block := realblock.Block{Height: 1, ShardID: "s1", TxList: []tx.SignedTransaction{remote, local}}
	classification := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			"remote": {Track: "fast", Reason: "eligible"},
			"local":  {Track: "fast", Reason: "eligible"},
		},
		Dependencies:  map[string][]string{"remote": {}, "local": {}},
		StateWaitKeys: map[string][]string{"remote": {"k:asset:remote"}},
	}
	schedule := ScheduleResult{Ordered: []tx.SignedTransaction{remote, local}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		select {
		case <-ctx.Done():
			return RemoteStateReadyEvent{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: stateReadinessToken(item, access), Value: "7", HomeShard: "s0", LatencyMS: 25}, nil
	}
	events, metrics, outcomes, _, err := executeMetaTrackSchedule(ctx, schedule, classification, block, map[string]string{"s1::asset:local": "3"}, 1, 0, fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected two completions, got %d", len(outcomes))
	}
	localCompletion := -1
	remoteReady := -1
	remoteDispatch := -1
	for index, event := range events {
		if event.TxID == "local" && strings.HasPrefix(event.DecisionReason, "actual_completion") {
			localCompletion = index
		}
		if event.TxID == "remote" && strings.HasPrefix(event.DecisionReason, "actual_state_ready:") {
			remoteReady = index
		}
		if event.TxID == "remote" && event.DecisionReason == "actual_dispatch" {
			remoteDispatch = index
		}
	}
	if localCompletion < 0 || remoteReady < 0 || remoteDispatch < 0 {
		t.Fatalf("missing state-ready scheduler evidence: local_completion=%d remote_ready=%d remote_dispatch=%d events=%#v", localCompletion, remoteReady, remoteDispatch, events)
	}
	if localCompletion >= remoteReady || remoteReady >= remoteDispatch {
		t.Fatalf("ready local work must complete before remote StateReady, then remote dispatch: local=%d ready=%d dispatch=%d", localCompletion, remoteReady, remoteDispatch)
	}
	if metrics["state_ready_scheduler_mode"] != "transaction_level_suspend_resume" || metrics["state_wait_blocked_count"] != 1 || metrics["state_ready_wakeup_count"] != 1 || metrics["remote_state_fetch_count"] != 1 || metrics["remote_state_fetch_completed_count"] != 1 {
		t.Fatalf("unexpected StateReady metrics: %#v", metrics)
	}
}

func TestMetaTrackScheduleKeepsDistinctHistoricalVersionsForSameKey(t *testing.T) {
	key := "world/shared"
	first := tx.SignedTransaction{
		TxID:             "tx-v3",
		AccessListSchema: "alien_worlds_logical_access_v1",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "alien_worlds_contract_semantic_state"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 5, StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: 3, ProducedVersion: 5}}},
	}
	second := tx.SignedTransaction{
		TxID:             "tx-v7",
		AccessListSchema: "alien_worlds_logical_access_v1",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "alien_worlds_contract_semantic_state"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 9, StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: 7, ProducedVersion: 9}}},
	}
	block := realblock.Block{Height: 1, ShardID: "s1", TxList: []tx.SignedTransaction{first, second}}
	classification := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			first.TxID:  {Track: "conservative", Reason: "versioned"},
			second.TxID: {Track: "conservative", Reason: "versioned"},
		},
		Dependencies: map[string][]string{first.TxID: {}, second.TxID: {}},
		StateWaitKeys: map[string][]string{
			first.TxID:  {stateReadinessToken(first, first.AccessList[0])},
			second.TxID: {stateReadinessToken(second, second.AccessList[0])},
		},
	}
	schedule := ScheduleResult{Ordered: []tx.SignedTransaction{first, second}}
	versions := map[uint64]string{3: "historical-v3", 7: "historical-v7"}
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		dependency, ok := stateVersionDependencyForKey(item, access.Key)
		if !ok {
			return RemoteStateReadyEvent{}, fmt.Errorf("missing state version for %s", item.TxID)
		}
		return RemoteStateReadyEvent{
			TxID:           item.TxID,
			Key:            access.Key,
			ReadinessToken: stateReadinessToken(item, access),
			Value:          versions[dependency.RequiredVersion],
			StateVersion:   dependency.RequiredVersion,
			HomeShard:      "s0",
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, metrics, outcomes, _, err := executeMetaTrackSchedule(ctx, schedule, classification, block, map[string]string{"s1::" + key: "latest-must-not-leak"}, 2, 0, fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected two outcomes, got %d", len(outcomes))
	}
	writes := map[string]string{}
	for _, outcome := range outcomes {
		if value, ok := outcome.Delta.WriteSet[key]; ok {
			writes[outcome.TxID] = value
		}
	}
	serial := execution.NewSerialExecutor()
	expectedFirst := map[string]string{"s1::" + key: versions[3]}
	_, firstDelta := serial.ExecuteTransaction(block, first, expectedFirst, 0)
	expectedSecond := map[string]string{"s1::" + key: versions[7]}
	_, secondDelta := serial.ExecuteTransaction(block, second, expectedSecond, 1)
	if writes[first.TxID] != firstDelta.WriteSet[key] {
		t.Fatalf("first transaction did not execute from exact historical v3: got=%s want=%s", writes[first.TxID], firstDelta.WriteSet[key])
	}
	if writes[second.TxID] != secondDelta.WriteSet[key] {
		t.Fatalf("second transaction did not execute from exact historical v7: got=%s want=%s", writes[second.TxID], secondDelta.WriteSet[key])
	}
	if writes[first.TxID] == writes[second.TxID] {
		t.Fatalf("distinct required versions collapsed to one visible value: %#v", writes)
	}
	if metrics["remote_state_fetch_count"] != 2 || metrics["remote_state_fetch_completed_count"] != 2 {
		t.Fatalf("unexpected historical StateReady evidence: %#v", metrics)
	}
}

func TestMetaTrackAdmissiblePlacementBreaksConnectedCoaccessCollapse(t *testing.T) {
	routing := metaTrackRouting{}
	shards := []string{"s0", "s1"}
	records := make([]WorkloadRecord, 0, 31)
	for index := 0; index < 31; index++ {
		records = append(records, WorkloadRecord{
			Index:     index,
			LogicalID: fmt.Sprintf("chain-%02d", index),
			AccessList: []tx.AccessItem{
				{Key: fmt.Sprintf("state:%02d", index), Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
				{Key: fmt.Sprintf("state:%02d", index+1), Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
			},
		})
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 3, Records: records, ShardIDs: shards})
	if plan.PlacementCapacity <= 0 {
		t.Fatalf("missing admissible capacity: %#v", plan)
	}
	usedStates := map[string]bool{}
	for _, placement := range plan.StatePlacements {
		usedStates[placement.ExecutionShard] = true
	}
	if len(usedStates) != 2 {
		t.Fatalf("connected coaccess component collapsed to one state-placement shard: %#v", plan.ShardLoadAfter)
	}
	for shard, load := range plan.ShardLoadAfter {
		if load > plan.PlacementCapacity {
			t.Fatalf("state-placement load exceeded admissible capacity on %s: load=%d cap=%d", shard, load, plan.PlacementCapacity)
		}
	}
	usedTx := map[string]bool{}
	for _, placement := range plan.TransactionPlacements {
		usedTx[placement.ExecutionShard] = true
	}
	if len(usedTx) != 2 {
		t.Fatalf("connected coaccess workload still routed every transaction to one shard: %#v", plan.TransactionPlacements)
	}
	second := routing.PlanBatch(BatchRoutingInput{BatchIndex: 3, Records: records, ShardIDs: shards})
	if plan.PlanDigest != second.PlanDigest {
		t.Fatalf("admissible placement must remain deterministic: %s != %s", plan.PlanDigest, second.PlanDigest)
	}
}
