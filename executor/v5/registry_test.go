package v5

import (
	"context"
	"fmt"
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
	if hash.(RoutingPlugin).Route(input).ShardID == meta.(RoutingPlugin).Route(input).ShardID {
		t.Fatal("routing factories did not change assignment")
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
	aggregateDecision := aggregate.(CommitPlugin).DecideCommit(CommitInput{ShardID: "s0", Height: 1, Transactions: transactions, StateDelta: stateDelta})
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
	fetch := access.BuildFetchRequest(StateFetchInput{RequestID: "req", TxID: "tx", Key: "k", HomeShard: "s0", ExecutionShard: "s1", AccessKind: "read"})
	if fetch.RequestID != "req" || fetch.Key != "k" || fetch.HomeShard != "s0" || fetch.ExecutionShard != "s1" {
		t.Fatalf("state access plugin did not build fetch request: %#v", fetch)
	}
	apply := access.BuildDeltaApplyRequest(StateDeltaApplyInput{RequestID: "apply", TxID: "tx", TxIDs: []string{"tx"}, Key: "k", Value: "7", HomeShard: "s0", ExecutionShard: "s1"})
	if apply.RequestID != "apply" || apply.Key != "k" || apply.Value != "7" {
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
	if routeA.ShardID != "s1" || routeA.Reason != "coaccess_affinity:coaccess:hot-update" {
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
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{Payload: "v5_cross:s1", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}); got.Track != "conservative" || got.Reason != "remote_or_cross_shard_boundary" {
		t.Fatalf("cross-shard transaction must be conservative: %#v", got)
	}
	if got := dual.(ExecutionPlugin).Classify(tx.SignedTransaction{SourceKind: "cross_shard_relay", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}); got.Track != "conservative" || got.Reason != "remote_or_cross_shard_boundary" {
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

func TestDualTrackBatchClassificationAllowsIndependentOrdinaryWrites(t *testing.T) {
	execution := dualTrackExecution{}
	first := tx.SignedTransaction{TxID: "first", Sender: "alice", AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:bob", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	second := tx.SignedTransaction{TxID: "second", Sender: "carol", AccessList: []tx.AccessItem{{Key: "balance:carol", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:dave", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:carol", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{first, second}})
	if result.Decisions["first"].Track != "fast" || result.Decisions["second"].Track != "fast" {
		t.Fatalf("independent ordinary writes should be fast at batch level: %#v", result.Decisions)
	}
}

func TestDualTrackBatchClassificationKeepsDependencyRisksConservative(t *testing.T) {
	execution := dualTrackExecution{}
	items := []tx.SignedTransaction{
		{TxID: "writer", AccessList: []tx.AccessItem{{Key: "balance:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "reader", AccessList: []tx.AccessItem{{Key: "balance:shared", Mode: tx.AccessRead}}},
		{TxID: "nonce-first", Sender: "alice", AccessList: []tx.AccessItem{{Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "nonce-second", Sender: "alice", AccessList: []tx.AccessItem{{Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "missing"},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if result.Decisions["reader"].Track != "conservative" || !strings.Contains(result.Decisions["reader"].Reason, "raw_dependency") {
		t.Fatalf("RAW reader should be conservative: %#v", result.Decisions["reader"])
	}
	if result.Decisions["nonce-first"].Track != "fast" {
		t.Fatalf("first sender nonce write can be fast if otherwise independent: %#v", result.Decisions["nonce-first"])
	}
	if result.Decisions["nonce-second"].Track != "conservative" || !strings.Contains(result.Decisions["nonce-second"].Reason, "nonce_order_dependency") {
		t.Fatalf("same sender follow-up nonce write should be conservative: %#v", result.Decisions["nonce-second"])
	}
	if result.Decisions["missing"].Track != "conservative" {
		t.Fatalf("missing access list should be conservative: %#v", result.Decisions["missing"])
	}
	if result.ConflictEdgeCount == 0 || result.DeduplicatedEdgeCount == 0 {
		t.Fatalf("dependency edges were not recorded: %#v", result)
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

	if !strings.Contains(decision.Reason, "remote_or_cross_shard_boundary") {
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
	if !sawScheduleDepth(schedule.Events, 3, 2, 1) {
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

func TestMetaTrackBlockExecutorReleasesDependentTransactionAfterCompletion(t *testing.T) {
	first := tx.SignedTransaction{TxID: "alice-0", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 10, StateKeys: []string{"balance:alice", "balance:bob", "nonce:alice"}, AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:bob", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
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

func runMetaTrackExecutorTestBlock(t *testing.T, workerCount int, txs []tx.SignedTransaction, config map[string]any) BlockExecutionResult {
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
	result, err := executorPlugin.ExecuteBlock(context.Background(), BlockExecutionInput{Block: block, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: workerCount, Execution: dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}, Scheduler: builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.BusinessAttempts) != len(txs) {
		t.Fatalf("expected one business attempt per tx, got %d for %d", len(result.BusinessAttempts), len(txs))
	}
	for _, attempt := range result.BusinessAttempts {
		if attempt.Attempt != 1 || !attempt.FinalCompletion {
			t.Fatalf("unexpected retry/final attempt evidence: %#v", attempt)
		}
	}
	return result
}

func independentTransferTxs(count int) []tx.SignedTransaction {
	items := make([]tx.SignedTransaction, 0, count)
	for index := 0; index < count; index++ {
		sender := fmt.Sprintf("sender-%d", index)
		receiver := fmt.Sprintf("receiver-%d", index)
		items = append(items, tx.SignedTransaction{TxID: fmt.Sprintf("tx-%d", index), Sender: sender, Receiver: receiver, Nonce: 0, Value: 10, StateKeys: []string{"balance:" + sender, "balance:" + receiver, "nonce:" + sender}, AccessList: []tx.AccessItem{{Key: "balance:" + sender, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "balance:" + receiver, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "nonce:" + sender, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}})
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
	decision := aggregate.DecideCommit(CommitInput{ShardID: "s0", Height: 7, Transactions: transactions, StateDelta: []state.StateKV{{Key: "s0::coaccess:hot-update", Value: "3"}}})
	if decision.Applied || decision.LogicalUpdates != 3 || decision.PhysicalUpdates != 1 || len(decision.PhysicalStateDelta) != 1 {
		t.Fatalf("unexpected aggregation decision: %#v", decision)
	}
	if decision.PreAggregationPhysicalOps != 1 || decision.PostAggregationPhysicalOps != 1 || decision.AggregatedKeyCount != 1 || decision.AggregatedLogicalDeltaCount != 3 {
		t.Fatalf("unexpected same-unit aggregation counters: %#v", decision)
	}
	physical := decision.PhysicalStateDelta[0]
	if physical.UpdateSemantics != "commutative_delta" || physical.Delta != 3 || strings.Join(physical.TxIDs, "|") != "tx-1|tx-2|tx-3" {
		t.Fatalf("aggregation did not produce a witnessed physical delta: %#v", physical)
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
		t.Fatal("cost-aware placement must record candidate score evidence")
	}
	if !hasCandidateScoreComponents(first.PlacementScores, "coaccess:hot-update") {
		t.Fatalf("placement scores must expose real candidate components: %#v", first.PlacementScores)
	}
	if first.TransactionPlacements[0].ExecutionShard == "" || first.TransactionPlacements[0].CoaccessGroup == "" {
		t.Fatalf("missing transaction placement: %#v", first.TransactionPlacements[0])
	}
}

func hasCandidateScoreComponents(scores []PlacementScore, key string) bool {
	for _, score := range scores {
		if score.Key == key && (score.CoaccessLocalityGain != 0 || score.PredictedRemoteReadCost != 0 || score.PredictedRemoteWritebackCost != 0 || score.MovementOrWritebackPenalty != 0) {
			return true
		}
	}
	return false
}

func TestMetaTrackPlacementFallsBackWhenPredictedBenefitIsNotPositive(t *testing.T) {
	routing := metaTrackRouting{}
	shards := []string{"s0", "s1", "s2"}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "cold", StateKeys: []string{"asset:cold"}, AccessList: []tx.AccessItem{{Key: "asset:cold", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 12, Records: records, ShardIDs: shards})
	placement := findStatePlacement(t, plan, "asset:cold")
	if placement.ExecutionShard != placement.HomeShard || placement.Reason != "placement_fallback_hash_home" || plan.PlacementFallbackCount != 1 {
		t.Fatalf("cold state should fall back to home placement: %#v fallback=%d", placement, plan.PlacementFallbackCount)
	}
	if len(plan.PlacementScores) != len(shards) {
		t.Fatalf("expected one score per candidate shard, got %#v", plan.PlacementScores)
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
	_, _, scores, _ := chooseStatePlacement(row, "s0", []string{"s0", "s1", "s2"}, map[string]int{}, edges, placed)
	gains := map[string]int{}
	for _, score := range scores {
		gains[score.CandidateShard] = score.CoaccessLocalityGain
	}
	if gains["s0"] != 0 || gains["s1"] != 8 || gains["s2"] != 20 {
		t.Fatalf("candidate coaccess gains must use only neighbors placed on that candidate: %#v", gains)
	}
}

func TestMetaTrackBatchPlanPinsOrderedNonceWritesToHomeShard(t *testing.T) {
	routing := metaTrackRouting{}
	shards := []string{"s0", "s1", "s2", "s3"}
	nonceKey := "nonce:0xhot"
	home := shards[stableKey([]string{nonceKey})%len(shards)]
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "a", SourceShard: "s0", AccessList: []tx.AccessItem{{Key: nonceKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{Index: 1, LogicalID: "b", SourceShard: "s1", AccessList: []tx.AccessItem{{Key: nonceKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 11, Records: records, ShardIDs: shards})
	placement := findStatePlacement(t, plan, nonceKey)
	if placement.HomeShard != home || placement.ExecutionShard != home || placement.Reason != "ordered_nonce_home_shard" {
		t.Fatalf("ordered nonce writes must stay on their stable home shard, got %#v want %s", placement, home)
	}
	for _, txPlacement := range plan.TransactionPlacements {
		if txPlacement.ExecutionShard != home {
			t.Fatalf("transaction with ordered nonce write must follow nonce placement: %#v want %s", txPlacement, home)
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
