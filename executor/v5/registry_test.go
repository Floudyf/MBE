package v5

import (
	"context"
	"testing"

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
		{StateKeys: []string{"coaccess:hot-update"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{StateKeys: []string{"coaccess:hot-update"}, AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	if normal.(CommitPlugin).DecideCommit(CommitInput{Transactions: transactions}).Applied || !aggregate.(CommitPlugin).DecideCommit(CommitInput{ShardID: "s0", Height: 1, Transactions: transactions}).Applied {
		t.Fatal("commit factories did not change aggregation")
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
	conservative := tx.SignedTransaction{TxID: "conservative", AccessList: []tx.AccessItem{{Key: "state:rw", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}

	ordered := scheduler.Order([]tx.SignedTransaction{conservative, fast}, execution)

	if ordered[0].TxID != "fast" || ordered[1].TxID != "conservative" {
		t.Fatalf("fast_first_scheduler did not move fast track first: %#v", ordered)
	}
}

func TestFastFirstSchedulerEmitsQueueWaitAndWakeupEvidence(t *testing.T) {
	scheduler := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}
	execution := dualTrackExecution{}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	first := tx.SignedTransaction{TxID: "first", AccessList: []tx.AccessItem{{Key: "state:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	second := tx.SignedTransaction{TxID: "second", AccessList: []tx.AccessItem{{Key: "state:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}

	schedule := scheduler.Schedule([]tx.SignedTransaction{first, second, fast}, execution)

	if schedule.Ordered[0].TxID != "fast" {
		t.Fatalf("fast track work should dispatch first, got %#v", schedule.Ordered)
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
		{AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{AccessList: []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
	}
	decision := aggregate.DecideCommit(CommitInput{ShardID: "s0", Height: 7, Transactions: transactions, StateDelta: []state.StateKV{{Key: "s0::coaccess:hot-update", Value: "3"}}})
	if !decision.Applied || decision.LogicalUpdates != 3 || decision.PhysicalUpdates != 1 || len(decision.PhysicalStateDelta) != 1 {
		t.Fatalf("unexpected aggregation decision: %#v", decision)
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
	if hotPlacement.Frequency != 2 || hotPlacement.Reason != "coaccess_frequency_load_balance" {
		t.Fatalf("unexpected hot placement: %#v", hotPlacement)
	}
	if first.TransactionPlacements[0].ExecutionShard == "" || first.TransactionPlacements[0].CoaccessGroup == "" {
		t.Fatalf("missing transaction placement: %#v", first.TransactionPlacements[0])
	}
	if first.TransactionPlacements[0].HomeShard == first.TransactionPlacements[0].ExecutionShard && first.RemoteAccessEstimate == 0 {
		t.Fatal("expected batch plan to report home/execution model and remote estimate when placement diverges")
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
