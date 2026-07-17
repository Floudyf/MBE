package v5

import (
	"testing"

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
	input := RoutingInput{Index: 2, StateKeys: []string{"coaccess:hot-update"}, ShardIDs: []string{"s0", "s1", "s2", "s3"}}
	if hash.(RoutingPlugin).Route(input).ShardID == meta.(RoutingPlugin).Route(input).ShardID {
		t.Fatal("routing factories did not change assignment")
	}
	serial, _ := r.Create("execution", "serial_execution_baseline", nil)
	dual, _ := r.Create("execution", "dual_track_execution", nil)
	item := tx.SignedTransaction{Payload: "v5_safe"}
	if serial.(ExecutionPlugin).Classify(item).Track == dual.(ExecutionPlugin).Classify(item).Track {
		t.Fatal("execution factories did not change track")
	}
	normal, _ := r.Create("commit", "normal_commit", nil)
	aggregate, _ := r.Create("commit", "commutative_hot_update_aggregation", nil)
	transactions := []tx.SignedTransaction{{Payload: "v5_commutative", StateKeys: []string{"coaccess:hot-update"}}, {Payload: "v5_commutative", StateKeys: []string{"coaccess:hot-update"}}}
	if normal.(CommitPlugin).DecideCommit(CommitInput{Transactions: transactions}).Applied || !aggregate.(CommitPlugin).DecideCommit(CommitInput{ShardID: "s0", Height: 1, Transactions: transactions}).Applied {
		t.Fatal("commit factories did not change aggregation")
	}
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
