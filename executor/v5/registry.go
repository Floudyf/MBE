package v5

import (
	"context"
	"fmt"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// Plugin is the common manifest/factory contract. Category interfaces below
// carry the behavior used by the runtime; identifiers stay at registration.
type Plugin interface {
	ID() string
	Category() string
	Validate(map[string]any) error
}

type WorkloadPlugin interface {
	Plugin
	BuildWorkloadItem(WorkloadInput) WorkloadItem
	NewIterator(WorkloadPlan, int, string) (WorkloadIterator, error)
}
type AdmissionPlugin interface {
	Plugin
	Admit(tx.SignedTransaction) error
}
type TxPoolPlugin interface {
	Plugin
	Capacity() int
}
type ShardingPlugin interface {
	Plugin
	ShardFor([]string, []string) string
}
type RoutingPlugin interface {
	Plugin
	Route(RoutingInput) RoutingDecision
}
type BlockProducerPlugin interface {
	Plugin
	BlockSize() int
}
type ConsensusPlugin interface {
	Plugin
	Quorum(int) int
}
type NetworkPlugin interface {
	Plugin
	TransportKind() string
}
type ExecutionPlugin interface {
	Plugin
	Classify(tx.SignedTransaction) ExecutionDecision
}
type SchedulerPlugin interface {
	Plugin
	Order([]tx.SignedTransaction) []tx.SignedTransaction
}
type BlockExecutorPlugin interface {
	Plugin
	ExecuteBlock(context.Context, BlockExecutionInput) (BlockExecutionResult, error)
}
type StateAccessPlugin interface {
	Plugin
	AccessMode() string
}
type StateStoragePlugin interface {
	Plugin
	Durable() bool
}
type CrossShardPlugin interface {
	Plugin
	IsCrossShard(tx.SignedTransaction) bool
}
type CommitPlugin interface {
	Plugin
	DecideCommit(CommitInput) CommitDecision
}
type FaultPlugin interface {
	Plugin
	Enabled() bool
}
type MetricsPlugin interface {
	Plugin
	MetricKeys() []string
}
type ObservabilityPlugin interface {
	Plugin
	Observe(string)
}

type WorkloadInput struct {
	Index, Shards, Seed, TimeoutEvery int
	CrossShard                        bool
}
type BlockExecutionInput struct {
	Block             realblock.Block
	BaseStateSnapshot map[string]string
	NodeID            string
	ShardID           string
	WorkerCount       int
}
type BlockExecutionResult struct {
	ExecutionResult        execution.Result `json:"execution_result"`
	StateDelta             []state.StateKV  `json:"state_delta"`
	PlanDigest             string           `json:"execution_plan_digest"`
	WorkerCount            int              `json:"worker_count"`
	BlockExecutionMS       int64            `json:"block_execution_ms"`
	TransactionExecutionMS int64            `json:"transaction_execution_ms"`
	DeterministicApplyMS   int64            `json:"deterministic_apply_ms"`
}
type WorkloadItem struct {
	Payload   string
	StateKeys []string
}
type WorkloadRecord struct {
	Index            int
	LogicalID        string
	SenderID         string
	ReceiverID       string
	OperationType    string
	RoutingSourceKey string
	RoutingTargetKey string
	Payload          string
	StateKeys        []string
	CrossShard       bool
	SourceShard      string
	TargetShard      string
	SourceEventID    string
	TimestampMS      int64
	Value            int64
}
type WorkloadReplaySummary struct {
	DatasetID                string         `json:"dataset_id,omitempty"`
	VariantID                string         `json:"variant_id,omitempty"`
	TruthLabel               string         `json:"truth_label,omitempty"`
	SourceSHA256             string         `json:"source_sha256,omitempty"`
	MaterializedSHA256       string         `json:"materialized_sha256,omitempty"`
	ExpectedCount            int            `json:"expected_count"`
	ReadCount                int            `json:"read_count"`
	SubmittedCount           int            `json:"submitted_count"`
	RejectedCount            int            `json:"rejected_count"`
	IdentityCount            int            `json:"identity_count"`
	MappingDigest            string         `json:"mapping_digest,omitempty"`
	NonceContinuity          bool           `json:"nonce_continuity"`
	SignaturePassCount       int            `json:"signature_pass_count"`
	ExpectedCrossShardCount  int            `json:"expected_cross_shard_count"`
	ActualCrossShardCount    int            `json:"actual_cross_shard_count"`
	ExpectedCrossShardRatio  float64        `json:"expected_cross_shard_ratio"`
	ActualCrossShardRatio    float64        `json:"actual_cross_shard_ratio"`
	ReplayMode               string         `json:"replay_mode"`
	NoFallback               bool           `json:"no_fallback"`
	ShardLoadDistribution    map[string]int `json:"shard_load_distribution,omitempty"`
	MaxAverageShardLoadRatio float64        `json:"max_average_shard_load_ratio,omitempty"`
	IdentityMappingVersion   string         `json:"identity_mapping_version,omitempty"`
}
type WorkloadIterator interface {
	Next(context.Context) (WorkloadRecord, error)
	Close() error
	Summary() WorkloadReplaySummary
}
type RoutingInput struct {
	Index               int
	StateKeys, ShardIDs []string
	CrossShard          bool
}
type RoutingDecision struct{ ShardID, Reason string }
type ExecutionDecision struct{ Track, Reason string }
type CommitInput struct {
	ShardID      string
	Height       uint64
	Transactions []tx.SignedTransaction
}
type CommitDecision struct {
	AggregationGroupID              string
	LogicalUpdates, PhysicalUpdates int
	Applied                         bool
}

type Factory func(map[string]any) (Plugin, error)
type Registry struct{ factories map[string]Factory }

func NewRegistry() *Registry { return &Registry{factories: map[string]Factory{}} }
func (r *Registry) Register(category, id string, factory Factory) error {
	key := category + ":" + id
	if _, exists := r.factories[key]; exists {
		return fmt.Errorf("duplicate plugin %s", key)
	}
	r.factories[key] = factory
	return nil
}
func (r *Registry) Create(category, id string, config map[string]any) (Plugin, error) {
	factory, ok := r.factories[category+":"+id]
	if !ok {
		return nil, fmt.Errorf("unknown plugin %s:%s", category, id)
	}
	return factory(config)
}

var Categories = []string{"workload", "transaction_admission", "txpool", "sharding", "routing", "block_producer", "consensus", "network", "execution", "scheduler", "block_executor", "state_access", "state_storage", "cross_shard", "commit", "fault_injection", "metrics", "observability"}

type basicPlugin struct {
	category, id string
	config       map[string]any
}

func (p basicPlugin) ID() string                    { return p.id }
func (p basicPlugin) Category() string              { return p.category }
func (p basicPlugin) Validate(map[string]any) error { return nil }

type builtinWorkload struct{ basicPlugin }

func (p builtinWorkload) BuildWorkloadItem(input WorkloadInput) WorkloadItem {
	payload := "v5_safe"
	keys := []string{"shard:account", fmt.Sprintf("asset:%d", input.Index)}
	switch input.Index % 8 {
	case 2, 3:
		payload, keys = "v5_commutative", []string{"shard:account", "coaccess:hot-update"}
	case 4:
		payload, keys = "v5_conflict", []string{"shard:account", "coaccess:conflict"}
	}
	if input.CrossShard && input.Shards > 1 {
		payload = "v5_cross"
	}
	if !input.CrossShard && input.TimeoutEvery > 0 && (input.Index+1)%input.TimeoutEvery == 0 {
		payload = "v5_timeout"
	}
	return WorkloadItem{Payload: payload, StateKeys: keys}
}

func (p builtinWorkload) NewIterator(plan WorkloadPlan, shards int, dataDir string) (WorkloadIterator, error) {
	return NewSyntheticIterator(p, plan, shards), nil
}

type canonicalTraceWorkload struct{ basicPlugin }

func (p canonicalTraceWorkload) BuildWorkloadItem(WorkloadInput) WorkloadItem {
	return WorkloadItem{Payload: "dataset_replay_unavailable", StateKeys: []string{"dataset:invalid"}}
}

func (p canonicalTraceWorkload) NewIterator(plan WorkloadPlan, shards int, dataDir string) (WorkloadIterator, error) {
	return NewCanonicalTraceIterator(plan, shards, dataDir)
}

type builtinAdmission struct{ basicPlugin }

func (p builtinAdmission) Admit(item tx.SignedTransaction) error { return tx.Verify(item) }

type builtinTxPool struct{ basicPlugin }

func (p builtinTxPool) Capacity() int {
	if value, ok := p.config["capacity"].(float64); ok {
		return int(value)
	}
	return 4096
}

type builtinSharding struct{ basicPlugin }

func (p builtinSharding) ShardFor(keys, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	return shards[stableKey(keys)%len(shards)]
}

type hashRouting struct{ basicPlugin }

func (p hashRouting) Route(input RoutingInput) RoutingDecision {
	return RoutingDecision{ShardID: builtinSharding{}.ShardFor(input.StateKeys, input.ShardIDs), Reason: "state_key_hash"}
}

type metaTrackRouting struct{ basicPlugin }

func (p metaTrackRouting) Route(input RoutingInput) RoutingDecision {
	if len(input.ShardIDs) == 0 {
		return RoutingDecision{}
	}
	for _, key := range input.StateKeys {
		if strings.Contains(key, "coaccess:") {
			return RoutingDecision{ShardID: input.ShardIDs[(input.Index*3+1)%len(input.ShardIDs)], Reason: "coaccess_affinity"}
		}
	}
	return RoutingDecision{ShardID: input.ShardIDs[(input.Index*3+1)%len(input.ShardIDs)], Reason: "metatrack_affinity"}
}

type builtinBlockProducer struct{ basicPlugin }

func (p builtinBlockProducer) BlockSize() int {
	switch value := p.config["block_size"].(type) {
	case int:
		return value
	case float64:
		return int(value)
	}
	return 10
}

type builtinConsensus struct{ basicPlugin }

func (p builtinConsensus) Quorum(n int) int { return (2*n)/3 + 1 }

type builtinNetwork struct{ basicPlugin }

func (p builtinNetwork) TransportKind() string { return "localhost_tcp" }

type serialExecution struct{ basicPlugin }

func (p serialExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "serial", Reason: "serial_execution"}
}

type dualTrackExecution struct{ basicPlugin }

func (p dualTrackExecution) Classify(item tx.SignedTransaction) ExecutionDecision {
	if strings.Contains(item.Payload, "v5_safe") || strings.Contains(item.Payload, "v5_commutative") {
		return ExecutionDecision{Track: "fast", Reason: "access_list_safe"}
	}
	return ExecutionDecision{Track: "conservative", Reason: "conflict_or_cross_shard"}
}

type builtinScheduler struct{ basicPlugin }

func (p builtinScheduler) Order(items []tx.SignedTransaction) []tx.SignedTransaction { return items }

type serialBlockExecutor struct{ basicPlugin }

func (p serialBlockExecutor) ExecuteBlock(_ context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workerCount := input.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	executor := execution.NewSerialExecutor()
	result := executor.ExecuteBlock(input.Block, input.BaseStateSnapshot)
	delta := make([]state.StateKV, 0, len(result.StateDelta))
	for _, item := range result.StateDelta {
		delta = append(delta, state.StateKV{Key: item.Key, Value: item.Value})
	}
	return BlockExecutionResult{ExecutionResult: result, StateDelta: delta, PlanDigest: result.PlanDigest, WorkerCount: workerCount}, nil
}

type builtinStateAccess struct{ basicPlugin }

func (p builtinStateAccess) AccessMode() string { return "direct" }

type builtinStateStorage struct{ basicPlugin }

func (p builtinStateStorage) Durable() bool { return true }

type builtinCrossShard struct{ basicPlugin }

func (p builtinCrossShard) IsCrossShard(item tx.SignedTransaction) bool {
	return strings.HasPrefix(item.Payload, "v5_cross:")
}

type normalCommit struct{ basicPlugin }

func (p normalCommit) DecideCommit(input CommitInput) CommitDecision {
	return CommitDecision{LogicalUpdates: len(input.Transactions), PhysicalUpdates: len(input.Transactions)}
}

type aggregationCommit struct{ basicPlugin }

func (p aggregationCommit) DecideCommit(input CommitInput) CommitDecision {
	d := CommitDecision{LogicalUpdates: len(input.Transactions), PhysicalUpdates: len(input.Transactions)}
	commutative := 0
	keys := map[string]bool{}
	for _, item := range input.Transactions {
		keys[strings.Join(item.StateKeys, "|")] = true
		if strings.Contains(item.Payload, "v5_commutative") {
			commutative++
		}
	}
	if commutative >= 2 {
		d.PhysicalUpdates = len(keys)
		d.Applied = true
		d.AggregationGroupID = fmt.Sprintf("%s:%d", input.ShardID, input.Height)
	}
	return d
}

type builtinFault struct{ basicPlugin }

func (p builtinFault) Enabled() bool { return p.id != "faults_disabled" }

type builtinMetrics struct{ basicPlugin }

func (p builtinMetrics) MetricKeys() []string {
	return []string{"finality_ms", "finalized_count", "throughput_tps"}
}

type builtinObserver struct{ basicPlugin }

func (p builtinObserver) Observe(string) {}

func stableKey(keys []string) int {
	sum := 0
	for _, key := range keys {
		for _, ch := range key {
			sum += int(ch)
		}
	}
	return sum
}
func makeBasic(category, id string, config map[string]any) basicPlugin {
	return basicPlugin{category: category, id: id, config: config}
}

func BuiltinRegistry() *Registry {
	r := NewRegistry()
	register := func(category, id string, factory Factory) {
		if err := r.Register(category, id, factory); err != nil {
			panic(err)
		}
	}
	register("workload", "deterministic_signed_synthetic", func(c map[string]any) (Plugin, error) {
		return builtinWorkload{makeBasic("workload", "deterministic_signed_synthetic", c)}, nil
	})
	register("workload", "canonical_trace_replay", func(c map[string]any) (Plugin, error) {
		return canonicalTraceWorkload{makeBasic("workload", "canonical_trace_replay", c)}, nil
	})
	register("transaction_admission", "signature_nonce_admission", func(c map[string]any) (Plugin, error) {
		return builtinAdmission{makeBasic("transaction_admission", "signature_nonce_admission", c)}, nil
	})
	register("txpool", "fifo_per_node_mempool", func(c map[string]any) (Plugin, error) {
		return builtinTxPool{makeBasic("txpool", "fifo_per_node_mempool", c)}, nil
	})
	register("sharding", "deterministic_state_key_sharding", func(c map[string]any) (Plugin, error) {
		return builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", c)}, nil
	})
	register("routing", "hash_routing_baseline", func(c map[string]any) (Plugin, error) {
		return hashRouting{makeBasic("routing", "hash_routing_baseline", c)}, nil
	})
	register("routing", "metatrack_coaccess_routing", func(c map[string]any) (Plugin, error) {
		return metaTrackRouting{makeBasic("routing", "metatrack_coaccess_routing", c)}, nil
	})
	register("block_producer", "time_or_count_block_producer", func(c map[string]any) (Plugin, error) {
		return builtinBlockProducer{makeBasic("block_producer", "time_or_count_block_producer", c)}, nil
	})
	register("consensus", "pbft_style_consensus", func(c map[string]any) (Plugin, error) {
		return builtinConsensus{makeBasic("consensus", "pbft_style_consensus", c)}, nil
	})
	register("network", "localhost_tcp_typed_network", func(c map[string]any) (Plugin, error) {
		return builtinNetwork{makeBasic("network", "localhost_tcp_typed_network", c)}, nil
	})
	register("execution", "serial_execution_baseline", func(c map[string]any) (Plugin, error) {
		return serialExecution{makeBasic("execution", "serial_execution_baseline", c)}, nil
	})
	register("execution", "dual_track_execution", func(c map[string]any) (Plugin, error) {
		return dualTrackExecution{makeBasic("execution", "dual_track_execution", c)}, nil
	})
	register("scheduler", "fifo_serial_scheduler", func(c map[string]any) (Plugin, error) {
		return builtinScheduler{makeBasic("scheduler", "fifo_serial_scheduler", c)}, nil
	})
	register("scheduler", "fast_first_scheduler", func(c map[string]any) (Plugin, error) {
		return builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", c)}, nil
	})
	register("block_executor", "serial_block_executor", func(c map[string]any) (Plugin, error) {
		return serialBlockExecutor{makeBasic("block_executor", "serial_block_executor", c)}, nil
	})
	register("state_access", "direct_state_access", func(c map[string]any) (Plugin, error) {
		return builtinStateAccess{makeBasic("state_access", "direct_state_access", c)}, nil
	})
	register("state_storage", "persistent_local_state_store", func(c map[string]any) (Plugin, error) {
		return builtinStateStorage{makeBasic("state_storage", "persistent_local_state_store", c)}, nil
	})
	register("cross_shard", "relay_certificate_protocol", func(c map[string]any) (Plugin, error) {
		return builtinCrossShard{makeBasic("cross_shard", "relay_certificate_protocol", c)}, nil
	})
	register("commit", "normal_commit", func(c map[string]any) (Plugin, error) {
		return normalCommit{makeBasic("commit", "normal_commit", c)}, nil
	})
	register("commit", "commutative_hot_update_aggregation", func(c map[string]any) (Plugin, error) {
		return aggregationCommit{makeBasic("commit", "commutative_hot_update_aggregation", c)}, nil
	})
	register("fault_injection", "faults_disabled", func(c map[string]any) (Plugin, error) {
		return builtinFault{makeBasic("fault_injection", "faults_disabled", c)}, nil
	})
	register("fault_injection", "network_delay_drop", func(c map[string]any) (Plugin, error) {
		return builtinFault{makeBasic("fault_injection", "network_delay_drop", c)}, nil
	})
	register("metrics", "runtime_core_metrics", func(c map[string]any) (Plugin, error) {
		return builtinMetrics{makeBasic("metrics", "runtime_core_metrics", c)}, nil
	})
	register("observability", "node_network_consensus_observer", func(c map[string]any) (Plugin, error) {
		return builtinObserver{makeBasic("observability", "node_network_consensus_observer", c)}, nil
	})
	return r
}

// RuntimePlugins is dependency injection for a node/client execution path.
// The registry is the only place that resolves a manifest plugin identifier.
type RuntimePlugins struct {
	Workload      WorkloadPlugin
	Admission     AdmissionPlugin
	TxPool        TxPoolPlugin
	Sharding      ShardingPlugin
	Routing       RoutingPlugin
	BlockProducer BlockProducerPlugin
	Consensus     ConsensusPlugin
	Network       NetworkPlugin
	Execution     ExecutionPlugin
	Scheduler     SchedulerPlugin
	BlockExecutor BlockExecutorPlugin
	StateAccess   StateAccessPlugin
	StateStorage  StateStoragePlugin
	CrossShard    CrossShardPlugin
	Commit        CommitPlugin
	Fault         FaultPlugin
	Metrics       MetricsPlugin
	Observability ObservabilityPlugin
}

func InstantiatePlugins(profile map[string]PluginConfig) (RuntimePlugins, error) {
	registry := BuiltinRegistry()
	created := map[string]Plugin{}
	for _, category := range Categories {
		item, ok := profile[category]
		if !ok {
			return RuntimePlugins{}, fmt.Errorf("missing plugin profile for %s", category)
		}
		plugin, err := registry.Create(category, item.PluginID, item.Config)
		if err != nil {
			return RuntimePlugins{}, err
		}
		created[category] = plugin
	}
	p := RuntimePlugins{}
	var ok bool
	if p.Workload, ok = created["workload"].(WorkloadPlugin); !ok {
		return p, fmt.Errorf("workload behavior missing")
	}
	if p.Admission, ok = created["transaction_admission"].(AdmissionPlugin); !ok {
		return p, fmt.Errorf("admission behavior missing")
	}
	if p.TxPool, ok = created["txpool"].(TxPoolPlugin); !ok {
		return p, fmt.Errorf("txpool behavior missing")
	}
	if p.Sharding, ok = created["sharding"].(ShardingPlugin); !ok {
		return p, fmt.Errorf("sharding behavior missing")
	}
	if p.Routing, ok = created["routing"].(RoutingPlugin); !ok {
		return p, fmt.Errorf("routing behavior missing")
	}
	if p.BlockProducer, ok = created["block_producer"].(BlockProducerPlugin); !ok {
		return p, fmt.Errorf("block producer behavior missing")
	}
	if p.Consensus, ok = created["consensus"].(ConsensusPlugin); !ok {
		return p, fmt.Errorf("consensus behavior missing")
	}
	if p.Network, ok = created["network"].(NetworkPlugin); !ok {
		return p, fmt.Errorf("network behavior missing")
	}
	if p.Execution, ok = created["execution"].(ExecutionPlugin); !ok {
		return p, fmt.Errorf("execution behavior missing")
	}
	if p.Scheduler, ok = created["scheduler"].(SchedulerPlugin); !ok {
		return p, fmt.Errorf("scheduler behavior missing")
	}
	if p.BlockExecutor, ok = created["block_executor"].(BlockExecutorPlugin); !ok {
		return p, fmt.Errorf("block executor behavior missing")
	}
	if p.StateAccess, ok = created["state_access"].(StateAccessPlugin); !ok {
		return p, fmt.Errorf("state access behavior missing")
	}
	if p.StateStorage, ok = created["state_storage"].(StateStoragePlugin); !ok {
		return p, fmt.Errorf("state storage behavior missing")
	}
	if p.CrossShard, ok = created["cross_shard"].(CrossShardPlugin); !ok {
		return p, fmt.Errorf("cross shard behavior missing")
	}
	if p.Commit, ok = created["commit"].(CommitPlugin); !ok {
		return p, fmt.Errorf("commit behavior missing")
	}
	if p.Fault, ok = created["fault_injection"].(FaultPlugin); !ok {
		return p, fmt.Errorf("fault behavior missing")
	}
	if p.Metrics, ok = created["metrics"].(MetricsPlugin); !ok {
		return p, fmt.Errorf("metrics behavior missing")
	}
	if p.Observability, ok = created["observability"].(ObservabilityPlugin); !ok {
		return p, fmt.Errorf("observability behavior missing")
	}
	return p, nil
}
