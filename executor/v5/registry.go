package v5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/faults"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
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
	NewIterator(WorkloadPlan, int, string, ShardingPlugin) (WorkloadIterator, error)
}
type AdmissionPlugin interface {
	Plugin
	Admit(tx.SignedTransaction) error
}
type TxPoolPlugin interface {
	Plugin
	Capacity() int
	CreatePool(TxPoolInput) *mempool.Mempool
}
type ShardingPlugin interface {
	Plugin
	ShardFor([]string, []string) string
}
type RoutingPlugin interface {
	Plugin
	Route(RoutingInput) RoutingDecision
}
type BatchRoutingPlugin interface {
	RoutingPlugin
	PlanBatch(BatchRoutingInput) BatchRoutingPlan
}
type BlockProducerPlugin interface {
	Plugin
	BlockSize() int
	Interval() time.Duration
	ShouldProduce(BlockProductionInput) bool
	BuildCandidate(BlockProductionInput) (realblock.Block, error)
}
type ConsensusPlugin interface {
	Plugin
	Quorum(int) int
	Timeout() time.Duration
	ProposalPolicy() string
	VotePolicy() string
}
type NetworkPlugin interface {
	Plugin
	TransportKind() string
	CreateTransport(NetworkInput) *p2p.Transport
}
type ExecutionPlugin interface {
	Plugin
	Classify(tx.SignedTransaction) ExecutionDecision
}
type BatchExecutionPlugin interface {
	ExecutionPlugin
	ClassifyBatch(BatchClassificationInput) BatchClassificationResult
}
type SchedulerPlugin interface {
	Plugin
	Order([]tx.SignedTransaction, ExecutionPlugin) []tx.SignedTransaction
	Schedule([]tx.SignedTransaction, ExecutionPlugin) ScheduleResult
}
type BlockExecutorPlugin interface {
	Plugin
	ExecuteBlock(context.Context, BlockExecutionInput) (BlockExecutionResult, error)
}
type StateAccessPlugin interface {
	Plugin
	AccessMode() string
	BuildFetchRequest(StateFetchInput) StateFetchRequest
	BuildDeltaApplyRequest(StateDeltaApplyInput) StateDeltaApplyRequest
}
type StateStoragePlugin interface {
	Plugin
	Durable() bool
	Open(StateStorageInput) (*state.DB, *storage.BlockStore, error)
	Snapshot(*state.DB) map[string]string
	Root(*state.DB) string
	ApplyBatch(*state.DB, []state.StateKV) error
	Checkpoint(*state.DB) (state.FileCheckpoint, error)
	Rollback(*state.DB, state.FileCheckpoint) error
	PersistDelta(*state.DB, state.DeltaMetadata, []state.StateKV, state.PersistenceOptions) (state.PersistenceMetrics, error)
	SnapshotIfDue(*state.DB, uint64, state.PersistenceOptions) (state.PersistenceMetrics, error)
}
type CrossShardPlugin interface {
	Plugin
	IsCrossShard(tx.SignedTransaction) bool
	SourceLock(CrossShardRelayInput) CrossShardEvent
	TargetCommit(CrossShardFinalizeInput) CrossShardEvent
	HandleFinalize(CrossShardFinalizeInput) CrossShardEvent
	TimeoutRefund(CrossShardFinalizeInput, string) CrossShardEvent
	BuildRelay(CrossShardRelayInput) Relay
	BuildFinalize(CrossShardFinalizeInput) Finalize
}
type CommitPlugin interface {
	Plugin
	DecideCommit(CommitInput) CommitDecision
}
type FaultPlugin interface {
	Plugin
	Enabled() bool
	Policy(map[string]any) faults.Policy
}
type MetricsPlugin interface {
	Plugin
	MetricKeys() []string
	Consume(RuntimeEvent) []RuntimeMetric
}
type ObservabilityPlugin interface {
	Plugin
	Observe(RuntimeEvent) []string
}

type TxPoolInput struct {
	NodeID  string
	ShardID string
	Policy  mempool.Policy
	Nonces  *account.NonceManager
}
type NetworkInput struct {
	NodeID     string
	ListenAddr string
	Peers      []p2p.Peer
	Handler    p2p.Handler
}
type StateStorageInput struct {
	DataDir string
	NodeID  string
	ShardID string
}
type BlockProductionInput struct {
	Pool             *mempool.Mempool
	Proposer         *realblock.Proposer
	Limit            int
	Now              time.Time
	SystemDeltaReady bool
}
type RuntimeEvent struct {
	TimestampMS int64
	Type        string
	NodeID      string
	ShardID     string
	TxID        string
	BlockHash   string
	Height      uint64
	Success     bool
	Error       string
	Attributes  map[string]any
}
type RuntimeMetric struct {
	Key   string
	Value any
}
type StateFetchInput struct {
	RequestID      string
	TxID           string
	BlockHash      string
	Key            string
	HomeShard      string
	ExecutionShard string
	AccessKind     string
}
type StateDeltaApplyInput struct {
	RequestID       string
	TxID            string
	TxIDs           []string
	BlockHash       string
	Key             string
	Value           string
	UpdateSemantics string
	Delta           int64
	BaseValue       string
	BaseValueDigest string
	ApplyOrigin     string
	DeltaKind       string
	HasInitialValue bool
	InitialValue    int64
	HomeShard       string
	ExecutionShard  string
	SourceKey       string
	SourceHeight    uint64
}
type CrossShardRelayInput struct {
	Tx          tx.SignedTransaction
	LogicalTxID string
	SourceShard string
	TargetShard string
}
type CrossShardFinalizeInput struct {
	TxID        string
	LogicalTxID string
	SourceShard string
	TargetShard string
}
type CrossShardEvent struct {
	TxID        string
	LogicalTxID string
	SourceShard string
	TargetShard string
	Stage       string
	Success     bool
	Error       string
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
	Execution         ExecutionPlugin
	Scheduler         SchedulerPlugin
}
type BlockExecutionResult struct {
	ExecutionResult        execution.Result `json:"execution_result"`
	StateDelta             []state.StateKV  `json:"state_delta"`
	PlanDigest             string           `json:"execution_plan_digest"`
	WorkerCount            int              `json:"worker_count"`
	BlockExecutionMS       int64            `json:"block_execution_ms"`
	TransactionExecutionMS int64            `json:"transaction_execution_ms"`
	DeterministicApplyMS   int64            `json:"deterministic_apply_ms"`
	PersistenceMetrics     map[string]any   `json:"persistence_metrics,omitempty"`
	ScheduleEvents         []ScheduleEvent  `json:"schedule_events,omitempty"`
	ActualMetrics          map[string]any   `json:"actual_metrics,omitempty"`
	BusinessAttempts       []BusinessExecutionAttempt
}
type BusinessExecutionAttempt struct {
	BlockHeight     uint64
	TxID            string
	Track           string
	Attempt         int
	Reason          string
	Success         bool
	FinalCompletion bool
}
type WorkloadItem struct {
	Payload    string
	StateKeys  []string
	AccessList []tx.AccessItem
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
	AccessList       []tx.AccessItem
	AccessListSchema string
	AccessListSource string
	AccessListDigest string
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
	AccessList          []tx.AccessItem
	SourceShard         string
	CrossShard          bool
	Sharding            ShardingPlugin
}
type RoutingDecision struct{ ShardID, Reason string }
type BatchRoutingInput struct {
	BatchIndex int
	Records    []WorkloadRecord
	ShardIDs   []string
	Sharding   ShardingPlugin
}
type BatchRoutingPlan struct {
	BatchIndex             int
	PlanDigest             string
	ShardingPluginID       string
	PlacementPolicy        string
	TransactionPolicy      string
	PlacementBudget        int
	PlacementMinBudget     int
	PlacementMu            string
	AccessMatrix           []AccessMatrixRow
	StateFrequency         []StateFrequencyRow
	CoaccessEdges          []CoaccessEdge
	StatePlacements        []StatePlacement
	TransactionPlacements  []TransactionPlacement
	ShardLoadBefore        map[string]int
	ShardLoadAfter         map[string]int
	PlacementScores        []PlacementScore
	PlacementFallbackCount int
	RemoteAccessEstimate   int
	RoutingOverhead        int
}
type AccessMatrixRow struct {
	LogicalID string
	TxIndex   int
	Key       string
	Mode      tx.AccessMode
}
type StateFrequencyRow struct {
	Key        string
	Frequency  int
	WriteCount int
	ReadCount  int
}
type CoaccessEdge struct {
	LeftKey  string
	RightKey string
	Weight   int
}
type StatePlacement struct {
	Key            string
	HomeShard      string
	ExecutionShard string
	Frequency      int
	Reason         string
}
type PlacementScore struct {
	Key                          string
	CandidateShard               string
	CoaccessLocalityGain         int
	PredictedRemoteReadCost      int
	PredictedRemoteWritebackCost int
	ShardTxLoadPenalty           int
	ShardStateLoadPenalty        int
	OrderedStatePenalty          int
	MovementOrWritebackPenalty   int
	Score                        int
	Fallback                     bool
}
type TransactionPlacement struct {
	LogicalID         string
	TxIndex           int
	HomeShard         string
	ExecutionShard    string
	TargetShard       string
	CoaccessGroup     string
	Reason            string
	RemoteAccessCount int
}
type ExecutionDecision struct{ Track, Reason string }
type BatchClassificationInput struct {
	Transactions         []tx.SignedTransaction
	RemoteStateReadiness map[string]bool
}
type BatchClassificationResult struct {
	Decisions             map[string]ExecutionDecision
	Dependencies          map[string][]string
	ReasonCodes           map[string][]string
	ConflictEdgeCount     int
	RAWDependencyEdges    int
	DeduplicatedEdgeCount int
	DependencyChainMax    int
	SCCCount              int
}
type ScheduleResult struct {
	Ordered []tx.SignedTransaction
	Events  []ScheduleEvent
}
type ScheduleEvent struct {
	TxID                   string
	Track                  string
	QueueName              string
	DecisionReason         string
	LocalExecution         bool
	StolenWork             bool
	Blocked                bool
	Wakeup                 bool
	ReadyQueueDepth        int
	FastQueueDepth         int
	ConservativeQueueDepth int
	DependencyWaitMS       int64
	SchedulerIdleMS        int64
}
type CommitInput struct {
	ShardID      string
	Height       uint64
	Transactions []tx.SignedTransaction
	TxDeltas     []execution.TxDelta
	StateDelta   []state.StateKV
}
type CommitDecision struct {
	AggregationGroupID              string
	LogicalUpdates, PhysicalUpdates int
	Applied                         bool
	PhysicalStateDelta              []state.StateKV
	PreAggregationPhysicalOps       int
	PostAggregationPhysicalOps      int
	AggregatedKeyCount              int
	AggregatedLogicalDeltaCount     int
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
	accessList := []tx.AccessItem{{Key: fmt.Sprintf("asset:%d", input.Index), Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	switch input.Index % 8 {
	case 2, 3:
		payload, keys = "v5_commutative", []string{"shard:account", "coaccess:hot-update"}
		accessList = []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}
	case 4:
		payload, keys = "v5_conflict", []string{"shard:account", "coaccess:conflict"}
		accessList = []tx.AccessItem{{Key: "coaccess:conflict", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	}
	if input.CrossShard && input.Shards > 1 {
		payload = "v5_cross"
	}
	if !input.CrossShard && input.TimeoutEvery > 0 && (input.Index+1)%input.TimeoutEvery == 0 {
		payload = "v5_timeout"
	}
	return WorkloadItem{Payload: payload, StateKeys: keys, AccessList: accessList}
}

func (p builtinWorkload) NewIterator(plan WorkloadPlan, shards int, dataDir string, sharding ShardingPlugin) (WorkloadIterator, error) {
	return NewSyntheticIterator(p, plan, shards), nil
}

type canonicalTraceWorkload struct{ basicPlugin }

func (p canonicalTraceWorkload) BuildWorkloadItem(WorkloadInput) WorkloadItem {
	return WorkloadItem{Payload: "dataset_replay_unavailable", StateKeys: []string{"dataset:invalid"}}
}

func (p canonicalTraceWorkload) NewIterator(plan WorkloadPlan, shards int, dataDir string, sharding ShardingPlugin) (WorkloadIterator, error) {
	return NewCanonicalTraceIteratorWithSharding(plan, shards, dataDir, sharding)
}

type builtinAdmission struct{ basicPlugin }

func (p builtinAdmission) Admit(item tx.SignedTransaction) error { return tx.Verify(item) }

type builtinTxPool struct{ basicPlugin }

func (p builtinTxPool) Capacity() int {
	if value := intValue(p.config["capacity"]); value > 0 {
		return value
	}
	return 4096
}

func (p builtinTxPool) CreatePool(input TxPoolInput) *mempool.Mempool {
	policy := input.Policy
	policy.Capacity = p.Capacity()
	return mempool.New(input.NodeID, input.ShardID, policy, input.Nonces)
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
	if input.SourceShard != "" {
		return RoutingDecision{ShardID: input.SourceShard, Reason: "source_shard_home"}
	}
	return RoutingDecision{ShardID: shardFor(input.Sharding, input.StateKeys, input.ShardIDs), Reason: "state_key_hash"}
}

type metaTrackRouting struct{ basicPlugin }

func (p metaTrackRouting) Route(input RoutingInput) RoutingDecision {
	if len(input.ShardIDs) == 0 {
		return RoutingDecision{}
	}
	record := WorkloadRecord{Index: input.Index, LogicalID: fmt.Sprintf("tx-%d", input.Index), StateKeys: input.StateKeys, AccessList: input.AccessList, SourceShard: input.SourceShard, CrossShard: input.CrossShard}
	plan := p.PlanBatch(BatchRoutingInput{BatchIndex: input.Index, Records: []WorkloadRecord{record}, ShardIDs: input.ShardIDs, Sharding: input.Sharding})
	if len(plan.TransactionPlacements) > 0 {
		placement := plan.TransactionPlacements[0]
		return RoutingDecision{ShardID: placement.ExecutionShard, Reason: placement.Reason}
	}
	return RoutingDecision{ShardID: shardFor(input.Sharding, input.StateKeys, input.ShardIDs), Reason: "metatrack_access_affinity"}
}

func (p metaTrackRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	plan := BatchRoutingPlan{BatchIndex: input.BatchIndex, ShardingPluginID: shardingPluginID(input.Sharding), PlacementPolicy: "frequency_coaccess_v1", TransactionPolicy: "majority_coverage_v1", PlacementMinBudget: 1, PlacementMu: "1.0", ShardLoadBefore: map[string]int{}, ShardLoadAfter: map[string]int{}}
	if len(input.ShardIDs) == 0 {
		return plan
	}
	for _, shard := range input.ShardIDs {
		plan.ShardLoadBefore[shard] = 0
		plan.ShardLoadAfter[shard] = 0
	}
	frequency := map[string]*StateFrequencyRow{}
	coaccess := map[string]int{}
	for _, record := range input.Records {
		accessItems := normalizedAccessItems(record)
		keys := make([]string, 0, len(accessItems))
		seen := map[string]bool{}
		for _, access := range accessItems {
			if access.Key == "" {
				continue
			}
			plan.AccessMatrix = append(plan.AccessMatrix, AccessMatrixRow{LogicalID: firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)), TxIndex: record.Index, Key: access.Key, Mode: access.Mode})
			row := frequency[access.Key]
			if row == nil {
				row = &StateFrequencyRow{Key: access.Key}
				frequency[access.Key] = row
			}
			row.Frequency++
			if isReadMode(access.Mode) {
				row.ReadCount++
			}
			if isWriteMode(access.Mode) {
				row.WriteCount++
			}
			if !seen[access.Key] {
				keys = append(keys, access.Key)
				seen[access.Key] = true
			}
		}
		sort.Strings(keys)
		for left := 0; left < len(keys); left++ {
			for right := left + 1; right < len(keys); right++ {
				coaccess[keyPair(keys[left], keys[right])]++
			}
		}
	}
	for _, row := range frequency {
		plan.StateFrequency = append(plan.StateFrequency, *row)
	}
	sort.Slice(plan.StateFrequency, func(i, j int) bool {
		if plan.StateFrequency[i].Frequency != plan.StateFrequency[j].Frequency {
			return plan.StateFrequency[i].Frequency > plan.StateFrequency[j].Frequency
		}
		if plan.StateFrequency[i].WriteCount != plan.StateFrequency[j].WriteCount {
			return plan.StateFrequency[i].WriteCount > plan.StateFrequency[j].WriteCount
		}
		return plan.StateFrequency[i].Key < plan.StateFrequency[j].Key
	})
	for pair, weight := range coaccess {
		left, right, _ := strings.Cut(pair, "\x00")
		plan.CoaccessEdges = append(plan.CoaccessEdges, CoaccessEdge{LeftKey: left, RightKey: right, Weight: weight})
	}
	sort.Slice(plan.CoaccessEdges, func(i, j int) bool {
		if plan.CoaccessEdges[i].LeftKey != plan.CoaccessEdges[j].LeftKey {
			return plan.CoaccessEdges[i].LeftKey < plan.CoaccessEdges[j].LeftKey
		}
		return plan.CoaccessEdges[i].RightKey < plan.CoaccessEdges[j].RightKey
	})

	placementByKey := map[string]StatePlacement{}
	plan.PlacementBudget = maxInt(plan.PlacementMinBudget, len(plan.StateFrequency)/len(input.ShardIDs))
	remotePlacements := 0
	for _, row := range plan.StateFrequency {
		home := shardFor(input.Sharding, []string{row.Key}, input.ShardIDs)
		executionShard, reason, scores, fallback := chooseStatePlacement(row, home, input.ShardIDs, plan.ShardLoadAfter, plan.CoaccessEdges, placementByKey)
		if executionShard != home {
			if remotePlacements >= plan.PlacementBudget {
				executionShard = home
				reason = "placement_budget_home_fallback"
				fallback = true
			} else {
				remotePlacements++
			}
		}
		plan.PlacementScores = append(plan.PlacementScores, scores...)
		if fallback {
			plan.PlacementFallbackCount++
		}
		plan.ShardLoadAfter[executionShard] += row.Frequency
		placement := StatePlacement{Key: row.Key, HomeShard: home, ExecutionShard: executionShard, Frequency: row.Frequency, Reason: reason}
		placementByKey[row.Key] = placement
		plan.StatePlacements = append(plan.StatePlacements, placement)
	}
	sort.Slice(plan.StatePlacements, func(i, j int) bool { return plan.StatePlacements[i].Key < plan.StatePlacements[j].Key })
	sort.Slice(plan.PlacementScores, func(i, j int) bool {
		if plan.PlacementScores[i].Key != plan.PlacementScores[j].Key {
			return plan.PlacementScores[i].Key < plan.PlacementScores[j].Key
		}
		return plan.PlacementScores[i].CandidateShard < plan.PlacementScores[j].CandidateShard
	})

	transactionLoad := map[string]int{}
	for _, record := range input.Records {
		accessItems := normalizedAccessItems(record)
		homeShard := firstNonEmpty(record.SourceShard, shardFor(input.Sharding, record.StateKeys, input.ShardIDs))
		targetShard := record.TargetShard
		if targetShard == "" && record.CrossShard && len(input.ShardIDs) > 1 {
			targetShard = nextShardAfter(homeShard, input.ShardIDs)
		}
		executionShard, group, remote := transactionExecutionShard(input.ShardIDs, placementByKey, accessItems, homeShard, transactionLoad)
		transactionLoad[executionShard]++
		if remote > 0 {
			plan.RemoteAccessEstimate += remote
		}
		reason := "metatrack_batch_affinity"
		if remote == 0 {
			reason = "metatrack_local_affinity"
		}
		if strings.Contains(group, "coaccess:") {
			reason = "coaccess_affinity:" + group
		}
		plan.TransactionPlacements = append(plan.TransactionPlacements, TransactionPlacement{LogicalID: firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)), TxIndex: record.Index, HomeShard: homeShard, ExecutionShard: executionShard, TargetShard: targetShard, CoaccessGroup: group, Reason: reason, RemoteAccessCount: remote})
	}
	sort.Slice(plan.TransactionPlacements, func(i, j int) bool {
		return plan.TransactionPlacements[i].TxIndex < plan.TransactionPlacements[j].TxIndex
	})
	plan.RoutingOverhead = plan.RemoteAccessEstimate + len(plan.CoaccessEdges)
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}

func normalizedAccessItems(record WorkloadRecord) []tx.AccessItem {
	if len(record.AccessList) > 0 {
		items := append([]tx.AccessItem(nil), record.AccessList...)
		sort.Slice(items, func(i, j int) bool {
			if items[i].Key != items[j].Key {
				return items[i].Key < items[j].Key
			}
			return items[i].Mode < items[j].Mode
		})
		return items
	}
	items := make([]tx.AccessItem, 0, len(record.StateKeys))
	for _, key := range record.StateKeys {
		items = append(items, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "legacy_state_key"})
	}
	return items
}

func transactionExecutionShard(shardIDs []string, placementByKey map[string]StatePlacement, accesses []tx.AccessItem, homeShard string, currentLoad map[string]int) (string, string, int) {
	if len(shardIDs) == 0 {
		return "", "", 0
	}
	score := map[string]int{}
	groupKeys := []string{}
	remote := 0
	orderedShard := ""
	for _, access := range accesses {
		placement, ok := placementByKey[access.Key]
		if !ok {
			continue
		}
		score[placement.ExecutionShard]++
		groupKeys = append(groupKeys, access.Key)
		if placement.HomeShard != placement.ExecutionShard {
			remote++
		}
		if isOrderedNonceAccess(access) {
			orderedShard = firstNonEmpty(orderedShard, placement.ExecutionShard)
		}
	}
	sort.Strings(groupKeys)
	if orderedShard != "" {
		return orderedShard, strings.Join(groupKeys, "+"), remote
	}
	best := firstNonEmpty(homeShard, shardIDs[0])
	bestScore := -1
	bestRemote := 1 << 30
	for _, shard := range shardIDs {
		candidateScore := score[shard]
		candidateRemote := len(accesses) - candidateScore
		if candidateScore > bestScore || (candidateScore == bestScore && candidateRemote < bestRemote) || (candidateScore == bestScore && candidateRemote == bestRemote && currentLoad[shard] < currentLoad[best]) || (candidateScore == bestScore && candidateRemote == bestRemote && currentLoad[shard] == currentLoad[best] && shard < best) {
			best = shard
			bestScore = candidateScore
			bestRemote = candidateRemote
		}
	}
	return best, strings.Join(groupKeys, "+"), remote
}

func isOrderedNonceWrite(row StateFrequencyRow) bool {
	return strings.HasPrefix(row.Key, "nonce:") && row.WriteCount > 0
}

func chooseStatePlacement(row StateFrequencyRow, home string, shards []string, load map[string]int, edges []CoaccessEdge, placed map[string]StatePlacement) (string, string, []PlacementScore, bool) {
	if len(shards) == 0 {
		return "", "no_shards", nil, true
	}
	bestShard := home
	bestAffinity := -1
	scores := make([]PlacementScore, 0, len(shards))
	for _, shard := range shards {
		score := PlacementScore{Key: row.Key, CandidateShard: shard}
		score.CoaccessLocalityGain = coaccessLocalityGainForCandidate(row.Key, shard, edges, placed)
		score.ShardStateLoadPenalty = load[shard]
		score.Score = score.CoaccessLocalityGain
		if score.CoaccessLocalityGain > bestAffinity || (score.CoaccessLocalityGain == bestAffinity && load[shard] < load[bestShard]) || (score.CoaccessLocalityGain == bestAffinity && load[shard] == load[bestShard] && shard < bestShard) {
			bestShard = shard
			bestAffinity = score.CoaccessLocalityGain
		}
		scores = append(scores, score)
	}
	if bestShard == "" {
		bestShard = shards[0]
	}
	if isOrderedNonceWrite(row) {
		for index := range scores {
			scores[index].Fallback = scores[index].CandidateShard == home
		}
		return home, "nonce_home_constraint", scores, false
	}
	if bestAffinity > 0 {
		return bestShard, "frequency_coaccess_affinity", scores, false
	}
	for index := range scores {
		if scores[index].CandidateShard == home {
			scores[index].Fallback = true
		}
	}
	return home, "frequency_coaccess_home_no_affinity", scores, true
}

func coaccessLocalityGainForCandidate(key, candidate string, edges []CoaccessEdge, placed map[string]StatePlacement) int {
	gain := 0
	for _, edge := range edges {
		neighbor := ""
		switch key {
		case edge.LeftKey:
			neighbor = edge.RightKey
		case edge.RightKey:
			neighbor = edge.LeftKey
		}
		if neighbor == "" {
			continue
		}
		if placement, ok := placed[neighbor]; ok && placement.ExecutionShard == candidate {
			gain += edge.Weight
		}
	}
	return gain
}

func isOrderedNonceAccess(access tx.AccessItem) bool {
	return strings.HasPrefix(access.Key, "nonce:") && isWriteMode(access.Mode)
}

func isWriteMode(mode tx.AccessMode) bool {
	return mode == tx.AccessWrite || mode == tx.AccessReadWrite || mode == tx.AccessCommutativeDelta
}

func isReadMode(mode tx.AccessMode) bool {
	return mode == tx.AccessRead || mode == tx.AccessReadWrite || mode == tx.AccessCommutativeDelta
}

func keyPair(left, right string) string {
	if right < left {
		left, right = right, left
	}
	return left + "\x00" + right
}

func uniqueStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := []string{items[0]}
	for _, item := range items[1:] {
		if item != out[len(out)-1] {
			out = append(out, item)
		}
	}
	return out
}

func dependencyChainMax(graph map[string][]string) int {
	visited := map[string]int{}
	visiting := map[string]bool{}
	var visit func(string) int
	visit = func(node string) int {
		if value := visited[node]; value > 0 {
			return value
		}
		if visiting[node] {
			return 0
		}
		visiting[node] = true
		best := 0
		for _, next := range graph[node] {
			if depth := visit(next); depth > best {
				best = depth
			}
		}
		visiting[node] = false
		visited[node] = best + 1
		return visited[node]
	}
	maxDepth := 0
	for node := range graph {
		if depth := visit(node); depth > maxDepth {
			maxDepth = depth
		}
	}
	if maxDepth > 0 {
		return maxDepth - 1
	}
	return 0
}

func stronglyConnectedComponentCount(graph map[string][]string) int {
	index := 0
	stack := []string{}
	onStack := map[string]bool{}
	indexes := map[string]int{}
	lowlink := map[string]int{}
	count := 0
	var connect func(string)
	connect = func(node string) {
		index++
		indexes[node] = index
		lowlink[node] = index
		stack = append(stack, node)
		onStack[node] = true
		for _, next := range graph[node] {
			if indexes[next] == 0 {
				connect(next)
				if lowlink[next] < lowlink[node] {
					lowlink[node] = lowlink[next]
				}
			} else if onStack[next] && indexes[next] < lowlink[node] {
				lowlink[node] = indexes[next]
			}
		}
		if lowlink[node] == indexes[node] {
			size := 0
			for {
				last := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[last] = false
				size++
				if last == node {
					break
				}
			}
			if size > 1 {
				count++
			}
		}
	}
	nodes := map[string]bool{}
	for node, nexts := range graph {
		nodes[node] = true
		for _, next := range nexts {
			nodes[next] = true
		}
	}
	for node := range nodes {
		if indexes[node] == 0 {
			connect(node)
		}
	}
	return count
}

func leastLoadedShard(shards []string, load map[string]int, tieBreaker string) string {
	best := shards[stableKey([]string{tieBreaker})%len(shards)]
	for _, shard := range shards {
		if load[shard] < load[best] || (load[shard] == load[best] && shard < best) {
			best = shard
		}
	}
	return best
}

func shardFor(sharding ShardingPlugin, keys, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	if sharding != nil {
		if shard := sharding.ShardFor(keys, shards); shard != "" {
			return shard
		}
	}
	return shards[stableKey(keys)%len(shards)]
}

func shardingPluginID(sharding ShardingPlugin) string {
	if sharding == nil || sharding.ID() == "" {
		return "deterministic_state_key_sharding"
	}
	return sharding.ID()
}

func nextShardAfter(current string, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	for index, shard := range shards {
		if shard == current {
			return shards[(index+1)%len(shards)]
		}
	}
	return shards[0]
}

func routingPlanDigest(plan BatchRoutingPlan) string {
	copyPlan := plan
	copyPlan.PlanDigest = ""
	payload, _ := json.Marshal(copyPlan)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

type builtinBlockProducer struct{ basicPlugin }

func (p builtinBlockProducer) BlockSize() int {
	switch value := p.config["block_size"].(type) {
	case int:
		return value
	case float64:
		return int(value)
	}
	return 100
}

func (p builtinBlockProducer) Interval() time.Duration {
	if value := intValue(p.config["interval_ms"]); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 75 * time.Millisecond
}

func (p builtinBlockProducer) ShouldProduce(input BlockProductionInput) bool {
	return (input.Pool != nil && input.Pool.Len() > 0) || input.SystemDeltaReady
}

func (p builtinBlockProducer) BuildCandidate(input BlockProductionInput) (realblock.Block, error) {
	if input.Proposer == nil || input.Pool == nil {
		return realblock.Block{}, fmt.Errorf("block producer requires proposer and pool")
	}
	limit := input.Limit
	if limit <= 0 {
		limit = p.BlockSize()
	}
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	return input.Proposer.Build(input.Pool, limit, now)
}

type builtinConsensus struct{ basicPlugin }

func (p builtinConsensus) Quorum(n int) int { return (2*n)/3 + 1 }
func (p builtinConsensus) Timeout() time.Duration {
	if value := intValue(p.config["timeout_ms"]); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 2 * time.Second
}
func (p builtinConsensus) ProposalPolicy() string { return "leader_preprepare" }
func (p builtinConsensus) VotePolicy() string     { return "prepare_quorum_commit" }

type builtinNetwork struct{ basicPlugin }

func (p builtinNetwork) TransportKind() string { return "localhost_tcp" }

func (p builtinNetwork) CreateTransport(input NetworkInput) *p2p.Transport {
	return p2p.NewTransport(input.NodeID, input.ListenAddr, input.Peers, input.Handler)
}

type serialExecution struct{ basicPlugin }

func (p serialExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "serial", Reason: "serial_execution"}
}

type dualTrackExecution struct{ basicPlugin }

func (p dualTrackExecution) Classify(item tx.SignedTransaction) ExecutionDecision {
	if len(item.AccessList) == 0 {
		return ExecutionDecision{Track: "conservative", Reason: "missing_structured_access_list"}
	}
	if hasRemoteExecutionBoundary(item) {
		return ExecutionDecision{Track: "conservative", Reason: "remote_or_cross_shard_boundary"}
	}
	commutative := false
	for _, access := range item.AccessList {
		switch access.Mode {
		case tx.AccessRead:
			continue
		case tx.AccessCommutativeDelta:
			commutative = true
		default:
			return ExecutionDecision{Track: "conservative", Reason: "non_commutative_write:" + access.Key}
		}
	}
	if commutative {
		return ExecutionDecision{Track: "fast", Reason: "commutative_delta_access"}
	}
	return ExecutionDecision{Track: "fast", Reason: "read_only_access"}
}

func (p dualTrackExecution) ClassifyBatch(input BatchClassificationInput) BatchClassificationResult {
	result := BatchClassificationResult{
		Decisions:    map[string]ExecutionDecision{},
		Dependencies: map[string][]string{},
		ReasonCodes:  map[string][]string{},
	}

	lastWriter := map[string]string{}
	readers := map[string][]string{}
	lastSenderTx := map[string]string{}
	edges := map[string]bool{}
	graph := map[string][]string{}

	addDependency := func(from, to, key, kind string) {
		if from == "" || to == "" || from == to {
			return
		}

		edgeKey := from + "->" + to + ":" + key + ":" + kind
		if edges[edgeKey] {
			return
		}

		edges[edgeKey] = true
		result.ConflictEdgeCount++
		result.Dependencies[to] = append(result.Dependencies[to], from)
		graph[from] = append(graph[from], to)

		if kind == "raw" {
			result.RAWDependencyEdges++
		}
	}

	for index, item := range input.Transactions {
		txID := firstNonEmpty(
			item.TxID,
			fmt.Sprintf("tx-%d", index),
		)

		decision := p.Classify(item)
		reasons := []string{}

		// Nonce 顺序属于交易级约束。
		// 每个带 Sender 的交易都必须进入同一发送者的依赖链，
		// 不能被空访问列表或跨片边界的提前返回绕过。
		if item.Sender != "" {
			if previous := lastSenderTx[item.Sender]; previous != "" {
				addDependency(
					previous,
					txID,
					"nonce:"+item.Sender,
					"nonce",
				)
				reasons = append(
					reasons,
					"nonce_order_dependency",
				)
			}

			lastSenderTx[item.Sender] = txID
		}

		// 没有结构化访问列表时，仍保留已经建立的 Nonce 依赖。
		if len(item.AccessList) == 0 {
			if len(reasons) > 0 {
				reasons = append(reasons, decision.Reason)
				sort.Strings(reasons)
				reasons = uniqueStrings(reasons)

				result.ReasonCodes[txID] = reasons
				result.Decisions[txID] = ExecutionDecision{
					Track:  "conservative",
					Reason: strings.Join(reasons, "|"),
				}
			} else {
				result.ReasonCodes[txID] = []string{
					decision.Reason,
				}
				result.Decisions[txID] = decision
			}

			continue
		}

		// 跨片交易继续进入 conservative track，
		// 同时保留发送者 Nonce 依赖。
		if hasRemoteExecutionBoundary(item) {
			reasons = append(reasons, decision.Reason)
			sort.Strings(reasons)
			reasons = uniqueStrings(reasons)

			result.ReasonCodes[txID] = reasons
			result.Decisions[txID] = ExecutionDecision{
				Track:  "conservative",
				Reason: strings.Join(reasons, "|"),
			}

			continue
		}

		// 构建基于访问列表的 RAW、WAR 和 WAW 依赖。
		for _, access := range item.AccessList {
			if access.Key == "" {
				reasons = append(
					reasons,
					"missing_access_key",
				)
				continue
			}

			if input.RemoteStateReadiness != nil {
				if ready, ok := input.RemoteStateReadiness[access.Key]; ok && !ready {
					reasons = append(
						reasons,
						"remote_state_unresolved",
					)
				}
			}

			if writer := lastWriter[access.Key]; writer != "" {
				if isWriteMode(access.Mode) {
					addDependency(
						writer,
						txID,
						access.Key,
						"waw",
					)
					reasons = append(
						reasons,
						"waw_dependency",
					)
				} else {
					addDependency(
						writer,
						txID,
						access.Key,
						"raw",
					)
					reasons = append(
						reasons,
						"raw_dependency",
					)
				}
			}

			if isWriteMode(access.Mode) {
				for _, reader := range readers[access.Key] {
					addDependency(
						reader,
						txID,
						access.Key,
						"war",
					)
					reasons = append(
						reasons,
						"war_dependency",
					)
				}

				readers[access.Key] = nil
				lastWriter[access.Key] = txID
			} else {
				readers[access.Key] = append(
					readers[access.Key],
					txID,
				)
			}
		}

		if len(reasons) > 0 {
			sort.Strings(reasons)
			reasons = uniqueStrings(reasons)

			result.ReasonCodes[txID] = reasons
			result.Decisions[txID] = ExecutionDecision{
				Track:  "conservative",
				Reason: strings.Join(reasons, "|"),
			}

			continue
		}

		if decision.Track == "conservative" &&
			strings.HasPrefix(
				decision.Reason,
				"non_commutative_write:",
			) {

			result.ReasonCodes[txID] = []string{
				"batch_independent_write",
			}
			result.Decisions[txID] = ExecutionDecision{
				Track:  "fast",
				Reason: "batch_independent_write",
			}

			continue
		}

		result.ReasonCodes[txID] = []string{
			decision.Reason,
		}
		result.Decisions[txID] = decision
	}

	result.DeduplicatedEdgeCount = len(edges)

	for txID, deps := range result.Dependencies {
		sort.Strings(deps)
		result.Dependencies[txID] = uniqueStrings(deps)
	}

	result.DependencyChainMax = dependencyChainMax(graph)
	result.SCCCount = stronglyConnectedComponentCount(graph)

	return result
}

func hasRemoteExecutionBoundary(item tx.SignedTransaction) bool {
	if strings.HasPrefix(item.Payload, "v5_cross:") {
		return true
	}
	switch item.SourceKind {
	case "cross_shard_relay", "relay_certificate":
		return true
	default:
		return false
	}
}

type builtinScheduler struct{ basicPlugin }

func (p builtinScheduler) Order(items []tx.SignedTransaction, execution ExecutionPlugin) []tx.SignedTransaction {
	return p.Schedule(items, execution).Ordered
}

func (p builtinScheduler) Schedule(items []tx.SignedTransaction, execution ExecutionPlugin) ScheduleResult {
	ordered := append([]tx.SignedTransaction(nil), items...)
	result := ScheduleResult{Ordered: ordered}
	classification := batchClassification(ordered, execution)
	decisions := classification.Decisions
	fastDepth, conservativeDepth := 0, 0
	for _, item := range ordered {
		decision := decisionForTx(item, decisions, execution)
		if decision.Track == "fast" {
			fastDepth++
		} else if decision.Track == "conservative" {
			conservativeDepth++
		}
		result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "enqueue:" + decision.Reason, LocalExecution: true, ReadyQueueDepth: fastDepth + conservativeDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth})
	}
	if p.ID() != "fast_first_scheduler" || execution == nil {
		for _, item := range ordered {
			decision := decisionForTx(item, decisions, execution)
			if decision.Track == "fast" && fastDepth > 0 {
				fastDepth--
			} else if decision.Track == "conservative" && conservativeDepth > 0 {
				conservativeDepth--
			}
			result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "dispatch_fifo", LocalExecution: true, ReadyQueueDepth: fastDepth + conservativeDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth})
		}
		return result
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		left := decisionForTx(ordered[i], decisions, execution)
		right := decisionForTx(ordered[j], decisions, execution)
		if left.Track == right.Track {
			return false
		}
		return left.Track == "fast"
	})
	dependencies := classification.Dependencies
	if len(dependencies) == 0 {
		dependencies = scheduleDependenciesByOrder(ordered)
	}
	reverse := map[string][]string{}
	depCount := map[string]int{}
	byID := map[string]tx.SignedTransaction{}
	fastQueue := []string{}
	conservativeQueue := []string{}
	blocked := map[string]bool{}
	for _, item := range ordered {
		txID := txIdentifier(item)
		byID[txID] = item
		decision := decisionForTx(item, decisions, execution)
		deps := dependencies[txID]
		depCount[txID] = len(deps)
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], txID)
		}
		if len(deps) > 0 {
			blocked[txID] = true
			result.Events = append(result.Events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: "blocked_waiting", DecisionReason: "wait_for_dependencies:" + strings.Join(deps, "|"), LocalExecution: true, Blocked: true, ReadyQueueDepth: len(fastQueue) + len(conservativeQueue), FastQueueDepth: len(fastQueue), ConservativeQueueDepth: len(conservativeQueue), DependencyWaitMS: int64(len(deps))})
			continue
		}
		if decision.Track == "fast" {
			fastQueue = append(fastQueue, txID)
		} else {
			conservativeQueue = append(conservativeQueue, txID)
		}
	}
	dispatchOrder := make([]tx.SignedTransaction, 0, len(ordered))
	completed := map[string]bool{}
	for len(dispatchOrder) < len(ordered) {
		txID := ""
		if len(fastQueue) > 0 {
			txID, fastQueue = fastQueue[0], fastQueue[1:]
		} else if len(conservativeQueue) > 0 {
			txID, conservativeQueue = conservativeQueue[0], conservativeQueue[1:]
		} else {
			for _, item := range ordered {
				candidate := txIdentifier(item)
				if !completed[candidate] {
					txID = candidate
					delete(blocked, txID)
					break
				}
			}
			if txID == "" {
				break
			}
		}
		item := byID[txID]
		decision := decisionForTx(item, decisions, execution)
		completed[txID] = true
		dispatchOrder = append(dispatchOrder, item)
		idleMS := int64(0)
		if len(dispatchOrder) == len(ordered) && len(fastQueue) == 0 && len(conservativeQueue) == 0 && len(blocked) == 0 {
			idleMS = 1
		}
		result.Events = append(result.Events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "dispatch_fast_first", LocalExecution: true, ReadyQueueDepth: len(fastQueue) + len(conservativeQueue), FastQueueDepth: len(fastQueue), ConservativeQueueDepth: len(conservativeQueue), SchedulerIdleMS: idleMS})
		for _, dependent := range reverse[txID] {
			if completed[dependent] || !blocked[dependent] {
				continue
			}
			depCount[dependent]--
			if depCount[dependent] > 0 {
				continue
			}
			delete(blocked, dependent)
			dependentItem := byID[dependent]
			dependentDecision := decisionForTx(dependentItem, decisions, execution)
			if dependentDecision.Track == "fast" {
				fastQueue = append(fastQueue, dependent)
			} else {
				conservativeQueue = append(conservativeQueue, dependent)
			}
			result.Events = append(result.Events, ScheduleEvent{TxID: dependent, Track: dependentDecision.Track, QueueName: queueNameForTrack(dependentDecision.Track), DecisionReason: "dependencies_satisfied:" + txID, LocalExecution: true, Wakeup: true, ReadyQueueDepth: len(fastQueue) + len(conservativeQueue), FastQueueDepth: len(fastQueue), ConservativeQueueDepth: len(conservativeQueue), DependencyWaitMS: 1})
		}
	}
	result.Ordered = dispatchOrder
	return result
}

func classifyForSchedule(item tx.SignedTransaction, execution ExecutionPlugin) ExecutionDecision {
	if execution == nil {
		return ExecutionDecision{Track: "serial", Reason: "no_execution_classifier"}
	}
	decision := execution.Classify(item)
	if decision.Track == "" {
		decision.Track = "conservative"
	}
	return decision
}

func batchClassification(items []tx.SignedTransaction, execution ExecutionPlugin) BatchClassificationResult {
	batch, ok := execution.(BatchExecutionPlugin)
	if !ok {
		return BatchClassificationResult{}
	}
	return batch.ClassifyBatch(BatchClassificationInput{Transactions: items})
}

func decisionForTx(item tx.SignedTransaction, decisions map[string]ExecutionDecision, execution ExecutionPlugin) ExecutionDecision {
	if decisions != nil {
		if decision, ok := decisions[item.TxID]; ok {
			return decision
		}
	}
	return classifyForSchedule(item, execution)
}

func queueNameForTrack(track string) string {
	if track == "fast" {
		return "fast_queue"
	}
	if track == "conservative" {
		return "conservative_queue"
	}
	return track + "_queue"
}

func queueDepthsFor(items []tx.SignedTransaction, execution ExecutionPlugin, decisions map[string]ExecutionDecision) (int, int) {
	fast, conservative := 0, 0
	for _, item := range items {
		decision := decisionForTx(item, decisions, execution)
		if decision.Track == "fast" {
			fast++
		} else if decision.Track == "conservative" {
			conservative++
		}
	}
	return fast, conservative
}

func accessItemsConflict(left, right tx.AccessItem) bool {
	if left.Key == "" || right.Key == "" || left.Key != right.Key {
		return false
	}
	return isWriteMode(left.Mode) || isWriteMode(right.Mode)
}

func declaredAccessViolation(declared []tx.AccessItem, delta execution.TxDelta) string {
	declaredModes := map[string]tx.AccessMode{}
	declaredSemantics := map[string]string{}
	for _, access := range declared {
		if access.Key == "" {
			continue
		}
		declaredModes[access.Key] = access.Mode
		declaredSemantics[access.Key] = access.UpdateSemantics
	}
	for _, read := range delta.ReadSet {
		mode, ok := declaredModes[read.Key]
		if !ok {
			return "undeclared_read:" + read.Key
		}
		if !isReadMode(mode) {
			return "read_not_allowed:" + read.Key
		}
	}
	for key := range delta.WriteSet {
		mode, ok := declaredModes[key]
		if !ok {
			return "undeclared_write:" + key
		}
		if mode == tx.AccessRead && declaredSemantics[key] == "seller_nonce_state" && delta.WriteSet[key] == "0" {
			continue
		}
		if !isWriteMode(mode) {
			return "write_not_allowed:" + key
		}
	}
	return ""
}

func scheduleDependenciesByOrder(items []tx.SignedTransaction) map[string][]string {
	dependencies := map[string][]string{}
	dispatched := map[string][]tx.AccessItem{}
	for _, item := range items {
		txID := txIdentifier(item)
		deps := []string{}
		for seenTx, previousAccesses := range dispatched {
			for _, current := range item.AccessList {
				if current.Key == "" {
					continue
				}
				for _, previous := range previousAccesses {
					if accessItemsConflict(current, previous) {
						deps = append(deps, seenTx)
						break
					}
				}
			}
		}
		sort.Strings(deps)
		if len(deps) > 0 {
			dependencies[txID] = uniqueStrings(deps)
		}
		dispatched[txID] = item.AccessList
	}
	return dependencies
}

func txIdentifier(item tx.SignedTransaction) string {
	if item.TxID != "" {
		return item.TxID
	}
	return stableTextDigest(strings.Join(append([]string{item.Sender, item.Receiver, fmt.Sprint(item.Nonce), item.Payload}, item.StateKeys...), "|"))
}

type serialBlockExecutor struct{ basicPlugin }

func (p serialBlockExecutor) ExecuteBlock(_ context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
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

type blockSTMBlockExecutor struct{ basicPlugin }

func (p blockSTMBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	executor := execution.NewBlockSTMExecutor(workerCount)
	if mode := strings.TrimSpace(fmt.Sprint(p.config["execution_mode"])); mode == "correctness" || mode == "performance" {
		executor.ExecutionMode = mode
	}
	if oracle := strings.TrimSpace(fmt.Sprint(p.config["oracle_mode"])); oracle == "full" || oracle == "sampled" || oracle == "off" {
		executor.OracleMode = oracle
	}
	if maxIncarnations := intValue(p.config["maximum_incarnations"]); maxIncarnations > 0 {
		executor.MaximumIncarnations = maxIncarnations
	}
	if action := strings.TrimSpace(fmt.Sprint(p.config["incarnation_limit_action"])); action == "fail" || action == "serial_fallback" {
		executor.IncarnationLimitAction = action
	}
	result, err := executor.ExecuteBlock(ctx, input.Block, input.BaseStateSnapshot)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result.BlockSTMMetrics = executor.Metrics
	delta := make([]state.StateKV, 0, len(result.StateDelta))
	for _, item := range result.StateDelta {
		delta = append(delta, state.StateKV{Key: item.Key, Value: item.Value})
	}
	return BlockExecutionResult{ExecutionResult: result, StateDelta: delta, PlanDigest: result.PlanDigest, WorkerCount: result.WorkerCount}, nil
}

const metaTrackBlockExecutorID = "metatrack_block_executor"

type metaTrackBlockExecutor struct{ basicPlugin }

func (p metaTrackBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	if workerCount < 1 {
		workerCount = 1
	}
	businessDelay := durationConfigMS(p.config, "business_execution_delay_ms")
	scheduler := input.Scheduler
	if scheduler == nil {
		scheduler = builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}
	}
	executionPlugin := input.Execution
	if executionPlugin == nil {
		executionPlugin = dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	}
	schedule := scheduler.Schedule(input.Block.TxList, executionPlugin)
	if len(schedule.Ordered) != len(input.Block.TxList) {
		return BlockExecutionResult{}, fmt.Errorf("metatrack block executor schedule length mismatch")
	}
	classification := batchClassification(input.Block.TxList, executionPlugin)
	planEvents, actualMetrics, outcomes, attempts, err := executeMetaTrackSchedule(ctx, schedule, classification, input.Block, input.BaseStateSnapshot, workerCount, businessDelay)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	working := copyRegistryStringMap(input.BaseStateSnapshot)
	before := state.RootOfSnapshot(working)
	result := execution.Result{BlockHash: input.Block.BlockHash, Height: input.Block.Height, StateRootBefore: before, Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: metaTrackBlockExecutorID, ExecutorVersion: "1.0.0", WorkerCount: workerCount}
	originalIndex := map[string]int{}
	for index, item := range input.Block.TxList {
		originalIndex[txIdentifier(item)] = index
	}
	receiptsByIndex := make([]execution.Receipt, len(input.Block.TxList))
	deltasByIndex := make([]execution.TxDelta, len(input.Block.TxList))
	receiptSet := make([]bool, len(input.Block.TxList))
	finalSeen := map[string]bool{}
	duplicateFinalCompletions := 0
	for _, outcome := range outcomes {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		if finalSeen[outcome.TxID] {
			duplicateFinalCompletions++
			continue
		}
		finalSeen[outcome.TxID] = true
		for key, value := range outcome.Delta.WriteSet {
			working[qualifyStateKey(input.Block.ShardID, key)] = value
		}
		receipt := outcome.Receipt
		receipt.StateRootAfterTx = state.RootOfSnapshot(working)
		delta := outcome.Delta
		index := originalIndex[outcome.TxID]
		delta.OriginalIndex = index
		delta.Receipt = receipt
		receiptsByIndex[index] = receipt
		deltasByIndex[index] = delta
		receiptSet[index] = true
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	for index, ok := range receiptSet {
		if !ok {
			return BlockExecutionResult{}, fmt.Errorf("metatrack block executor missing final result at consensus index %d", index)
		}
		result.Receipts = append(result.Receipts, receiptsByIndex[index])
		result.TxDeltas = append(result.TxDeltas, deltasByIndex[index])
	}
	result.StateRootAfter = state.RootOfSnapshot(working)
	result.ReceiptRoot = execution.ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = executionStateDelta(input.BaseStateSnapshot, working)
	plan := buildMetaTrackExecutionPlan(input.Block, schedule.Ordered, originalIndex, workerCount)
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	actualMetrics["duplicate_final_completion_count"] = duplicateFinalCompletions
	actualMetrics["unique_final_logical_completion_count"] = len(finalSeen)
	return BlockExecutionResult{ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: result.PlanDigest, WorkerCount: workerCount, ScheduleEvents: planEvents, ActualMetrics: actualMetrics, BusinessAttempts: attempts}, nil
}

type metaTrackExecutionOutcome struct {
	Tx             tx.SignedTransaction
	TxID           string
	Track          string
	WorkerID       int
	AssignedWorker int
	Stolen         bool
	Attempt        int
	Receipt        execution.Receipt
	Delta          execution.TxDelta
}

func executeMetaTrackSchedule(ctx context.Context, schedule ScheduleResult, classification BatchClassificationResult, block realblock.Block, baseSnapshot map[string]string, workerCount int, businessDelay time.Duration) ([]ScheduleEvent, map[string]any, []metaTrackExecutionOutcome, []BusinessExecutionAttempt, error) {
	ordered := append([]tx.SignedTransaction(nil), schedule.Ordered...)
	byID := map[string]tx.SignedTransaction{}
	decisionByID := map[string]ExecutionDecision{}
	for _, item := range block.TxList {
		txID := txIdentifier(item)
		byID[txID] = item
		decisionByID[txID] = decisionForTx(item, classification.Decisions, nil)
	}
	deps := map[string][]string{}
	for _, item := range ordered {
		txID := txIdentifier(item)
		deps[txID] = append([]string(nil), classification.Dependencies[txID]...)
	}
	if len(classification.Dependencies) == 0 {
		deps = scheduleDependenciesByOrder(ordered)
	}
	reverse := map[string][]string{}
	depCount := map[string]int{}
	fastReady := []string{}
	conservativeReady := []string{}
	blocked := map[string]bool{}
	events := []ScheduleEvent{}
	maxReadyQueueDepth := 0
	maxFastReadyQueueDepth := 0
	maxConservativeReadyQueueDepth := 0
	maxDependencyFrontierWidth := 0
	recordDepths := func() {
		if depth := len(fastReady) + len(conservativeReady); depth > maxReadyQueueDepth {
			maxReadyQueueDepth = depth
		}
		if len(fastReady) > maxFastReadyQueueDepth {
			maxFastReadyQueueDepth = len(fastReady)
		}
		if len(conservativeReady) > maxConservativeReadyQueueDepth {
			maxConservativeReadyQueueDepth = len(conservativeReady)
		}
	}
	for _, item := range ordered {
		txID := txIdentifier(item)
		depCount[txID] = len(deps[txID])
		decision := decisionByID[txID]
		if decision.Track == "" {
			decision.Track = "conservative"
		}
		for _, dep := range deps[txID] {
			reverse[dep] = append(reverse[dep], txID)
		}
		if depCount[txID] > 0 {
			blocked[txID] = true
			events = append(events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: "blocked_queue", DecisionReason: "actual_wait_for_dependencies", LocalExecution: true, Blocked: true, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady), DependencyWaitMS: int64(depCount[txID])})
			continue
		}
		if decision.Track == "fast" {
			fastReady = append(fastReady, txID)
		} else {
			conservativeReady = append(conservativeReady, txID)
		}
		recordDepths()
		events = append(events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "actual_ready_initial", LocalExecution: true, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady)})
	}
	recordDepths()
	type job struct {
		seq            int
		txID           string
		snapshot       map[string]string
		attempt        int
		assignedWorker int
	}
	type completion struct {
		seq     int
		outcome metaTrackExecutionOutcome
		err     error
	}
	workerQueues := make([]chan job, workerCount)
	for index := range workerQueues {
		workerQueues[index] = make(chan job, len(ordered))
	}
	completions := make(chan completion, len(ordered))
	var wg sync.WaitGroup
	var inflightBusiness int64
	var maxInflightBusiness int64
	var businessExecuteInvocations int64
	var stealAttemptCount int64
	var stealSuccessCount int64
	var stolenTaskCount int64
	var fastFallbackCount int64
	var discardedTentativeCount int64
	var conservativeReexecutionCount int64
	workerExecutionCount := make([]int, workerCount)
	var workerMu sync.Mutex
	workerDone := make(chan struct{})
	nextWorkerJob := func(workerID int) (job, bool) {
		for {
			select {
			case <-ctx.Done():
				return job{}, false
			case <-workerDone:
				return job{}, false
			default:
			}
			select {
			case item, ok := <-workerQueues[workerID]:
				return item, ok
			default:
			}
			for offset := 1; offset < len(workerQueues); offset++ {
				victimID := (workerID + offset) % len(workerQueues)
				atomic.AddInt64(&stealAttemptCount, 1)
				select {
				case item, ok := <-workerQueues[victimID]:
					if !ok {
						continue
					}
					atomic.AddInt64(&stealSuccessCount, 1)
					return item, true
				default:
				}
			}
			select {
			case <-ctx.Done():
				return job{}, false
			case <-workerDone:
				return job{}, false
			case item, ok := <-workerQueues[workerID]:
				return item, ok
			case <-time.After(time.Millisecond):
			}
		}
	}
	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				item, ok := nextWorkerJob(workerID)
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				current := atomic.AddInt64(&inflightBusiness, 1)
				for {
					observed := atomic.LoadInt64(&maxInflightBusiness)
					if current <= observed || atomic.CompareAndSwapInt64(&maxInflightBusiness, observed, current) {
						break
					}
				}
				atomic.AddInt64(&businessExecuteInvocations, 1)
				workerMu.Lock()
				workerExecutionCount[workerID]++
				workerMu.Unlock()
				if businessDelay > 0 {
					select {
					case <-ctx.Done():
						atomic.AddInt64(&inflightBusiness, -1)
						return
					case <-time.After(businessDelay):
					}
				}
				single := block
				txItem := byID[item.txID]
				single.TxList = []tx.SignedTransaction{txItem}
				single.TxIDs = []string{txItem.TxID}
				singleResult := execution.NewSerialExecutor().ExecuteBlock(single, item.snapshot)
				atomic.AddInt64(&inflightBusiness, -1)
				if len(singleResult.Receipts) != 1 || len(singleResult.TxDeltas) != 1 {
					completions <- completion{seq: item.seq, err: fmt.Errorf("metatrack block executor missing single-tx result for %s", item.txID)}
					continue
				}
				stolen := workerID != item.assignedWorker
				if stolen {
					atomic.AddInt64(&stolenTaskCount, 1)
				}
				completions <- completion{seq: item.seq, outcome: metaTrackExecutionOutcome{Tx: txItem, TxID: item.txID, Track: decisionByID[item.txID].Track, WorkerID: workerID, AssignedWorker: item.assignedWorker, Stolen: stolen, Attempt: item.attempt, Receipt: singleResult.Receipts[0], Delta: singleResult.TxDeltas[0]}}
			}
		}(workerID)
	}
	dispatchSeq := 0
	completed := map[string]bool{}
	executionOutcomes := make([]metaTrackExecutionOutcome, 0, len(ordered))
	attempts := make([]BusinessExecutionAttempt, 0, len(ordered))
	attemptByID := map[string]int{}
	workingSnapshot := copyRegistryStringMap(baseSnapshot)
	dispatch := func(txID string) error {
		attemptByID[txID]++
		snapshot := copyRegistryStringMap(workingSnapshot)
		assignedWorker := 0
		if workerCount > 0 {
			assignedWorker = dispatchSeq % workerCount
			if workerCount > 1 && dispatchSeq%2 == 1 {
				assignedWorker = 0
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case workerQueues[assignedWorker] <- job{seq: dispatchSeq, txID: txID, snapshot: snapshot, attempt: attemptByID[txID], assignedWorker: assignedWorker}:
			dispatchSeq++
			return nil
		}
	}
	var closeWorkersOnce sync.Once
	closeWorkers := func() {
		closeWorkersOnce.Do(func() {
			close(workerDone)
			for _, queue := range workerQueues {
				close(queue)
			}
		})
	}
	nextReady := func() string {
		if len(fastReady) > 0 {
			txID := fastReady[0]
			fastReady = fastReady[1:]
			return txID
		}
		if len(conservativeReady) > 0 {
			txID := conservativeReady[0]
			conservativeReady = conservativeReady[1:]
			return txID
		}
		return ""
	}
	inFlight := 0
	dispatchCapacity := func() error {
		for inFlight < workerCount {
			txID := nextReady()
			if txID == "" {
				return nil
			}
			decision := decisionByID[txID]
			events = append(events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "actual_dispatch", LocalExecution: true, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady)})
			if err := dispatch(txID); err != nil {
				return err
			}
			inFlight++
		}
		return nil
	}
	if err := dispatchCapacity(); err != nil {
		closeWorkers()
		wg.Wait()
		return nil, nil, nil, nil, err
	}
	pendingCompletions := map[int]completion{}
	nextCompletionSeq := 0
	for len(executionOutcomes) < len(ordered) {
		if inFlight == 0 && len(fastReady) == 0 && len(conservativeReady) == 0 {
			for _, item := range ordered {
				txID := txIdentifier(item)
				if !completed[txID] {
					delete(blocked, txID)
					decision := decisionByID[txID]
					conservativeReady = append(conservativeReady, txID)
					recordDepths()
					events = append(events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: "conservative_queue", DecisionReason: "actual_cycle_break_conservative", LocalExecution: true, Wakeup: true, ReadyQueueDepth: len(conservativeReady), ConservativeQueueDepth: len(conservativeReady)})
					break
				}
			}
			if err := dispatchCapacity(); err != nil {
				closeWorkers()
				wg.Wait()
				return nil, nil, nil, nil, err
			}
		}
		var done completion
		for {
			if item, ok := pendingCompletions[nextCompletionSeq]; ok {
				done = item
				delete(pendingCompletions, nextCompletionSeq)
				nextCompletionSeq++
				break
			}
			select {
			case <-ctx.Done():
				closeWorkers()
				wg.Wait()
				return nil, nil, nil, nil, ctx.Err()
			case item := <-completions:
				pendingCompletions[item.seq] = item
			}
		}
		inFlight--
		if done.err != nil {
			closeWorkers()
			wg.Wait()
			return nil, nil, nil, nil, done.err
		}
		doneID := done.outcome.TxID
		if completed[doneID] {
			continue
		}
		outcomeTrack := done.outcome.Track
		if outcomeTrack == "" {
			outcomeTrack = decisionByID[doneID].Track
		}
		if outcomeTrack == "fast" {
			if reason := declaredAccessViolation(done.outcome.Tx.AccessList, done.outcome.Delta); reason != "" {
				atomic.AddInt64(&fastFallbackCount, 1)
				atomic.AddInt64(&discardedTentativeCount, 1)
				atomic.AddInt64(&conservativeReexecutionCount, 1)
				decisionByID[doneID] = ExecutionDecision{Track: "conservative", Reason: "fast_fallback:" + reason}
				conservativeReady = append(conservativeReady, doneID)
				recordDepths()
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: doneID, Track: "fast", Attempt: done.outcome.Attempt, Reason: "fast_fallback:" + reason, Success: done.outcome.Receipt.Success, FinalCompletion: false})
				events = append(events, ScheduleEvent{TxID: doneID, Track: "conservative", QueueName: "conservative_queue", DecisionReason: "fast_fallback:" + reason, LocalExecution: true, Wakeup: true, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady), DependencyWaitMS: 1})
				if err := dispatchCapacity(); err != nil {
					closeWorkers()
					wg.Wait()
					return nil, nil, nil, nil, err
				}
				continue
			}
		}
		completed[doneID] = true
		if done.outcome.Receipt.Success {
			for key, value := range done.outcome.Delta.WriteSet {
				workingSnapshot[qualifyStateKey(block.ShardID, key)] = value
			}
		} else {
			// Failed transactions are terminal outcomes, but their speculative
			// writes must not enter the block working snapshot or commit plan.
			done.outcome.Delta.WriteSet = nil
			done.outcome.Delta.Success = false
			done.outcome.Delta.Error = done.outcome.Receipt.Error
		}
		executionOutcomes = append(executionOutcomes, done.outcome)
		attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: doneID, Track: done.outcome.Track, Attempt: done.outcome.Attempt, Reason: fmt.Sprintf("worker_%d_completion", done.outcome.WorkerID), Success: done.outcome.Receipt.Success, FinalCompletion: true})
		decision := decisionByID[doneID]
		events = append(events, ScheduleEvent{TxID: doneID, Track: decision.Track, QueueName: "completion_channel", DecisionReason: fmt.Sprintf("actual_completion:worker_%d", done.outcome.WorkerID), LocalExecution: true, StolenWork: done.outcome.Stolen, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady)})
		releasedThisCompletion := 0
		for _, dependent := range reverse[doneID] {
			if completed[dependent] || !blocked[dependent] {
				continue
			}
			depCount[dependent]--
			if depCount[dependent] > 0 {
				continue
			}
			delete(blocked, dependent)
			dependentDecision := decisionByID[dependent]
			if dependentDecision.Track == "fast" {
				fastReady = append(fastReady, dependent)
			} else {
				conservativeReady = append(conservativeReady, dependent)
			}
			releasedThisCompletion++
			recordDepths()
			events = append(events, ScheduleEvent{TxID: dependent, Track: dependentDecision.Track, QueueName: queueNameForTrack(dependentDecision.Track), DecisionReason: "actual_dependencies_released:" + doneID, LocalExecution: true, Wakeup: true, ReadyQueueDepth: len(fastReady) + len(conservativeReady), FastQueueDepth: len(fastReady), ConservativeQueueDepth: len(conservativeReady), DependencyWaitMS: 1})
		}
		if releasedThisCompletion > maxDependencyFrontierWidth {
			maxDependencyFrontierWidth = releasedThisCompletion
		}
		if err := dispatchCapacity(); err != nil {
			closeWorkers()
			wg.Wait()
			return nil, nil, nil, nil, err
		}
	}
	closeWorkers()
	wg.Wait()
	workerCounts := make([]int, len(workerExecutionCount))
	copy(workerCounts, workerExecutionCount)
	dispatchCount := countScheduleEvents(events, func(event ScheduleEvent) bool { return strings.HasPrefix(event.DecisionReason, "actual_dispatch") })
	blockedCount := countScheduleEvents(events, func(event ScheduleEvent) bool { return event.Blocked })
	wakeupCount := countScheduleEvents(events, func(event ScheduleEvent) bool { return event.Wakeup })
	completionChannelCount := countScheduleEvents(events, func(event ScheduleEvent) bool { return strings.HasPrefix(event.DecisionReason, "actual_completion") })
	metrics := map[string]any{
		"configured_worker_count":               workerCount,
		"max_ready_queue_depth":                 maxReadyQueueDepth,
		"max_fast_ready_queue_depth":            maxFastReadyQueueDepth,
		"max_conservative_ready_queue_depth":    maxConservativeReadyQueueDepth,
		"max_dependency_frontier_width":         maxDependencyFrontierWidth,
		"max_inflight_business_executions":      int(atomic.LoadInt64(&maxInflightBusiness)),
		"worker_execution_count":                workerCounts,
		"steal_attempt_count":                   int(atomic.LoadInt64(&stealAttemptCount)),
		"steal_success_count":                   int(atomic.LoadInt64(&stealSuccessCount)),
		"stolen_task_count":                     int(atomic.LoadInt64(&stolenTaskCount)),
		"submitted_logical_tx_count":            len(ordered),
		"scheduler_dispatch_event_count":        dispatchCount,
		"blocked_event_count":                   blockedCount,
		"wakeup_event_count":                    wakeupCount,
		"completion_channel_event_count":        completionChannelCount,
		"business_execute_invocation_count":     int(atomic.LoadInt64(&businessExecuteInvocations)),
		"fast_fallback_count":                   int(atomic.LoadInt64(&fastFallbackCount)),
		"discarded_tentative_result_count":      int(atomic.LoadInt64(&discardedTentativeCount)),
		"conservative_reexecution_count":        int(atomic.LoadInt64(&conservativeReexecutionCount)),
		"retry_execution_count":                 int(atomic.LoadInt64(&conservativeReexecutionCount)),
		"reexecution_count":                     int(atomic.LoadInt64(&conservativeReexecutionCount)),
		"validator_execution_completion_count":  len(executionOutcomes),
		"unique_final_logical_completion_count": len(executionOutcomes),
		"duplicate_final_completion_count":      0,
	}
	return events, metrics, executionOutcomes, attempts, nil
}

func durationConfigMS(config map[string]any, key string) time.Duration {
	if config == nil {
		return 0
	}
	switch value := config[key].(type) {
	case int:
		return time.Duration(value) * time.Millisecond
	case int64:
		return time.Duration(value) * time.Millisecond
	case float64:
		return time.Duration(value) * time.Millisecond
	case string:
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
			return time.Duration(parsed) * time.Millisecond
		}
	}
	return 0
}

func qualifyStateKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func countScheduleEvents(events []ScheduleEvent, predicate func(ScheduleEvent) bool) int {
	count := 0
	for _, event := range events {
		if predicate(event) {
			count++
		}
	}
	return count
}

func buildMetaTrackExecutionPlan(block realblock.Block, ordered []tx.SignedTransaction, originalIndex map[string]int, workerCount int) execution.ExecutionPlan {
	ids := make([]string, 0, len(ordered))
	indexes := make([]int, 0, len(ordered))
	readKeys := map[string]bool{}
	writeKeys := map[string]bool{}
	for _, item := range ordered {
		txID := txIdentifier(item)
		ids = append(ids, txID)
		indexes = append(indexes, originalIndex[txID])
		collectDeclaredAccessKeys(item, readKeys, writeKeys)
	}
	readList := sortedBoolKeys(readKeys)
	writeList := sortedBoolKeys(writeKeys)
	declaredDigest := stableJSONDigest(map[string]any{"read_keys": readList, "write_keys": writeList})
	plan := execution.ExecutionPlan{EngineID: metaTrackBlockExecutorID, EngineVersion: "1.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height, OrderedTransactionIDs: ids, OriginalTransactionIdxs: indexes, DeclaredAccessSetDigest: declaredDigest, DeclaredReadKeyCount: len(readList), DeclaredWriteKeyCount: len(writeList), WorkerCount: workerCount}
	plan.PlanDigest = stableJSONDigest(struct {
		EngineID                string   `json:"engine_id"`
		EngineVersion           string   `json:"engine_version"`
		BlockHash               string   `json:"block_hash"`
		BlockHeight             uint64   `json:"block_height"`
		OrderedTransactionIDs   []string `json:"ordered_transaction_ids"`
		OriginalTransactionIdxs []int    `json:"original_transaction_indexes"`
		DeclaredAccessSetDigest string   `json:"declared_access_set_digest"`
		DeclaredReadKeyCount    int      `json:"declared_read_key_count"`
		DeclaredWriteKeyCount   int      `json:"declared_write_key_count"`
		WorkerCount             int      `json:"worker_count"`
	}{plan.EngineID, plan.EngineVersion, plan.BlockHash, plan.BlockHeight, plan.OrderedTransactionIDs, plan.OriginalTransactionIdxs, plan.DeclaredAccessSetDigest, plan.DeclaredReadKeyCount, plan.DeclaredWriteKeyCount, plan.WorkerCount})
	return plan
}

func collectDeclaredAccessKeys(item tx.SignedTransaction, readKeys, writeKeys map[string]bool) {
	if len(item.AccessList) > 0 {
		for _, access := range item.AccessList {
			switch access.Mode {
			case tx.AccessRead:
				readKeys[access.Key] = true
			case tx.AccessWrite:
				writeKeys[access.Key] = true
			case tx.AccessReadWrite, tx.AccessCommutativeDelta:
				readKeys[access.Key] = true
				writeKeys[access.Key] = true
			}
		}
		return
	}
	for _, key := range item.StateKeys {
		readKeys[key] = true
		writeKeys[key] = true
	}
	for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
		readKeys[key] = true
		writeKeys[key] = true
	}
}

func sortedBoolKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func stableJSONDigest(value any) string {
	payload, _ := json.Marshal(value)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func copyRegistryStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func executionStateDelta(before, after map[string]string) []execution.StateUpdate {
	keys := make([]string, 0, len(after))
	for key, value := range after {
		if before[key] != value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]execution.StateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, execution.StateUpdate{Key: key, Value: after[key]})
	}
	return out
}

func stateKVsFromExecutionDelta(delta []execution.StateUpdate) []state.StateKV {
	out := make([]state.StateKV, 0, len(delta))
	for _, item := range delta {
		out = append(out, state.StateKV{Key: item.Key, Value: item.Value})
	}
	return out
}

func configuredWorkerCount(config map[string]any, fallback int) int {
	switch value := config["worker_count"].(type) {
	case int:
		if value > 0 {
			return value
		}
	case float64:
		if value > 0 {
			return int(value)
		}
	}
	if fallback > 0 {
		return fallback
	}
	return 1
}

type builtinStateAccess struct{ basicPlugin }

func (p builtinStateAccess) AccessMode() string { return "direct" }

func (p builtinStateAccess) BuildFetchRequest(input StateFetchInput) StateFetchRequest {
	return StateFetchRequest{RequestID: input.RequestID, TxID: input.TxID, BlockHash: input.BlockHash, Key: input.Key, HomeShard: input.HomeShard, ExecutionShard: input.ExecutionShard, AccessKind: input.AccessKind}
}

func (p builtinStateAccess) BuildDeltaApplyRequest(input StateDeltaApplyInput) StateDeltaApplyRequest {
	return StateDeltaApplyRequest{RequestID: input.RequestID, TxID: input.TxID, TxIDs: append([]string(nil), input.TxIDs...), BlockHash: input.BlockHash, Key: input.Key, Value: input.Value, UpdateSemantics: input.UpdateSemantics, Delta: input.Delta, BaseValue: input.BaseValue, BaseValueDigest: input.BaseValueDigest, ApplyOrigin: input.ApplyOrigin, DeltaKind: input.DeltaKind, HasInitialValue: input.HasInitialValue, InitialValue: input.InitialValue, HomeShard: input.HomeShard, ExecutionShard: input.ExecutionShard, SourceKey: input.SourceKey, SourceHeight: input.SourceHeight}
}

type builtinStateStorage struct{ basicPlugin }

func (p builtinStateStorage) Durable() bool { return true }

func (p builtinStateStorage) Open(input StateStorageInput) (*state.DB, *storage.BlockStore, error) {
	db, err := state.Open(input.DataDir, input.ShardID)
	if err != nil {
		return nil, nil, err
	}
	return db, storage.NewBlockStore(input.DataDir, input.NodeID, input.ShardID), nil
}

func (p builtinStateStorage) Snapshot(db *state.DB) map[string]string {
	if db == nil {
		return map[string]string{}
	}
	return db.Snapshot()
}

func (p builtinStateStorage) Root(db *state.DB) string {
	if db == nil {
		return state.RootOfSnapshot(nil)
	}
	return db.Root()
}

func (p builtinStateStorage) ApplyBatch(db *state.DB, updates []state.StateKV) error {
	if db != nil {
		return db.ApplyDeterministicBatch(updates)
	}
	return nil
}

func (p builtinStateStorage) Checkpoint(db *state.DB) (state.FileCheckpoint, error) {
	if db == nil {
		return state.FileCheckpoint{}, fmt.Errorf("state storage checkpoint requires db")
	}
	return db.Checkpoint()
}

func (p builtinStateStorage) Rollback(db *state.DB, checkpoint state.FileCheckpoint) error {
	if db == nil {
		return fmt.Errorf("state storage rollback requires db")
	}
	return db.Rollback(checkpoint)
}

func (p builtinStateStorage) PersistDelta(db *state.DB, meta state.DeltaMetadata, updates []state.StateKV, opts state.PersistenceOptions) (state.PersistenceMetrics, error) {
	if db == nil {
		return state.PersistenceMetrics{}, fmt.Errorf("state storage wal append requires db")
	}
	return db.PersistDelta(meta, updates, opts)
}

func (p builtinStateStorage) SnapshotIfDue(db *state.DB, height uint64, opts state.PersistenceOptions) (state.PersistenceMetrics, error) {
	if db == nil {
		return state.PersistenceMetrics{}, fmt.Errorf("state storage snapshot requires db")
	}
	return db.SnapshotIfDue(height, opts)
}

type builtinCrossShard struct{ basicPlugin }

func (p builtinCrossShard) IsCrossShard(item tx.SignedTransaction) bool {
	return strings.HasPrefix(item.Payload, "v5_cross:")
}

func (p builtinCrossShard) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "SourceLock", Success: true}
}

func (p builtinCrossShard) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "TargetCommit", Success: true}
}

func (p builtinCrossShard) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "SourceFinalize", Success: true}
}

func (p builtinCrossShard) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "Refund", Success: true, Error: reason}
}

func (p builtinCrossShard) BuildRelay(input CrossShardRelayInput) Relay {
	return Relay{Tx: input.Tx, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

func (p builtinCrossShard) BuildFinalize(input CrossShardFinalizeInput) Finalize {
	return Finalize{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

type normalCommit struct{ basicPlugin }

func (p normalCommit) DecideCommit(input CommitInput) CommitDecision {
	physicalOps := physicalStateWriteCount(input)
	return CommitDecision{LogicalUpdates: len(input.Transactions), PhysicalUpdates: physicalOps, PhysicalStateDelta: append([]state.StateKV(nil), input.StateDelta...), PreAggregationPhysicalOps: physicalOps, PostAggregationPhysicalOps: physicalOps}
}

type aggregationCommit struct{ basicPlugin }

func (p aggregationCommit) DecideCommit(input CommitInput) CommitDecision {
	physicalOps := physicalStateWriteCount(input)
	d := CommitDecision{LogicalUpdates: maxInt(len(input.Transactions), physicalOps), PhysicalUpdates: physicalOps, PhysicalStateDelta: append([]state.StateKV(nil), input.StateDelta...), PreAggregationPhysicalOps: physicalOps, PostAggregationPhysicalOps: physicalOps}
	aggregated := aggregateCommutativeStateDelta(input)
	aggregationApplied := false
	if len(aggregated) > 0 {
		d.PhysicalStateDelta = aggregated
		d.PhysicalUpdates = len(aggregated)
		d.PostAggregationPhysicalOps = len(aggregated)
		savedPhysicalOps := 0
		for _, item := range aggregated {
			if len(item.TxIDs) > 1 && item.UpdateSemantics == "commutative_delta" {
				d.AggregatedKeyCount++
				d.AggregatedLogicalDeltaCount += len(item.TxIDs)
				savedPhysicalOps += len(item.TxIDs) - 1
			}
		}
		if savedPhysicalOps > 0 {
			d.PreAggregationPhysicalOps = d.PostAggregationPhysicalOps + savedPhysicalOps
			d.LogicalUpdates = maxInt(len(input.Transactions), d.PreAggregationPhysicalOps)
		}
		aggregationApplied = true
	}
	if aggregationApplied && d.PostAggregationPhysicalOps > 0 && d.PostAggregationPhysicalOps < d.PreAggregationPhysicalOps {
		d.Applied = true
		d.AggregationGroupID = fmt.Sprintf("%s:%d", input.ShardID, input.Height)
	}
	return d
}

type commutativeAggregationCandidate struct {
	delta int64
	txIDs []string
	valid bool
}

func aggregateCommutativeStateDelta(input CommitInput) []state.StateKV {
	if len(input.StateDelta) == 0 || len(input.Transactions) == 0 {
		return nil
	}
	txDeltaByID := map[string]execution.TxDelta{}
	for _, delta := range input.TxDeltas {
		if delta.TxID != "" {
			txDeltaByID[delta.TxID] = delta
		}
	}
	candidates := map[string]*commutativeAggregationCandidate{}
	banned := map[string]bool{}
	for index, item := range input.Transactions {
		txID := firstNonEmpty(item.TxID, fmt.Sprintf("tx-%d", index))
		if delta, ok := txDeltaByID[txID]; ok && !delta.Success {
			continue
		}
		for _, access := range item.AccessList {
			if access.Key == "" {
				continue
			}
			if access.Mode != tx.AccessCommutativeDelta {
				if isWriteMode(access.Mode) {
					banned[access.Key] = true
				}
				continue
			}
			if access.UpdateSemantics != "" && access.UpdateSemantics != "add" && access.UpdateSemantics != "commutative_delta" && access.UpdateSemantics != "market_sale_counter" {
				banned[access.Key] = true
				continue
			}
			if delta, ok := txDeltaByID[txID]; ok && !writeSetContainsLogicalKey(delta.WriteSet, access.Key) {
				banned[access.Key] = true
				continue
			}
			candidate := candidates[access.Key]
			if candidate == nil {
				candidate = &commutativeAggregationCandidate{valid: true}
				candidates[access.Key] = candidate
			}
			candidate.delta += access.Delta
			candidate.txIDs = append(candidate.txIDs, txID)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	emitted := map[string]bool{}
	out := make([]state.StateKV, 0, len(input.StateDelta))
	changed := false
	for _, item := range input.StateDelta {
		logicalKey := unqualifyStateKey(item.Key)
		candidate := candidates[logicalKey]
		if candidate == nil || !candidate.valid || banned[logicalKey] || len(candidate.txIDs) < 2 {
			out = append(out, item)
			continue
		}
		if emitted[logicalKey] {
			changed = true
			continue
		}
		next := item
		next.TxIDs = sortedCopy(candidate.txIDs)
		next.UpdateSemantics = "commutative_delta"
		next.Delta = candidate.delta
		out = append(out, next)
		emitted[logicalKey] = true
		if len(candidate.txIDs) > 1 || item.UpdateSemantics != next.UpdateSemantics || item.Delta != next.Delta || len(item.TxIDs) != len(next.TxIDs) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return out
}

func writeSetContainsLogicalKey(writeSet map[string]string, logicalKey string) bool {
	if len(writeSet) == 0 {
		return true
	}
	for key := range writeSet {
		if stateKeysReferToSameLogicalKey(key, logicalKey) {
			return true
		}
	}
	return false
}

func unqualifyStateKey(key string) string {
	if index := strings.Index(key, "::"); index >= 0 && index+2 < len(key) {
		return key[index+2:]
	}
	return key
}

func sortedCopy(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}

func physicalStateWriteCount(input CommitInput) int {
	if len(input.StateDelta) > 0 {
		return len(input.StateDelta)
	}
	return len(input.Transactions)
}

func metatrackAffinityKeys(accessList []tx.AccessItem, fallback []string) []string {
	keys := make([]string, 0, len(accessList))
	for _, access := range accessList {
		if access.Mode == tx.AccessWrite || access.Mode == tx.AccessReadWrite || access.Mode == tx.AccessCommutativeDelta {
			keys = append(keys, access.Key)
		}
	}
	if len(keys) == 0 {
		keys = append(keys, fallback...)
	}
	return keys
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

type builtinFault struct{ basicPlugin }

func (p builtinFault) Enabled() bool { return p.id != "faults_disabled" }

func (p builtinFault) Policy(plan map[string]any) faults.Policy {
	mode := fmt.Sprint(plan["mode"])
	if mode == "" || mode == "disabled" || !p.Enabled() {
		return faults.Policy{}
	}
	policy := faults.Policy{Enabled: true, DelayMS: intValue(plan["delay_ms"]), DropRate: floatValue(plan["drop_rate"]), Seed: int64(intValue(plan["seed"]))}
	if policy.DelayMS == 0 {
		policy.DelayMS = intValue(plan["network_delay_ms"])
	}
	if raw, ok := plan["drop_message_types"].([]any); ok {
		for _, item := range raw {
			policy.DropMessageTypes = append(policy.DropMessageTypes, fmt.Sprint(item))
		}
	}
	if raw, ok := plan["target_peer_ids"].([]any); ok {
		for _, item := range raw {
			policy.TargetPeerIDs = append(policy.TargetPeerIDs, fmt.Sprint(item))
		}
	}
	return policy
}

type builtinMetrics struct{ basicPlugin }

func (p builtinMetrics) MetricKeys() []string {
	return []string{"tx_admitted_count", "tx_rejected_count", "block_proposed_count", "consensus_reached_count", "execution_finished_count", "state_committed_count", "tx_finalized_count", "tx_failed_count"}
}

func (p builtinMetrics) Consume(event RuntimeEvent) []RuntimeMetric {
	key := runtimeEventMetricKey(event.Type)
	if key == "" {
		return nil
	}
	return []RuntimeMetric{{Key: key, Value: 1}}
}

func runtimeEventMetricKey(eventType string) string {
	switch eventType {
	case "TxAdmitted":
		return "tx_admitted_count"
	case "TxRejected":
		return "tx_rejected_count"
	case "BlockProposed":
		return "block_proposed_count"
	case "ConsensusReached":
		return "consensus_reached_count"
	case "ExecutionFinished":
		return "execution_finished_count"
	case "StateCommitted":
		return "state_committed_count"
	case "TxFinalized":
		return "tx_finalized_count"
	case "TxFailed":
		return "tx_failed_count"
	default:
		return ""
	}
}

type builtinObserver struct{ basicPlugin }

func (p builtinObserver) Observe(event RuntimeEvent) []string {
	return []string{
		fmt.Sprint(event.TimestampMS),
		event.Type,
		event.NodeID,
		event.ShardID,
		event.TxID,
		event.BlockHash,
		fmt.Sprint(event.Height),
		fmt.Sprint(event.Success),
		event.Error,
		stableDigest(event.Attributes),
	}
}

func stableKey(keys []string) int {
	sum := 0
	for _, key := range keys {
		for _, ch := range key {
			sum += int(ch)
		}
	}
	return sum
}

func stableDigest(value any) string {
	payload, _ := json.Marshal(value)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
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
	register("block_executor", "metatrack_block_executor", func(c map[string]any) (Plugin, error) {
		return metaTrackBlockExecutor{makeBasic("block_executor", "metatrack_block_executor", c)}, nil
	})
	register("block_executor", "block_stm_block_executor", func(c map[string]any) (Plugin, error) {
		return blockSTMBlockExecutor{makeBasic("block_executor", "block_stm_block_executor", c)}, nil
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
