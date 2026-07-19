package v5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
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
type BatchRoutingPlugin interface {
	RoutingPlugin
	PlanBatch(BatchRoutingInput) BatchRoutingPlan
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
}
type RoutingDecision struct{ ShardID, Reason string }
type BatchRoutingInput struct {
	BatchIndex int
	Records    []WorkloadRecord
	ShardIDs   []string
}
type BatchRoutingPlan struct {
	BatchIndex            int
	PlanDigest            string
	AccessMatrix          []AccessMatrixRow
	StateFrequency        []StateFrequencyRow
	CoaccessEdges         []CoaccessEdge
	StatePlacements       []StatePlacement
	TransactionPlacements []TransactionPlacement
	ShardLoadBefore       map[string]int
	ShardLoadAfter        map[string]int
	RemoteAccessEstimate  int
	RoutingOverhead       int
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
	if input.SourceShard != "" {
		return RoutingDecision{ShardID: input.SourceShard, Reason: "source_shard_home"}
	}
	return RoutingDecision{ShardID: builtinSharding{}.ShardFor(input.StateKeys, input.ShardIDs), Reason: "state_key_hash"}
}

type metaTrackRouting struct{ basicPlugin }

func (p metaTrackRouting) Route(input RoutingInput) RoutingDecision {
	if len(input.ShardIDs) == 0 {
		return RoutingDecision{}
	}
	record := WorkloadRecord{Index: input.Index, LogicalID: fmt.Sprintf("tx-%d", input.Index), StateKeys: input.StateKeys, AccessList: input.AccessList, SourceShard: input.SourceShard, CrossShard: input.CrossShard}
	plan := p.PlanBatch(BatchRoutingInput{BatchIndex: input.Index, Records: []WorkloadRecord{record}, ShardIDs: input.ShardIDs})
	if len(plan.TransactionPlacements) > 0 {
		placement := plan.TransactionPlacements[0]
		return RoutingDecision{ShardID: placement.ExecutionShard, Reason: placement.Reason}
	}
	return RoutingDecision{ShardID: input.ShardIDs[stableKey(input.StateKeys)%len(input.ShardIDs)], Reason: "metatrack_access_affinity"}
}

func (p metaTrackRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	plan := BatchRoutingPlan{BatchIndex: input.BatchIndex, ShardLoadBefore: map[string]int{}, ShardLoadAfter: map[string]int{}}
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
			if isWriteMode(access.Mode) {
				row.WriteCount++
			} else {
				row.ReadCount++
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
	sort.Slice(plan.StateFrequency, func(i, j int) bool { return plan.StateFrequency[i].Key < plan.StateFrequency[j].Key })
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
	for _, row := range plan.StateFrequency {
		home := input.ShardIDs[stableKey([]string{row.Key})%len(input.ShardIDs)]
		executionShard := home
		reason := "home_shard_locality"
		if isOrderedNonceWrite(row) {
			reason = "ordered_nonce_home_shard"
		} else if row.Frequency > 1 {
			executionShard = leastLoadedShard(input.ShardIDs, plan.ShardLoadAfter, row.Key)
			reason = "coaccess_frequency_load_balance"
		}
		plan.ShardLoadAfter[executionShard] += row.Frequency
		placement := StatePlacement{Key: row.Key, HomeShard: home, ExecutionShard: executionShard, Frequency: row.Frequency, Reason: reason}
		placementByKey[row.Key] = placement
		plan.StatePlacements = append(plan.StatePlacements, placement)
	}
	sort.Slice(plan.StatePlacements, func(i, j int) bool { return plan.StatePlacements[i].Key < plan.StatePlacements[j].Key })

	transactionLoad := map[string]int{}
	for _, record := range input.Records {
		accessItems := normalizedAccessItems(record)
		homeShard := firstNonEmpty(record.SourceShard, input.ShardIDs[stableKey(record.StateKeys)%len(input.ShardIDs)])
		targetShard := record.TargetShard
		if targetShard == "" && record.CrossShard && len(input.ShardIDs) > 1 {
			targetShard = input.ShardIDs[(stableKey(record.StateKeys)+1)%len(input.ShardIDs)]
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
		score[placement.ExecutionShard] += placement.Frequency
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
	for _, shard := range shardIDs {
		candidateScore := score[shard]
		if candidateScore > bestScore || (candidateScore == bestScore && currentLoad[shard] < currentLoad[best]) || (candidateScore == bestScore && currentLoad[shard] == currentLoad[best] && shard < best) {
			best = shard
			bestScore = candidateScore
		}
	}
	return best, strings.Join(groupKeys, "+"), remote
}

func isOrderedNonceWrite(row StateFrequencyRow) bool {
	return strings.HasPrefix(row.Key, "nonce:") && row.WriteCount > 0
}

func isOrderedNonceAccess(access tx.AccessItem) bool {
	return strings.HasPrefix(access.Key, "nonce:") && isWriteMode(access.Mode)
}

func isWriteMode(mode tx.AccessMode) bool {
	return mode == tx.AccessWrite || mode == tx.AccessReadWrite || mode == tx.AccessCommutativeDelta
}

func keyPair(left, right string) string {
	if right < left {
		left, right = right, left
	}
	return left + "\x00" + right
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
	fastDepth, conservativeDepth := 0, 0
	for _, item := range ordered {
		decision := classifyForSchedule(item, execution)
		if decision.Track == "fast" {
			fastDepth++
		} else if decision.Track == "conservative" {
			conservativeDepth++
		}
		result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "enqueue:" + decision.Reason, LocalExecution: true, ReadyQueueDepth: fastDepth + conservativeDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth})
	}
	if p.ID() != "fast_first_scheduler" || execution == nil {
		for _, item := range ordered {
			decision := classifyForSchedule(item, execution)
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
		left := execution.Classify(ordered[i])
		right := execution.Classify(ordered[j])
		if left.Track == right.Track {
			return false
		}
		return left.Track == "fast"
	})
	result.Ordered = ordered
	dispatched := map[string][]tx.AccessItem{}
	fastDepth, conservativeDepth = queueDepthsFor(ordered, execution)
	blockedDepth := 0
	for _, item := range ordered {
		decision := classifyForSchedule(item, execution)
		deps := scheduleDependencies(item, dispatched)
		if len(deps) > 0 {
			blockedDepth++
			result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: "blocked_waiting", DecisionReason: "wait_for_dependencies:" + strings.Join(deps, "|"), LocalExecution: true, Blocked: true, ReadyQueueDepth: fastDepth + conservativeDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth, DependencyWaitMS: int64(len(deps))})
			blockedDepth--
			result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "dependencies_satisfied:" + strings.Join(deps, "|"), LocalExecution: true, Wakeup: true, ReadyQueueDepth: fastDepth + conservativeDepth + blockedDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth, DependencyWaitMS: int64(len(deps))})
		}
		if decision.Track == "fast" && fastDepth > 0 {
			fastDepth--
		} else if decision.Track == "conservative" && conservativeDepth > 0 {
			conservativeDepth--
		}
		idleMS := int64(0)
		if fastDepth == 0 && conservativeDepth == 0 && blockedDepth == 0 {
			idleMS = 1
		}
		result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: decision.Track, QueueName: queueNameForTrack(decision.Track), DecisionReason: "dispatch_fast_first", LocalExecution: true, ReadyQueueDepth: fastDepth + conservativeDepth, FastQueueDepth: fastDepth, ConservativeQueueDepth: conservativeDepth, SchedulerIdleMS: idleMS})
		dispatched[item.TxID] = item.AccessList
	}
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

func queueNameForTrack(track string) string {
	if track == "fast" {
		return "fast_queue"
	}
	if track == "conservative" {
		return "conservative_queue"
	}
	return track + "_queue"
}

func queueDepthsFor(items []tx.SignedTransaction, execution ExecutionPlugin) (int, int) {
	fast, conservative := 0, 0
	for _, item := range items {
		decision := classifyForSchedule(item, execution)
		if decision.Track == "fast" {
			fast++
		} else if decision.Track == "conservative" {
			conservative++
		}
	}
	return fast, conservative
}

func scheduleDependencies(item tx.SignedTransaction, dispatched map[string][]tx.AccessItem) []string {
	if len(item.AccessList) == 0 {
		return nil
	}
	deps := []string{}
	for seenTx, previousAccesses := range dispatched {
		for _, current := range item.AccessList {
			if current.Key == "" {
				continue
			}
			for _, previous := range previousAccesses {
				if accessItemsConflict(current, previous) {
					deps = append(deps, current.Key+"@"+seenTx)
					break
				}
			}
		}
	}
	sort.Strings(deps)
	return deps
}

func accessItemsConflict(left, right tx.AccessItem) bool {
	if left.Key == "" || right.Key == "" || left.Key != right.Key {
		return false
	}
	return isWriteMode(left.Mode) || isWriteMode(right.Mode)
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

type builtinStateStorage struct{ basicPlugin }

func (p builtinStateStorage) Durable() bool { return true }

type builtinCrossShard struct{ basicPlugin }

func (p builtinCrossShard) IsCrossShard(item tx.SignedTransaction) bool {
	return strings.HasPrefix(item.Payload, "v5_cross:")
}

type normalCommit struct{ basicPlugin }

func (p normalCommit) DecideCommit(input CommitInput) CommitDecision {
	return CommitDecision{LogicalUpdates: len(input.Transactions), PhysicalUpdates: physicalStateWriteCount(input), PhysicalStateDelta: append([]state.StateKV(nil), input.StateDelta...)}
}

type aggregationCommit struct{ basicPlugin }

func (p aggregationCommit) DecideCommit(input CommitInput) CommitDecision {
	d := CommitDecision{LogicalUpdates: len(input.Transactions), PhysicalUpdates: physicalStateWriteCount(input), PhysicalStateDelta: append([]state.StateKV(nil), input.StateDelta...)}
	commutativeGroups := map[string]bool{}
	physical := 0
	for _, item := range input.Transactions {
		commutativeOnly := false
		nonCommutativeWrite := false
		for _, access := range item.AccessList {
			if access.Mode == tx.AccessCommutativeDelta {
				commutativeOnly = true
			} else if access.Mode == tx.AccessWrite || access.Mode == tx.AccessReadWrite {
				nonCommutativeWrite = true
			}
		}
		if commutativeOnly && !nonCommutativeWrite {
			for _, access := range item.AccessList {
				if access.Mode == tx.AccessCommutativeDelta {
					commutativeGroups[access.Key] = true
				}
			}
			continue
		}
		physical++
	}
	physical += len(commutativeGroups)
	if physical > 0 && physical < d.LogicalUpdates {
		d.PhysicalUpdates = maxInt(1, physical)
		d.Applied = true
		d.AggregationGroupID = fmt.Sprintf("%s:%d", input.ShardID, input.Height)
	}
	return d
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
