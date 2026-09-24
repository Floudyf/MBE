package v5

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	calvinStatefulRoutingID        = "calvin_global_routing"
	calvinStatelessRoutingID       = "stateless_calvin_global_routing"
	calvinAdmissionID              = "calvin_declared_access_admission"
	calvinExecutionID              = "calvin_execution"
	calvinSchedulerID              = "calvin_deterministic_scheduler"
	calvinStatelessSchedulerID     = "stateless_calvin_deterministic_scheduler"
	calvinStatefulExecutorID       = "calvin_block_executor"
	calvinStatelessExecutorID      = "stateless_calvin_block_executor"
	calvinStatefulStateAccessID    = "calvin_partition_state_access"
	calvinStatelessStateAccessID   = "stateless_calvin_state_access"
	calvinStateStorageID           = "calvin_partition_state_store"
	calvinCrossPartitionID         = "calvin_no_2pc_coordinator"
	calvinStatefulMode             = "stateful"
	calvinStatelessMode            = "stateless"
	calvinExecutionEngineVersion   = "3.0.0"
	calvinStatelessEngineVersion   = "4.0.0"
	calvinConsensusPlanAlgorithmID = "calvin_consensus_version_plan_v1"
)

// ExecutionShardStorageIdentityCapability lets an algorithm keep one consensus
// ordering domain while persisting state under its logical execution partition.
// Existing storage plugins do not implement it, so their historical semantics
// are unchanged.
type ExecutionShardStorageIdentityCapability interface {
	StateStoragePlugin
	UseExecutionShardStorageIdentity() bool
}

type calvinGlobalRouting struct{ basicPlugin }

func (p calvinGlobalRouting) Route(input RoutingInput) RoutingDecision {
	// Preserve the logical workload/execution route (source shard when supplied,
	// otherwise deterministic state-key hash). client.go separately maps this
	// logical shard to Calvin's single physical PBFT ordering domain. Returning
	// the first shard here would silently collapse every workload to s0.
	decision := hashRouting{p.basicPlugin}.Route(input)
	decision.Reason = "calvin_global_ordering_logical_route"
	return decision
}

func (p calvinGlobalRouting) CrossShardFinalityMode() string {
	return CrossShardFinalityCalvinGlobalCommit
}

// statelessCalvinGlobalRouting keeps Calvin's single global ordering domain,
// but opts into only the generic deterministic state-home/execution metadata
// required for physical remote fetch/writeback. It deliberately does not
// advertise MetaTrack native StateReady/frontier/admission semantics.
type statelessCalvinGlobalRouting struct{ basicPlugin }

func (p statelessCalvinGlobalRouting) Route(input RoutingInput) RoutingDecision {
	decision := hashRouting{p.basicPlugin}.Route(input)
	decision.Reason = "stateless_calvin_source_or_state_hash_execution"
	return decision
}

func (p statelessCalvinGlobalRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	// Calvin's declared-access contract is the signed transaction AccessList.
	// Layered datasets may additionally expose SchedulingAccessList for MetaTrack
	// planning; that wider scheduling-only surface must never change Calvin state
	// homes, remote-access counts, or exact-version dependencies.
	records := append([]WorkloadRecord(nil), input.Records...)
	for index := range records {
		records[index].SchedulingAccessList = nil
	}
	base := statelessHashRouting{basicPlugin: p.basicPlugin}
	input.Records = records
	plan := base.PlanBatch(input)
	plan.TransactionPolicy = "stateless_calvin_signed_access_source_hash_v1"
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}
func (p statelessCalvinGlobalRouting) StatelessDirectExecution() bool     { return true }
func (p statelessCalvinGlobalRouting) BindExecutionRoutingMetadata() bool { return true }
func (p statelessCalvinGlobalRouting) BindBatchProjectionMetadata() bool  { return false }
func (p statelessCalvinGlobalRouting) BatchExecutionPlanAlgorithmID() string {
	return "stateless_calvin_batch_execution_plan_v1"
}
func (p statelessCalvinGlobalRouting) SignedBatchExecutionPlan() bool  { return false }
func (p statelessCalvinGlobalRouting) NativeVersionedStateReady() bool { return false }
func (p statelessCalvinGlobalRouting) StatelessVersionAdmission() bool { return false }
func (p statelessCalvinGlobalRouting) CrossShardFinalityMode() string {
	return CrossShardFinalityCalvinGlobalCommit
}

type calvinAdmission struct{ basicPlugin }

func (p calvinAdmission) Admit(item tx.SignedTransaction) error {
	if err := tx.Verify(item); err != nil {
		return err
	}
	if len(item.AccessList) == 0 {
		return fmt.Errorf("calvin_access_violation: signed AccessList is required")
	}
	seen := map[string]bool{}
	for _, access := range item.AccessList {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			return fmt.Errorf("calvin_access_violation: empty access key")
		}
		if seen[key] {
			return fmt.Errorf("calvin_access_violation: duplicate access key %s", key)
		}
		seen[key] = true
		switch access.Mode {
		case tx.AccessRead, tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		default:
			return fmt.Errorf("calvin_access_violation: unsupported access mode %s for %s", access.Mode, key)
		}
	}
	return nil
}

type calvinExecution struct{ basicPlugin }

func (p calvinExecution) Classify(item tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "calvin", Reason: "global_order_deterministic_locking"}
}

type calvinScheduler struct{ basicPlugin }

func (p calvinScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}

func (p calvinScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	out := ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
	for _, item := range items {
		out.Events = append(out.Events, ScheduleEvent{
			TxID:           item.TxID,
			Track:          "calvin",
			QueueName:      "calvin_global_order",
			DecisionReason: "pbft_bound_global_serial_order",
			LocalExecution: true,
		})
	}
	return out
}

type statelessCalvinScheduler struct{ calvinScheduler }

type calvinConsensusVersionBinding struct {
	TxID          string                      `json:"tx_id"`
	OriginalIndex int                         `json:"original_index"`
	Dependencies  []tx.StateVersionDependency `json:"dependencies"`
}

type calvinConsensusPlan struct {
	Version                 string                          `json:"version"`
	AlgorithmID             string                          `json:"algorithm_id"`
	BlockHeight             uint64                          `json:"block_height"`
	OrderingDomain          string                          `json:"ordering_domain"`
	OrderedTransactionIDs   []string                        `json:"ordered_transaction_ids"`
	VersionBindings         []calvinConsensusVersionBinding `json:"version_bindings"`
	VersionBindingCount     int                             `json:"version_binding_count"`
	BlockStartReadSemantics string                          `json:"block_start_read_semantics"`
	PlanDigest              string                          `json:"plan_digest"`
}

func calvinConsensusVersion(height uint64, index int) (uint64, error) {
	if height > uint64(^uint32(0)) {
		return 0, fmt.Errorf("calvin block height exceeds consensus version namespace: %d", height)
	}
	if index < 0 || uint64(index+1) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("calvin transaction index exceeds consensus version namespace: %d", index)
	}
	return (height << 32) | uint64(index+1), nil
}

func buildCalvinConsensusPlan(block realblock.Block) (calvinConsensusPlan, error) {
	plan := calvinConsensusPlan{
		Version:                 "1.0.0",
		AlgorithmID:             calvinConsensusPlanAlgorithmID,
		BlockHeight:             block.Height,
		OrderingDomain:          block.ShardID,
		OrderedTransactionIDs:   transactionIDs(block.TxList),
		BlockStartReadSemantics: "current_committed_home_state_at_block_start",
	}
	lastWriter := map[string]uint64{}
	for index, item := range block.TxList {
		accesses, err := calvinCanonicalAccesses(item.AccessList)
		if err != nil {
			return calvinConsensusPlan{}, err
		}
		version, err := calvinConsensusVersion(block.Height, index)
		if err != nil {
			return calvinConsensusPlan{}, err
		}
		binding := calvinConsensusVersionBinding{TxID: item.TxID, OriginalIndex: index}
		for _, access := range accesses {
			dep := tx.StateVersionDependency{Key: access.Key, RequiredVersion: lastWriter[access.Key]}
			if calvinIsWriteMode(access.Mode) {
				dep.ProducedVersion = version
			}
			binding.Dependencies = append(binding.Dependencies, dep)
		}
		for _, access := range accesses {
			if calvinIsWriteMode(access.Mode) {
				lastWriter[access.Key] = version
			}
		}
		plan.VersionBindingCount += len(binding.Dependencies)
		plan.VersionBindings = append(plan.VersionBindings, binding)
	}
	plan.PlanDigest = stableJSONDigest(calvinConsensusPlanDigestProjection(plan))
	return plan, nil
}

func calvinConsensusPlanDigestProjection(plan calvinConsensusPlan) any {
	copy := plan
	copy.PlanDigest = ""
	return copy
}

func decodeCalvinConsensusPlan(block realblock.Block) (calvinConsensusPlan, error) {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != calvinConsensusPlanAlgorithmID {
		return calvinConsensusPlan{}, fmt.Errorf("calvin consensus version plan missing")
	}
	if stableTextDigest(string(block.ExecutionPlan.Payload)) != block.ExecutionPlan.PayloadDigest {
		return calvinConsensusPlan{}, fmt.Errorf("calvin consensus version plan payload digest mismatch")
	}
	var plan calvinConsensusPlan
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &plan); err != nil {
		return calvinConsensusPlan{}, fmt.Errorf("decode calvin consensus version plan: %w", err)
	}
	if plan.AlgorithmID != calvinConsensusPlanAlgorithmID || plan.PlanDigest != block.ExecutionPlan.PlanDigest {
		return calvinConsensusPlan{}, fmt.Errorf("calvin consensus version plan envelope mismatch")
	}
	return plan, nil
}

func calvinConsensusDependenciesByIndex(block realblock.Block) ([][]tx.StateVersionDependency, calvinConsensusPlan, error) {
	plan, err := decodeCalvinConsensusPlan(block)
	if err != nil {
		return nil, calvinConsensusPlan{}, err
	}
	if len(plan.OrderedTransactionIDs) != len(block.TxList) || len(plan.VersionBindings) != len(block.TxList) {
		return nil, calvinConsensusPlan{}, fmt.Errorf("calvin consensus version plan transaction count mismatch")
	}
	out := make([][]tx.StateVersionDependency, len(block.TxList))
	for index, item := range block.TxList {
		if plan.OrderedTransactionIDs[index] != item.TxID {
			return nil, calvinConsensusPlan{}, fmt.Errorf("calvin consensus version order mismatch at index %d", index)
		}
		binding := plan.VersionBindings[index]
		if binding.OriginalIndex != index || binding.TxID != item.TxID {
			return nil, calvinConsensusPlan{}, fmt.Errorf("calvin consensus version binding mismatch at index %d", index)
		}
		out[index] = append([]tx.StateVersionDependency(nil), binding.Dependencies...)
	}
	return out, plan, nil
}

func (p statelessCalvinScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildCalvinConsensusPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   calvinConsensusPlanAlgorithmID,
		PayloadDigest: stableTextDigest(string(raw)),
		PlanDigest:    plan.PlanDigest,
		Payload:       raw,
	}
	schedule := p.Schedule(block.TxList, nil)
	return ConsensusExecutionPlanningResult{Block: block, Events: schedule.Events}, nil
}

func (p statelessCalvinScheduler) VerifyBlockPlan(block realblock.Block) error {
	supplied, err := decodeCalvinConsensusPlan(block)
	if err != nil {
		return err
	}
	expected, err := buildCalvinConsensusPlan(block)
	if err != nil {
		return err
	}
	if supplied.PlanDigest != expected.PlanDigest || block.ExecutionPlan.PlanDigest != expected.PlanDigest {
		return fmt.Errorf("calvin consensus version plan digest mismatch")
	}
	if stableJSONDigest(calvinConsensusPlanDigestProjection(supplied)) != stableJSONDigest(calvinConsensusPlanDigestProjection(expected)) {
		return fmt.Errorf("calvin consensus version plan semantic mismatch")
	}
	return nil
}

type calvinStateAccess struct {
	builtinStateAccess
	mode string
}

func (p calvinStateAccess) AccessMode() string { return p.mode }

type calvinPartitionStateStore struct{ builtinStateStorage }

func (p calvinPartitionStateStore) UseExecutionShardStorageIdentity() bool { return true }

// The methods below are explicit wrappers so this type continues to satisfy
// StateStoragePlugin even if embedding behavior changes later.
func (p calvinPartitionStateStore) Durable() bool { return p.builtinStateStorage.Durable() }
func (p calvinPartitionStateStore) Open(input StateStorageInput) (*state.DB, *storage.BlockStore, error) {
	return p.builtinStateStorage.Open(input)
}

type calvinCrossPartition struct{ basicPlugin }

func (p calvinCrossPartition) IsCrossShard(tx.SignedTransaction) bool { return false }
func (p calvinCrossPartition) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "CalvinDeterministicLock", Success: true}
}
func (p calvinCrossPartition) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "CalvinLocalWrite", Success: true}
}
func (p calvinCrossPartition) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "CalvinLockRelease", Success: true}
}
func (p calvinCrossPartition) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "CalvinExecutionFailure", Success: false, Error: reason}
}
func (p calvinCrossPartition) BuildRelay(input CrossShardRelayInput) Relay {
	return Relay{Tx: input.Tx, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}
func (p calvinCrossPartition) BuildFinalize(input CrossShardFinalizeInput) Finalize {
	return Finalize{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

func registerCalvinPlugins(register func(string, string, Factory)) {
	register("routing", calvinStatefulRoutingID, func(c map[string]any) (Plugin, error) {
		return calvinGlobalRouting{basicPlugin: makeBasic("routing", calvinStatefulRoutingID, c)}, nil
	})
	register("routing", calvinStatelessRoutingID, func(c map[string]any) (Plugin, error) {
		return statelessCalvinGlobalRouting{basicPlugin: makeBasic("routing", calvinStatelessRoutingID, c)}, nil
	})
	register("transaction_admission", calvinAdmissionID, func(c map[string]any) (Plugin, error) {
		return calvinAdmission{makeBasic("transaction_admission", calvinAdmissionID, c)}, nil
	})
	register("execution", calvinExecutionID, func(c map[string]any) (Plugin, error) {
		return calvinExecution{makeBasic("execution", calvinExecutionID, c)}, nil
	})
	register("scheduler", calvinSchedulerID, func(c map[string]any) (Plugin, error) {
		return calvinScheduler{makeBasic("scheduler", calvinSchedulerID, c)}, nil
	})
	register("scheduler", calvinStatelessSchedulerID, func(c map[string]any) (Plugin, error) {
		return statelessCalvinScheduler{calvinScheduler{makeBasic("scheduler", calvinStatelessSchedulerID, c)}}, nil
	})
	register("block_executor", calvinStatefulExecutorID, func(c map[string]any) (Plugin, error) {
		return calvinBlockExecutor{basicPlugin: makeBasic("block_executor", calvinStatefulExecutorID, c), mode: calvinStatefulMode}, nil
	})
	register("block_executor", calvinStatelessExecutorID, func(c map[string]any) (Plugin, error) {
		return calvinBlockExecutor{basicPlugin: makeBasic("block_executor", calvinStatelessExecutorID, c), mode: calvinStatelessMode}, nil
	})
	register("state_access", calvinStatefulStateAccessID, func(c map[string]any) (Plugin, error) {
		return calvinStateAccess{builtinStateAccess: builtinStateAccess{makeBasic("state_access", calvinStatefulStateAccessID, c)}, mode: "calvin_partition_local_plus_read_result"}, nil
	})
	register("state_access", calvinStatelessStateAccessID, func(c map[string]any) (Plugin, error) {
		return calvinStateAccess{builtinStateAccess: builtinStateAccess{makeBasic("state_access", calvinStatelessStateAccessID, c)}, mode: "stateless_calvin_remote_home_fetch_writeback"}, nil
	})
	register("state_storage", calvinStateStorageID, func(c map[string]any) (Plugin, error) {
		return calvinPartitionStateStore{builtinStateStorage{makeBasic("state_storage", calvinStateStorageID, c)}}, nil
	})
	register("cross_shard", calvinCrossPartitionID, func(c map[string]any) (Plugin, error) {
		return calvinCrossPartition{makeBasic("cross_shard", calvinCrossPartitionID, c)}, nil
	})
}

func validateCalvinPluginCombination(plugins RuntimePlugins) error {
	routingID := ""
	if plugins.Routing != nil {
		routingID = plugins.Routing.ID()
	}
	executorID := ""
	if plugins.BlockExecutor != nil {
		executorID = plugins.BlockExecutor.ID()
	}
	calvinSelected := routingID == calvinStatefulRoutingID || routingID == calvinStatelessRoutingID || executorID == calvinStatefulExecutorID || executorID == calvinStatelessExecutorID
	if !calvinSelected {
		return nil
	}
	stateful := routingID == calvinStatefulRoutingID || executorID == calvinStatefulExecutorID
	stateless := routingID == calvinStatelessRoutingID || executorID == calvinStatelessExecutorID
	if stateful && stateless {
		return fmt.Errorf("Calvin stateful and stateless profiles may not be mixed")
	}
	requiredSchedulerID := calvinSchedulerID
	if stateless {
		requiredSchedulerID = calvinStatelessSchedulerID
	}
	required := map[string]string{
		"transaction_admission": calvinAdmissionID,
		"execution":             calvinExecutionID,
		"scheduler":             requiredSchedulerID,
		"state_storage":         calvinStateStorageID,
		"cross_shard":           calvinCrossPartitionID,
		"consensus":             "pbft_style_consensus",
		"network":               "localhost_tcp_typed_network",
		"commit":                "normal_commit",
	}
	selected := map[string]Plugin{
		"transaction_admission": plugins.Admission,
		"execution":             plugins.Execution,
		"scheduler":             plugins.Scheduler,
		"state_storage":         plugins.StateStorage,
		"cross_shard":           plugins.CrossShard,
		"consensus":             plugins.Consensus,
		"network":               plugins.Network,
		"commit":                plugins.Commit,
	}
	for category, id := range required {
		plugin := selected[category]
		if plugin == nil || plugin.ID() != id {
			actual := "<nil>"
			if plugin != nil {
				actual = plugin.ID()
			}
			return fmt.Errorf("Calvin requires %s:%s, got %s", category, id, actual)
		}
	}
	if stateful {
		if routingID != calvinStatefulRoutingID || executorID != calvinStatefulExecutorID || plugins.StateAccess == nil || plugins.StateAccess.ID() != calvinStatefulStateAccessID {
			return fmt.Errorf("Stateful Calvin requires stateful routing, state access, and block executor together")
		}
	} else {
		if routingID != calvinStatelessRoutingID || executorID != calvinStatelessExecutorID || plugins.StateAccess == nil || plugins.StateAccess.ID() != calvinStatelessStateAccessID {
			return fmt.Errorf("Stateless Calvin requires stateless routing, state access, and block executor together")
		}
	}
	return nil
}

func calvinExecutionShardIDsFromPlan(plan Plan) []string {
	seen := map[string]bool{}
	for _, node := range plan.NodeConfigs {
		if shardID := strings.TrimSpace(effectiveExecutionShardID(node)); shardID != "" {
			seen[shardID] = true
		}
	}
	out := make([]string, 0, len(seen))
	for shardID := range seen {
		out = append(out, shardID)
	}
	sort.Strings(out)
	return out
}

var _ RoutingPlugin = calvinGlobalRouting{}
var _ CrossShardFinalityCapability = calvinGlobalRouting{}
var _ AdmissionPlugin = calvinAdmission{}
var _ ExecutionPlugin = calvinExecution{}
var _ SchedulerPlugin = calvinScheduler{}
var _ ConsensusExecutionPlanner = statelessCalvinScheduler{}
var _ StateAccessPlugin = calvinStateAccess{}
var _ StateStoragePlugin = calvinPartitionStateStore{}
var _ ExecutionShardStorageIdentityCapability = calvinPartitionStateStore{}
var _ CrossShardPlugin = calvinCrossPartition{}
