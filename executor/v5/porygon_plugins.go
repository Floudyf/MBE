package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	porygonRoutingID       = "porygon_stateless_routing"
	porygonBlockProducerID = "porygon_transaction_block_producer"
	porygonExecutionID     = "porygon_execution"
	porygonSchedulerID     = "porygon_pipeline_scheduler"
	porygonBlockExecutorID = "porygon_block_executor"
	porygonStateAccessID   = "porygon_remote_state_access"
	porygonCrossShardID    = "porygon_cross_shard_coordinator"

	porygonProposalEvidenceID = "porygon_transaction_block_v1"
	porygonPlanAlgorithmID    = "porygon_3d_parallelism_plan_v1"
)

type porygonTransactionBlockEvidence struct {
	Version             string   `json:"version"`
	ShardID             string   `json:"shard_id"`
	Height              uint64   `json:"height"`
	TransactionCount    int      `json:"transaction_count"`
	TransactionIDs      []string `json:"transaction_ids"`
	TransactionRoot     string   `json:"transaction_root"`
	AccessRoot          string   `json:"access_root"`
	FullBodyDigest      string   `json:"full_body_digest"`
	WitnessPolicy       string   `json:"witness_policy"`
	WitnessThreshold    int      `json:"witness_threshold"`
	DataAvailabilityRef string   `json:"data_availability_reference"`
}

type porygonTxAssignment struct {
	TxID           string   `json:"tx_id"`
	OriginalIndex  int      `json:"original_index"`
	ExecutionShard int      `json:"execution_shard"`
	InvolvedShards []int    `json:"involved_shards"`
	CrossShard     bool     `json:"cross_shard"`
	Wave           int      `json:"wave"`
	LockedKeys     []string `json:"locked_keys,omitempty"`
}

type porygonPipelineStage struct {
	BatchHeight uint64 `json:"batch_height"`
	Stage       string `json:"stage"`
	LogicalSlot uint64 `json:"logical_slot"`
	Committee   string `json:"committee"`
}

type porygonExecutionPlan struct {
	Version                  string                 `json:"version"`
	AlgorithmID              string                 `json:"algorithm_id"`
	BlockHeight              uint64                 `json:"block_height"`
	TransactionBlockDigest   string                 `json:"transaction_block_digest"`
	TransactionRoot          string                 `json:"transaction_root"`
	AccessRoot               string                 `json:"access_root"`
	WitnessPolicy            string                 `json:"witness_policy"`
	WitnessThreshold         int                    `json:"witness_threshold"`
	ExecutionShardCount      int                    `json:"execution_shard_count"`
	ExecutionCommitteeCount  int                    `json:"execution_committee_count"`
	CrossBatchWitness        bool                   `json:"cross_batch_witness"`
	PipelineEnabled          bool                   `json:"pipeline_enabled"`
	Assignments              []porygonTxAssignment  `json:"assignments"`
	Waves                    [][]string             `json:"waves"`
	SerializationOrder       []string               `json:"serialization_order"`
	Pipeline                 []porygonPipelineStage `json:"pipeline"`
	IntraShardTransactionCnt int                    `json:"intra_shard_transaction_count"`
	CrossShardTransactionCnt int                    `json:"cross_shard_transaction_count"`
	SingleShardExecutionCnt  int                    `json:"single_shard_execution_count"`
	MultiShardUpdateCnt      int                    `json:"multi_shard_update_count"`
	StateLockCount           int                    `json:"state_lock_count"`
	PlanDigest               string                 `json:"plan_digest"`
}

type porygonBlockProducer struct{ basicPlugin }
type porygonExecution struct{ basicPlugin }
type porygonScheduler struct{ basicPlugin }
type porygonStateAccess struct{ builtinStateAccess }
type porygonCrossShard struct{ basicPlugin }

// ProposalEvidenceVerifier is an opt-in block-producer validation hook.  The
// shared runtime uses it only when the selected producer implements it, so the
// existing Aria/Groundhog validation path is unchanged.
type ProposalEvidenceVerifier interface {
	VerifyProposalEvidence(realblock.Block) error
}

type porygonStatelessRouting struct{ basicPlugin }

func (p porygonStatelessRouting) Route(input RoutingInput) RoutingDecision {
	return statelessHashRouting{p.basicPlugin}.Route(input)
}
func (p porygonStatelessRouting) StatelessDirectExecution() bool     { return true }
func (p porygonStatelessRouting) BindExecutionRoutingMetadata() bool { return false }
func (p porygonStatelessRouting) BindBatchProjectionMetadata() bool  { return false }
func (p porygonStatelessRouting) BatchExecutionPlanAlgorithmID() string {
	return "porygon_stateless_batch_execution_plan_v1"
}
func (p porygonStatelessRouting) SignedBatchExecutionPlan() bool  { return false }
func (p porygonStatelessRouting) NativeVersionedStateReady() bool { return false }
func (p porygonStatelessRouting) StatelessVersionAdmission() bool { return false }
func (p porygonStatelessRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	// Porygon is an independent baseline.  The current MetaTrack layered schema
	// may carry a SchedulingAccessList beside the real execution AccessList.
	// Never let that MetaTrack-only planning layer change Porygon placement or
	// access evidence: sanitize it before reusing the stateless-home planner.
	records := append([]WorkloadRecord(nil), input.Records...)
	for index := range records {
		records[index].SchedulingAccessList = nil
		records[index].SchedulingAccessDigest = ""
		records[index].SchedulingAccessSchema = ""
		records[index].SchedulingAccessSource = ""
	}
	input.Records = records
	plan := statelessHashRouting{p.basicPlugin}.PlanBatch(input)
	plan.PlacementPolicy = "porygon_state_home_hash_v1"
	plan.TransactionPolicy = "porygon_single_execution_shard_v1"
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}

func (p porygonBlockProducer) BlockSize() int {
	if value := intValue(p.config["transaction_block_size"]); value > 0 {
		return value
	}
	if value := intValue(p.config["block_size"]); value > 0 {
		return value
	}
	return 100
}
func (p porygonBlockProducer) Interval() time.Duration {
	if value := intValue(p.config["interval_ms"]); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 75 * time.Millisecond
}
func (p porygonBlockProducer) ShouldProduce(input BlockProductionInput) bool {
	return (input.Pool != nil && input.Pool.Len() > 0) || input.SystemDeltaReady
}
func (p porygonBlockProducer) BuildCandidate(input BlockProductionInput) (realblock.Block, error) {
	if input.Proposer == nil || input.Pool == nil {
		return realblock.Block{}, fmt.Errorf("porygon transaction-block producer requires proposer and pool")
	}
	limit := input.Limit
	if limit <= 0 {
		limit = p.BlockSize()
	}
	reserved := input.Pool.ReserveReady(limit)
	if len(reserved) == 0 {
		return realblock.Block{}, fmt.Errorf("empty_mempool")
	}
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	candidate, err := input.Proposer.BuildFromReserved(reserved, now)
	if err != nil {
		input.Pool.ReleaseReserved(reserved)
		return realblock.Block{}, err
	}
	evidence := buildPorygonTransactionBlockEvidence(candidate, porygonWitnessThreshold(p.config))
	if err := attachProposalEvidence(&candidate, porygonProposalEvidenceID, evidence); err != nil {
		input.Pool.ReleaseReserved(reserved)
		return realblock.Block{}, err
	}
	return candidate, nil
}

func (p porygonBlockProducer) VerifyProposalEvidence(block realblock.Block) error {
	envelope := block.ProposalEvidence
	if envelope == nil {
		return fmt.Errorf("porygon transaction block evidence missing")
	}
	if envelope.AlgorithmID != porygonProposalEvidenceID {
		return fmt.Errorf("porygon proposal evidence algorithm mismatch: %s", envelope.AlgorithmID)
	}
	if envelope.PayloadDigest == "" || len(envelope.Payload) == 0 {
		return fmt.Errorf("porygon proposal evidence envelope is incomplete")
	}
	if stableTextDigest(string(envelope.Payload)) != envelope.PayloadDigest {
		return fmt.Errorf("porygon proposal evidence payload digest mismatch")
	}
	_, err := decodePorygonTransactionBlockEvidence(block)
	return err
}

func buildPorygonTransactionBlockEvidence(block realblock.Block, threshold int) porygonTransactionBlockEvidence {
	txIDs := transactionIDs(block.TxList)
	return porygonTransactionBlockEvidence{
		Version:             "1.0.0",
		ShardID:             block.ShardID,
		Height:              block.Height,
		TransactionCount:    len(block.TxList),
		TransactionIDs:      txIDs,
		TransactionRoot:     stableJSONDigest(txIDs),
		AccessRoot:          porygonAccessRoot(block.TxList),
		FullBodyDigest:      stableJSONDigest(block.TxList),
		WitnessPolicy:       "full_body_validator_recompute_before_pbft_vote",
		WitnessThreshold:    threshold,
		DataAvailabilityRef: "proposal_body_present_and_recomputed_by_each_voting_validator",
	}
}

func decodePorygonTransactionBlockEvidence(block realblock.Block) (porygonTransactionBlockEvidence, error) {
	var evidence porygonTransactionBlockEvidence
	if block.ProposalEvidence == nil || block.ProposalEvidence.AlgorithmID != porygonProposalEvidenceID {
		return evidence, fmt.Errorf("porygon transaction block evidence missing")
	}
	if err := json.Unmarshal(block.ProposalEvidence.Payload, &evidence); err != nil {
		return evidence, fmt.Errorf("decode porygon transaction block evidence: %w", err)
	}
	expected := buildPorygonTransactionBlockEvidence(block, evidence.WitnessThreshold)
	if evidence.ShardID != expected.ShardID || evidence.Height != expected.Height || evidence.TransactionCount != expected.TransactionCount ||
		evidence.TransactionRoot != expected.TransactionRoot || evidence.AccessRoot != expected.AccessRoot || evidence.FullBodyDigest != expected.FullBodyDigest ||
		!sameStringList(evidence.TransactionIDs, expected.TransactionIDs) {
		return evidence, fmt.Errorf("porygon transaction block evidence mismatch")
	}
	if evidence.WitnessThreshold < 1 {
		return evidence, fmt.Errorf("porygon witness threshold must be positive")
	}
	return evidence, nil
}

func (p porygonExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "porygon", Reason: "global_order_plus_execution_subcommittee"}
}

func (p porygonScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p porygonScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	result := ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
	for _, item := range items {
		result.Events = append(result.Events, ScheduleEvent{TxID: item.TxID, Track: "porygon", QueueName: "porygon_ordered", DecisionReason: "ordering_committee_global_order", LocalExecution: true})
	}
	return result
}
func (p porygonScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildPorygonPlan(block, p.config)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: porygonPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p porygonScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != porygonPlanAlgorithmID {
		return fmt.Errorf("porygon execution plan missing")
	}
	var supplied porygonExecutionPlan
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &supplied); err != nil {
		return fmt.Errorf("decode porygon plan: %w", err)
	}
	expected, err := buildPorygonPlan(block, p.config)
	if err != nil {
		return err
	}
	if supplied.PlanDigest != expected.PlanDigest || block.ExecutionPlan.PlanDigest != expected.PlanDigest {
		return fmt.Errorf("porygon execution plan digest mismatch")
	}
	if stableJSONDigest(porygonPlanDigestProjection(supplied)) != stableJSONDigest(porygonPlanDigestProjection(expected)) {
		return fmt.Errorf("porygon execution plan semantic mismatch")
	}
	return nil
}

func buildPorygonPlan(block realblock.Block, config map[string]any) (porygonExecutionPlan, error) {
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return porygonExecutionPlan{}, err
	}
	shardCount := intValue(config["execution_shard_count"])
	if shardCount < 1 {
		shardCount = 4
	}
	committeeCount := intValue(config["execution_committee_count"])
	if committeeCount < 1 {
		committeeCount = 3
	}
	pipelineEnabled := porygonBool(config, "pipeline_enabled", true)
	crossBatchWitness := porygonBool(config, "cross_batch_witness", true)

	plan := porygonExecutionPlan{
		Version:                 "1.0.0",
		AlgorithmID:             porygonPlanAlgorithmID,
		BlockHeight:             block.Height,
		TransactionBlockDigest:  evidence.FullBodyDigest,
		TransactionRoot:         evidence.TransactionRoot,
		AccessRoot:              evidence.AccessRoot,
		WitnessPolicy:           evidence.WitnessPolicy,
		WitnessThreshold:        evidence.WitnessThreshold,
		ExecutionShardCount:     shardCount,
		ExecutionCommitteeCount: committeeCount,
		CrossBatchWitness:       crossBatchWitness,
		PipelineEnabled:         pipelineEnabled,
	}

	prior := []porygonTxAssignment{}
	maxWave := -1
	lastCrossWave := -1
	waves := map[int][]string{}
	for index, item := range block.TxList {
		accesses := porygonCanonicalAccesses(item)
		involvedSet := map[int]bool{}
		locked := []string{}
		for _, access := range accesses {
			if access.Key == "" {
				continue
			}
			involvedSet[porygonExecutionShard(access.Key, shardCount)] = true
			if isWriteMode(access.Mode) {
				locked = append(locked, access.Key)
			}
		}
		involved := make([]int, 0, len(involvedSet))
		for shard := range involvedSet {
			involved = append(involved, shard)
		}
		sort.Ints(involved)
		sort.Strings(locked)
		cross := len(involved) > 1
		executionShard := 0
		if len(involved) > 0 {
			executionShard = involved[0]
		}
		wave := 0
		if lastCrossWave >= 0 {
			wave = lastCrossWave + 1
		}
		for _, previous := range prior {
			if previous.Wave >= wave && porygonAssignmentsConflict(previous, item, block.TxList) {
				wave = previous.Wave + 1
			}
		}
		if cross {
			if maxWave >= wave {
				wave = maxWave + 1
			}
			lastCrossWave = wave
		}
		if wave > maxWave {
			maxWave = wave
		}
		assignment := porygonTxAssignment{TxID: item.TxID, OriginalIndex: index, ExecutionShard: executionShard, InvolvedShards: involved, CrossShard: cross, Wave: wave, LockedKeys: uniqueStrings(locked)}
		prior = append(prior, assignment)
		plan.Assignments = append(plan.Assignments, assignment)
		waves[wave] = append(waves[wave], item.TxID)
		plan.SerializationOrder = append(plan.SerializationOrder, item.TxID)
		if cross {
			plan.CrossShardTransactionCnt++
			plan.SingleShardExecutionCnt++
			plan.MultiShardUpdateCnt += len(involved)
			plan.StateLockCount += len(assignment.LockedKeys)
		} else {
			plan.IntraShardTransactionCnt++
		}
	}
	for wave := 0; wave <= maxWave; wave++ {
		plan.Waves = append(plan.Waves, append([]string(nil), waves[wave]...))
	}
	plan.Pipeline = porygonPipelineForHeight(block.Height, committeeCount, pipelineEnabled, crossBatchWitness)
	plan.PlanDigest = stableJSONDigest(porygonPlanDigestProjection(plan))
	return plan, nil
}

func porygonPlanDigestProjection(plan porygonExecutionPlan) any {
	copy := plan
	copy.PlanDigest = ""
	return copy
}

func porygonPipelineForHeight(height uint64, committeeCount int, enabled, crossBatchWitness bool) []porygonPipelineStage {
	if !enabled {
		return []porygonPipelineStage{
			{BatchHeight: height, Stage: "witness", LogicalSlot: height*4 + 0, Committee: porygonECName(height, committeeCount)},
			{BatchHeight: height, Stage: "ordering", LogicalSlot: height*4 + 1, Committee: "OC"},
			{BatchHeight: height, Stage: "execution", LogicalSlot: height*4 + 2, Committee: porygonECName(height, committeeCount)},
			{BatchHeight: height, Stage: "commit", LogicalSlot: height*4 + 3, Committee: "OC"},
		}
	}
	base := height
	stages := []porygonPipelineStage{
		{BatchHeight: height, Stage: "witness", LogicalSlot: base, Committee: porygonECName(height, committeeCount)},
		{BatchHeight: height, Stage: "ordering", LogicalSlot: base + 1, Committee: "OC"},
		{BatchHeight: height, Stage: "execution", LogicalSlot: base + 2, Committee: porygonECName(height, committeeCount)},
		{BatchHeight: height, Stage: "commit", LogicalSlot: base + 3, Committee: "OC"},
	}
	if crossBatchWitness {
		stages = append(stages, porygonPipelineStage{BatchHeight: height + 1, Stage: "cross_batch_witness", LogicalSlot: base + 1, Committee: porygonECName(height, committeeCount)})
	}
	return stages
}

func porygonECName(height uint64, count int) string {
	if count < 1 {
		count = 1
	}
	return fmt.Sprintf("EC%d", int(height%uint64(count))+1)
}

func porygonAssignmentsConflict(previous porygonTxAssignment, current tx.SignedTransaction, blockItems []tx.SignedTransaction) bool {
	if previous.CrossShard {
		return true
	}
	var prior tx.SignedTransaction
	if previous.OriginalIndex >= 0 && previous.OriginalIndex < len(blockItems) {
		prior = blockItems[previous.OriginalIndex]
	}
	for _, left := range porygonCanonicalAccesses(prior) {
		for _, right := range porygonCanonicalAccesses(current) {
			if accessItemsConflict(left, right) {
				return true
			}
		}
	}
	return false
}

func porygonCanonicalAccesses(item tx.SignedTransaction) []tx.AccessItem {
	// Porygon must bind the execution AccessList itself, never MetaTrack's
	// SchedulingAccessList overlay introduced by the declared-access frontier.
	accesses := append([]tx.AccessItem(nil), item.AccessList...)
	if len(accesses) == 0 {
		for _, key := range item.StateKeys {
			accesses = append(accesses, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "legacy_state_key"})
		}
	}
	sort.Slice(accesses, func(i, j int) bool {
		if accesses[i].Key != accesses[j].Key {
			return accesses[i].Key < accesses[j].Key
		}
		return accesses[i].Mode < accesses[j].Mode
	})
	return accesses
}

func porygonAccessRoot(items []tx.SignedTransaction) string {
	rows := make([]map[string]any, 0, len(items))
	for _, item := range items {
		rows = append(rows, map[string]any{"tx_id": item.TxID, "accesses": porygonCanonicalAccesses(item)})
	}
	return stableJSONDigest(rows)
}

func porygonExecutionShard(key string, count int) int {
	if count < 1 {
		return 0
	}
	return stableKey([]string{strings.ToLower(strings.TrimSpace(key))}) % count
}

func porygonWitnessThreshold(config map[string]any) int {
	if value := intValue(config["witness_threshold"]); value > 0 {
		return value
	}
	return 1
}

func porygonBool(config map[string]any, key string, fallback bool) bool {
	if config == nil {
		return fallback
	}
	switch value := config[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return fallback
}

func (p porygonStateAccess) AccessMode() string { return "porygon_remote_stateless" }

func (p porygonCrossShard) IsCrossShard(item tx.SignedTransaction) bool {
	if strings.HasPrefix(item.Payload, "v5_cross:") {
		return true
	}
	count := intValue(p.config["execution_shard_count"])
	if count < 1 {
		count = 4
	}
	shards := map[int]bool{}
	for _, access := range porygonCanonicalAccesses(item) {
		if access.Key != "" {
			shards[porygonExecutionShard(access.Key, count)] = true
		}
	}
	return len(shards) > 1
}
func (p porygonCrossShard) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonStateLock", Success: true}
}
func (p porygonCrossShard) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonMultiShardUpdate", Success: true}
}
func (p porygonCrossShard) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonUnlock", Success: true}
}
func (p porygonCrossShard) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonAbort", Success: false, Error: reason}
}
func (p porygonCrossShard) BuildRelay(input CrossShardRelayInput) Relay {
	return Relay{Tx: input.Tx, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}
func (p porygonCrossShard) BuildFinalize(input CrossShardFinalizeInput) Finalize {
	return Finalize{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

func registerPorygonPlugins(register func(string, string, Factory)) {
	register("routing", porygonRoutingID, func(c map[string]any) (Plugin, error) {
		return porygonStatelessRouting{makeBasic("routing", porygonRoutingID, c)}, nil
	})
	register("block_producer", porygonBlockProducerID, func(c map[string]any) (Plugin, error) {
		return porygonBlockProducer{makeBasic("block_producer", porygonBlockProducerID, c)}, nil
	})
	register("execution", porygonExecutionID, func(c map[string]any) (Plugin, error) {
		return porygonExecution{makeBasic("execution", porygonExecutionID, c)}, nil
	})
	register("scheduler", porygonSchedulerID, func(c map[string]any) (Plugin, error) {
		return porygonScheduler{makeBasic("scheduler", porygonSchedulerID, c)}, nil
	})
	register("block_executor", porygonBlockExecutorID, func(c map[string]any) (Plugin, error) {
		return porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, c)}, nil
	})
	register("state_access", porygonStateAccessID, func(c map[string]any) (Plugin, error) {
		return porygonStateAccess{builtinStateAccess{makeBasic("state_access", porygonStateAccessID, c)}}, nil
	})
	register("cross_shard", porygonCrossShardID, func(c map[string]any) (Plugin, error) {
		return porygonCrossShard{makeBasic("cross_shard", porygonCrossShardID, c)}, nil
	})
}

func validatePorygonPluginCombination(plugins RuntimePlugins) error {
	selected := []bool{
		plugins.Routing != nil && plugins.Routing.ID() == porygonRoutingID,
		plugins.BlockProducer != nil && plugins.BlockProducer.ID() == porygonBlockProducerID,
		plugins.Execution != nil && plugins.Execution.ID() == porygonExecutionID,
		plugins.Scheduler != nil && plugins.Scheduler.ID() == porygonSchedulerID,
		plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == porygonBlockExecutorID,
		plugins.StateAccess != nil && plugins.StateAccess.ID() == porygonStateAccessID,
		plugins.CrossShard != nil && plugins.CrossShard.ID() == porygonCrossShardID,
	}
	anySelected := false
	allSelected := true
	for _, value := range selected {
		anySelected = anySelected || value
		allSelected = allSelected && value
	}
	if !anySelected {
		return nil
	}
	if !allSelected {
		return fmt.Errorf("Porygon routing, block producer, execution, scheduler, block executor, state access, and cross-shard plugins must be selected together")
	}
	if plugins.Consensus == nil || plugins.Consensus.ID() != "pbft_style_consensus" {
		return fmt.Errorf("Porygon MBE fairness profile requires consensus:pbft_style_consensus")
	}
	if plugins.StateStorage == nil || plugins.StateStorage.ID() != "persistent_local_state_store" {
		return fmt.Errorf("Porygon requires state_storage:persistent_local_state_store")
	}
	if plugins.Commit == nil || plugins.Commit.ID() != "normal_commit" {
		return fmt.Errorf("Porygon requires commit:normal_commit")
	}
	return nil
}

// Compile-time interface assertions keep the additive integration honest.
var _ RoutingRuntimeCapabilities = porygonStatelessRouting{}
var _ BatchRoutingPlugin = porygonStatelessRouting{}
var _ BlockProducerPlugin = porygonBlockProducer{}
var _ ProposalEvidenceVerifier = porygonBlockProducer{}
var _ ExecutionPlugin = porygonExecution{}
var _ ConsensusExecutionPlanner = porygonScheduler{}
var _ StateAccessPlugin = porygonStateAccess{}
var _ CrossShardPlugin = porygonCrossShard{}
var _ = context.Background
