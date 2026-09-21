package v5

import (
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

	porygonProposalEvidenceID = "porygon_transaction_block_v2"
	porygonPlanAlgorithmID    = "porygon_3d_parallelism_plan_v3"
)

type porygonTransactionBlockEvidence struct {
	Version             string   `json:"version"`
	OrderingDomain      string   `json:"ordering_domain"`
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
	StateShards    []int    `json:"state_shards"`
	WriteShards    []int    `json:"write_shards"`
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
	OrderingDomain           string                 `json:"ordering_domain"`
	PhysicalShardCount       int                    `json:"physical_shard_count"`
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

// ProposalEvidenceVerifier is an opt-in block-producer validation hook used by
// the v7+ shared runtime. Existing Aria/Groundhog validation is untouched.
type ProposalEvidenceVerifier interface {
	VerifyProposalEvidence(realblock.Block) error
}

// porygonStatelessRouting deliberately implements only RoutingPlugin.
// It must not implement BatchRoutingPlugin or RoutingRuntimeCapabilities:
// those interfaces activate the shared MetaTrack/stateless remote-state CAS
// control plane, which is not Porygon's OC/ESC execution model.
type porygonStatelessRouting struct{ basicPlugin }

// MBE_PORYGON_UNIFIED_SHARD_V16_FINALITY_CAPABILITY_20260921:
// Workload-level cross-shard classification does not invoke the legacy
// relay/finalize protocol in Porygon. One global ordering-domain durable commit
// is the terminal protocol outcome. This does NOT make Porygon a
// RoutingRuntimeCapabilities or BatchRoutingPlugin implementation.
func (p porygonStatelessRouting) CrossShardFinalityMode() string {
	return CrossShardFinalityPorygonGlobalCommit
}

func (p porygonStatelessRouting) Route(input RoutingInput) RoutingDecision {
	decision := hashRouting{p.basicPlugin}.Route(input)
	decision.Reason = "porygon_execution_shard_route"
	return decision
}

// MBE_PORYGON_ESC_OWNERSHIP_TIMING_TRUTH_V19_20260921: common block_size
// is the authoritative formal-experiment control. transaction_block_size is
// retained only as a legacy fallback when block_size is genuinely absent.
func (p porygonBlockProducer) BlockSize() int {
	if value := intValue(p.config["block_size"]); value > 0 {
		return value
	}
	if value := intValue(p.config["transaction_block_size"]); value > 0 {
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
	return input.Pool != nil && input.Pool.Len() > 0
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
		Version:             "2.0.0",
		OrderingDomain:      block.ShardID,
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
	if evidence.OrderingDomain != expected.OrderingDomain || evidence.Height != expected.Height || evidence.TransactionCount != expected.TransactionCount ||
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
	if stableTextDigest(string(block.ExecutionPlan.Payload)) != block.ExecutionPlan.PayloadDigest {
		return fmt.Errorf("porygon execution plan payload digest mismatch")
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
		Version:                 "3.0.0",
		AlgorithmID:             porygonPlanAlgorithmID,
		BlockHeight:             block.Height,
		OrderingDomain:          block.ShardID,
		PhysicalShardCount:      1,
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

	assignments := make([]porygonTxAssignment, 0, len(block.TxList))
	waves := map[int][]string{}
	maxWave := -1
	for index, item := range block.TxList {
		accesses := porygonCanonicalAccesses(item)
		if len(accesses) == 0 {
			return porygonExecutionPlan{}, fmt.Errorf("porygon requires signed AccessList for transaction %s", item.TxID)
		}
		executionShard := porygonAccountShard(item, shardCount)
		stateShardSet := map[int]bool{}
		writeShardSet := map[int]bool{}
		lockKeys := make([]string, 0, len(accesses))
		for _, access := range accesses {
			if strings.TrimSpace(access.Key) == "" {
				return porygonExecutionPlan{}, fmt.Errorf("porygon access list contains empty key for transaction %s", item.TxID)
			}
			stateShard := porygonStateShard(access.Key, shardCount)
			stateShardSet[stateShard] = true
			lockKeys = append(lockKeys, access.Key)
			if isWriteMode(access.Mode) {
				writeShardSet[stateShard] = true
			}
		}
		stateShards := sortedIntSet(stateShardSet)
		writeShards := sortedIntSet(writeShardSet)
		involvedSet := map[int]bool{executionShard: true}
		for _, shard := range stateShards {
			involvedSet[shard] = true
		}
		involved := sortedIntSet(involvedSet)
		current := porygonTxAssignment{
			TxID:           item.TxID,
			OriginalIndex:  index,
			ExecutionShard: executionShard,
			StateShards:    stateShards,
			WriteShards:    writeShards,
			InvolvedShards: involved,
			CrossShard:     len(involved) > 1,
			LockedKeys:     uniqueStrings(lockKeys),
		}
		wave := 0
		for previousIndex, previous := range assignments {
			if porygonAssignmentsConflict(previous, current, block.TxList[previousIndex], item) && wave <= previous.Wave {
				wave = previous.Wave + 1
			}
		}
		current.Wave = wave
		assignments = append(assignments, current)
		plan.Assignments = append(plan.Assignments, current)
		waves[wave] = append(waves[wave], item.TxID)
		plan.SerializationOrder = append(plan.SerializationOrder, item.TxID)
		if wave > maxWave {
			maxWave = wave
		}
		if current.CrossShard {
			plan.CrossShardTransactionCnt++
			plan.SingleShardExecutionCnt++
			plan.MultiShardUpdateCnt += len(current.WriteShards)
			plan.StateLockCount += len(current.LockedKeys)
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
		stages = append(stages, porygonPipelineStage{BatchHeight: height + 1, Stage: "cross_batch_witness", LogicalSlot: base + 1, Committee: porygonECName(height+1, committeeCount)})
	}
	return stages
}

func porygonECName(height uint64, count int) string {
	if count < 1 {
		count = 1
	}
	return fmt.Sprintf("EC%d", int(height%uint64(count))+1)
}

func porygonAssignmentsConflict(previous, current porygonTxAssignment, previousItem, currentItem tx.SignedTransaction) bool {
	// One ESC is modeled as a sequential execution lane. Independent ESCs may
	// execute concurrently when their declared state locks do not conflict.
	if previous.ExecutionShard == current.ExecutionShard {
		return true
	}
	if (previous.CrossShard || current.CrossShard) && porygonStringSetsOverlap(previous.LockedKeys, current.LockedKeys) {
		return true
	}
	for _, left := range porygonCanonicalAccesses(previousItem) {
		for _, right := range porygonCanonicalAccesses(currentItem) {
			if accessItemsConflict(left, right) {
				return true
			}
		}
	}
	return false
}

func porygonCanonicalAccesses(item tx.SignedTransaction) []tx.AccessItem {
	// Deliberately bind only the signed execution AccessList. Never consult the
	// MetaTrack SchedulingAccessList or future execution observations.
	accesses := append([]tx.AccessItem(nil), item.AccessList...)
	sort.Slice(accesses, func(i, j int) bool {
		if accesses[i].Key != accesses[j].Key {
			return accesses[i].Key < accesses[j].Key
		}
		if accesses[i].Mode != accesses[j].Mode {
			return accesses[i].Mode < accesses[j].Mode
		}
		if accesses[i].UpdateSemantics != accesses[j].UpdateSemantics {
			return accesses[i].UpdateSemantics < accesses[j].UpdateSemantics
		}
		return accesses[i].Delta < accesses[j].Delta
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

func porygonAccountShard(item tx.SignedTransaction, count int) int {
	if count < 1 {
		return 0
	}
	identity := strings.TrimSpace(item.Sender)
	if identity == "" {
		identity = strings.TrimSpace(item.LogicalTxID)
	}
	if identity == "" {
		identity = strings.TrimSpace(item.TxID)
	}
	return stableKey([]string{strings.ToLower(identity)}) % count
}

func porygonStateShard(key string, count int) int {
	if count < 1 {
		return 0
	}
	return stableKey([]string{strings.ToLower(strings.TrimSpace(key))}) % count
}

func sortedIntSet(values map[int]bool) []int {
	out := make([]int, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Ints(out)
	return out
}

func porygonStringSetsOverlap(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	seen := make(map[string]bool, len(left))
	for _, value := range left {
		seen[value] = true
	}
	for _, value := range right {
		if seen[value] {
			return true
		}
	}
	return false
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

func (p porygonStateAccess) AccessMode() string { return "porygon_signed_access_projection" }

// Logical cross-ESC coordination is owned entirely by the Porygon consensus-
// bound plan and block executor. Returning false here is intentional: the MBE
// CrossShardPlugin interface drives the physical Relay/Finalize protocol for
// independent ledger shards, which would be semantically wrong for Porygon ESCs.
func (p porygonCrossShard) IsCrossShard(tx.SignedTransaction) bool { return false }
func (p porygonCrossShard) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonLogicalStateLock", Success: true}
}
func (p porygonCrossShard) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonLogicalMultiShardUpdate", Success: true}
}
func (p porygonCrossShard) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonLogicalUnlock", Success: true}
}
func (p porygonCrossShard) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "PorygonLogicalAbort", Success: false, Error: reason}
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
	scheduler, schedulerOK := plugins.Scheduler.(porygonScheduler)
	executor, executorOK := plugins.BlockExecutor.(porygonBlockExecutor)
	if schedulerOK && executorOK {
		for _, key := range []string{"execution_shard_count", "execution_committee_count"} {
			left := intValue(scheduler.config[key])
			right := intValue(executor.config[key])
			if left == 0 {
				if key == "execution_shard_count" {
					left = 4
				} else {
					left = 3
				}
			}
			if right == 0 {
				if key == "execution_shard_count" {
					right = 4
				} else {
					right = 3
				}
			}
			if left != right {
				return fmt.Errorf("Porygon scheduler and block executor %s must match", key)
			}
		}
		for _, key := range []string{"pipeline_enabled", "cross_batch_witness"} {
			if porygonBool(scheduler.config, key, true) != porygonBool(executor.config, key, true) {
				return fmt.Errorf("Porygon scheduler and block executor %s must match", key)
			}
		}
	}
	return nil
}

// Compile-time assertions intentionally exclude BatchRoutingPlugin and
// RoutingRuntimeCapabilities. Re-adding either would reactivate MetaTrack's
// remote-state/CAS runtime path and is therefore a correctness regression.
var _ RoutingPlugin = porygonStatelessRouting{}
var _ BlockProducerPlugin = porygonBlockProducer{}
var _ ProposalEvidenceVerifier = porygonBlockProducer{}
var _ ExecutionPlugin = porygonExecution{}
var _ ConsensusExecutionPlanner = porygonScheduler{}
var _ StateAccessPlugin = porygonStateAccess{}
var _ CrossShardPlugin = porygonCrossShard{}
