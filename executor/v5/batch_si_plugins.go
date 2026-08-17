package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	batchSIExecutionID = "batch_si_execution"
	batchSISchedulerID = "batch_si_scheduler"
)

type batchSIExecution struct{ basicPlugin }
type batchSIScheduler struct{ basicPlugin }
type batchSIBlockExecutor struct{ basicPlugin }

func registerBatchSIPlugins(register func(string, string, Factory)) {
	register("execution", batchSIExecutionID, func(config map[string]any) (Plugin, error) {
		return batchSIExecution{basicPlugin: makeBasic("execution", batchSIExecutionID, config)}, nil
	})
	register("scheduler", batchSISchedulerID, func(config map[string]any) (Plugin, error) {
		plugin := batchSIScheduler{basicPlugin: makeBasic("scheduler", batchSISchedulerID, config)}
		if err := plugin.Validate(config); err != nil {
			return nil, err
		}
		return plugin, nil
	})
	register("block_executor", execution.BatchSIBlockExecutorID, func(config map[string]any) (Plugin, error) {
		plugin := batchSIBlockExecutor{basicPlugin: makeBasic("block_executor", execution.BatchSIBlockExecutorID, config)}
		if err := plugin.Validate(config); err != nil {
			return nil, err
		}
		return plugin, nil
	})
}

func (p batchSIExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "batch_si", Reason: "batch_si_pre_consensus_planner"}
}

func (p batchSIScheduler) Validate(_ map[string]any) error {
	return batchSIConfigFromPlugin(p.config, 1).Validate()
}

func (p batchSIScheduler) Order(items []tx.SignedTransaction, classifier ExecutionPlugin) []tx.SignedTransaction {
	return p.Schedule(items, classifier).Ordered
}

// Schedule preserves the generic SchedulerPlugin contract for catalog checks.
// The real runtime uses PlanBlock so that OFAS-deferred transactions are removed
// before PBFT and released back to the mempool.
func (p batchSIScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	candidate := realblock.Block{TxList: append([]tx.SignedTransaction(nil), items...)}
	planned, err := execution.BuildBatchSIPlan(candidate, batchSIConfigFromPlugin(p.config, 1))
	if err != nil {
		return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...), Events: []ScheduleEvent{{Track: "batch_si", QueueName: "planning_error", DecisionReason: err.Error(), Blocked: true}}}
	}
	ordered := append([]tx.SignedTransaction(nil), planned.Ordered...)
	ordered = append(ordered, planned.Deferred...)
	return ScheduleResult{Ordered: ordered, Events: batchSIScheduleEvents(planned)}
}

func (p batchSIScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	config := batchSIConfigFromPlugin(p.config, 1)
	planned, err := execution.BuildBatchSIPlan(block, config)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	if len(planned.Ordered) == 0 && len(block.TxList) > 0 {
		return ConsensusExecutionPlanningResult{}, fmt.Errorf("batch-si planning deferred every candidate transaction")
	}
	block.TxList = append([]tx.SignedTransaction(nil), planned.Ordered...)
	block.TxIDs = make([]string, 0, len(block.TxList))
	for _, item := range block.TxList {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	// The exact first-pass AWRT -> WRBP -> OFAS result is consensus-bound.
	// Candidate TxID order plus deferred signed transactions remain in the plan as
	// verification evidence; accepted transactions stay only in the block body. The
	// accepted block is the fixed projection of the first pass, not a second WRBP run.
	raw, err := execution.MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	payloadDigest := stableTextDigest(string(raw))
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   execution.BatchSIPlanAlgorithmID,
		PayloadDigest: payloadDigest,
		PlanDigest:    planned.Plan.PlanDigest,
		Payload:       append(json.RawMessage(nil), raw...),
	}
	return ConsensusExecutionPlanningResult{
		Block:    block,
		Deferred: append([]tx.SignedTransaction(nil), planned.Deferred...),
		Events:   batchSIScheduleEvents(planned),
	}, nil
}

func (p batchSIScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != execution.BatchSIPlanAlgorithmID {
		return fmt.Errorf("batch-si execution plan is missing or has the wrong algorithm id")
	}
	plan, err := execution.ParseBatchSIPlan(block.ExecutionPlan.Payload)
	if err != nil {
		return err
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("batch-si envelope plan digest mismatch")
	}
	return execution.VerifyBatchSIPlan(block, plan, batchSIConfigFromPlugin(p.config, 1))
}

func batchSIScheduleEvents(planned execution.BatchSIPlanningResult) []ScheduleEvent {
	batchByID := map[string]int{}
	for _, batch := range planned.Plan.Batches {
		for _, txID := range batch.OrderedTransactionIDs {
			batchByID[txID] = batch.BatchNumber
		}
	}
	events := make([]ScheduleEvent, 0, len(planned.Ordered)+len(planned.Deferred))
	for _, item := range planned.Ordered {
		events = append(events, ScheduleEvent{
			TxID:            item.TxID,
			Track:           "batch_si",
			QueueName:       fmt.Sprintf("batch_%d", batchByID[item.TxID]),
			DecisionReason:  "batch_si_accepted",
			LocalExecution:  true,
			ReadyQueueDepth: planned.Plan.Metrics.MaximumBatchWidth,
		})
	}
	for _, item := range planned.Deferred {
		events = append(events, ScheduleEvent{
			TxID:           item.TxID,
			Track:          "batch_si",
			QueueName:      "mempool_deferred",
			DecisionReason: "batch_si_ofas_cycle_deferred",
			Blocked:        true,
		})
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Blocked != events[j].Blocked {
			return !events[i].Blocked
		}
		return events[i].TxID < events[j].TxID
	})
	return events
}

func (p batchSIBlockExecutor) Validate(_ map[string]any) error {
	return batchSIConfigFromPlugin(p.config, 1).Validate()
}

func (p batchSIBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	config := batchSIConfigFromPlugin(p.config, input.WorkerCount)
	if input.Block.ExecutionPlan == nil || input.Block.ExecutionPlan.AlgorithmID != execution.BatchSIPlanAlgorithmID {
		return BlockExecutionResult{}, fmt.Errorf("batch-si execution plan is missing before execution")
	}
	parseStarted := time.Now()
	consensusPlan, err := execution.ParseBatchSIPlan(input.Block.ExecutionPlan.Payload)
	planParseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	executor := execution.NewBatchSIExecutor(config)
	result, err := executor.ExecuteConsensusPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, consensusPlan, input.ExecutionPlanVerified)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	metrics := executor.Metrics
	deferredTxIDs := make([]string, 0, len(consensusPlan.DeferredTransactions))
	for _, item := range consensusPlan.DeferredTransactions {
		deferredTxIDs = append(deferredTxIDs, item.TxID)
	}
	acceptedTxIDs := transactionIDs(input.Block.TxList)
	actual := map[string]any{
		"batch_si_metrics":                         metrics,
		"configured_worker_count":                  metrics.WorkerCount,
		"maximum_parallel_width":                   metrics.MaximumObservedParallelWidth,
		"batch_count":                              metrics.BatchCount,
		"maximum_batch_width":                      metrics.MaximumBatchWidth,
		"average_batch_width_milli":                metrics.AverageBatchWidthMilli,
		"awrt_address_count":                       metrics.AWRTAddressCount,
		"awrt_write_reference_count":               metrics.AWRTWriteReferenceCount,
		"write_opportunity_reuse_count":            metrics.WriteOpportunityReuseCount,
		"dependency_edge_count":                    metrics.DependencyEdgeCount,
		"abort_count":                              0,
		"deferred_transaction_count":               metrics.OFASAbortedTransactionCount,
		"batch_si_first_pass_candidate_count":      metrics.CandidateTransactionCount,
		"batch_si_first_pass_accepted_count":       metrics.AcceptedTransactionCount,
		"batch_si_first_pass_ofas_abort_count":     metrics.FirstPassOFASAbortedTransactionCount,
		"batch_si_first_pass_ofas_abort_rate":      batchSIRatio(metrics.FirstPassOFASAbortedTransactionCount, metrics.CandidateTransactionCount),
		"batch_si_deferred_tx_ids":                 deferredTxIDs,
		"batch_si_accepted_tx_ids":                 acceptedTxIDs,
		"planning_iteration_count":                 metrics.PlanningIterationCount,
		"batch_snapshot_count":                     metrics.SnapshotCount,
		"batch_snapshot_create_ms":                 metrics.BatchSnapshotCreateMS,
		"transaction_execution_ms":                 metrics.TransactionExecutionMS,
		"deterministic_materialization_ms":         metrics.DeterministicMaterializationMS,
		"state_commitment_ms":                      metrics.StateCommitmentMS,
		"batch_si_executor_plan_parse_ms":          planParseMS,
		"batch_si_executor_plan_verify_ms":         metrics.PlanVerificationMS,
		"batch_si_executor_plan_verify_mode":       metrics.PlanVerificationMode,
		"batch_si_plan_payload_bytes":              metrics.PlanPayloadBytes,
		"batch_si_worker_pool_setup_ms":            metrics.WorkerPoolSetupMS,
		"batch_si_worker_pool_wait_ms":             metrics.WorkerPoolWaitMS,
		"batch_si_worker_pool_create_count":        metrics.WorkerPoolCreateCount,
		"batch_si_batch_barrier_count":             metrics.BatchBarrierCount,
		"batch_si_execution_plan_preverified":      input.ExecutionPlanVerified,
		"batch_si_executor_full_verify_count":      boolToInt(!input.ExecutionPlanVerified),
		"batch_si_executor_full_verify_skip_count": boolToInt(input.ExecutionPlanVerified),
		"state_root_version":                       result.StateRootVersion,
		"batch_si_partition_mode":                  metrics.PartitionMode,
		"batch_si_ordering_mode":                   metrics.OrderingMode,
		"batch_si_priority_mode":                   metrics.PriorityMode,
		"batch_si_execution_mode":                  metrics.ExecutionMode,
		"serializable":                             true,
		"batch_si_cross_scheme_algorithm_reuse":    false,
		"batch_si_execution_plan_algorithm_id":     execution.BatchSIPlanAlgorithmID,
		"batch_si_execution_plan_digest_verified":  true,
	}
	return BlockExecutionResult{
		ExecutionResult:        result,
		StateDelta:             stateKVsFromExecutionDelta(result.StateDelta),
		PlanDigest:             result.PlanDigest,
		WorkerCount:            result.WorkerCount,
		TransactionExecutionMS: metrics.TransactionExecutionMS,
		DeterministicApplyMS:   metrics.DeterministicMaterializationMS,
		StateCommitmentMS:      metrics.StateCommitmentMS,
		StateRootVersion:       result.StateRootVersion,
		ActualMetrics:          actual,
	}, nil
}

func batchSIRatio(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func batchSIConfigFromPlugin(config map[string]any, workerFallback int) execution.BatchSIConfig {
	defaults := execution.DefaultBatchSIConfig()
	workerCount := configuredWorkerCount(config, workerFallback)
	partitionMode := batchSIStringConfig(config, "partition_mode", defaults.PartitionMode)
	orderingMode := batchSIStringConfig(config, "ordering_mode", defaults.OrderingMode)
	priorityMode := batchSIStringConfig(config, "priority_mode", defaults.PriorityMode)
	executionMode := batchSIStringConfig(config, "execution_mode", defaults.ExecutionMode)
	return execution.BatchSIConfig{
		WorkerCount:   workerCount,
		PartitionMode: partitionMode,
		OrderingMode:  orderingMode,
		PriorityMode:  priorityMode,
		ExecutionMode: executionMode,
	}
}

func batchSIStringConfig(config map[string]any, key, fallback string) string {
	value, ok := config[key]
	if !ok || value == nil {
		return fallback
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return fallback
	}
	return text
}

func validateBatchSIPluginCombination(p RuntimePlugins) error {
	schedulerSelected := p.Scheduler != nil && p.Scheduler.ID() == batchSISchedulerID
	executorSelected := p.BlockExecutor != nil && p.BlockExecutor.ID() == execution.BatchSIBlockExecutorID
	executionSelected := p.Execution != nil && p.Execution.ID() == batchSIExecutionID
	if !schedulerSelected && !executorSelected && !executionSelected {
		return nil
	}
	if !(schedulerSelected && executorSelected && executionSelected) {
		return fmt.Errorf("batch_si_execution, batch_si_scheduler, and batch_si_block_executor must be selected together")
	}
	required := map[string]string{
		"routing":        "hash_routing_baseline",
		"block_producer": "time_or_count_block_producer",
		"state_access":   "direct_state_access",
		"state_storage":  "persistent_local_state_store",
		"commit":         "normal_commit",
	}
	selected := map[string]Plugin{
		"routing":        p.Routing,
		"block_producer": p.BlockProducer,
		"state_access":   p.StateAccess,
		"state_storage":  p.StateStorage,
		"commit":         p.Commit,
	}
	for category, pluginID := range required {
		plugin := selected[category]
		if plugin == nil || plugin.ID() != pluginID {
			return fmt.Errorf("Batch-SI requires %s:%s", category, pluginID)
		}
	}
	scheduler, ok := p.Scheduler.(batchSIScheduler)
	if !ok {
		return fmt.Errorf("Batch-SI scheduler implementation mismatch")
	}
	executor, ok := p.BlockExecutor.(batchSIBlockExecutor)
	if !ok {
		return fmt.Errorf("Batch-SI executor implementation mismatch")
	}
	schedulerConfig := batchSIConfigFromPlugin(scheduler.config, 1)
	executorConfig := batchSIConfigFromPlugin(executor.config, 1)
	if schedulerConfig.PartitionMode != executorConfig.PartitionMode ||
		schedulerConfig.OrderingMode != executorConfig.OrderingMode ||
		schedulerConfig.PriorityMode != executorConfig.PriorityMode {
		return fmt.Errorf("Batch-SI scheduler and executor planning configuration mismatch")
	}
	return nil
}
