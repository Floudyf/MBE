package v5

import (
	"context"
	"fmt"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

const (
	groundhogBlockProducerID              = "groundhog_block_producer"
	groundhogBlockExecutorID              = "groundhog_block_executor"
	groundhogCandidateSelectionEvidenceID = "groundhog_candidate_selection_v1"
)

type groundhogCandidateSelectionEvidence struct {
	ShardID            string                                `json:"shard_id"`
	Height             uint64                                `json:"height"`
	PoolDepthBefore    int                                   `json:"pool_depth_before"`
	CandidateCount     int                                   `json:"candidate_count"`
	SelectedCount      int                                   `json:"selected_count"`
	DeferredCount      int                                   `json:"deferred_count"`
	ScanLimit          int                                   `json:"scan_limit"`
	SelectedTxIDs      []string                              `json:"selected_tx_ids"`
	SelectedLogicalIDs []string                              `json:"selected_logical_ids"`
	DeferredTxIDs      []string                              `json:"deferred_tx_ids"`
	DeferredLogicalIDs []string                              `json:"deferred_logical_ids"`
	DeferredReasons    map[string]string                     `json:"deferred_reasons"`
	Metrics            execution.GroundhogMetrics            `json:"metrics"`
	Trace              []execution.GroundhogTransactionTrace `json:"trace"`
}

type groundhogBlockProducer struct{ basicPlugin }

func (p groundhogBlockProducer) BlockSize() int {
	if value := intValue(p.config["block_size"]); value > 0 {
		return value
	}
	return 100
}

func (p groundhogBlockProducer) Interval() time.Duration {
	if value := intValue(p.config["interval_ms"]); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 75 * time.Millisecond
}

func groundhogPaperScanLimit(depth, blockSize int) int {
	if depth <= 0 {
		return 0
	}
	return depth
}

func (p groundhogBlockProducer) ShouldProduce(input BlockProductionInput) bool {
	return (input.Pool != nil && input.Pool.Len() > 0) || input.SystemDeltaReady
}

func (p groundhogBlockProducer) BuildCandidate(input BlockProductionInput) (realblock.Block, error) {
	if input.Proposer == nil || input.Pool == nil {
		return realblock.Block{}, fmt.Errorf("groundhog block producer requires proposer and pool")
	}
	limit := input.Limit
	if limit <= 0 {
		limit = p.BlockSize()
	}
	if limit < 1 {
		return realblock.Block{}, fmt.Errorf("groundhog block producer requires positive block size")
	}
	depth := input.Pool.Len()
	// Groundhog Algorithm 2 draws candidates from the available transaction
	// stream until the block is large enough.  For MBE's finite ready-pool
	// snapshot the faithful boundary is therefore the entire currently ready
	// pool, not an arbitrary block_size*multiplier prefix.
	scanLimit := groundhogPaperScanLimit(depth, limit)
	candidates := input.Pool.ReserveReady(scanLimit)
	if len(candidates) == 0 {
		return realblock.Block{}, fmt.Errorf("empty_mempool")
	}

	ctx := input.Context
	if ctx == nil {
		ctx = context.Background()
	}
	workerCount := input.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	executor := execution.NewGroundhogExecutor(workerCount)
	if orderedSetLimit := intValue(p.config["ordered_set_limit"]); orderedSetLimit > 0 {
		executor.OrderedSetLimit = orderedSetLimit
	}
	selection, err := executor.SelectCandidateTransactions(
		ctx,
		input.Proposer.ShardID,
		input.Proposer.NextHeight,
		candidates,
		input.BaseStateSnapshot,
		limit,
	)
	if err != nil {
		input.Pool.ReleaseReserved(candidates)
		return realblock.Block{}, err
	}
	if len(selection.Deferred) > 0 {
		input.Pool.ReleaseReserved(selection.Deferred)
	}
	if len(selection.Selected) == 0 {
		return realblock.Block{}, fmt.Errorf("empty_mempool")
	}
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}
	candidate, err := input.Proposer.BuildFromReserved(selection.Selected, now)
	if err != nil {
		input.Pool.ReleaseReserved(selection.Selected)
		return realblock.Block{}, err
	}
	evidence := groundhogCandidateSelectionEvidence{
		ShardID:            candidate.ShardID,
		Height:             candidate.Height,
		PoolDepthBefore:    depth,
		CandidateCount:     len(candidates),
		SelectedCount:      len(selection.Selected),
		DeferredCount:      len(selection.Deferred),
		ScanLimit:          scanLimit,
		SelectedTxIDs:      transactionIDs(selection.Selected),
		SelectedLogicalIDs: logicalTransactionIDs(selection.Selected),
		DeferredTxIDs:      transactionIDs(selection.Deferred),
		DeferredLogicalIDs: logicalTransactionIDs(selection.Deferred),
		DeferredReasons:    cloneStringMap(selection.DeferredReasons),
		Metrics:            selection.Metrics,
		Trace:              append([]execution.GroundhogTransactionTrace(nil), selection.Trace...),
	}
	if err := attachProposalEvidence(&candidate, groundhogCandidateSelectionEvidenceID, evidence); err != nil {
		input.Pool.ReleaseReserved(selection.Selected)
		return realblock.Block{}, err
	}
	return candidate, nil
}

type groundhogBlockExecutor struct{ basicPlugin }

func (p groundhogBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	executor := execution.NewGroundhogExecutor(workerCount)
	if orderedSetLimit := intValue(p.config["ordered_set_limit"]); orderedSetLimit > 0 {
		executor.OrderedSetLimit = orderedSetLimit
	}
	result, err := executor.ExecuteBlock(ctx, input.Block, input.BaseStateSnapshot)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	metrics := executor.Metrics
	actualMetrics := map[string]any{
		"groundhog_metrics":                      metrics,
		"groundhog_transaction_trace":            executor.Trace,
		"groundhog_execution_attempt_count":      metrics.ExecutionAttemptCount,
		"groundhog_reservation_count":            metrics.ReservationCount,
		"groundhog_constraint_conflict_count":    metrics.ConstraintConflictCount,
		"groundhog_reservation_rollback_count":   metrics.ReservationRollbackCount,
		"groundhog_integer_merge_count":          metrics.IntegerMergeCount,
		"groundhog_bytes_merge_count":            metrics.BytesMergeCount,
		"groundhog_ordered_set_merge_count":      metrics.OrderedSetMergeCount,
		"groundhog_modified_key_count":           metrics.ModifiedKeyCount,
		"groundhog_reservation_parallel_width":   metrics.ReservationParallelWidth,
		"groundhog_reservation_engine":           metrics.ReservationEngine,
		"maximum_parallel_width":                 metrics.MaximumParallelWidth,
		"abort_count":                            metrics.ConstraintConflictCount,
		"reexecution_count":                      0,
		"serializable":                           true,
		"serial_equivalent":                      false,
		"groundhog_semantics_serializable":       true,
		"groundhog_fallback_mode":                metrics.FallbackMode,
		"groundhog_snapshot_semantics":           metrics.SnapshotSemantics,
		"groundhog_typed_modification_semantics": metrics.TypedModificationSemantics,
	}
	return BlockExecutionResult{
		ExecutionResult: result,
		StateDelta:      stateKVsFromExecutionDelta(result.StateDelta),
		PlanDigest:      result.PlanDigest,
		WorkerCount:     result.WorkerCount,
		ScheduleEvents:  groundhogScheduleEvents(executor.Trace),
		ActualMetrics:   actualMetrics,
	}, nil
}

func groundhogScheduleEvents(traces []execution.GroundhogTransactionTrace) []ScheduleEvent {
	events := make([]ScheduleEvent, 0, len(traces))
	for _, trace := range traces {
		event := ScheduleEvent{
			TxID:           trace.TxID,
			Track:          "groundhog_unordered_block",
			QueueName:      "groundhog_committed",
			DecisionReason: "groundhog_typed_modifications_committed",
			LocalExecution: true,
		}
		if trace.Status == "terminal_failure" || trace.Status == "selected_terminal_failure" {
			event.QueueName = "groundhog_terminal_failure"
			event.DecisionReason = trace.Reason
		}
		if trace.Status == "deferred" || trace.Status == "fixed_block_conflict" {
			event.QueueName = "groundhog_deferred"
			event.DecisionReason = trace.Reason
			event.Blocked = true
		}
		events = append(events, event)
	}
	return events
}

func registerGroundhogPlugins(register func(string, string, Factory)) {
	register("block_producer", groundhogBlockProducerID, func(c map[string]any) (Plugin, error) {
		return groundhogBlockProducer{makeBasic("block_producer", groundhogBlockProducerID, c)}, nil
	})
	register("block_executor", groundhogBlockExecutorID, func(c map[string]any) (Plugin, error) {
		return groundhogBlockExecutor{makeBasic("block_executor", groundhogBlockExecutorID, c)}, nil
	})
}

func validateGroundhogPluginCombination(plugins RuntimePlugins) error {
	producerSelected := plugins.BlockProducer != nil && plugins.BlockProducer.ID() == groundhogBlockProducerID
	executorSelected := plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == groundhogBlockExecutorID
	if producerSelected != executorSelected {
		return fmt.Errorf("groundhog_block_producer and groundhog_block_executor must be selected together")
	}
	if !producerSelected {
		return nil
	}
	producer, producerOK := plugins.BlockProducer.(groundhogBlockProducer)
	executor, executorOK := plugins.BlockExecutor.(groundhogBlockExecutor)
	if !producerOK || !executorOK {
		return fmt.Errorf("groundhog plugin implementation type mismatch")
	}
	producerSetLimit := intValue(producer.config["ordered_set_limit"])
	if producerSetLimit < 1 {
		producerSetLimit = execution.GroundhogOrderedSetInitialLimit
	}
	executorSetLimit := intValue(executor.config["ordered_set_limit"])
	if executorSetLimit < 1 {
		executorSetLimit = execution.GroundhogOrderedSetInitialLimit
	}
	if producerSetLimit != executorSetLimit {
		return fmt.Errorf("groundhog producer/executor ordered_set_limit mismatch: producer=%d executor=%d", producerSetLimit, executorSetLimit)
	}
	if producerSetLimit < 1 || producerSetLimit > execution.GroundhogOrderedSetMaximumLimit {
		return fmt.Errorf("groundhog ordered_set_limit must be between 1 and %d", execution.GroundhogOrderedSetMaximumLimit)
	}
	required := []struct {
		category string
		actual   Plugin
		id       string
	}{
		{"routing", plugins.Routing, "hash_routing_baseline"},
		{"execution", plugins.Execution, "serial_execution_baseline"},
		{"scheduler", plugins.Scheduler, "fifo_serial_scheduler"},
		{"state_access", plugins.StateAccess, "direct_state_access"},
		{"state_storage", plugins.StateStorage, "persistent_local_state_store"},
		{"commit", plugins.Commit, "normal_commit"},
	}
	for _, item := range required {
		if item.actual == nil || item.actual.ID() != item.id {
			actual := "<nil>"
			if item.actual != nil {
				actual = item.actual.ID()
			}
			return fmt.Errorf("groundhog requires %s:%s, got %s", item.category, item.id, actual)
		}
	}
	return nil
}
