package v5

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
)

type optmeBlockExecutor struct {
	basicPlugin
	mode string
}

type optmeSimulationResult struct {
	index   int
	receipt execution.Receipt
	delta   execution.TxDelta
}

type optmeExecutionMetrics struct {
	Mode                             string `json:"mode"`
	WorkerCount                      int    `json:"worker_count"`
	SimulationMS                     int64  `json:"simulation_ms"`
	GraphSchedulingMS                int64  `json:"graph_scheduling_ms"`
	CommitMS                         int64  `json:"commit_ms"`
	ReexecutionMS                    int64  `json:"reexecution_ms"`
	ValidationMS                     int64  `json:"validation_ms"`
	ObservedReadCount                int    `json:"observed_read_count"`
	ObservedWriteCount               int    `json:"observed_write_count"`
	AddressCount                     int    `json:"address_count"`
	UnitCount                        int    `json:"unit_count"`
	SequenceCount                    int    `json:"sequence_count"`
	MaximumSequenceWidth             int    `json:"maximum_sequence_width"`
	EarlyAbortCount                  int    `json:"early_abort_count"`
	EarlyDetectionCount              int    `json:"early_detection_count"`
	HierarchicalAbortCount           int    `json:"hierarchical_abort_count"`
	ReorderedCount                   int    `json:"reordered_transaction_count"`
	RescheduledTransactionCount      int    `json:"rescheduled_transaction_count"`
	RescheduledEpochCount            int    `json:"rescheduled_epoch_count"`
	SimulationFailedCount            int    `json:"simulation_failed_count"`
	ReexecutionCount                 int    `json:"reexecution_count"`
	ReexecutionSimulationFailedCount int    `json:"reexecution_simulation_failed_count"`
	ReexecutionInvalidCount          int    `json:"reexecution_invalid_count"`
}

type optmeTxEvidence struct {
	TxID                  string `json:"tx_id"`
	LogicalTxID           string `json:"logical_tx_id"`
	OriginalIndex         int    `json:"original_index"`
	SimulationSuccess     bool   `json:"simulation_success"`
	EarlyDetected         bool   `json:"early_detected"`
	HierarchicalAborted   bool   `json:"hierarchical_aborted"`
	Reordered             bool   `json:"reordered"`
	MainSequence          int    `json:"main_sequence"`
	RescheduleEpoch       int    `json:"reschedule_epoch"`
	Reexecuted            bool   `json:"reexecuted"`
	ReexecutionSuccess    bool   `json:"reexecution_success"`
	SecondPassInvalidated bool   `json:"second_pass_invalidated"`
	TerminalSuccess       bool   `json:"terminal_success"`
}

func optmeObservedFromDeltas(deltas []execution.TxDelta) []optmeObservedTx {
	out := make([]optmeObservedTx, 0, len(deltas))
	for _, d := range deltas {
		reads := make([]string, 0, len(d.ReadSet))
		for _, r := range d.ReadSet {
			if r.Key != "" {
				reads = append(reads, r.Key)
			}
		}
		writes := make([]string, 0, len(d.WriteSet))
		for k := range d.WriteSet {
			if k != "" {
				writes = append(writes, k)
			}
		}
		out = append(out, optmeObservedTx{Index: d.OriginalIndex, TxID: d.TxID, ReadKeys: uniqueSortedStrings(reads), WriteKeys: uniqueSortedStrings(writes)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

func runOptmeSimulation(ctx context.Context, block realblock.Block, base map[string]string, workers int) ([]optmeSimulationResult, int, error) {
	if workers < 1 {
		workers = 1
	}
	pool := newFixedBlockWorkerPool(ctx, workers)
	defer pool.Close()
	serial := execution.NewSerialExecutor()
	results := make([]optmeSimulationResult, len(block.TxList))
	var firstErr error
	var errMu sync.Mutex
	tasks := make([]func(), len(block.TxList))
	for i, item := range block.TxList {
		idx := i
		txItem := item
		tasks[i] = func() {
			if err := ctx.Err(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			receipt, delta := serial.ExecuteTransaction(block, txItem, base, idx)
			results[idx] = optmeSimulationResult{index: idx, receipt: receipt, delta: delta}
		}
	}
	observed, err := pool.Run(tasks)
	if err != nil {
		return nil, observed, err
	}
	if firstErr != nil {
		return nil, observed, firstErr
	}
	return results, observed, nil
}

func optmeApplyDelta(block realblock.Block, working map[string]string, commitment *state.Commitment, delta execution.TxDelta, receipt execution.Receipt) (execution.TxDelta, execution.Receipt) {
	keys := make([]string, 0, len(delta.WriteSet))
	for k := range delta.WriteSet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if delta.Success {
		for _, k := range keys {
			q := literatureQualifiedKey(block.ShardID, k)
			working[q] = delta.WriteSet[k]
			commitment.Set(q, delta.WriteSet[k])
		}
	}
	receipt.StateRootAfterTx = commitment.Root()
	delta.Receipt = receipt
	delta.Success = receipt.Success
	delta.Error = receipt.Error
	return delta, receipt
}

func txIndexSet(items []int) map[int]bool {
	m := map[int]bool{}
	for _, i := range items {
		m[i] = true
	}
	return m
}

// MBE_OPTME_V20_TX_EVIDENCE: preserve the author scheduler while making MBE
// simulation-failure terminalization and tx-level mechanism evidence explicit.
func (p optmeBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	workers := configuredWorkerCount(p.config, input.WorkerCount)
	if workers < 1 {
		workers = 1
	}
	base := input.BaseStateSnapshot
	working := literatureCopyStringMap(base)
	commitmentStarted := time.Now()
	commitment := state.CloneOrBuild(input.BaseStateCommitment, working)
	before := commitment.Root()
	commitmentMS := time.Since(commitmentStarted)

	evidence := make([]optmeTxEvidence, len(input.Block.TxList))
	for idx, item := range input.Block.TxList {
		logicalID := item.LogicalTxID
		if logicalID == "" {
			logicalID = item.TxID
		}
		evidence[idx] = optmeTxEvidence{TxID: item.TxID, LogicalTxID: logicalID, OriginalIndex: idx}
	}

	simStarted := time.Now()
	simulated, maxSimulationWidth, err := runOptmeSimulation(ctx, input.Block, base, workers)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	simulationMS := time.Since(simStarted)
	deltas := make([]execution.TxDelta, len(simulated))
	receipts := make([]execution.Receipt, len(simulated))
	successfulDeltas := make([]execution.TxDelta, 0, len(simulated))
	observedReads, observedWrites, simulationFailedCount := 0, 0, 0
	for i, r := range simulated {
		deltas[i] = r.delta
		receipts[i] = r.receipt
		if !r.delta.Success {
			simulationFailedCount++
			continue
		}
		evidence[i].SimulationSuccess = true
		successfulDeltas = append(successfulDeltas, r.delta)
		observedReads += len(r.delta.ReadSet)
		observedWrites += len(r.delta.WriteSet)
	}
	// MBE_OPTME_V23_MIN_TRUTH: keep OptME scheduling unchanged; only fail closed
	// if actual simulation RW escapes the signed AccessList used by Stateless projection.
	if err := validateOptMEV23ObservedAccessCoverage(input.Block, deltas); err != nil {
		return BlockExecutionResult{}, err
	}
	observed := optmeObservedFromDeltas(successfulDeltas)
	provider, ok := input.Scheduler.(optmeObservedScheduleProvider)
	if !ok {
		return BlockExecutionResult{}, fmt.Errorf("OptME scheduler does not expose observed-RW scheduling")
	}
	scheduleStarted := time.Now()
	plan := provider.BuildObservedSchedule(observed, workers)
	schedulingMS := time.Since(scheduleStarted)

	for seqIndex, seq := range plan.Sequences {
		for _, idx := range seq {
			if idx >= 0 && idx < len(evidence) {
				evidence[idx].MainSequence = seqIndex + 1
			}
		}
	}
	for _, idx := range plan.EarlyDetected {
		if idx >= 0 && idx < len(evidence) {
			evidence[idx].EarlyDetected = true
		}
	}
	for _, idx := range plan.HierarchicalAborted {
		if idx >= 0 && idx < len(evidence) {
			evidence[idx].HierarchicalAborted = true
		}
	}
	for _, idx := range plan.Reordered {
		if idx >= 0 && idx < len(evidence) {
			evidence[idx].Reordered = true
		}
	}
	for epochIndex, epoch := range plan.RescheduleEpochs {
		for _, idx := range epoch {
			if idx >= 0 && idx < len(evidence) {
				evidence[idx].RescheduleEpoch = epochIndex + 1
			}
		}
	}

	result := execution.Result{BlockHash: input.Block.BlockHash, Height: input.Block.Height, StateRootBefore: before, Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: p.ID(), ExecutorVersion: optmeEngineVersion, WorkerCount: workers, StateRootVersion: state.CommitmentVersion}
	allDeltas := make([]execution.TxDelta, 0, len(input.Block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(input.Block.TxList))
	events := []ScheduleEvent{}
	attempts := []BusinessExecutionAttempt{}
	committed := map[int]bool{}

	// Author _simulate() filter_map excludes failed simulations from the ACG.
	// MBE still owes every PBFT transaction a terminal receipt, so failed
	// simulations are terminalized here without entering OptME scheduling.
	for idx, r := range simulated {
		if r.delta.Success {
			continue
		}
		rc := r.receipt
		commitmentStarted = time.Now()
		rc.StateRootAfterTx = commitment.Root()
		commitmentMS += time.Since(commitmentStarted)
		d := r.delta
		d.Receipt = rc
		allDeltas = append(allDeltas, d)
		allReceipts = append(allReceipts, rc)
		committed[idx] = true
		events = append(events, ScheduleEvent{TxID: input.Block.TxList[idx].TxID, Track: "optme", QueueName: "simulation_failed", DecisionReason: "author_source_simulation_filtered_mbe_terminal_failure", LocalExecution: true, Blocked: true})
		attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 1, Reason: "optme_simulation_failed_filtered_from_acg", Success: false, FinalCompletion: true})
	}

	commitStarted := time.Now()
	// The source commits each OptME sequence concurrently. MBE materializes the
	// already-simulated disjoint effects in deterministic index order so receipt
	// roots remain reproducible while preserving the final state.
	for seqIndex, seq := range plan.Sequences {
		sorted := append([]int(nil), seq...)
		sort.Ints(sorted)
		for _, idx := range sorted {
			if idx < 0 || idx >= len(deltas) {
				return BlockExecutionResult{}, fmt.Errorf("OptME schedule references invalid transaction index %d", idx)
			}
			if !deltas[idx].Success {
				return BlockExecutionResult{}, fmt.Errorf("OptME schedule references simulation-failed transaction index %d", idx)
			}
			d, rc := optmeApplyDelta(input.Block, working, commitment, deltas[idx], receipts[idx])
			deltas[idx] = d
			receipts[idx] = rc
			committed[idx] = true
			allDeltas = append(allDeltas, d)
			allReceipts = append(allReceipts, rc)
			events = append(events, ScheduleEvent{TxID: input.Block.TxList[idx].TxID, Track: "optme", QueueName: fmt.Sprintf("sequence_%d", seqIndex+1), DecisionReason: "author_source_hierarchical_schedule", LocalExecution: true, ReadyQueueDepth: len(seq)})
			attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 1, Reason: "optme_simulation_effect_commit", Success: d.Success, FinalCompletion: true})
		}
	}
	commitMS := time.Since(commitStarted)

	reexecutionCount, reexecutionSimulationFailedCount, invalidCount := 0, 0, 0
	var reexecutionMS, validationMS time.Duration
	serial := execution.NewSerialExecutor()
	for epochIndex, epoch := range plan.RescheduleEpochs {
		if len(epoch) == 0 {
			continue
		}
		snapshot := literatureCopyStringMap(working)
		start := time.Now()
		rerun := make([]optmeSimulationResult, len(epoch))
		pool := newFixedBlockWorkerPool(ctx, workers)
		tasks := make([]func(), len(epoch))
		var firstErr error
		var errMu sync.Mutex
		for pos, idx := range epoch {
			pos2, idx2 := pos, idx
			tasks[pos] = func() {
				if err := ctx.Err(); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				rc, dl := serial.ExecuteTransaction(input.Block, input.Block.TxList[idx2], snapshot, idx2)
				rerun[pos2] = optmeSimulationResult{index: idx2, receipt: rc, delta: dl}
			}
		}
		_, runErr := pool.Run(tasks)
		pool.Close()
		reexecutionMS += time.Since(start)
		if runErr != nil {
			return BlockExecutionResult{}, runErr
		}
		if firstErr != nil {
			return BlockExecutionResult{}, firstErr
		}
		reexecutionCount += len(rerun)
		rerunDeltas := make([]execution.TxDelta, 0, len(rerun))
		for _, rr := range rerun {
			rerunDeltas = append(rerunDeltas, rr.delta)
		}
		if err := validateOptMEV23ObservedAccessCoverage(input.Block, rerunDeltas); err != nil {
			return BlockExecutionResult{}, fmt.Errorf("OptME v23 reexecution access boundary: %w", err)
		}
		validateStarted := time.Now()
		usedWrites := map[string]bool{}
		sort.SliceStable(rerun, func(i, j int) bool { return rerun[i].index < rerun[j].index })
		for _, rr := range rerun {
			evidence[rr.index].Reexecuted = true
			if !rr.delta.Success {
				reexecutionSimulationFailedCount++
				rc := rr.receipt
				commitmentStarted = time.Now()
				rc.StateRootAfterTx = commitment.Root()
				commitmentMS += time.Since(commitmentStarted)
				d := rr.delta
				d.Receipt = rc
				allDeltas = append(allDeltas, d)
				allReceipts = append(allReceipts, rc)
				committed[rr.index] = true
				events = append(events, ScheduleEvent{TxID: input.Block.TxList[rr.index].TxID, Track: "optme", QueueName: fmt.Sprintf("reschedule_epoch_%d", epochIndex+1), DecisionReason: "author_source_second_pass_simulation_failed", LocalExecution: true, Blocked: true})
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 2, Reason: "optme_reexecution_simulation_failed", Success: false, FinalCompletion: true})
				continue
			}
			evidence[rr.index].ReexecutionSuccess = true
			conflict := false
			for k := range rr.delta.WriteSet {
				if usedWrites[k] {
					conflict = true
					break
				}
			}
			if conflict {
				invalidCount++
				evidence[rr.index].SecondPassInvalidated = true
				rc := rr.receipt
				rc.Success = false
				rc.Error = "optme_second_pass_invalidated_no_source_fallback"
				d := rr.delta
				d.Success = false
				d.Error = rc.Error
				d.WriteSet = map[string]string{}
				d.Receipt = rc
				commitmentStarted = time.Now()
				rc.StateRootAfterTx = commitment.Root()
				commitmentMS += time.Since(commitmentStarted)
				d.Receipt = rc
				allDeltas = append(allDeltas, d)
				allReceipts = append(allReceipts, rc)
				committed[rr.index] = true
				events = append(events, ScheduleEvent{TxID: input.Block.TxList[rr.index].TxID, Track: "optme", QueueName: fmt.Sprintf("reschedule_epoch_%d", epochIndex+1), DecisionReason: "author_source_second_pass_invalidated_no_fallback", LocalExecution: true, Blocked: true})
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 2, Reason: "optme_second_pass_invalidated_no_source_fallback", Success: false, FinalCompletion: true})
				continue
			}
			for k := range rr.delta.WriteSet {
				usedWrites[k] = true
			}
			d, rc := optmeApplyDelta(input.Block, working, commitment, rr.delta, rr.receipt)
			allDeltas = append(allDeltas, d)
			allReceipts = append(allReceipts, rc)
			committed[rr.index] = true
			events = append(events, ScheduleEvent{TxID: input.Block.TxList[rr.index].TxID, Track: "optme", QueueName: fmt.Sprintf("reschedule_epoch_%d", epochIndex+1), DecisionReason: "author_source_reexecute_validate_commit", LocalExecution: true, ReadyQueueDepth: len(epoch)})
			attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 2, Reason: "optme_reexecution", Success: d.Success, FinalCompletion: true})
		}
		validationMS += time.Since(validateStarted)
	}

	if len(committed) != len(input.Block.TxList) {
		return BlockExecutionResult{}, fmt.Errorf("OptME produced terminal results for %d/%d transactions", len(committed), len(input.Block.TxList))
	}
	sort.Slice(allDeltas, func(i, j int) bool { return allDeltas[i].OriginalIndex < allDeltas[j].OriginalIndex })
	byTxReceipt := map[string]execution.Receipt{}
	for _, rc := range allReceipts {
		byTxReceipt[rc.TxID] = rc
	}
	orderedReceipts := make([]execution.Receipt, 0, len(input.Block.TxList))
	for idx, item := range input.Block.TxList {
		rc, ok := byTxReceipt[item.TxID]
		if !ok {
			return BlockExecutionResult{}, fmt.Errorf("OptME terminal receipt missing for %s", item.TxID)
		}
		orderedReceipts = append(orderedReceipts, rc)
		evidence[idx].TerminalSuccess = rc.Success
	}
	result.TxDeltas = allDeltas
	result.Receipts = orderedReceipts
	for _, rc := range orderedReceipts {
		if rc.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	result.StateRootAfter = commitment.Root()
	result.ReceiptRoot = execution.ReceiptRoot(result.Receipts)
	for k, v := range working {
		result.StateUpdates[k] = v
	}
	result.StateDelta = literatureStateDelta(base, working)
	planDigest := stableJSONDigest(map[string]any{
		"algorithm":            "optme_author_source_4cac103_parallel_acg_mbe_v20",
		"block_hash":           input.Block.BlockHash,
		"sequences":            plan.Sequences,
		"reschedule_epochs":    plan.RescheduleEpochs,
		"early_detected":       plan.EarlyDetected,
		"hierarchical_aborted": plan.HierarchicalAborted,
		"reordered":            plan.Reordered,
		"rescheduled":          plan.Rescheduled,
	})
	result.Plan = execution.ExecutionPlan{EngineID: p.ID(), EngineVersion: optmeEngineVersion, BlockHash: input.Block.BlockHash, BlockHeight: input.Block.Height, OrderedTransactionIDs: transactionIDs(input.Block.TxList), WorkerCount: workers, PlanDigest: planDigest}
	result.PlanDigest = planDigest
	result.TransactionExecutionMS = (simulationMS + reexecutionMS).Milliseconds()
	result.DeterministicMaterializationMS = commitMS.Milliseconds()
	result.StateCommitmentMS = commitmentMS.Milliseconds()
	metrics := optmeExecutionMetrics{
		Mode: p.mode, WorkerCount: workers,
		SimulationMS: simulationMS.Milliseconds(), GraphSchedulingMS: schedulingMS.Milliseconds(), CommitMS: commitMS.Milliseconds(), ReexecutionMS: reexecutionMS.Milliseconds(), ValidationMS: validationMS.Milliseconds(),
		ObservedReadCount: observedReads, ObservedWriteCount: observedWrites, AddressCount: plan.AddressCount, UnitCount: plan.UnitCount, SequenceCount: len(plan.Sequences), MaximumSequenceWidth: plan.MaxWidth,
		EarlyAbortCount: len(plan.Rescheduled), EarlyDetectionCount: len(plan.EarlyDetected), HierarchicalAbortCount: len(plan.HierarchicalAborted), ReorderedCount: len(plan.Reordered), RescheduledTransactionCount: len(plan.Rescheduled), RescheduledEpochCount: len(plan.RescheduleEpochs),
		SimulationFailedCount: simulationFailedCount, ReexecutionCount: reexecutionCount, ReexecutionSimulationFailedCount: reexecutionSimulationFailedCount, ReexecutionInvalidCount: invalidCount,
	}
	actual := map[string]any{
		"optme_metrics":                             metrics,
		"optme_mode":                                p.mode,
		"optme_source_commit":                       "4cac103bd98440670d71219dfa185b8516ea6512",
		"optme_parallel_acg":                        true,
		"optme_simulation_ms":                       metrics.SimulationMS,
		"optme_graph_scheduling_ms":                 metrics.GraphSchedulingMS,
		"optme_commit_ms":                           metrics.CommitMS,
		"optme_reexecution_ms":                      metrics.ReexecutionMS,
		"optme_validation_ms":                       metrics.ValidationMS,
		"optme_observed_read_count":                 observedReads,
		"optme_observed_write_count":                observedWrites,
		"optme_address_count":                       plan.AddressCount,
		"optme_unit_count":                          plan.UnitCount,
		"optme_sequence_count":                      len(plan.Sequences),
		"optme_maximum_sequence_width":              plan.MaxWidth,
		"optme_early_abort_count":                   len(plan.Rescheduled),
		"optme_early_detection_count":               len(plan.EarlyDetected),
		"optme_hierarchical_abort_count":            len(plan.HierarchicalAborted),
		"optme_reordered_transaction_count":         len(plan.Reordered),
		"optme_rescheduled_transaction_count":       len(plan.Rescheduled),
		"optme_rescheduled_epoch_count":             len(plan.RescheduleEpochs),
		"optme_simulation_failed_count":             simulationFailedCount,
		"optme_reexecution_count":                   reexecutionCount,
		"optme_reexecution_simulation_failed_count": reexecutionSimulationFailedCount,
		"optme_reexecution_invalid_count":           invalidCount,
		"optme_transaction_evidence":                evidence,
		"maximum_parallel_width":                    maxInt(plan.MaxWidth, maxSimulationWidth),
		"abort_count":                               len(plan.Rescheduled),
		"reexecution_count":                         reexecutionCount,
		"serializable":                              true,
		"optme_plan_digest":                         planDigest,
	}
	return BlockExecutionResult{ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: planDigest, WorkerCount: workers, BlockExecutionMS: (simulationMS + schedulingMS + commitMS + reexecutionMS + validationMS + commitmentMS).Milliseconds(), TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS, StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion, ScheduleEvents: events, ActualMetrics: actual, BusinessAttempts: attempts}, nil
}
