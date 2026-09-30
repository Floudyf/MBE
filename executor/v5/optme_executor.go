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
	Mode                    string `json:"mode"`
	WorkerCount             int    `json:"worker_count"`
	SimulationMS            int64  `json:"simulation_ms"`
	GraphSchedulingMS       int64  `json:"graph_scheduling_ms"`
	CommitMS                int64  `json:"commit_ms"`
	ReexecutionMS           int64  `json:"reexecution_ms"`
	ValidationMS            int64  `json:"validation_ms"`
	ObservedReadCount       int    `json:"observed_read_count"`
	ObservedWriteCount      int    `json:"observed_write_count"`
	AddressCount            int    `json:"address_count"`
	UnitCount               int    `json:"unit_count"`
	SequenceCount           int    `json:"sequence_count"`
	MaximumSequenceWidth    int    `json:"maximum_sequence_width"`
	EarlyAbortCount         int    `json:"early_abort_count"`
	ReorderedCount          int    `json:"reordered_transaction_count"`
	RescheduledEpochCount   int    `json:"rescheduled_epoch_count"`
	ReexecutionCount        int    `json:"reexecution_count"`
	ReexecutionInvalidCount int    `json:"reexecution_invalid_count"`
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

	simStarted := time.Now()
	simulated, maxSimulationWidth, err := runOptmeSimulation(ctx, input.Block, base, workers)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	simulationMS := time.Since(simStarted)
	deltas := make([]execution.TxDelta, len(simulated))
	receipts := make([]execution.Receipt, len(simulated))
	observedReads, observedWrites := 0, 0
	for i, r := range simulated {
		deltas[i] = r.delta
		receipts[i] = r.receipt
		observedReads += len(r.delta.ReadSet)
		observedWrites += len(r.delta.WriteSet)
	}
	observed := optmeObservedFromDeltas(deltas)
	provider, ok := input.Scheduler.(optmeObservedScheduleProvider)
	if !ok {
		return BlockExecutionResult{}, fmt.Errorf("OptME scheduler does not expose observed-RW scheduling")
	}
	scheduleStarted := time.Now()
	plan := provider.BuildObservedSchedule(observed)
	schedulingMS := time.Since(scheduleStarted)

	result := execution.Result{BlockHash: input.Block.BlockHash, Height: input.Block.Height, StateRootBefore: before, Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: p.ID(), ExecutorVersion: optmeEngineVersion, WorkerCount: workers, StateRootVersion: state.CommitmentVersion}
	allDeltas := make([]execution.TxDelta, 0, len(input.Block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(input.Block.TxList))
	events := []ScheduleEvent{}
	attempts := []BusinessExecutionAttempt{}
	committed := map[int]bool{}
	commitStarted := time.Now()
	// The source commits each OptME sequence concurrently. MBE materializes the already-simulated
	// disjoint effects in deterministic transaction-index order inside a sequence so receipt roots
	// remain reproducible while the final state is identical to the parallel commit.
	for seqIndex, seq := range plan.Sequences {
		sorted := append([]int(nil), seq...)
		sort.Ints(sorted)
		for _, idx := range sorted {
			if idx < 0 || idx >= len(deltas) {
				return BlockExecutionResult{}, fmt.Errorf("OptME schedule references invalid transaction index %d", idx)
			}
			d, r := optmeApplyDelta(input.Block, working, commitment, deltas[idx], receipts[idx])
			deltas[idx] = d
			receipts[idx] = r
			committed[idx] = true
			allDeltas = append(allDeltas, d)
			allReceipts = append(allReceipts, r)
			events = append(events, ScheduleEvent{TxID: input.Block.TxList[idx].TxID, Track: "optme", QueueName: fmt.Sprintf("sequence_%d", seqIndex+1), DecisionReason: "author_source_hierarchical_schedule", LocalExecution: true, ReadyQueueDepth: len(seq)})
			attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 1, Reason: "optme_simulation_effect_commit", Success: d.Success, FinalCompletion: true})
		}
	}
	commitMS := time.Since(commitStarted)

	reexecutionCount, invalidCount := 0, 0
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
		// Author validation accepts the first transactions whose write sets are pairwise disjoint
		// within this re-execution group. Later write/write conflicts are invalidated; the author's
		// third fallback is commented out, so MBE records an explicit terminal failure receipt.
		validateStarted := time.Now()
		usedWrites := map[string]bool{}
		sort.SliceStable(rerun, func(i, j int) bool { return rerun[i].index < rerun[j].index })
		for _, rr := range rerun {
			conflict := false
			for k := range rr.delta.WriteSet {
				if usedWrites[k] {
					conflict = true
					break
				}
			}
			if conflict {
				invalidCount++
				r := rr.receipt
				r.Success = false
				r.Error = "optme_second_pass_invalidated_no_source_fallback"
				d := rr.delta
				d.Success = false
				d.Error = r.Error
				d.WriteSet = map[string]string{}
				d.Receipt = r
				commitmentStarted = time.Now()
				r.StateRootAfterTx = commitment.Root()
				commitmentMS += time.Since(commitmentStarted)
				d.Receipt = r
				allDeltas = append(allDeltas, d)
				allReceipts = append(allReceipts, r)
				committed[rr.index] = true
				events = append(events, ScheduleEvent{TxID: input.Block.TxList[rr.index].TxID, Track: "optme", QueueName: fmt.Sprintf("reschedule_epoch_%d", epochIndex+1), DecisionReason: "author_source_second_pass_invalidated_no_fallback", LocalExecution: true, Blocked: true})
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 2, Reason: "optme_second_pass_invalidated_no_source_fallback", Success: false, FinalCompletion: true})
				continue
			}
			for k := range rr.delta.WriteSet {
				usedWrites[k] = true
			}
			d, r := optmeApplyDelta(input.Block, working, commitment, rr.delta, rr.receipt)
			allDeltas = append(allDeltas, d)
			allReceipts = append(allReceipts, r)
			committed[rr.index] = true
			events = append(events, ScheduleEvent{TxID: input.Block.TxList[rr.index].TxID, Track: "optme", QueueName: fmt.Sprintf("reschedule_epoch_%d", epochIndex+1), DecisionReason: "author_source_reexecute_validate_commit", LocalExecution: true, ReadyQueueDepth: len(epoch)})
			attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: input.Block.Height, TxID: d.TxID, Track: p.ID(), Attempt: 2, Reason: "optme_reexecution", Success: d.Success, FinalCompletion: true})
		}
		validationMS += time.Since(validateStarted)
	}
	// Defensive fail-closed: every consensus transaction must have a terminal receipt.
	if len(committed) != len(input.Block.TxList) {
		return BlockExecutionResult{}, fmt.Errorf("OptME produced terminal results for %d/%d transactions", len(committed), len(input.Block.TxList))
	}

	sort.Slice(allDeltas, func(i, j int) bool { return allDeltas[i].OriginalIndex < allDeltas[j].OriginalIndex })
	byTxReceipt := map[string]execution.Receipt{}
	for _, r := range allReceipts {
		byTxReceipt[r.TxID] = r
	}
	orderedReceipts := make([]execution.Receipt, 0, len(input.Block.TxList))
	for _, item := range input.Block.TxList {
		orderedReceipts = append(orderedReceipts, byTxReceipt[item.TxID])
	}
	result.TxDeltas = allDeltas
	result.Receipts = orderedReceipts
	for _, r := range orderedReceipts {
		if r.Success {
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
	planDigest := stableJSONDigest(map[string]any{"algorithm": "optme_author_source_4cac103", "block_hash": input.Block.BlockHash, "sequences": plan.Sequences, "reschedule_epochs": plan.RescheduleEpochs, "early_aborted": plan.EarlyAborted, "reordered": plan.Reordered})
	result.Plan = execution.ExecutionPlan{EngineID: p.ID(), EngineVersion: optmeEngineVersion, BlockHash: input.Block.BlockHash, BlockHeight: input.Block.Height, OrderedTransactionIDs: transactionIDs(input.Block.TxList), WorkerCount: workers, PlanDigest: planDigest}
	result.PlanDigest = planDigest
	result.TransactionExecutionMS = (simulationMS + reexecutionMS).Milliseconds()
	result.DeterministicMaterializationMS = commitMS.Milliseconds()
	result.StateCommitmentMS = commitmentMS.Milliseconds()
	metrics := optmeExecutionMetrics{Mode: p.mode, WorkerCount: workers, SimulationMS: simulationMS.Milliseconds(), GraphSchedulingMS: schedulingMS.Milliseconds(), CommitMS: commitMS.Milliseconds(), ReexecutionMS: reexecutionMS.Milliseconds(), ValidationMS: validationMS.Milliseconds(), ObservedReadCount: observedReads, ObservedWriteCount: observedWrites, AddressCount: plan.AddressCount, UnitCount: plan.UnitCount, SequenceCount: len(plan.Sequences), MaximumSequenceWidth: plan.MaxWidth, EarlyAbortCount: len(plan.EarlyAborted), ReorderedCount: len(plan.Reordered), RescheduledEpochCount: len(plan.RescheduleEpochs), ReexecutionCount: reexecutionCount, ReexecutionInvalidCount: invalidCount}
	actual := map[string]any{"optme_metrics": metrics, "optme_mode": p.mode, "optme_source_commit": "4cac103bd98440670d71219dfa185b8516ea6512", "optme_simulation_ms": metrics.SimulationMS, "optme_graph_scheduling_ms": metrics.GraphSchedulingMS, "optme_commit_ms": metrics.CommitMS, "optme_reexecution_ms": metrics.ReexecutionMS, "optme_validation_ms": metrics.ValidationMS, "optme_observed_read_count": observedReads, "optme_observed_write_count": observedWrites, "optme_address_count": plan.AddressCount, "optme_unit_count": plan.UnitCount, "optme_sequence_count": len(plan.Sequences), "optme_maximum_sequence_width": plan.MaxWidth, "optme_early_abort_count": len(plan.EarlyAborted), "optme_reordered_transaction_count": len(plan.Reordered), "optme_rescheduled_epoch_count": len(plan.RescheduleEpochs), "optme_reexecution_count": reexecutionCount, "optme_reexecution_invalid_count": invalidCount, "maximum_parallel_width": maxInt(plan.MaxWidth, maxSimulationWidth), "abort_count": len(plan.EarlyAborted), "reexecution_count": reexecutionCount, "serializable": true, "optme_plan_digest": planDigest}
	return BlockExecutionResult{ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: planDigest, WorkerCount: workers, BlockExecutionMS: (simulationMS + schedulingMS + commitMS + reexecutionMS + validationMS + commitmentMS).Milliseconds(), TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS, StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion, ScheduleEvents: events, ActualMetrics: actual, BusinessAttempts: attempts}, nil
}
