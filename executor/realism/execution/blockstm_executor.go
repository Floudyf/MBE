package execution

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution/blockstm"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const BlockSTMExecutorID = "block_stm_block_executor"
const BlockSTMExecutorVersion = "0.2.1"

type BlockSTMMetrics struct {
	WorkerCount                     int         `json:"worker_count"`
	MaximumParallelWidth            int         `json:"maximum_parallel_width"`
	ExecutionTaskCount              int         `json:"execution_task_count"`
	ValidationTaskCount             int         `json:"validation_task_count"`
	AbortCount                      int         `json:"abort_count"`
	DependencyAbortCount            int         `json:"dependency_abort_count"`
	ValidationAbortCount            int         `json:"validation_abort_count"`
	ReexecutionCount                int         `json:"reexecution_count"`
	EstimateCount                   int         `json:"estimate_count"`
	EstimateMarkCount               int         `json:"estimate_mark_count"`
	EstimateReadCount               int         `json:"estimate_read_count"`
	DependencyWaitCount             int         `json:"dependency_wait_count"`
	DependencyResumeCount           int         `json:"dependency_resume_count"`
	ValidatedSpeculativeResultCount int         `json:"validated_speculative_result_count"`
	SpeculativeReadCount            int         `json:"speculative_read_count"`
	ValidationFailureCount          int         `json:"validation_failure_count"`
	CommittedTransactionCount       int         `json:"committed_transaction_count"`
	MaximumIncarnation              int         `json:"maximum_incarnation"`
	MaximumConcurrentExecutions     int         `json:"maximum_concurrent_executions"`
	SchedulerQueuePeak              int         `json:"scheduler_queue_peak"`
	StaleTaskCount                  int         `json:"stale_task_count"`
	SerialOracleMS                  int64       `json:"serial_oracle_ms"`
	MaterializationMS               int64       `json:"materialization_ms"`
	IncarnationLimitHitCount        int         `json:"incarnation_limit_hit_count"`
	SerialFallbackCount             int         `json:"serial_fallback_count"`
	BusinessExecutionCount          int         `json:"business_execution_invocation_count"`
	IncarnationHistogram            map[int]int `json:"incarnation_histogram"`
}

type BlockSTMProgress struct {
	BlockHeight         uint64 `json:"block_height"`
	TransactionCount    int    `json:"transaction_count"`
	ExecutionTaskCount  int    `json:"execution_task_count"`
	ValidationTaskCount int    `json:"validation_task_count"`
	ValidatedCount      int    `json:"validated_count"`
	AbortCount          int    `json:"abort_count"`
	ReexecutionCount    int    `json:"reexecution_count"`
	ActiveTaskCount     int    `json:"active_task_count"`
	SchedulerQueueLen   int    `json:"scheduler_queue_length"`
	MaximumIncarnation  int    `json:"maximum_incarnation"`
	CurrentTxnIndex     int    `json:"current_txn_index"`
	CurrentIncarnation  int    `json:"current_incarnation"`
	LastProgressAtMS    int64  `json:"last_progress_at_ms"`
}

type BlockSTMExecutor struct {
	DefaultInitialBalance  int64
	WorkerCount            int
	ExecutionMode          string
	OracleMode             string
	MaximumIncarnations    int
	IncarnationLimitAction string
	Metrics                BlockSTMMetrics
	Progress               func(BlockSTMProgress)
	serialSemantics        *SerialExecutor
}

func NewBlockSTMExecutor(workerCount int) *BlockSTMExecutor {
	if workerCount < 1 {
		workerCount = 1
	}
	return &BlockSTMExecutor{DefaultInitialBalance: 1_000_000, WorkerCount: workerCount, ExecutionMode: "correctness", OracleMode: "full", MaximumIncarnations: 0, IncarnationLimitAction: "fail", serialSemantics: NewSerialExecutor()}
}

func (e *BlockSTMExecutor) ExecuteBlock(ctx context.Context, b block.Block, base map[string]string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	workerCount := e.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	memory := blockstm.NewMVMemory()
	logicalBase := logicalSnapshot(b.ShardID, base)
	captured := make([]blockstm.CapturedReads, len(b.TxList))
	readSets := make([][]ReadObservation, len(b.TxList))
	writeSets := make([]map[string]string, len(b.TxList))
	receipts := make([]Receipt, len(b.TxList))
	incarnations := make([]int, len(b.TxList))
	validationGeneration := make([]uint64, len(b.TxList))
	metrics := BlockSTMMetrics{WorkerCount: workerCount, IncarnationHistogram: map[int]int{}}
	scheduler := blockstm.NewScheduler(len(b.TxList))
	dependencies := blockstm.NewDependencyRegistry()
	validated := make([]bool, len(b.TxList))
	executed := make([]bool, len(b.TxList))
	waiting := make([]bool, len(b.TxList))
	validationQueued := make([]bool, len(b.TxList))
	validatedCount := 0
	maximumIncarnationObserved := 0
	readersByKey := map[string]map[int]bool{}
	readKeysByTxn := make([][]string, len(b.TxList))

	clearReadIndex := func(index int) {
		if index < 0 || index >= len(readKeysByTxn) {
			return
		}
		for _, key := range readKeysByTxn[index] {
			readers := readersByKey[key]
			delete(readers, index)
			if len(readers) == 0 {
				delete(readersByKey, key)
			}
		}
		readKeysByTxn[index] = nil
	}
	indexCapturedReads := func(index int, reads blockstm.CapturedReads) {
		clearReadIndex(index)
		seen := map[string]bool{}
		for _, read := range reads.Reads {
			if read.Key == "" || seen[read.Key] {
				continue
			}
			seen[read.Key] = true
			if readersByKey[read.Key] == nil {
				readersByKey[read.Key] = map[int]bool{}
			}
			readersByKey[read.Key][index] = true
			readKeysByTxn[index] = append(readKeysByTxn[index], read.Key)
		}
	}
	scheduleValidation := func(index int) bool {
		if index < 0 || index >= len(b.TxList) || validated[index] || !executed[index] || waiting[index] || validationQueued[index] {
			return false
		}
		validationQueued[index] = true
		scheduler.ScheduleValidationGeneration(
			blockstm.Version{Txn: blockstm.TxnIndex(index), Incarnation: blockstm.Incarnation(incarnations[index])},
			validationGeneration[index],
		)
		return true
	}
	scheduleValidationRange := func(start int) bool {
		if start < 0 {
			start = 0
		}
		scheduled := false
		for index := start; index < len(b.TxList); index++ {
			if scheduleValidation(index) {
				scheduled = true
			}
		}
		return scheduled
	}
	scheduleReadyValidations := func() bool {
		return scheduleValidationRange(0)
	}
	invalidateValidationReaders := func(lowerIndex int, writes map[string]string) bool {
		targets := map[int]bool{}
		for key := range writes {
			for readerIndex := range readersByKey[key] {
				if readerIndex > lowerIndex {
					targets[readerIndex] = true
				}
			}
		}
		orderedTargets := make([]int, 0, len(targets))
		for index := range targets {
			orderedTargets = append(orderedTargets, index)
		}
		sort.Ints(orderedTargets)
		scheduled := false
		for _, index := range orderedTargets {
			if !executed[index] || waiting[index] {
				continue
			}
			if validated[index] {
				validated[index] = false
				validatedCount--
			}
			// Any queued or in-flight validation for this reader was computed
			// before the lower transaction published its first version for an
			// observed location. Supersede it and validate the same execution
			// result against the new MVMemory. Readers of unrelated locations
			// remain valid because their captured descriptors cannot change.
			validationGeneration[index]++
			validationQueued[index] = false
			if scheduleValidation(index) {
				scheduled = true
			}
		}
		return scheduled
	}
	reportProgress := func(currentIndex, currentIncarnation, activeTasks int) {
		if e.Progress == nil {
			return
		}
		e.Progress(BlockSTMProgress{
			BlockHeight:         b.Height,
			TransactionCount:    len(b.TxList),
			ExecutionTaskCount:  metrics.ExecutionTaskCount,
			ValidationTaskCount: metrics.ValidationTaskCount,
			ValidatedCount:      validatedCount,
			AbortCount:          metrics.AbortCount,
			ReexecutionCount:    metrics.ReexecutionCount,
			ActiveTaskCount:     activeTasks,
			SchedulerQueueLen:   scheduler.QueueLen(),
			MaximumIncarnation:  maximumIncarnationObserved,
			CurrentTxnIndex:     currentIndex,
			CurrentIncarnation:  currentIncarnation,
			LastProgressAtMS:    time.Now().UnixMilli(),
		})
	}

	jobs := make(chan blockSTMTaskInput)
	results := make(chan blockSTMTaskResult, workerCount)
	var activeExecutions int64
	var maxExecutions int64
	var workers sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for input := range jobs {
				taskResult := e.runBlockSTMTask(ctx, b, base, logicalBase, memory, input, workerID, &activeExecutions, &maxExecutions)
				results <- taskResult
			}
		}(worker)
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()
	activeTasks := 0
	dispatch := func() bool {
		task, ok := scheduler.Next()
		if !ok {
			return false
		}
		if queueLen := scheduler.QueueLen(); queueLen+activeTasks+1 > metrics.SchedulerQueuePeak {
			metrics.SchedulerQueuePeak = queueLen + activeTasks + 1
		}
		input := blockSTMTaskInput{Task: task}
		index := int(task.Version.Txn)
		if index >= 0 && index < len(b.TxList) {
			if task.Kind == blockstm.TaskValidate {
				input.Captured = cloneCapturedReads(captured[index])
			} else {
				input.PreviousWrites = copyStringMap(writeSets[index])
			}
		}
		select {
		case jobs <- input:
			activeTasks++
			return true
		case <-ctx.Done():
			return false
		}
	}

	reportProgress(-1, 0, 0)
	for validatedCount < len(b.TxList) {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		scheduleReadyValidations()
		for activeTasks < workerCount && dispatch() {
		}
		if activeTasks == 0 {
			scheduled := scheduleReadyValidations()
			for index := range b.TxList {
				if validated[index] || waiting[index] || executed[index] {
					continue
				}
				scheduler.ScheduleExecution(blockstm.Version{Txn: blockstm.TxnIndex(index), Incarnation: blockstm.Incarnation(incarnations[index])})
				scheduled = true
			}
			if scheduled && dispatch() {
				continue
			}
			for index := range b.TxList {
				if !validated[index] && waiting[index] {
					return Result{}, fmt.Errorf("block-stm scheduler stalled on unresolved dependency: tx=%d incarnation=%d", index, incarnations[index])
				}
			}
			return Result{}, fmt.Errorf("block-stm scheduler stalled without runnable work: validated=%d total=%d", validatedCount, len(b.TxList))
		}
		select {
		case taskResult := <-results:
			activeTasks--
			if taskResult.Err != nil {
				return Result{}, taskResult.Err
			}
			index := int(taskResult.Version.Txn)
			if index < 0 || index >= len(b.TxList) || int(taskResult.Version.Incarnation) != incarnations[index] {
				metrics.StaleTaskCount++
				reportProgress(index, int(taskResult.Version.Incarnation), activeTasks)
				continue
			}
			if taskResult.Kind == blockstm.TaskValidate && taskResult.Generation != validationGeneration[index] {
				metrics.StaleTaskCount++
				reportProgress(index, int(taskResult.Version.Incarnation), activeTasks)
				continue
			}
			if validated[index] {
				metrics.StaleTaskCount++
				reportProgress(index, int(taskResult.Version.Incarnation), activeTasks)
				continue
			}
			switch taskResult.Kind {
			case blockstm.TaskExecute:
				metrics.ExecutionTaskCount++
				metrics.BusinessExecutionCount++
				metrics.SpeculativeReadCount += len(taskResult.Captured.Reads)
				if taskResult.Version.Incarnation > 0 {
					metrics.ReexecutionCount++
				}
				if taskResult.Dependency != nil {
					dependencyIndex := int(taskResult.Dependency.Txn)
					dependencyResolved := dependencyIndex >= 0 && dependencyIndex < len(executed) && executed[dependencyIndex] && incarnations[dependencyIndex] > int(taskResult.Dependency.Incarnation)
					if dependencyResolved {
						waiting[index] = false
						scheduler.ScheduleExecution(taskResult.Version)
						metrics.DependencyResumeCount++
						break
					}
					next := scheduler.AbortAndWait(taskResult.Version)
					incarnations[index] = int(next.Incarnation)
					metrics.AbortCount = scheduler.AbortCount()
					metrics.DependencyAbortCount++
					if incarnations[index] > maximumIncarnationObserved {
						maximumIncarnationObserved = incarnations[index]
					}
					if e.MaximumIncarnations > 0 && incarnations[index] >= e.MaximumIncarnations {
						metrics.IncarnationLimitHitCount++
						if e.IncarnationLimitAction == "serial_fallback" {
							metrics.SerialFallbackCount++
							return e.serialFallbackResult(b, base, workerCount, metrics), nil
						}
						return Result{}, fmt.Errorf("block-stm maximum incarnations exceeded for tx %s", b.TxList[index].TxID)
					}
					waiting[index] = true
					executed[index] = false
					validationQueued[index] = false
					clearReadIndex(index)
					dependencies.RegisterTask(blockstm.SchedulerTask{Kind: blockstm.TaskExecute, Version: next}, *taskResult.Dependency)
					metrics.DependencyWaitCount++
					metrics.EstimateReadCount++
					readSets[index] = append([]ReadObservation(nil), taskResult.ReadSet...)
					break
				}
				captured[index] = taskResult.Captured
				readSets[index] = append([]ReadObservation(nil), taskResult.ReadSet...)
				writeSets[index] = taskResult.WriteSet
				receipts[index] = taskResult.Receipt
				executed[index] = true
				waiting[index] = false
				indexCapturedReads(index, taskResult.Captured)

				// An execution dependency is resolved when the blocking
				// transaction finishes its next incarnation, not when that
				// incarnation is later validated.
				for incarnation := 0; incarnation < int(taskResult.Version.Incarnation); incarnation++ {
					resolved := blockstm.Version{Txn: taskResult.Version.Txn, Incarnation: blockstm.Incarnation(incarnation)}
					for _, waiter := range dependencies.ResolveTasks(resolved) {
						waiterIndex := int(waiter.Version.Txn)
						if waiterIndex < 0 || waiterIndex >= len(waiting) || !waiting[waiterIndex] || int(waiter.Version.Incarnation) != incarnations[waiterIndex] {
							continue
						}
						waiting[waiterIndex] = false
						executed[waiterIndex] = false
						validationQueued[waiterIndex] = false
						scheduler.ResumeTask(waiter)
						metrics.DependencyResumeCount++
					}
				}
				if taskResult.WroteNewLocation {
					// Block-STM lowers the validation frontier when a transaction
					// publishes a location that did not exist in its previous
					// incarnation.  Higher transactions may already have validated
					// against the absence of that version, so their successful
					// validation state must be revoked before re-validation.
					scheduleValidation(index)
					invalidateValidationReaders(index, taskResult.WriteSet)
				} else {
					scheduleValidation(index)
				}

			case blockstm.TaskValidate:
				metrics.ValidationTaskCount++
				validationQueued[index] = false
				if taskResult.Validation.Valid {
					validated[index] = true
					validatedCount++
					metrics.ValidatedSpeculativeResultCount++
					scheduler.Commit(taskResult.Version)
					break
				}

				metrics.ValidationFailureCount++
				if taskResult.Validation.Dependency != nil {
					metrics.EstimateReadCount++
				}
				abortedWrites := writeSets[index]
				for key := range abortedWrites {
					memory.MarkEstimate(key, taskResult.Version)
					metrics.EstimateCount++
					metrics.EstimateMarkCount++
				}
				next := scheduler.Abort(taskResult.Version)
				metrics.AbortCount = scheduler.AbortCount()
				metrics.ValidationAbortCount++
				incarnations[index] = int(next.Incarnation)
				validationGeneration[index]++
				if incarnations[index] > maximumIncarnationObserved {
					maximumIncarnationObserved = incarnations[index]
				}
				executed[index] = false
				waiting[index] = false
				validationQueued[index] = false
				clearReadIndex(index)

				// A successful validation abort schedules every higher
				// transaction that has already executed for optimistic
				// re-validation. Transactions still in E or currently executing
				// will schedule their own validation when execution finishes.
				for higher := index + 1; higher < len(b.TxList); higher++ {
					if !executed[higher] || waiting[higher] {
						continue
					}
					if validated[higher] {
						validated[higher] = false
						validatedCount--
					}
					validationGeneration[higher]++
					validationQueued[higher] = false
					scheduleValidation(higher)
				}
				if e.MaximumIncarnations > 0 && incarnations[index] >= e.MaximumIncarnations {
					metrics.IncarnationLimitHitCount++
					if e.IncarnationLimitAction == "serial_fallback" {
						metrics.SerialFallbackCount++
						return e.serialFallbackResult(b, base, workerCount, metrics), nil
					}
					return Result{}, fmt.Errorf("block-stm maximum incarnations exceeded for tx %s", b.TxList[index].TxID)
				}
			}
			reportProgress(index, int(taskResult.Version.Incarnation), activeTasks)
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	metrics.MaximumConcurrentExecutions = int(atomic.LoadInt64(&maxExecutions))
	if metrics.MaximumConcurrentExecutions > metrics.MaximumParallelWidth {
		metrics.MaximumParallelWidth = metrics.MaximumConcurrentExecutions
	}

	serialWorking := copySnapshot(base)
	materializeStarted := time.Now()
	result := Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: state.RootOfSnapshot(copySnapshot(base)), Deterministic: true, EVMExecution: false, FabricExecution: false, StateUpdates: map[string]string{}, BlockExecutorID: BlockSTMExecutorID, ExecutorVersion: BlockSTMExecutorVersion, WorkerCount: workerCount}
	for index, item := range b.TxList {
		if !validated[index] {
			return Result{}, fmt.Errorf("block-stm missing validated incarnation for tx %s", item.TxID)
		}
		for key, value := range writeSets[index] {
			serialWorking[qualifyKey(b.ShardID, key)] = value
		}
		receipt := receipts[index]
		if receipt.TxID == "" {
			return Result{}, fmt.Errorf("block-stm missing materialized receipt for tx %s", item.TxID)
		}
		receipt.StateRootAfterTx = state.RootOfSnapshot(serialWorking)
		delta := TxDelta{TxID: item.TxID, OriginalIndex: index, ReadSet: readSets[index], WriteSet: writeSets[index], Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, receipt)
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
		metrics.CommittedTransactionCount++
		metrics.IncarnationHistogram[incarnations[index]]++
		if incarnations[index] > metrics.MaximumIncarnation {
			metrics.MaximumIncarnation = incarnations[index]
		}
	}
	result.StateRootAfter = state.RootOfSnapshot(serialWorking)
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	for key, value := range serialWorking {
		result.StateUpdates[key] = value
	}
	result.StateDelta = stateDelta(base, serialWorking)
	metrics.MaterializationMS = time.Since(materializeStarted).Milliseconds()

	if e.shouldRunSerialOracle() {
		oracleStarted := time.Now()
		serialOracle := NewSerialExecutor().ExecuteBlock(b, base)
		metrics.SerialOracleMS = time.Since(oracleStarted).Milliseconds()
		if !sameExecutionOutput(serialOracle, result) {
			return Result{}, fmt.Errorf("block-stm ordered materialization diverged from serial oracle: serial_root=%s got_root=%s serial_receipt=%s got_receipt=%s serial_delta=%v got_delta=%v serial_receipts=%v got_receipts=%v", serialOracle.StateRootAfter, result.StateRootAfter, serialOracle.ReceiptRoot, result.ReceiptRoot, serialOracle.StateDelta, result.StateDelta, serialOracle.Receipts, result.Receipts)
		}
	}
	declared := declaredAccessSet(b.TxList)
	plan := buildBlockSTMPlan(b, declared, workerCount)
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	result.BlockSTMMetrics = metrics
	e.Metrics = metrics
	if result.StateRootAfter != state.RootOfSnapshot(serialWorking) {
		return Result{}, fmt.Errorf("block-stm ordered materialization root mismatch")
	}
	result.SerialEquivalent = true
	reportProgress(-1, metrics.MaximumIncarnation, 0)
	return result, nil
}

func (e *BlockSTMExecutor) serialFallbackResult(b block.Block, base map[string]string, workerCount int, metrics BlockSTMMetrics) Result {
	serial := NewSerialExecutor().ExecuteBlock(b, base)
	serial.BlockExecutorID = BlockSTMExecutorID
	serial.ExecutorVersion = BlockSTMExecutorVersion
	serial.WorkerCount = workerCount
	serial.SerialEquivalent = true
	plan := buildBlockSTMPlan(b, declaredAccessSet(b.TxList), workerCount)
	serial.Plan = plan
	serial.PlanDigest = plan.PlanDigest
	metrics.CommittedTransactionCount = len(serial.TxDeltas)
	metrics.MaterializationMS = 0
	if metrics.IncarnationHistogram == nil {
		metrics.IncarnationHistogram = map[int]int{}
	}
	for range serial.TxDeltas {
		metrics.IncarnationHistogram[0]++
	}
	serial.BlockSTMMetrics = metrics
	e.Metrics = metrics
	return serial
}

type blockSTMTaskInput struct {
	Task           blockstm.SchedulerTask
	Captured       blockstm.CapturedReads
	PreviousWrites map[string]string
}

func cloneCapturedReads(input blockstm.CapturedReads) blockstm.CapturedReads {
	return blockstm.CapturedReads{Reads: append([]blockstm.ReadDescriptor(nil), input.Reads...)}
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

type blockSTMTaskResult struct {
	Kind             blockstm.SchedulerTaskKind
	Version          blockstm.Version
	Generation       uint64
	Captured         blockstm.CapturedReads
	ReadSet          []ReadObservation
	WriteSet         map[string]string
	WroteNewLocation bool
	Receipt          Receipt
	Validation       blockstm.ValidationResult
	Dependency       *blockstm.Version
	Err              error
}

type stmOverlay struct {
	shardID     string
	base        map[string]string
	logicalBase map[string]string
	memory      *blockstm.MVMemory
	reader      blockstm.TxnIndex
	writes      map[string]string
	reads       []ReadObservation
	captured    blockstm.CapturedReads
	dependency  *blockstm.Version
}

func newSTMOverlay(shardID string, base, logicalBase map[string]string, memory *blockstm.MVMemory, reader blockstm.TxnIndex) *stmOverlay {
	// base and logicalBase are immutable for the lifetime of ExecuteBlock.
	// Sharing them avoids copying the complete state for every incarnation.
	return &stmOverlay{shardID: shardID, base: base, logicalBase: logicalBase, memory: memory, reader: reader, writes: map[string]string{}}
}

func (o *stmOverlay) get(key string) string {
	if value, ok := o.writes[key]; ok {
		o.reads = append(o.reads, ReadObservation{Key: key, Value: value, ValueDigest: digestValue(value), Source: "stm_local_write"})
		return value
	}
	read := o.memory.Read(key, o.reader, o.logicalBase)
	if read.Estimate && read.DependencyOn != nil {
		dependency := *read.DependencyOn
		o.dependency = &dependency
	}
	source := "stm_mvmemory_base"
	if read.Estimate {
		source = "stm_mvmemory_estimate"
	} else if !read.FromBase {
		source = fmt.Sprintf("stm_mvmemory_tx_%d_inc_%d", read.Version.Txn, read.Version.Incarnation)
	}
	o.reads = append(o.reads, ReadObservation{Key: key, Value: read.Value, ValueDigest: digestValue(read.Value), Source: source})
	o.captured.Add(read)
	return read.Value
}

func (o *stmOverlay) set(key, value string) {
	o.writes[key] = value
}

func (o *stmOverlay) snapshot() map[string]string {
	out := copySnapshot(o.base)
	for key, value := range o.writes {
		out[qualifyKey(o.shardID, key)] = value
	}
	return out
}

func (o *stmOverlay) logicalWrites() map[string]string {
	out := map[string]string{}
	for key, value := range o.writes {
		out[key] = value
	}
	return out
}

func (o *stmOverlay) ensureAccount(account string, balance int64) {
	if o.get("balance:"+account) == "" {
		o.setBalance(account, balance)
	}
	if o.get("nonce:"+account) == "" {
		o.setNonce(account, 0)
	}
}

func (o *stmOverlay) balance(account string) int64 {
	value, _ := strconv.ParseInt(o.get("balance:"+account), 10, 64)
	return value
}

func (o *stmOverlay) setBalance(account string, balance int64) {
	o.set("balance:"+account, strconv.FormatInt(balance, 10))
}

func (o *stmOverlay) nonce(account string) uint64 {
	value, _ := strconv.ParseUint(o.get("nonce:"+account), 10, 64)
	return value
}

func (o *stmOverlay) setNonce(account string, nonce uint64) {
	o.set("nonce:"+account, strconv.FormatUint(nonce, 10))
}

func (o *stmOverlay) applyCommutativeDeltas(accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessCommutativeDelta || access.Key == "" {
			continue
		}
		current, _ := strconv.ParseInt(o.get(access.Key), 10, 64)
		o.set(access.Key, strconv.FormatInt(current+access.Delta, 10))
	}
}

func (e *BlockSTMExecutor) runBlockSTMTask(ctx context.Context, b block.Block, base, logicalBase map[string]string, memory *blockstm.MVMemory, input blockSTMTaskInput, workerID int, activeExecutions, maxExecutions *int64) blockSTMTaskResult {
	task := input.Task
	if err := ctx.Err(); err != nil {
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Err: err}
	}
	index := int(task.Version.Txn)
	if index < 0 || index >= len(b.TxList) {
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Err: fmt.Errorf("block-stm task index out of range: %+v", task)}
	}
	switch task.Kind {
	case blockstm.TaskExecute:
		current := atomic.AddInt64(activeExecutions, 1)
		for {
			previous := atomic.LoadInt64(maxExecutions)
			if current <= previous || atomic.CompareAndSwapInt64(maxExecutions, previous, current) {
				break
			}
		}
		defer atomic.AddInt64(activeExecutions, -1)
		txnIndex := blockstm.TxnIndex(index)
		overlay := newSTMOverlay(b.ShardID, base, logicalBase, memory, txnIndex)
		receipt := e.executeTx(b, overlay, b.TxList[index])
		writes := overlay.logicalWrites()
		if overlay.dependency != nil {
			return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Captured: overlay.captured, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: writes, Receipt: receipt, Dependency: overlay.dependency}
		}
		previousWrites := input.PreviousWrites
		wroteNewLocation := false
		for key := range writes {
			if _, existed := previousWrites[key]; !existed {
				wroteNewLocation = true
			}
		}
		for key := range previousWrites {
			if _, ok := writes[key]; ok {
				continue
			}
			// MVMemory.record removes locations dropped by the latest
			// incarnation, including stale ESTIMATE entries.
			memory.ClearTxnVersions(key, task.Version.Txn)
		}
		for key, value := range writes {
			memory.Write(key, task.Version, value)
		}
		_ = workerID
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Captured: overlay.captured, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: writes, WroteNewLocation: wroteNewLocation, Receipt: receipt}
	case blockstm.TaskValidate:
		validation := memory.Validate(blockstm.TxnIndex(index), logicalBase, input.Captured)
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Validation: validation}
	default:
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Generation: task.Generation, Err: fmt.Errorf("unknown block-stm task kind %s", task.Kind)}
	}
}

func txnOrderFromInts(values []int) []blockstm.TxnIndex {
	out := make([]blockstm.TxnIndex, 0, len(values))
	for _, value := range values {
		out = append(out, blockstm.TxnIndex(value))
	}
	return out
}

func validateCapturedAgainstPrefix(shardID string, base map[string]string, validated []bool, writeSets []map[string]string, index int, captured blockstm.CapturedReads) blockstm.ValidationResult {
	prefix := copySnapshot(base)
	for lower := 0; lower < index; lower++ {
		if !validated[lower] {
			continue
		}
		for key, value := range writeSets[lower] {
			prefix[qualifyKey(shardID, key)] = value
		}
	}
	for _, expected := range captured.Reads {
		observed := blockstm.ReadDescriptor{Key: expected.Key, FromBase: true, Value: prefix[qualifyKey(shardID, expected.Key)]}
		if expected.Value != observed.Value {
			return blockstm.ValidationResult{Valid: false, FailedKey: expected.Key, Expected: expected, Observed: observed}
		}
	}
	return blockstm.ValidationResult{Valid: true}
}

func validateReadSetAgainstPrefix(shardID string, base map[string]string, validated []bool, writeSets []map[string]string, index int, reads []ReadObservation) blockstm.ValidationResult {
	prefix := copySnapshot(base)
	for lower := 0; lower < index; lower++ {
		if !validated[lower] {
			continue
		}
		for key, value := range writeSets[lower] {
			prefix[qualifyKey(shardID, key)] = value
		}
	}
	seen := map[string]bool{}
	for _, expected := range reads {
		if seen[expected.Key] {
			continue
		}
		seen[expected.Key] = true
		observed := prefix[qualifyKey(shardID, expected.Key)]
		if expected.Value != observed {
			return blockstm.ValidationResult{
				Valid:     false,
				FailedKey: expected.Key,
				Expected:  blockstm.ReadDescriptor{Key: expected.Key, FromBase: true, Value: expected.Value},
				Observed:  blockstm.ReadDescriptor{Key: expected.Key, FromBase: true, Value: observed},
			}
		}
	}
	return blockstm.ValidationResult{Valid: true}
}

func (e *BlockSTMExecutor) shouldRunSerialOracle() bool {
	mode := e.ExecutionMode
	if mode == "" {
		mode = "correctness"
	}
	oracle := e.OracleMode
	if oracle == "" {
		oracle = "full"
	}
	return mode == "correctness" && oracle == "full"
}

func sameExecutionOutput(left, right Result) bool {
	if left.StateRootBefore != right.StateRootBefore || left.StateRootAfter != right.StateRootAfter || left.ReceiptRoot != right.ReceiptRoot {
		return false
	}
	if left.SuccessfulTxs != right.SuccessfulTxs || left.FailedTxs != right.FailedTxs {
		return false
	}
	if len(left.Receipts) != len(right.Receipts) || len(left.StateDelta) != len(right.StateDelta) {
		return false
	}
	for index := range left.Receipts {
		if !reflect.DeepEqual(left.Receipts[index], right.Receipts[index]) {
			return false
		}
	}
	for index := range left.StateDelta {
		if left.StateDelta[index] != right.StateDelta[index] {
			return false
		}
	}
	return true
}

func qualifyKey(shardID, key string) string {
	if key == "" || strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func readSetMatchesSnapshot(shardID string, snapshot map[string]string, reads []ReadObservation) bool {
	for _, read := range reads {
		if snapshot[qualifyKey(shardID, read.Key)] != read.Value {
			return false
		}
	}
	return true
}

func (e *BlockSTMExecutor) executeTx(b block.Block, overlay txExecutionOverlay, item tx.SignedTransaction) Receipt {
	semantics := NewSerialExecutor()
	semantics.DefaultInitialBalance = e.DefaultInitialBalance
	return semantics.executeTxWithoutStateRoot(b, overlay, item)
}

func (e *BlockSTMExecutor) executeSpeculative(ctx context.Context, b block.Block, base, logicalBase map[string]string, memory *blockstm.MVMemory, captured []blockstm.CapturedReads, readSets [][]ReadObservation, writeSets []map[string]string, receipts []Receipt, metrics *BlockSTMMetrics) error {
	if len(b.TxList) == 0 {
		return nil
	}
	workerCount := minInt(e.WorkerCount, len(b.TxList))
	if workerCount < 1 {
		workerCount = 1
	}
	jobs := make(chan blockstm.SchedulerTask)
	errs := make(chan error, 1)
	var active int64
	var maxActive int64
	var executed int64
	var readCount int64
	var wg sync.WaitGroup
	order := speculativeExecutionOrder(len(b.TxList), workerCount)
	schedulerOrder := make([]blockstm.TxnIndex, 0, len(order))
	for _, index := range order {
		schedulerOrder = append(schedulerOrder, blockstm.TxnIndex(index))
	}
	scheduler := blockstm.NewSchedulerWithOrder(len(b.TxList), schedulerOrder)
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := ctx.Err(); err != nil {
					select {
					case errs <- err:
					default:
					}
					continue
				}
				if task.Kind != blockstm.TaskExecute {
					continue
				}
				index := int(task.Version.Txn)
				current := atomic.AddInt64(&active, 1)
				for {
					previous := atomic.LoadInt64(&maxActive)
					if current <= previous || atomic.CompareAndSwapInt64(&maxActive, previous, current) {
						break
					}
				}
				version := blockstm.Version{Txn: blockstm.TxnIndex(index), Incarnation: 0}
				txnIndex := blockstm.TxnIndex(index)
				overlay := newTxOverlay(b.ShardID, speculativeSnapshot(memory, base, logicalBase, b.ShardID, txnIndex))
				receipt := e.executeTx(b, overlay, b.TxList[index])
				atomic.AddInt64(&executed, 1)
				localCaptured := capturedFromOverlayWithMemory(overlay, memory, logicalBase, txnIndex)
				localWrites := overlay.logicalWrites()
				for key, value := range localWrites {
					memory.Write(key, version, value)
				}
				captured[index] = localCaptured
				readSets[index] = append([]ReadObservation(nil), overlay.reads...)
				writeSets[index] = localWrites
				receipts[index] = receipt
				atomic.AddInt64(&readCount, int64(len(localCaptured.Reads)))
				atomic.AddInt64(&active, -1)
			}
		}()
	}
	for {
		task, ok := scheduler.Next()
		if !ok {
			break
		}
		select {
		case jobs <- task:
		case err := <-errs:
			close(jobs)
			wg.Wait()
			return err
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	metrics.ExecutionTaskCount += int(executed)
	metrics.BusinessExecutionCount += int(executed)
	metrics.SpeculativeReadCount += int(readCount)
	if max := int(atomic.LoadInt64(&maxActive)); max > metrics.MaximumParallelWidth {
		metrics.MaximumParallelWidth = max
	}
	return nil
}

func speculativeExecutionOrder(count, workerCount int) []int {
	_ = workerCount
	out := make([]int, 0, count)
	for index := 0; index < count; index++ {
		out = append(out, index)
	}
	return out
}

func (e *BlockSTMExecutor) validateSpeculative(ctx context.Context, b block.Block, logicalBase map[string]string, memory *blockstm.MVMemory, captured []blockstm.CapturedReads, metrics *BlockSTMMetrics) ([]blockstm.ValidationResult, error) {
	results := make([]blockstm.ValidationResult, len(b.TxList))
	if len(b.TxList) == 0 {
		return results, nil
	}
	workerCount := minInt(e.WorkerCount, len(b.TxList))
	if workerCount < 1 {
		workerCount = 1
	}
	jobs := make(chan blockstm.SchedulerTask)
	errs := make(chan error, 1)
	var active int64
	var maxActive int64
	var validated int64
	var wg sync.WaitGroup
	order := make([]blockstm.TxnIndex, 0, len(b.TxList))
	for index := range b.TxList {
		order = append(order, blockstm.TxnIndex(index))
	}
	scheduler := blockstm.NewValidationSchedulerWithOrder(len(b.TxList), order)
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if err := ctx.Err(); err != nil {
					select {
					case errs <- err:
					default:
					}
					continue
				}
				if task.Kind != blockstm.TaskValidate {
					continue
				}
				index := int(task.Version.Txn)
				current := atomic.AddInt64(&active, 1)
				for {
					previous := atomic.LoadInt64(&maxActive)
					if current <= previous || atomic.CompareAndSwapInt64(&maxActive, previous, current) {
						break
					}
				}
				results[index] = memory.Validate(blockstm.TxnIndex(index), logicalBase, captured[index])
				atomic.AddInt64(&validated, 1)
				atomic.AddInt64(&active, -1)
			}
		}()
	}
	for {
		task, ok := scheduler.Next()
		if !ok {
			break
		}
		select {
		case jobs <- task:
		case err := <-errs:
			close(jobs)
			wg.Wait()
			return nil, err
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	metrics.ValidationTaskCount += int(validated)
	if max := int(atomic.LoadInt64(&maxActive)); max > metrics.MaximumParallelWidth {
		metrics.MaximumParallelWidth = max
	}
	return results, nil
}

func speculativeSnapshot(memory *blockstm.MVMemory, base, logicalBase map[string]string, shardID string, reader blockstm.TxnIndex) map[string]string {
	out := copySnapshot(base)
	for key := range logicalBase {
		read := memory.Read(key, reader, logicalBase)
		if !read.FromBase && !read.Estimate {
			out[prefixedKey(shardID, key)] = read.Value
		}
	}
	for key := range memory.Snapshot() {
		read := memory.Read(key, reader, logicalBase)
		if !read.FromBase && !read.Estimate {
			out[prefixedKey(shardID, key)] = read.Value
		}
	}
	return out
}

func logicalSnapshot(shardID string, snapshot map[string]string) map[string]string {
	out := map[string]string{}
	prefix := shardID + "::"
	for key, value := range snapshot {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			out[key[len(prefix):]] = value
			continue
		}
		out[key] = value
	}
	return out
}

func prefixedKey(shardID, key string) string {
	if len(key) >= len(shardID)+2 && key[:len(shardID)+2] == shardID+"::" {
		return key
	}
	return shardID + "::" + key
}

func capturedFromOverlay(overlay *txOverlay) blockstm.CapturedReads {
	var captured blockstm.CapturedReads
	for _, read := range overlay.reads {
		captured.Add(blockstm.ReadDescriptor{Key: read.Key, FromBase: true, Value: read.Value})
	}
	return captured
}

func capturedFromOverlayWithMemory(overlay *txOverlay, memory *blockstm.MVMemory, logicalBase map[string]string, reader blockstm.TxnIndex) blockstm.CapturedReads {
	var captured blockstm.CapturedReads
	for _, read := range overlay.reads {
		captured.Add(memory.Read(read.Key, reader, logicalBase))
	}
	return captured
}

func buildBlockSTMPlan(b block.Block, declared AccessSet, workerCount int) ExecutionPlan {
	plan := buildSerialPlan(b, declared)
	plan.EngineID = BlockSTMExecutorID
	plan.EngineVersion = BlockSTMExecutorVersion
	plan.WorkerCount = workerCount
	plan.PlanDigest = stableDigest(struct {
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
