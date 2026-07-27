package execution

import (
	"context"
	"fmt"
	"reflect"
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
const BlockSTMExecutorVersion = "0.1.0"

type BlockSTMMetrics struct {
	WorkerCount                     int         `json:"worker_count"`
	MaximumParallelWidth            int         `json:"maximum_parallel_width"`
	ExecutionTaskCount              int         `json:"execution_task_count"`
	ValidationTaskCount             int         `json:"validation_task_count"`
	AbortCount                      int         `json:"abort_count"`
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

type BlockSTMExecutor struct {
	DefaultInitialBalance  int64
	WorkerCount            int
	ExecutionMode          string
	OracleMode             string
	MaximumIncarnations    int
	IncarnationLimitAction string
	Metrics                BlockSTMMetrics
	serialSemantics        *SerialExecutor
}

func NewBlockSTMExecutor(workerCount int) *BlockSTMExecutor {
	if workerCount < 1 {
		workerCount = 1
	}
	return &BlockSTMExecutor{DefaultInitialBalance: 1_000_000, WorkerCount: workerCount, ExecutionMode: "correctness", OracleMode: "full", MaximumIncarnations: 16, IncarnationLimitAction: "fail", serialSemantics: NewSerialExecutor()}
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
	metrics := BlockSTMMetrics{WorkerCount: workerCount, IncarnationHistogram: map[int]int{}}
	initialOrder := txnOrderFromInts(speculativeExecutionOrder(len(b.TxList), workerCount))
	scheduler := blockstm.NewSchedulerWithOrder(len(b.TxList), initialOrder)
	dependencies := blockstm.NewDependencyRegistry()
	validated := make([]bool, len(b.TxList))
	executed := make([]bool, len(b.TxList))
	waiting := make([]bool, len(b.TxList))
	validationQueued := make([]bool, len(b.TxList))
	validatedCount := 0
	jobs := make(chan blockstm.SchedulerTask)
	results := make(chan blockSTMTaskResult, workerCount)
	var activeExecutions int64
	var maxExecutions int64
	var workers sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for task := range jobs {
				taskResult := e.runBlockSTMTask(ctx, b, base, logicalBase, memory, captured, readSets, validated, writeSets, task, workerID, &activeExecutions, &maxExecutions)
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
		select {
		case jobs <- task:
			activeTasks++
			return true
		case <-ctx.Done():
			return false
		}
	}
	for validatedCount < len(b.TxList) {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		for activeTasks < workerCount && dispatch() {
		}
		if activeTasks == 0 {
			if recoverBlockSTMSchedulerProgress(scheduler, validated, executed, validationQueued, incarnations, &metrics) {
				continue
			}
			return Result{}, fmt.Errorf("block-stm scheduler drained before all transactions validated: validated=%d total=%d", validatedCount, len(b.TxList))
		}
		select {
		case taskResult := <-results:
			if activeTasks > 0 {
				activeTasks--
			}
			if taskResult.Err != nil {
				return Result{}, taskResult.Err
			}
			index := int(taskResult.Version.Txn)
			if index < 0 || index >= len(b.TxList) || int(taskResult.Version.Incarnation) != incarnations[index] || validated[index] {
				if index >= 0 && index < len(validationQueued) {
					validationQueued[index] = false
				}
				metrics.StaleTaskCount++
				continue
			}
			switch taskResult.Kind {
			case blockstm.TaskExecute:
				if taskResult.Dependency != nil {
					dependencyIndex := int(taskResult.Dependency.Txn)
					if dependencyIndex >= 0 && dependencyIndex < len(validated) && validated[dependencyIndex] {
						waiting[index] = false
						scheduler.ScheduleExecution(taskResult.Version)
						metrics.DependencyResumeCount++
						continue
					}
					waiting[index] = true
					scheduler.Wait(taskResult.Version)
					dependencies.Register(taskResult.Version, *taskResult.Dependency)
					metrics.DependencyWaitCount++
					metrics.EstimateReadCount++
					readSets[index] = append([]ReadObservation(nil), taskResult.ReadSet...)
					continue
				}
				captured[index] = taskResult.Captured
				readSets[index] = append([]ReadObservation(nil), taskResult.ReadSet...)
				writeSets[index] = taskResult.WriteSet
				receipts[index] = taskResult.Receipt
				executed[index] = true
				waiting[index] = false
				metrics.ExecutionTaskCount++
				metrics.BusinessExecutionCount++
				metrics.SpeculativeReadCount += len(taskResult.Captured.Reads)
				if taskResult.Version.Incarnation > 0 {
					metrics.ReexecutionCount++
				}
				if (taskResult.Version.Incarnation > 0 || initialAttemptsComplete(executed, waiting)) && lowerTransactionsValidated(validated, index) {
					validationQueued[index] = true
					scheduler.ScheduleValidation(taskResult.Version)
				}
				for nextIndex := range b.TxList {
					if executed[nextIndex] && !validated[nextIndex] && !validationQueued[nextIndex] && (incarnations[nextIndex] > 0 || initialAttemptsComplete(executed, waiting)) && lowerTransactionsValidated(validated, nextIndex) {
						validationQueued[nextIndex] = true
						scheduler.ScheduleValidation(blockstm.Version{Txn: blockstm.TxnIndex(nextIndex), Incarnation: blockstm.Incarnation(incarnations[nextIndex])})
					}
				}
			case blockstm.TaskValidate:
				metrics.ValidationTaskCount++
				if taskResult.Validation.Valid {
					validated[index] = true
					validatedCount++
					metrics.ValidatedSpeculativeResultCount++
					scheduler.Commit(taskResult.Version)
					for incarnation := 0; incarnation <= int(taskResult.Version.Incarnation); incarnation++ {
						resolved := blockstm.Version{Txn: taskResult.Version.Txn, Incarnation: blockstm.Incarnation(incarnation)}
						for _, waiter := range dependencies.Resolve(resolved) {
							waiterIndex := int(waiter.Txn)
							if waiterIndex >= 0 && waiterIndex < len(waiting) && waiting[waiterIndex] {
								incarnations[waiterIndex] = int(waiter.Incarnation)
								waiting[waiterIndex] = false
								executed[waiterIndex] = false
								validationQueued[waiterIndex] = false
								scheduler.ScheduleExecution(waiter)
							} else {
								scheduler.Resume(waiter)
							}
							metrics.DependencyResumeCount++
						}
					}
					for nextIndex := range b.TxList {
						if executed[nextIndex] && !validated[nextIndex] && !validationQueued[nextIndex] && (incarnations[nextIndex] > 0 || initialAttemptsComplete(executed, waiting)) && lowerTransactionsValidated(validated, nextIndex) {
							validationQueued[nextIndex] = true
							scheduler.ScheduleValidation(blockstm.Version{Txn: blockstm.TxnIndex(nextIndex), Incarnation: blockstm.Incarnation(incarnations[nextIndex])})
						}
					}
					continue
				}
				metrics.ValidationFailureCount++
				validationQueued[index] = false
				if taskResult.Validation.Dependency != nil {
					waiting[index] = true
					scheduler.Wait(taskResult.Version)
					dependencies.Register(taskResult.Version, *taskResult.Validation.Dependency)
					metrics.DependencyWaitCount++
					metrics.EstimateReadCount++
					executed[index] = false
					continue
				}
				abortedWrites := writeSets[index]
				for key := range abortedWrites {
					memory.MarkEstimate(key, taskResult.Version)
					metrics.EstimateCount++
					metrics.EstimateMarkCount++
				}
				next := scheduler.Abort(taskResult.Version)
				metrics.AbortCount = scheduler.AbortCount()
				incarnations[index] = int(next.Incarnation)
				executed[index] = false
				waiting[index] = false
				for higher := index + 1; higher < len(b.TxList); higher++ {
					if validated[higher] || !executed[higher] || !capturedReadsTouchWrites(captured[higher], abortedWrites) {
						continue
					}
					higherWrites := writeSets[higher]
					higherVersion := blockstm.Version{Txn: blockstm.TxnIndex(higher), Incarnation: blockstm.Incarnation(incarnations[higher])}
					for key := range higherWrites {
						memory.MarkEstimate(key, higherVersion)
						metrics.EstimateCount++
						metrics.EstimateMarkCount++
					}
					nextHigher := scheduler.Abort(higherVersion)
					metrics.AbortCount = scheduler.AbortCount()
					incarnations[higher] = int(nextHigher.Incarnation)
					executed[higher] = false
					validationQueued[higher] = false
					waiting[higher] = true
					scheduler.Wait(nextHigher)
					dependencies.Register(nextHigher, next)
					metrics.DependencyWaitCount++
					metrics.EstimateReadCount++
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

type blockSTMTaskResult struct {
	Kind       blockstm.SchedulerTaskKind
	Version    blockstm.Version
	Captured   blockstm.CapturedReads
	ReadSet    []ReadObservation
	WriteSet   map[string]string
	Receipt    Receipt
	Validation blockstm.ValidationResult
	Dependency *blockstm.Version
	Err        error
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
	return &stmOverlay{shardID: shardID, base: copySnapshot(base), logicalBase: copySnapshot(logicalBase), memory: memory, reader: reader, writes: map[string]string{}}
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

func (e *BlockSTMExecutor) runBlockSTMTask(ctx context.Context, b block.Block, base, logicalBase map[string]string, memory *blockstm.MVMemory, captured []blockstm.CapturedReads, readSets [][]ReadObservation, validated []bool, writeSets []map[string]string, task blockstm.SchedulerTask, workerID int, activeExecutions, maxExecutions *int64) blockSTMTaskResult {
	if err := ctx.Err(); err != nil {
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Err: err}
	}
	index := int(task.Version.Txn)
	if index < 0 || index >= len(b.TxList) {
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Err: fmt.Errorf("block-stm task index out of range: %+v", task)}
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
			return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Captured: overlay.captured, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: writes, Receipt: receipt, Dependency: overlay.dependency}
		}
		for key := range writeSets[index] {
			if _, ok := writes[key]; ok {
				continue
			}
			memory.ClearTxnVersions(key, task.Version.Txn)
		}
		for key, value := range writes {
			memory.Write(key, task.Version, value)
		}
		_ = workerID
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Captured: overlay.captured, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: writes, Receipt: receipt}
	case blockstm.TaskValidate:
		validation := memory.Validate(blockstm.TxnIndex(index), logicalBase, captured[index])
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Validation: validation}
	default:
		return blockSTMTaskResult{Kind: task.Kind, Version: task.Version, Err: fmt.Errorf("unknown block-stm task kind %s", task.Kind)}
	}
}

func txnOrderFromInts(values []int) []blockstm.TxnIndex {
	out := make([]blockstm.TxnIndex, 0, len(values))
	for _, value := range values {
		out = append(out, blockstm.TxnIndex(value))
	}
	return out
}

func lowerTransactionsValidated(validated []bool, index int) bool {
	for lower := 0; lower < index; lower++ {
		if !validated[lower] {
			return false
		}
	}
	return true
}

func allTransactionsExecuted(executed []bool) bool {
	for _, ok := range executed {
		if !ok {
			return false
		}
	}
	return true
}

func initialAttemptsComplete(executed, waiting []bool) bool {
	for index := range executed {
		if !executed[index] && !waiting[index] {
			return false
		}
	}
	return true
}

func recoverBlockSTMSchedulerProgress(scheduler *blockstm.Scheduler, validated, executed, validationQueued []bool, incarnations []int, metrics *BlockSTMMetrics) bool {
	for index := range validated {
		if validated[index] {
			continue
		}
		version := blockstm.Version{Txn: blockstm.TxnIndex(index), Incarnation: blockstm.Incarnation(incarnations[index])}
		if executed[index] && !validationQueued[index] {
			validationQueued[index] = true
			scheduler.ScheduleValidation(version)
		} else {
			scheduler.ScheduleExecution(version)
		}
		if queueLen := scheduler.QueueLen(); queueLen > metrics.SchedulerQueuePeak {
			metrics.SchedulerQueuePeak = queueLen
		}
		metrics.StaleTaskCount++
		return true
	}
	return false
}

func capturedReadsTouchWrites(captured blockstm.CapturedReads, writes map[string]string) bool {
	if len(captured.Reads) == 0 || len(writes) == 0 {
		return false
	}
	for _, read := range captured.Reads {
		if _, ok := writes[read.Key]; ok {
			return true
		}
	}
	return false
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
	return semantics.executeTx(b, overlay, item)
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
	out := make([]int, 0, count)
	if workerCount <= 1 {
		for index := 0; index < count; index++ {
			out = append(out, index)
		}
		return out
	}
	for index := count - 1; index >= 0; index-- {
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
