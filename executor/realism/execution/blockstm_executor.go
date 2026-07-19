package execution

import (
	"context"
	"fmt"
	"reflect"
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
	WorkerCount               int         `json:"worker_count"`
	MaximumParallelWidth      int         `json:"maximum_parallel_width"`
	ExecutionTaskCount        int         `json:"execution_task_count"`
	ValidationTaskCount       int         `json:"validation_task_count"`
	AbortCount                int         `json:"abort_count"`
	ReexecutionCount          int         `json:"reexecution_count"`
	EstimateCount             int         `json:"estimate_count"`
	DependencyWaitCount       int         `json:"dependency_wait_count"`
	DependencyResumeCount     int         `json:"dependency_resume_count"`
	SpeculativeReadCount      int         `json:"speculative_read_count"`
	ValidationFailureCount    int         `json:"validation_failure_count"`
	CommittedTransactionCount int         `json:"committed_transaction_count"`
	MaximumIncarnation        int         `json:"maximum_incarnation"`
	IncarnationHistogram      map[int]int `json:"incarnation_histogram"`
}

type BlockSTMExecutor struct {
	DefaultInitialBalance int64
	WorkerCount           int
	Metrics               BlockSTMMetrics
	serialSemantics       *SerialExecutor
}

func NewBlockSTMExecutor(workerCount int) *BlockSTMExecutor {
	if workerCount < 1 {
		workerCount = 1
	}
	return &BlockSTMExecutor{DefaultInitialBalance: 1_000_000, WorkerCount: workerCount, serialSemantics: NewSerialExecutor()}
}

func (e *BlockSTMExecutor) ExecuteBlock(ctx context.Context, b block.Block, base map[string]string) (Result, error) {
	start := time.Now()
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
	writeSets := make([]map[string]string, len(b.TxList))
	incarnations := make([]int, len(b.TxList))
	metrics := BlockSTMMetrics{WorkerCount: workerCount, IncarnationHistogram: map[int]int{}}

	if err := e.executeSpeculative(ctx, b, base, logicalBase, memory, captured, writeSets, &metrics); err != nil {
		return Result{}, err
	}
	validationResults, err := e.validateSpeculative(ctx, b, logicalBase, memory, captured, &metrics)
	if err != nil {
		return Result{}, err
	}

	scheduler := blockstm.NewScheduler(len(b.TxList))
	dependencies := blockstm.NewDependencyRegistry()
	serialWorking := copySnapshot(base)
	result := Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: state.RootOfSnapshot(copySnapshot(base)), Deterministic: true, EVMExecution: false, FabricExecution: false, StateUpdates: map[string]string{}, BlockExecutorID: BlockSTMExecutorID, ExecutorVersion: BlockSTMExecutorVersion, WorkerCount: workerCount}
	for index, item := range b.TxList {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		txnIndex := blockstm.TxnIndex(index)
		version := blockstm.Version{Txn: txnIndex, Incarnation: blockstm.Incarnation(incarnations[index])}
		if result := validationResults[index]; !result.Valid {
			metrics.ValidationFailureCount++
			metrics.ReexecutionCount++
			if result.Dependency != nil {
				scheduler.Wait(version)
				dependencies.Register(version, *result.Dependency)
				metrics.DependencyWaitCount++
			}
			for key := range writeSets[index] {
				memory.MarkEstimate(key, blockstm.Version{Txn: txnIndex, Incarnation: blockstm.Incarnation(incarnations[index])})
				metrics.EstimateCount++
			}
			version = scheduler.Abort(version)
			metrics.AbortCount = scheduler.AbortCount()
			incarnations[index] = int(version.Incarnation)
			overlay := newTxOverlay(b.ShardID, serialWorking)
			_ = e.executeTx(b, overlay, item)
			captured[index] = capturedFromOverlay(overlay)
			writeSets[index] = overlay.logicalWrites()
			for key, value := range writeSets[index] {
				memory.Write(key, version, value)
			}
			if result.Dependency != nil {
				for _, waiter := range dependencies.Resolve(*result.Dependency) {
					scheduler.Resume(waiter)
					metrics.DependencyResumeCount++
				}
			}
		}
		overlay := newTxOverlay(b.ShardID, serialWorking)
		receipt := e.executeTx(b, overlay, item)
		serialWorking = overlay.snapshot()
		delta := TxDelta{TxID: item.TxID, OriginalIndex: index, ReadSet: overlay.reads, WriteSet: overlay.logicalWrites(), Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, receipt)
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
		scheduler.Commit(version)
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

	serialOracle := NewSerialExecutor().ExecuteBlock(b, base)
	if !sameExecutionOutput(serialOracle, result) {
		return Result{}, fmt.Errorf("block-stm ordered materialization diverged from serial oracle")
	}
	declared := declaredAccessSet(b.TxList)
	plan := buildBlockSTMPlan(b, declared, workerCount)
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	result.BlockSTMMetrics = metrics
	e.Metrics = metrics
	_ = start
	if result.StateRootAfter != state.RootOfSnapshot(serialWorking) {
		return Result{}, fmt.Errorf("block-stm ordered materialization root mismatch")
	}
	result.SerialEquivalent = true
	return result, nil
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

func (e *BlockSTMExecutor) executeTx(b block.Block, overlay *txOverlay, item tx.SignedTransaction) Receipt {
	semantics := NewSerialExecutor()
	semantics.DefaultInitialBalance = e.DefaultInitialBalance
	return semantics.executeTx(b, overlay, item)
}

func (e *BlockSTMExecutor) executeSpeculative(ctx context.Context, b block.Block, base, logicalBase map[string]string, memory *blockstm.MVMemory, captured []blockstm.CapturedReads, writeSets []map[string]string, metrics *BlockSTMMetrics) error {
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
				_ = e.executeTx(b, overlay, b.TxList[index])
				localCaptured := capturedFromOverlayWithMemory(overlay, memory, logicalBase, txnIndex)
				localWrites := overlay.logicalWrites()
				for key, value := range localWrites {
					memory.Write(key, version, value)
				}
				captured[index] = localCaptured
				writeSets[index] = localWrites
				atomic.AddInt64(&executed, 1)
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
