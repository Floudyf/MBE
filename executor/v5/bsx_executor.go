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
	"metaverse-chainlab/executor/realism/tx"
)

type bsxExecutorMetrics struct {
	AlgorithmID             string `json:"algorithm_id"`
	WorkerCount             int    `json:"worker_count"`
	WaveCount               int    `json:"wave_count"`
	MaximumWaveWidth        int    `json:"maximum_wave_width"`
	MaximumObservedParallel int    `json:"maximum_observed_parallel_width"`
	GraphEdgeCount          int    `json:"graph_edge_count"`
	GraphPairCheckCount     int    `json:"graph_pair_check_count"`
	GraphColorCount         int    `json:"graph_color_count"`
	TransactionExecutionMS  int64  `json:"transaction_execution_ms"`
	DeterministicApplyMS    int64  `json:"deterministic_materialization_ms"`
	StateCommitmentMS       int64  `json:"state_commitment_ms"`
}

type bsxWaveResult struct {
	Item    tx.SignedTransaction
	Receipt execution.Receipt
	Delta   execution.TxDelta
}

// executeBSXPlan is intentionally owned by the BSX implementation. CG, ACG and
// BSX no longer share a runtime executor; future changes to one algorithm's
// wave/materialization semantics cannot silently alter the other two.
func executeBSXPlan(ctx context.Context, block realblock.Block, base map[string]string, plan literatureGraphPlan, workerCount int) (BlockExecutionResult, error) {
	return executeBSXPlanWithCommitment(ctx, block, base, nil, plan, workerCount)
}

func executeBSXPlanWithCommitment(ctx context.Context, block realblock.Block, base map[string]string, baseCommitment *state.Commitment, plan literatureGraphPlan, workerCount int) (BlockExecutionResult, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	working := literatureCopyStringMap(base)
	commitmentStarted := time.Now()
	commitment := state.CloneOrBuild(baseCommitment, working)
	before := commitment.Root()
	var stateCommitmentDuration time.Duration
	stateCommitmentDuration += time.Since(commitmentStarted)
	byID := make(map[string]tx.SignedTransaction, len(block.TxList))
	indexByID := make(map[string]int, len(block.TxList))
	for index, item := range block.TxList {
		byID[item.TxID] = item
		indexByID[item.TxID] = index
	}
	result := execution.Result{
		BlockHash: block.BlockHash, Height: block.Height, StateRootBefore: before,
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: bsxBlockExecutorID,
		ExecutorVersion: "1.0.0", WorkerCount: workerCount, StateRootVersion: state.CommitmentVersion,
	}
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	maximumObserved := 0
	var executionDuration, applyDuration time.Duration
	for _, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		snapshot := working
		started := time.Now()
		waveResults, observed, err := executeBSXWave(ctx, block, wave, byID, indexByID, snapshot, workerCount)
		executionDuration += time.Since(started)
		if err != nil {
			return BlockExecutionResult{}, err
		}
		if observed > maximumObserved {
			maximumObserved = observed
		}
		for _, txResult := range waveResults {
			keys := make([]string, 0, len(txResult.Delta.WriteSet))
			for key := range txResult.Delta.WriteSet {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			started = time.Now()
			for _, key := range keys {
				working[literatureQualifiedKey(block.ShardID, key)] = txResult.Delta.WriteSet[key]
			}
			applyDuration += time.Since(started)
			commitmentStarted = time.Now()
			for _, key := range keys {
				commitment.Set(literatureQualifiedKey(block.ShardID, key), txResult.Delta.WriteSet[key])
			}
			receipt := txResult.Receipt
			receipt.StateRootAfterTx = commitment.Root()
			stateCommitmentDuration += time.Since(commitmentStarted)
			delta := txResult.Delta
			delta.Receipt = receipt
			allReceipts = append(allReceipts, receipt)
			allDeltas = append(allDeltas, delta)
			if receipt.Success {
				result.SuccessfulTxs++
			} else {
				result.FailedTxs++
			}
		}
	}
	result.Receipts = allReceipts
	result.TxDeltas = allDeltas
	result.StateRootAfter = commitment.Root()
	result.ReceiptRoot = execution.ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = literatureStateDelta(base, working)
	result.Plan = execution.ExecutionPlan{
		EngineID: bsxBlockExecutorID, EngineVersion: "1.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height,
		OrderedTransactionIDs:   append([]string(nil), plan.SerializationOrder...),
		DeclaredAccessSetDigest: plan.DeclaredAccessSetDigest,
		DeclaredReadKeyCount:    plan.DeclaredReadKeyCount,
		DeclaredWriteKeyCount:   plan.DeclaredWriteKeyCount,
		WorkerCount:             workerCount, PlanDigest: plan.PlanDigest,
	}
	for _, id := range plan.SerializationOrder {
		result.Plan.OriginalTransactionIdxs = append(result.Plan.OriginalTransactionIdxs, indexByID[id])
	}
	result.PlanDigest = plan.PlanDigest
	result.TransactionExecutionMS = executionDuration.Milliseconds()
	result.DeterministicMaterializationMS = applyDuration.Milliseconds()
	result.StateCommitmentMS = stateCommitmentDuration.Milliseconds()
	metrics := bsxExecutorMetrics{
		AlgorithmID: plan.AlgorithmID, WorkerCount: workerCount, WaveCount: plan.Metrics.WaveCount,
		MaximumWaveWidth: plan.Metrics.MaximumWaveWidth, MaximumObservedParallel: maximumObserved,
		GraphEdgeCount: plan.Metrics.EdgeCount, GraphPairCheckCount: plan.Metrics.PairChecks,
		GraphColorCount: plan.Metrics.ColorCount, TransactionExecutionMS: result.TransactionExecutionMS,
		DeterministicApplyMS: result.DeterministicMaterializationMS, StateCommitmentMS: result.StateCommitmentMS,
	}
	actual := map[string]any{
		"literature_graph_metrics":         metrics,
		"maximum_parallel_width":           maximumObserved,
		"dependency_edge_count":            plan.Metrics.EdgeCount,
		"wave_count":                       plan.Metrics.WaveCount,
		"maximum_wave_width":               plan.Metrics.MaximumWaveWidth,
		"graph_color_count":                plan.Metrics.ColorCount,
		"pairwise_conflict_check_count":    plan.Metrics.PairChecks,
		"abort_count":                      0,
		"reexecution_count":                0,
		"serializable":                     true,
		"literature_plan_algorithm_id":     plan.AlgorithmID,
		"literature_plan_digest_verified":  true,
		"transaction_execution_ms":         result.TransactionExecutionMS,
		"deterministic_materialization_ms": result.DeterministicMaterializationMS,
		"state_commitment_ms":              result.StateCommitmentMS,
		"state_root_version":               state.CommitmentVersion,
	}
	businessAttempts := make([]BusinessExecutionAttempt, 0, len(allDeltas))
	for _, delta := range allDeltas {
		businessAttempts = append(businessAttempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: delta.TxID, Track: bsxBlockExecutorID, Attempt: 1, Reason: "literature_graph_wave_execution", Success: delta.Success, FinalCompletion: true})
	}
	return BlockExecutionResult{
		ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: result.TransactionExecutionMS + result.DeterministicMaterializationMS + result.StateCommitmentMS,
		TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS,
		StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion,
		ActualMetrics: actual, BusinessAttempts: businessAttempts,
	}, nil
}

func executeBSXWave(ctx context.Context, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string, workers int) ([]bsxWaveResult, int, error) {
	results := make([]bsxWaveResult, len(wave))
	if len(wave) == 0 {
		return results, 0, nil
	}
	if workers > len(wave) {
		workers = len(wave)
	}
	type job struct{ index int }
	jobs := make(chan job)
	var wg sync.WaitGroup
	var active, maximum int
	var mu sync.Mutex
	var firstErr error
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			serial := execution.NewSerialExecutor()
			for task := range jobs {
				if ctx.Err() != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					mu.Unlock()
					continue
				}
				id := wave[task.index]
				item, ok := byID[id]
				if !ok {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("plan references unknown transaction %s", id)
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				active++
				if active > maximum {
					maximum = active
				}
				mu.Unlock()
				receipt, delta := serial.ExecuteTransaction(block, item, snapshot, indexByID[id])
				results[task.index] = bsxWaveResult{Item: item, Receipt: receipt, Delta: delta}
				mu.Lock()
				active--
				mu.Unlock()
			}
		}()
	}
	for index := range wave {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, maximum, ctx.Err()
		case jobs <- job{index: index}:
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, maximum, firstErr
	}
	return results, maximum, nil
}
