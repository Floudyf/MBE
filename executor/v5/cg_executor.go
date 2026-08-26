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

type cgExecutorMetrics struct {
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

type cgWaveResult struct {
	Item    tx.SignedTransaction
	Receipt execution.Receipt
	Delta   execution.TxDelta
}

// executeCGPlan is intentionally owned by the CG implementation. CG, ACG and
// BSX no longer share a runtime executor; future changes to one algorithm's
// wave/materialization semantics cannot silently alter the other two.
func executeCGPlan(ctx context.Context, block realblock.Block, base map[string]string, plan literatureGraphPlan, workerCount int) (BlockExecutionResult, error) {
	return executeCGPlanWithCommitment(ctx, block, base, nil, plan, workerCount)
}

func executeCGPlanWithCommitment(ctx context.Context, block realblock.Block, base map[string]string, baseCommitment *state.Commitment, plan literatureGraphPlan, workerCount int) (BlockExecutionResult, error) {
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
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: cgBlockExecutorID,
		ExecutorVersion: "1.0.0", WorkerCount: workerCount, StateRootVersion: state.CommitmentVersion,
	}
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	maximumObserved := 0
	var executionDuration, applyDuration time.Duration
	poolSetupStarted := time.Now()
	pool := newFixedBlockWorkerPool(ctx, workerCount)
	poolSetupDuration := time.Since(poolSetupStarted)
	executionDuration += poolSetupDuration
	defer pool.Close()
	serialExecutor := execution.NewSerialExecutor()
	for _, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		snapshot := working
		started := time.Now()
		waveResults, observed, err := executeCGWaveWithPool(ctx, pool, serialExecutor, block, wave, byID, indexByID, snapshot)
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

	// Cycle victims are excluded from state materialization and represented as
	// deterministic failed no-op terminal outcomes. This preserves one terminal
	// lifecycle result per admitted logical transaction while keeping the abort
	// visible to throughput/abort-rate analysis.
	for _, id := range plan.AbortedTransactionIDs {
		item, ok := byID[id]
		if !ok {
			return BlockExecutionResult{}, fmt.Errorf("cg plan abort references unknown transaction %s", id)
		}
		receipt := execution.Receipt{TxID: item.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: false, Error: "cg_cycle_aborted", ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...), StateRootAfterTx: commitment.Root()}
		delta := execution.TxDelta{TxID: item.TxID, OriginalIndex: indexByID[id], WriteSet: map[string]string{}, Receipt: receipt, Success: false, Error: receipt.Error}
		allReceipts = append(allReceipts, receipt)
		allDeltas = append(allDeltas, delta)
		result.FailedTxs++
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
		EngineID: cgBlockExecutorID, EngineVersion: "1.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height,
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
	metrics := cgExecutorMetrics{
		AlgorithmID: plan.AlgorithmID, WorkerCount: workerCount, WaveCount: plan.Metrics.WaveCount,
		MaximumWaveWidth: plan.Metrics.MaximumWaveWidth, MaximumObservedParallel: maximumObserved,
		GraphEdgeCount: plan.Metrics.EdgeCount, GraphPairCheckCount: plan.Metrics.PairChecks,
		GraphColorCount: plan.Metrics.ColorCount, TransactionExecutionMS: result.TransactionExecutionMS,
		DeterministicApplyMS: result.DeterministicMaterializationMS, StateCommitmentMS: result.StateCommitmentMS,
	}
	actual := map[string]any{
		"literature_graph_metrics":       metrics,
		"maximum_parallel_width":         maximumObserved,
		"dependency_edge_count":          plan.Metrics.EdgeCount,
		"wave_count":                     plan.Metrics.WaveCount,
		"maximum_wave_width":             plan.Metrics.MaximumWaveWidth,
		"graph_color_count":              plan.Metrics.ColorCount,
		"pairwise_conflict_check_count":  plan.Metrics.PairChecks,
		"graph_table_construction_ms":    plan.Metrics.GraphConstructionMS,
		"sorting_ms":                     plan.Metrics.SortingMS,
		"cg_candidate_transaction_count": plan.Metrics.TransactionCount,
		"cg_cycle_abort_count":           plan.Metrics.AbortCount,
		"cg_cycle_resolution_count":      plan.Metrics.CycleResolutionCount,
		"cg_cycle_abort_rate": func() float64 {
			if plan.Metrics.TransactionCount == 0 {
				return 0
			}
			return float64(plan.Metrics.AbortCount) / float64(plan.Metrics.TransactionCount)
		}(),
		"cg_planning_worker_count":         plan.Metrics.PlanningWorkerCount,
		"cg_validator_mode":                plan.ValidatorMode,
		"worker_pool_create_count":         1,
		"worker_pool_setup_ms":             poolSetupDuration.Milliseconds(),
		"wave_barrier_count":               len(plan.Waves),
		"abort_count":                      plan.Metrics.AbortCount,
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
		reason := "literature_graph_wave_execution"
		if delta.Error == "cg_cycle_aborted" {
			reason = "cg_cycle_aborted"
		}
		businessAttempts = append(businessAttempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: delta.TxID, Track: cgBlockExecutorID, Attempt: 1, Reason: reason, Success: delta.Success, FinalCompletion: true})
	}
	return BlockExecutionResult{
		ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: result.TransactionExecutionMS + result.DeterministicMaterializationMS + result.StateCommitmentMS,
		TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS,
		StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion,
		ActualMetrics: actual, BusinessAttempts: businessAttempts,
	}, nil
}

func executeCGWaveWithPool(ctx context.Context, pool *fixedBlockWorkerPool, serialExecutor *execution.SerialExecutor, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string) ([]cgWaveResult, int, error) {
	results := make([]cgWaveResult, len(wave))
	if len(wave) == 0 {
		return results, 0, nil
	}
	var firstErr error
	var errMu sync.Mutex
	tasks := make([]func(), len(wave))
	for index, id := range wave {
		taskIndex := index
		txID := id
		tasks[index] = func() {
			if err := ctx.Err(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			item, ok := byID[txID]
			if !ok {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("plan references unknown transaction %s", txID)
				}
				errMu.Unlock()
				return
			}
			receipt, delta := serialExecutor.ExecuteTransaction(block, item, snapshot, indexByID[txID])
			results[taskIndex] = cgWaveResult{Item: item, Receipt: receipt, Delta: delta}
		}
	}
	maximum, err := pool.Run(tasks)
	if err != nil {
		return nil, maximum, err
	}
	if firstErr != nil {
		return nil, maximum, firstErr
	}
	return results, maximum, nil
}

// executeCGWave is retained for focused tests and direct callers. The normal
// block path creates one pool and reuses it across all dependency waves.
func executeCGWave(ctx context.Context, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string, workers int) ([]cgWaveResult, int, error) {
	pool := newFixedBlockWorkerPool(ctx, workers)
	defer pool.Close()
	return executeCGWaveWithPool(ctx, pool, execution.NewSerialExecutor(), block, wave, byID, indexByID, snapshot)
}
