package v5

import (
	"context"
	"fmt"
	"sort"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type fabricPPCGExecutorMetrics struct {
	AlgorithmID             string `json:"algorithm_id"`
	WorkerCount             int    `json:"worker_count"`
	WaveCount               int    `json:"wave_count"`
	MaximumWaveWidth        int    `json:"maximum_wave_width"`
	MaximumObservedParallel int    `json:"maximum_observed_parallel_width"`
	GraphEdgeCount          int    `json:"graph_edge_count"`
	GraphPairCheckCount     int    `json:"graph_pair_check_count"`
	TransactionExecutionMS  int64  `json:"transaction_execution_ms"`
	DeterministicApplyMS    int64  `json:"deterministic_materialization_ms"`
	StateCommitmentMS       int64  `json:"state_commitment_ms"`
}

type fabricPPCGWaveResult struct {
	Item    tx.SignedTransaction
	Receipt execution.Receipt
	Delta   execution.TxDelta
}

func (p fabricPPCGBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("Fabric++ CG execution plan missing")
	}
	parseStarted := time.Now()
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, fabricPPCGPlanAlgorithmID)
	parseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	verifyStarted := time.Now()
	verifyMode := "full_recompute"
	if input.ExecutionPlanVerified {
		verifyMode = "preverified_projection"
		err = verifyPreverifiedLiteratureGraphProjection(input.Block, plan, fabricPPCGPlanAlgorithmID)
	} else {
		err = verifyFabricPPCGPlan(input.Block, plan)
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result, err := executeFabricPPCGPlanWithCommitment(
		ctx,
		input.Block,
		input.BaseStateSnapshot,
		input.BaseStateCommitment,
		plan,
		fabricPPCGExecutionWorkerCount(p.config, input.WorkerCount),
	)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if result.ActualMetrics == nil {
		result.ActualMetrics = map[string]any{}
	}
	result.ActualMetrics["literature_plan_parse_ms"] = parseMS
	result.ActualMetrics["literature_plan_verify_ms"] = verifyMS
	result.ActualMetrics["literature_plan_verify_mode"] = verifyMode
	result.ActualMetrics["literature_plan_preverified"] = input.ExecutionPlanVerified
	return result, nil
}

// Fabric++ Algorithm 1 is a transaction-reordering algorithm. It does not add a
// post-order parallel execution stage. The MBE block executor is therefore a
// serial materialization adapter and deliberately ignores the experiment worker
// setting for this method.
func fabricPPCGExecutionWorkerCount(_ map[string]any, _ int) int { return 1 }

func executeFabricPPCGPlanWithCommitment(
	ctx context.Context,
	block realblock.Block,
	base map[string]string,
	baseCommitment *state.Commitment,
	plan literatureGraphPlan,
	workerCount int,
) (BlockExecutionResult, error) {
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
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: fabricPPCGBlockExecutorID,
		ExecutorVersion: "1.0.0", WorkerCount: workerCount, StateRootVersion: state.CommitmentVersion,
	}
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	maximumObserved := 0
	var executionDuration, applyDuration time.Duration
	serialExecutor := execution.NewSerialExecutor()

	for _, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		// working is immutable while a wave executes. State changes are applied
		// only after every transaction in the wave has completed.
		snapshot := working
		started := time.Now()
		waveResults, observed, err := executeFabricPPCGWave(
			ctx, serialExecutor, block, wave, byID, indexByID, snapshot,
		)
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

	// Fabric++ Algorithm 1 removes cycle victims from S'. MBE maps each removed
	// logical transaction to an explicit deterministic terminal failed no-op so
	// fixed-workload lifecycle accounting remains closed without changing state.
	for _, id := range plan.AbortedTransactionIDs {
		item, ok := byID[id]
		if !ok {
			return BlockExecutionResult{}, fmt.Errorf("Fabric++ CG plan abort references unknown transaction %s", id)
		}
		receipt := execution.Receipt{
			TxID: item.TxID, BlockHash: block.BlockHash, Height: block.Height,
			Success: false, Error: "fabricpp_cycle_aborted", ExecutionCost: 1,
			StateKeys: append([]string(nil), item.StateKeys...), StateRootAfterTx: commitment.Root(),
		}
		delta := execution.TxDelta{
			TxID: item.TxID, OriginalIndex: indexByID[id], WriteSet: map[string]string{},
			Receipt: receipt, Success: false, Error: receipt.Error,
		}
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
		EngineID: fabricPPCGBlockExecutorID, EngineVersion: "1.0.0",
		BlockHash: block.BlockHash, BlockHeight: block.Height,
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

	metrics := fabricPPCGExecutorMetrics{
		AlgorithmID: plan.AlgorithmID, WorkerCount: workerCount,
		WaveCount: plan.Metrics.WaveCount, MaximumWaveWidth: plan.Metrics.MaximumWaveWidth,
		MaximumObservedParallel: maximumObserved, GraphEdgeCount: plan.Metrics.EdgeCount,
		GraphPairCheckCount:    plan.Metrics.PairChecks,
		TransactionExecutionMS: result.TransactionExecutionMS,
		DeterministicApplyMS:   result.DeterministicMaterializationMS,
		StateCommitmentMS:      result.StateCommitmentMS,
	}
	actual := map[string]any{
		"literature_graph_metrics":             metrics,
		"configured_worker_count":              workerCount,
		"maximum_parallel_width":               maximumObserved,
		"dependency_edge_count":                plan.Metrics.EdgeCount,
		"wave_count":                           plan.Metrics.WaveCount,
		"maximum_wave_width":                   plan.Metrics.MaximumWaveWidth,
		"graph_color_count":                    0,
		"pairwise_conflict_check_count":        plan.Metrics.PairChecks,
		"graph_table_construction_ms":          plan.Metrics.GraphConstructionMS,
		"sorting_ms":                           plan.Metrics.SortingMS,
		"fabricpp_candidate_transaction_count": plan.Metrics.TransactionCount,
		"fabricpp_conflict_edge_count":         plan.Metrics.EdgeCount,
		"fabricpp_cycle_abort_count":           plan.Metrics.AbortCount,
		"fabricpp_cycle_resolution_count":      plan.Metrics.CycleResolutionCount,
		"fabricpp_cycle_abort_rate": func() float64 {
			if plan.Metrics.TransactionCount == 0 {
				return 0
			}
			return float64(plan.Metrics.AbortCount) / float64(plan.Metrics.TransactionCount)
		}(),
		"fabricpp_planning_worker_count":      1,
		"fabricpp_validator_mode":             fabricPPCGValidatorMode,
		"fabricpp_conflict_definition":        "WS(Ti)_intersect_RS(Tj)_nonempty",
		"fabricpp_paper_conflict_orientation": "writer_to_reader",
		"fabricpp_serialization_constraint":   "reader_before_writer",
		"fabricpp_cycle_enumerator":           "johnson_exact_all_elementary_cycles",
		"fabricpp_cycle_victim_rule":          "maximum_cycle_participation_then_lower_original_ordinal",
		"fabricpp_ww_edge_policy":             "no_standalone_ww_edge",
		"wave_barrier_count":                  len(plan.Waves),
		"abort_count":                         plan.Metrics.AbortCount,
		"reexecution_count":                   0,
		"serializable":                        true,
		"literature_plan_algorithm_id":        plan.AlgorithmID,
		"literature_plan_digest_verified":     true,
		"transaction_execution_ms":            result.TransactionExecutionMS,
		"deterministic_materialization_ms":    result.DeterministicMaterializationMS,
		"state_commitment_ms":                 result.StateCommitmentMS,
		"state_root_version":                  state.CommitmentVersion,
	}

	businessAttempts := make([]BusinessExecutionAttempt, 0, len(allDeltas))
	for _, delta := range allDeltas {
		reason := "fabricpp_serializable_schedule_execution"
		if delta.Error == "fabricpp_cycle_aborted" {
			reason = "fabricpp_cycle_aborted"
		}
		businessAttempts = append(businessAttempts, BusinessExecutionAttempt{
			BlockHeight: block.Height, TxID: delta.TxID, Track: fabricPPCGBlockExecutorID,
			Attempt: 1, Reason: reason, Success: delta.Success, FinalCompletion: true,
		})
	}
	return BlockExecutionResult{
		ExecutionResult:        result,
		StateDelta:             stateKVsFromExecutionDelta(result.StateDelta),
		PlanDigest:             plan.PlanDigest,
		WorkerCount:            workerCount,
		BlockExecutionMS:       result.TransactionExecutionMS + result.DeterministicMaterializationMS + result.StateCommitmentMS,
		TransactionExecutionMS: result.TransactionExecutionMS,
		DeterministicApplyMS:   result.DeterministicMaterializationMS,
		StateCommitmentMS:      result.StateCommitmentMS,
		StateRootVersion:       state.CommitmentVersion,
		ActualMetrics:          actual,
		BusinessAttempts:       businessAttempts,
	}, nil
}

func executeFabricPPCGWave(
	ctx context.Context,
	serialExecutor *execution.SerialExecutor,
	block realblock.Block,
	wave []string,
	byID map[string]tx.SignedTransaction,
	indexByID map[string]int,
	snapshot map[string]string,
) ([]fabricPPCGWaveResult, int, error) {
	if len(wave) == 0 {
		return nil, 0, nil
	}
	if len(wave) != 1 {
		return nil, 0, fmt.Errorf("Fabric++ CG paper schedule must materialize as singleton waves, got width %d", len(wave))
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	txID := wave[0]
	item, ok := byID[txID]
	if !ok {
		return nil, 0, fmt.Errorf("Fabric++ CG plan references unknown transaction %s", txID)
	}
	receipt, delta := serialExecutor.ExecuteTransaction(block, item, snapshot, indexByID[txID])
	return []fabricPPCGWaveResult{{Item: item, Receipt: receipt, Delta: delta}}, 1, nil
}
