package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type porygonBlockExecutor struct{ basicPlugin }

type porygonWaveResult struct {
	Item    tx.SignedTransaction
	Receipt execution.Receipt
	Delta   execution.TxDelta
}

func (p porygonBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil || input.Block.ExecutionPlan.AlgorithmID != porygonPlanAlgorithmID {
		return BlockExecutionResult{}, fmt.Errorf("porygon execution plan missing")
	}
	parseStarted := time.Now()
	var plan porygonExecutionPlan
	if err := json.Unmarshal(input.Block.ExecutionPlan.Payload, &plan); err != nil {
		return BlockExecutionResult{}, fmt.Errorf("decode porygon execution plan: %w", err)
	}
	parseMS := time.Since(parseStarted).Milliseconds()

	verifyStarted := time.Now()
	verifyMode := "preverified_projection"
	if !input.ExecutionPlanVerified {
		verifyMode = "full_recompute"
		expected, err := buildPorygonPlan(input.Block, p.config)
		if err != nil {
			return BlockExecutionResult{}, err
		}
		if expected.PlanDigest != plan.PlanDigest || input.Block.ExecutionPlan.PlanDigest != plan.PlanDigest {
			return BlockExecutionResult{}, fmt.Errorf("porygon executor plan verification failed")
		}
	} else if input.Block.ExecutionPlan.PlanDigest != plan.PlanDigest {
		return BlockExecutionResult{}, fmt.Errorf("porygon preverified plan envelope mismatch")
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()

	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	if workerCount < 1 {
		workerCount = 1
	}
	return executePorygonPlan(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, workerCount, parseMS, verifyMS, verifyMode)
}

func executePorygonPlan(ctx context.Context, block realblock.Block, base map[string]string, baseCommitment *state.Commitment, plan porygonExecutionPlan, workerCount int, parseMS, verifyMS int64, verifyMode string) (BlockExecutionResult, error) {
	working := copyRegistryStringMap(base)
	commitmentStarted := time.Now()
	commitment := state.CloneOrBuild(baseCommitment, working)
	before := commitment.Root()
	stateCommitmentDuration := time.Since(commitmentStarted)

	byID := make(map[string]tx.SignedTransaction, len(block.TxList))
	indexByID := make(map[string]int, len(block.TxList))
	assignmentByID := make(map[string]porygonTxAssignment, len(plan.Assignments))
	for index, item := range block.TxList {
		if item.TxID == "" {
			return BlockExecutionResult{}, fmt.Errorf("porygon block contains empty transaction id")
		}
		byID[item.TxID] = item
		indexByID[item.TxID] = index
	}
	for _, assignment := range plan.Assignments {
		assignmentByID[assignment.TxID] = assignment
	}

	result := execution.Result{
		BlockHash: block.BlockHash, Height: block.Height, StateRootBefore: before,
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: porygonBlockExecutorID,
		ExecutorVersion: "1.0.0", WorkerCount: workerCount, StateRootVersion: state.CommitmentVersion,
	}
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	scheduleEvents := make([]ScheduleEvent, 0, len(block.TxList)*2)
	attempts := make([]BusinessExecutionAttempt, 0, len(block.TxList))

	serial := execution.NewSerialExecutor()
	var executionDuration, applyDuration time.Duration
	maximumObserved := int64(0)
	crossShardExecuted := 0
	multiShardUpdates := 0
	lockCount := 0

	for waveIndex, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		snapshot := copyRegistryStringMap(working)
		started := time.Now()
		waveResults, observed, err := executePorygonWave(ctx, serial, block, wave, byID, indexByID, snapshot, workerCount)
		executionDuration += time.Since(started)
		if err != nil {
			return BlockExecutionResult{}, err
		}
		if int64(observed) > maximumObserved {
			maximumObserved = int64(observed)
		}
		for _, txResult := range waveResults {
			assignment := assignmentByID[txResult.Item.TxID]
			if assignment.CrossShard {
				crossShardExecuted++
				multiShardUpdates += len(assignment.InvolvedShards)
				lockCount += len(assignment.LockedKeys)
			}
			scheduleEvents = append(scheduleEvents, ScheduleEvent{
				TxID: txResult.Item.TxID, Track: "porygon", QueueName: fmt.Sprintf("esc_%d", assignment.ExecutionShard),
				DecisionReason: fmt.Sprintf("porygon_wave_%d_single_shard_execution_cross=%t", waveIndex, assignment.CrossShard),
				LocalExecution: true,
			})

			keys := make([]string, 0, len(txResult.Delta.WriteSet))
			for key := range txResult.Delta.WriteSet {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			applyStarted := time.Now()
			for _, key := range keys {
				working[qualifyStateKey(block.ShardID, key)] = txResult.Delta.WriteSet[key]
			}
			applyDuration += time.Since(applyStarted)

			commitmentStarted = time.Now()
			for _, key := range keys {
				commitment.Set(qualifyStateKey(block.ShardID, key), txResult.Delta.WriteSet[key])
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
			attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: txResult.Item.TxID, Track: "porygon", Attempt: 1, Reason: fmt.Sprintf("esc_%d_wave_%d", assignment.ExecutionShard, waveIndex), Success: receipt.Success, FinalCompletion: true})
		}
	}

	// Receipts and deltas are returned in consensus transaction order even though
	// conflict-free ESC work executed in parallel waves.
	sort.SliceStable(allReceipts, func(i, j int) bool { return indexByID[allReceipts[i].TxID] < indexByID[allReceipts[j].TxID] })
	sort.SliceStable(allDeltas, func(i, j int) bool { return indexByID[allDeltas[i].TxID] < indexByID[allDeltas[j].TxID] })
	result.Receipts = allReceipts
	result.TxDeltas = allDeltas
	result.StateRootAfter = commitment.Root()
	result.ReceiptRoot = execution.ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = executionStateDelta(base, working)

	readKeys := map[string]bool{}
	writeKeys := map[string]bool{}
	for _, item := range block.TxList {
		for _, access := range porygonCanonicalAccesses(item) {
			if access.Key == "" {
				continue
			}
			if isReadMode(access.Mode) {
				readKeys[access.Key] = true
			}
			if isWriteMode(access.Mode) {
				writeKeys[access.Key] = true
			}
		}
	}
	result.Plan = execution.ExecutionPlan{
		EngineID: porygonBlockExecutorID, EngineVersion: "1.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height,
		OrderedTransactionIDs:   append([]string(nil), plan.SerializationOrder...),
		DeclaredAccessSetDigest: stableJSONDigest(map[string]any{"read_keys": sortedBoolKeys(readKeys), "write_keys": sortedBoolKeys(writeKeys)}),
		DeclaredReadKeyCount:    len(readKeys), DeclaredWriteKeyCount: len(writeKeys), WorkerCount: workerCount, PlanDigest: plan.PlanDigest,
	}
	for _, id := range plan.SerializationOrder {
		result.Plan.OriginalTransactionIdxs = append(result.Plan.OriginalTransactionIdxs, indexByID[id])
	}
	result.PlanDigest = plan.PlanDigest
	result.TransactionExecutionMS = executionDuration.Milliseconds()
	result.DeterministicMaterializationMS = applyDuration.Milliseconds()
	result.StateCommitmentMS = stateCommitmentDuration.Milliseconds()

	maxWaveWidth := 0
	for _, wave := range plan.Waves {
		if len(wave) > maxWaveWidth {
			maxWaveWidth = len(wave)
		}
	}
	pipelineOverlapSlots := porygonPipelineOverlapSlots(plan.Pipeline)
	metrics := map[string]any{
		"porygon_algorithm_id":                         plan.AlgorithmID,
		"porygon_transaction_block_digest":             plan.TransactionBlockDigest,
		"porygon_transaction_root":                     plan.TransactionRoot,
		"porygon_access_root":                          plan.AccessRoot,
		"porygon_witness_policy":                       plan.WitnessPolicy,
		"porygon_witness_threshold":                    plan.WitnessThreshold,
		"porygon_witnessed_block_count":                1,
		"porygon_validator_witness_verified":           true,
		"porygon_pipeline_enabled":                     plan.PipelineEnabled,
		"porygon_cross_batch_witness_enabled":          plan.CrossBatchWitness,
		"porygon_cross_batch_witness_count":            porygonCountPipelineStage(plan.Pipeline, "cross_batch_witness"),
		"porygon_pipeline_overlap_slot_count":          pipelineOverlapSlots,
		"porygon_execution_committee_count":            plan.ExecutionCommitteeCount,
		"porygon_execution_shard_count":                plan.ExecutionShardCount,
		"porygon_execution_wave_count":                 len(plan.Waves),
		"porygon_maximum_wave_width":                   maxWaveWidth,
		"porygon_intra_shard_transaction_count":        plan.IntraShardTransactionCnt,
		"porygon_cross_shard_transaction_count":        plan.CrossShardTransactionCnt,
		"porygon_single_shard_execution_count":         crossShardExecuted,
		"porygon_multi_shard_update_count":             multiShardUpdates,
		"porygon_state_lock_count":                     lockCount,
		"porygon_pipeline_trace":                       plan.Pipeline,
		"porygon_execution_assignments":                plan.Assignments,
		"porygon_plan_parse_ms":                        parseMS,
		"porygon_plan_verify_ms":                       verifyMS,
		"porygon_plan_verify_mode":                     verifyMode,
		"porygon_plan_digest_verified":                 true,
		"maximum_parallel_width":                       int(maximumObserved),
		"transaction_execution_ms":                     result.TransactionExecutionMS,
		"deterministic_materialization_ms":             result.DeterministicMaterializationMS,
		"state_commitment_ms":                          result.StateCommitmentMS,
		"state_root_version":                           state.CommitmentVersion,
		"abort_count":                                  0,
		"reexecution_count":                            0,
		"serializable":                                 true,
		"porygon_mbe_consensus_adaptation":             "shared_pbft_style_consensus",
		"porygon_witness_adaptation":                   "validator_full_body_recompute_before_pbft_vote",
		"porygon_pipeline_timing_truth_boundary":       "logical_protocol_slots;wall_clock_overlap_not_claimed",
		"porygon_cross_shard_atomicity_truth_boundary": "single_execution_wave_plus_deterministic_multi_key_commit",
	}
	return BlockExecutionResult{
		ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: result.TransactionExecutionMS + result.DeterministicMaterializationMS + result.StateCommitmentMS,
		TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS,
		StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion,
		ScheduleEvents: scheduleEvents, ActualMetrics: metrics, BusinessAttempts: attempts,
	}, nil
}

func executePorygonWave(ctx context.Context, serial *execution.SerialExecutor, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string, workerCount int) ([]porygonWaveResult, int, error) {
	if len(wave) == 0 {
		return nil, 0, nil
	}
	if workerCount < 1 {
		workerCount = 1
	}
	results := make([]porygonWaveResult, len(wave))
	semaphore := make(chan struct{}, workerCount)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	var inflight int64
	var maximum int64
	for index, txID := range wave {
		index := index
		txID := txID
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				errMu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				errMu.Unlock()
				return
			case semaphore <- struct{}{}:
			}
			current := atomic.AddInt64(&inflight, 1)
			for {
				observed := atomic.LoadInt64(&maximum)
				if current <= observed || atomic.CompareAndSwapInt64(&maximum, observed, current) {
					break
				}
			}
			defer func() {
				atomic.AddInt64(&inflight, -1)
				<-semaphore
			}()
			item, ok := byID[txID]
			if !ok {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("porygon plan references unknown transaction %s", txID)
				}
				errMu.Unlock()
				return
			}
			receipt, delta := serial.ExecuteTransaction(block, item, snapshot, indexByID[txID])
			results[index] = porygonWaveResult{Item: item, Receipt: receipt, Delta: delta}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, int(maximum), firstErr
	}
	return results, int(maximum), nil
}

func porygonCountPipelineStage(stages []porygonPipelineStage, target string) int {
	count := 0
	for _, stage := range stages {
		if stage.Stage == target {
			count++
		}
	}
	return count
}

func porygonPipelineOverlapSlots(stages []porygonPipelineStage) int {
	counts := map[uint64]int{}
	for _, stage := range stages {
		counts[stage.LogicalSlot]++
	}
	overlap := 0
	for _, count := range counts {
		if count > 1 {
			overlap++
		}
	}
	return overlap
}
