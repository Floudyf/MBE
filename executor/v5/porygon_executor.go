package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
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
		return BlockExecutionResult{}, fmt.Errorf("execution plan porygon plan missing")
	}
	parseStarted := time.Now()
	var plan porygonExecutionPlan
	if err := json.Unmarshal(input.Block.ExecutionPlan.Payload, &plan); err != nil {
		return BlockExecutionResult{}, fmt.Errorf("execution plan decode porygon plan: %w", err)
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
			return BlockExecutionResult{}, fmt.Errorf("execution plan porygon executor verification failed")
		}
		if stableJSONDigest(porygonPlanDigestProjection(expected)) != stableJSONDigest(porygonPlanDigestProjection(plan)) {
			return BlockExecutionResult{}, fmt.Errorf("execution plan porygon semantic projection mismatch")
		}
	} else if input.Block.ExecutionPlan.PlanDigest != plan.PlanDigest {
		return BlockExecutionResult{}, fmt.Errorf("execution plan porygon preverified envelope mismatch")
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()

	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	if workerCount < 1 {
		workerCount = 1
	}
	return executePorygonPlan(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, workerCount, parseMS, verifyMS, verifyMode, input.ExecutionShardID, input.PorygonWaveExchange)
}

func executePorygonPlan(ctx context.Context, block realblock.Block, base map[string]string, baseCommitment *state.Commitment, plan porygonExecutionPlan, workerCount int, parseMS, verifyMS int64, verifyMode, executionShardID string, waveExchange PorygonWaveExchangeFunc) (BlockExecutionResult, error) {
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
			return BlockExecutionResult{}, fmt.Errorf("execution plan porygon block contains empty transaction id")
		}
		byID[item.TxID] = item
		indexByID[item.TxID] = index
	}
	for _, assignment := range plan.Assignments {
		if _, exists := assignmentByID[assignment.TxID]; exists {
			return BlockExecutionResult{}, fmt.Errorf("execution plan duplicate porygon assignment for %s", assignment.TxID)
		}
		assignmentByID[assignment.TxID] = assignment
	}
	if len(assignmentByID) != len(block.TxList) {
		return BlockExecutionResult{}, fmt.Errorf("execution plan porygon assignment count mismatch")
	}

	result := execution.Result{
		BlockHash: block.BlockHash, Height: block.Height, StateRootBefore: before,
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: porygonBlockExecutorID,
		ExecutorVersion: "3.0.0", WorkerCount: workerCount, StateRootVersion: state.CommitmentVersion,
	}
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	scheduleEvents := make([]ScheduleEvent, 0, len(block.TxList)*2)
	attempts := make([]BusinessExecutionAttempt, 0, len(block.TxList))

	serial := execution.NewSerialExecutor()
	poolSetupStarted := time.Now()
	pool := newFixedBlockWorkerPool(ctx, workerCount)
	poolSetupDuration := time.Since(poolSetupStarted)
	defer pool.Close()

	var executionDuration, applyDuration time.Duration
	var exchangeWaitDuration time.Duration
	var criticalPathDuration time.Duration
	maximumObserved := 0
	crossShardExecuted := 0
	actualMultiShardUpdates := 0
	actualLockCount := 0
	escHistogram := map[string]int{}
	localBusinessExecutionCount := 0
	certifiedRemoteBusinessResultCount := 0
	escCertificateCount := 0
	distributedOwnership := strings.TrimSpace(executionShardID) != "" && waveExchange != nil

	for waveIndex, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		snapshot := working
		waveWallStarted := time.Now()
		waveResults := []porygonWaveResult{}
		localExecuted := map[string]bool{}
		observed := 0
		if distributedOwnership {
			localWave := make([]string, 0, len(wave))
			requiredSet := map[string]bool{}
			for _, txID := range wave {
				assignment, ok := assignmentByID[txID]
				if !ok {
					return BlockExecutionResult{}, fmt.Errorf("execution plan missing porygon assignment for %s", txID)
				}
				owner := fmt.Sprintf("s%d", assignment.ExecutionShard)
				requiredSet[owner] = true
				if owner == executionShardID {
					localWave = append(localWave, txID)
				}
			}
			requiredShards := sortedBoolKeys(requiredSet)
			localStarted := time.Now()
			localResults, localObserved, err := executePorygonWaveWithPool(ctx, pool, serial, block, localWave, byID, indexByID, snapshot)
			localDuration := time.Since(localStarted)
			executionDuration += localDuration
			if err != nil {
				return BlockExecutionResult{}, err
			}
			observed = localObserved
			localPayload := PorygonESCWaveResult{BlockHash: block.BlockHash, Height: block.Height, Wave: waveIndex, ExecutionShardID: executionShardID, Results: make([]PorygonWaveTxResult, 0, len(localResults)), BusinessExecutionUS: localDuration.Microseconds()}
			for _, item := range localResults {
				localExecuted[item.Item.TxID] = true
				localPayload.Results = append(localPayload.Results, PorygonWaveTxResult{TxID: item.Item.TxID, Receipt: item.Receipt, Delta: item.Delta})
			}
			exchangeStarted := time.Now()
			certificate, err := waveExchange(ctx, localPayload, requiredShards)
			exchangeWaitDuration += time.Since(exchangeStarted)
			if err != nil {
				return BlockExecutionResult{}, err
			}
			if err := validatePorygonESCWaveCertificate(certificate); err != nil {
				return BlockExecutionResult{}, err
			}
			if certificate.BlockHash != block.BlockHash || certificate.Height != block.Height || certificate.Wave != waveIndex {
				return BlockExecutionResult{}, fmt.Errorf("porygon ESC certificate block/wave mismatch")
			}
			escCertificateCount++
			seen := map[string]bool{}
			for _, entry := range certificate.Entries {
				for _, certified := range entry.Result.Results {
					item, ok := byID[certified.TxID]
					if !ok || seen[certified.TxID] {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC certificate has unknown/duplicate tx %s", certified.TxID)
					}
					assignment := assignmentByID[certified.TxID]
					if fmt.Sprintf("s%d", assignment.ExecutionShard) != entry.ExecutionShardID {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC certificate ownership mismatch for %s", certified.TxID)
					}
					// Certified remote results are still checked against the signed AccessList
					// before any deterministic global materialization. The ESC quorum certifies
					// agreement, not permission to touch undeclared state.
					if err := porygonValidateActualAccess(item, certified.Delta); err != nil {
						return BlockExecutionResult{}, err
					}
					seen[certified.TxID] = true
					waveResults = append(waveResults, porygonWaveResult{Item: item, Receipt: certified.Receipt, Delta: certified.Delta})
				}
			}
			if len(seen) != len(wave) {
				return BlockExecutionResult{}, fmt.Errorf("porygon ESC certificate tx coverage mismatch: got=%d want=%d", len(seen), len(wave))
			}
		} else {
			started := time.Now()
			var err error
			waveResults, observed, err = executePorygonWaveWithPool(ctx, pool, serial, block, wave, byID, indexByID, snapshot)
			executionDuration += time.Since(started)
			if err != nil {
				return BlockExecutionResult{}, err
			}
			for _, item := range waveResults {
				localExecuted[item.Item.TxID] = true
			}
		}
		criticalPathDuration += time.Since(waveWallStarted)
		if observed > maximumObserved {
			maximumObserved = observed
		}
		sort.SliceStable(waveResults, func(i, j int) bool {
			return indexByID[waveResults[i].Item.TxID] < indexByID[waveResults[j].Item.TxID]
		})
		for _, txResult := range waveResults {
			assignment, ok := assignmentByID[txResult.Item.TxID]
			if !ok {
				return BlockExecutionResult{}, fmt.Errorf("execution plan missing porygon assignment for %s", txResult.Item.TxID)
			}
			escHistogram[fmt.Sprintf("esc_%d", assignment.ExecutionShard)]++
			if assignment.CrossShard {
				crossShardExecuted++
				actualLockCount += len(assignment.LockedKeys)
			}
			scheduleEvents = append(scheduleEvents, ScheduleEvent{
				TxID: txResult.Item.TxID, Track: "porygon", QueueName: fmt.Sprintf("esc_%d", assignment.ExecutionShard),
				DecisionReason: fmt.Sprintf("porygon_wave_%d_single_esc_execution_cross=%t", waveIndex, assignment.CrossShard),
				LocalExecution: localExecuted[txResult.Item.TxID],
			})

			keys := make([]string, 0, len(txResult.Delta.WriteSet))
			updateShards := map[int]bool{}
			for key := range txResult.Delta.WriteSet {
				keys = append(keys, key)
				updateShards[porygonStateShard(key, plan.ExecutionShardCount)] = true
			}
			sort.Strings(keys)
			if assignment.CrossShard {
				actualMultiShardUpdates += len(updateShards)
			}

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
			if localExecuted[txResult.Item.TxID] {
				localBusinessExecutionCount++
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: txResult.Item.TxID, Track: "porygon", Attempt: 1, Reason: fmt.Sprintf("esc_%d_wave_%d_owned_execution", assignment.ExecutionShard, waveIndex), Success: receipt.Success, FinalCompletion: true})
			} else if distributedOwnership {
				certifiedRemoteBusinessResultCount++
			}
		}
	}

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
		EngineID: porygonBlockExecutorID, EngineVersion: "3.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height,
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
		"porygon_algorithm_id":                                plan.AlgorithmID,
		"porygon_transaction_block_digest":                    plan.TransactionBlockDigest,
		"porygon_transaction_root":                            plan.TransactionRoot,
		"porygon_access_root":                                 plan.AccessRoot,
		"porygon_witness_policy":                              plan.WitnessPolicy,
		"porygon_witness_threshold":                           plan.WitnessThreshold,
		"porygon_witness_threshold_configured":                plan.WitnessThreshold,
		"porygon_witness_threshold_enforced":                  false,
		"porygon_witness_validation_mode":                     "full_validator_recompute_before_pbft_vote",
		"porygon_witnessed_block_count":                       1,
		"porygon_validator_witness_verified":                  true,
		"porygon_pipeline_enabled":                            plan.PipelineEnabled,
		"porygon_cross_batch_witness_enabled":                 plan.CrossBatchWitness,
		"porygon_cross_batch_witness_count":                   porygonCountPipelineStage(plan.Pipeline, "cross_batch_witness"),
		"porygon_pipeline_overlap_slot_count":                 pipelineOverlapSlots,
		"porygon_execution_committee_count":                   plan.ExecutionCommitteeCount,
		"porygon_execution_shard_count":                       plan.ExecutionShardCount,
		"porygon_execution_wave_count":                        len(plan.Waves),
		"porygon_maximum_wave_width":                          maxWaveWidth,
		"porygon_intra_shard_transaction_count":               plan.IntraShardTransactionCnt,
		"porygon_cross_shard_transaction_count":               plan.CrossShardTransactionCnt,
		"porygon_logical_state_cross_shard_transaction_count": plan.CrossShardTransactionCnt,
		"porygon_logical_state_cross_shard_ratio":             float64(plan.CrossShardTransactionCnt) / float64(max(1, len(block.TxList))),
		"porygon_cross_shard_metric_truth_scope":              "execution_esc_plus_signed_access_state_shard_ownership;not_workload_source_target_ratio",
		"porygon_single_shard_execution_count":                crossShardExecuted,
		"porygon_multi_shard_update_count":                    actualMultiShardUpdates,
		"porygon_state_lock_count":                            actualLockCount,
		"porygon_pipeline_trace":                              plan.Pipeline,
		"porygon_execution_assignments":                       plan.Assignments,
		"porygon_execution_shard_histogram":                   escHistogram,
		"porygon_active_execution_shard_count":                len(escHistogram),
		"porygon_esc_execution_ownership_mode":                map[bool]string{true: "distributed_esc_quorum_result_exchange_v1", false: "legacy_all_replica_business_execution"}[distributedOwnership],
		"porygon_local_execution_shard_id":                    executionShardID,
		"porygon_local_business_execution_count":              localBusinessExecutionCount,
		"porygon_certified_remote_business_result_count":      certifiedRemoteBusinessResultCount,
		"porygon_esc_certificate_count":                       escCertificateCount,
		"porygon_business_execution_us":                       executionDuration.Microseconds(),
		"porygon_result_exchange_wait_us":                     exchangeWaitDuration.Microseconds(),
		"porygon_business_exchange_critical_path_us":          criticalPathDuration.Microseconds(),
		"porygon_execution_critical_path_us":                  (poolSetupDuration + criticalPathDuration + applyDuration + stateCommitmentDuration).Microseconds(),
		"porygon_deterministic_materialization_us":            applyDuration.Microseconds(),
		"porygon_state_commitment_us":                         stateCommitmentDuration.Microseconds(),
		"porygon_plan_parse_ms":                               parseMS,
		"porygon_plan_verify_ms":                              verifyMS,
		"porygon_plan_verify_mode":                            verifyMode,
		"porygon_plan_digest_verified":                        true,
		"porygon_physical_ordering_domain_count":              1,
		"porygon_storage_visibility_policy":                   "signed_access_projection",
		"porygon_meta_remote_state_control_plane_used":        false,
		"porygon_physical_relay_protocol_used":                false,
		"maximum_parallel_width":                              maximumObserved,
		"transaction_execution_ms":                            result.TransactionExecutionMS,
		"transaction_execution_us":                            executionDuration.Microseconds(),
		"deterministic_materialization_ms":                    result.DeterministicMaterializationMS,
		"state_commitment_ms":                                 result.StateCommitmentMS,
		"state_root_version":                                  state.CommitmentVersion,
		"worker_pool_create_count":                            1,
		"worker_pool_setup_ms":                                poolSetupDuration.Milliseconds(),
		"wave_barrier_count":                                  len(plan.Waves),
		"abort_count":                                         0,
		"reexecution_count":                                   0,
		"serializable":                                        true,
		"porygon_mbe_consensus_adaptation":                    "single_global_pbft_ordering_domain_with_esc_quorum_result_exchange",
		"porygon_storage_node_adaptation":                     "logical_storage_projection_over_mbe_persistent_state;separate_physical_storage_nodes_not_claimed",
		"porygon_witness_adaptation":                          "validator_full_body_recompute_before_pbft_vote",
		"porygon_pipeline_timing_truth_boundary":              "logical_protocol_slots;wall_clock_overlap_not_claimed",
		"porygon_wall_clock_pipeline_overlap_claimed":         false,
		"porygon_cross_shard_atomicity_truth_boundary":        "single_esc_execution_plus_atomic_deterministic_multi_key_materialization",
	}
	return BlockExecutionResult{
		ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: (poolSetupDuration + criticalPathDuration + applyDuration + stateCommitmentDuration).Milliseconds(),
		TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS,
		StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion,
		ScheduleEvents: scheduleEvents, ActualMetrics: metrics, BusinessAttempts: attempts,
	}, nil
}

func executePorygonWaveWithPool(ctx context.Context, pool *fixedBlockWorkerPool, serial *execution.SerialExecutor, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string) ([]porygonWaveResult, int, error) {
	results := make([]porygonWaveResult, len(wave))
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
					firstErr = fmt.Errorf("execution plan porygon references unknown transaction %s", txID)
				}
				errMu.Unlock()
				return
			}
			txSnapshot := porygonTransactionSnapshot(snapshot, block.ShardID, item)
			receipt, delta := serial.ExecuteTransaction(block, item, txSnapshot, indexByID[txID])
			if err := porygonValidateActualAccess(item, delta); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			results[taskIndex] = porygonWaveResult{Item: item, Receipt: receipt, Delta: delta}
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

func porygonTransactionSnapshot(base map[string]string, shardID string, item tx.SignedTransaction) map[string]string {
	out := make(map[string]string, len(item.AccessList))
	for _, access := range item.AccessList {
		if strings.TrimSpace(access.Key) == "" {
			continue
		}
		qualified := qualifyStateKey(shardID, access.Key)
		if value, ok := base[qualified]; ok {
			out[qualified] = value
		} else if value, ok := base[access.Key]; ok {
			out[qualified] = value
		}
	}
	return out
}

func porygonValidateActualAccess(item tx.SignedTransaction, delta execution.TxDelta) error {
	accesses := item.AccessList
	if len(accesses) == 0 {
		return fmt.Errorf("execution plan porygon access-list violation tx=%s: signed AccessList missing", item.TxID)
	}
	for _, read := range delta.ReadSet {
		if !porygonDeclaredAllows(accesses, read.Key, true, false) {
			return fmt.Errorf("execution plan porygon access-list violation tx=%s: undeclared read %s", item.TxID, read.Key)
		}
	}
	for key := range delta.WriteSet {
		if !porygonDeclaredAllows(accesses, key, false, true) {
			return fmt.Errorf("execution plan porygon access-list violation tx=%s: undeclared write %s", item.TxID, key)
		}
	}
	return nil
}

func porygonDeclaredAllows(accesses []tx.AccessItem, actualKey string, needRead, needWrite bool) bool {
	actual := porygonLogicalKey(actualKey)
	for _, access := range accesses {
		if porygonLogicalKey(access.Key) != actual {
			continue
		}
		if needRead && !isReadMode(access.Mode) {
			continue
		}
		if needWrite && !isWriteMode(access.Mode) {
			continue
		}
		return true
	}
	return false
}

func porygonLogicalKey(key string) string {
	for i := 0; i+1 < len(key); i++ {
		if key[i] == ':' && key[i+1] == ':' {
			return key[i+2:]
		}
	}
	return key
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
