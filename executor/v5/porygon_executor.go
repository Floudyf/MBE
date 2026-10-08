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

func porygonDistributedESCReady(executionShardID string, waveExchange PorygonWaveExchangeFunc, batchExchange PorygonBatchExchangeFunc) bool {
	return strings.TrimSpace(executionShardID) != "" && (batchExchange != nil || waveExchange != nil)
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
	executionRoleShardID := input.PorygonExecutionShardID
	if strings.TrimSpace(executionRoleShardID) == "" {
		executionRoleShardID = input.ExecutionShardID
	}
	if porygonBool(p.config, "require_distributed_esc", false) && !porygonDistributedESCReady(executionRoleShardID, input.PorygonWaveExchange, input.PorygonBatchExchange) {
		return BlockExecutionResult{}, fmt.Errorf("porygon formal distributed ESC wiring missing: execution_role_shard_id=%q batch_exchange=%t wave_exchange=%t", executionRoleShardID, input.PorygonBatchExchange != nil, input.PorygonWaveExchange != nil)
	}
	return executePorygonPlan(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, workerCount, parseMS, verifyMS, verifyMode, input.ExecutionShardID, executionRoleShardID, input.PorygonCrossBatchWitnessOverlap, input.PorygonWaveExchange, input.PorygonBatchExchange, input.PorygonMultiShardUpdate, input.PorygonStateFetch)
}

func executePorygonPlan(ctx context.Context, block realblock.Block, base map[string]string, baseCommitment *state.Commitment, plan porygonExecutionPlan, workerCount int, parseMS, verifyMS int64, verifyMode, storageShardID, executionShardID string, crossBatchWitnessOverlap bool, waveExchange PorygonWaveExchangeFunc, batchExchange PorygonBatchExchangeFunc, multiShardUpdate PorygonMultiShardUpdateFunc, stateFetch PorygonStateFetchFunc) (BlockExecutionResult, error) {
	plan, rollbackAbandonedCount := porygonApplyRollbackMask(block, plan)
	working := copyRegistryStringMap(base)
	commitmentStarted := time.Now()
	commitment := state.CloneOrBuild(baseCommitment, working)
	before := commitment.Root()
	stateCommitmentDuration := time.Since(commitmentStarted)
	var executionDuration, applyDuration time.Duration

	// Paper Figure 6: Proposal U is applied by this later EC before executing
	// the new L transaction blocks.  Newly pre-executed CTx writes are *not*
	// materialized in this round; they become S and are carried as a future U.
	paperProposalMode := false
	paperProposalUpdateCount := 0
	paperProposal := PorygonProposalBody{}
	if proposal, proposalErr := porygonProposalFromBlock(block); proposalErr == nil && proposal.Version == porygonCompactProposalVersion {
		paperProposalMode = true
		paperProposal = proposal
		updateStarted := time.Now()
		for _, update := range proposal.U {
			for _, row := range update.Updates {
				homeShard := fmt.Sprintf("s%d", porygonStateShard(row.Key, plan.ExecutionShardCount))
				if strings.TrimSpace(storageShardID) != "" && homeShard != storageShardID {
					continue
				}
				storageKey := qualifyStateKey(block.ShardID, row.Key)
				if strings.TrimSpace(storageShardID) != "" {
					storageKey = qualifyStateKey(homeShard, row.Key)
				}
				working[storageKey] = row.Value
				commitment.Set(storageKey, row.Value)
				paperProposalUpdateCount++
			}
		}
		applyDuration += time.Since(updateStarted)
	}

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

	var exchangeWaitDuration time.Duration
	var criticalPathDuration time.Duration
	maximumObserved := 0
	crossShardExecuted := 0
	actualMultiShardUpdates := 0
	actualLockCount := 0
	escHistogram := map[string]int{}
	localBusinessExecutionAttemptCount := 0
	localBusinessCommittedExecutionCount := 0
	certifiedRemoteBusinessResultCount := 0
	escCertificateCount := 0
	multiShardUpdateCertificateDigest := ""
	protocolGlobalStateRoot := ""
	certifiedPartitionRoots := map[string]string{}
	deferredPartitions := map[string]bool{}
	multiShardUpdateAttempt := 0
	partitionMaterializationUpdateCount := 0
	distributedOwnership := strings.TrimSpace(executionShardID) != "" && (batchExchange != nil || waveExchange != nil)
	localExecutedAll := map[string]bool{}
	distributedCertified := map[string]porygonWaveResult{}
	certifiedESCResultDigests := map[string]string{}
	distributedObservedByWave := map[int]int{}
	finalAssignments := append([]porygonTxAssignment(nil), plan.Assignments...)
	finalAssignmentByID := map[string]porygonTxAssignment{}
	for _, assignment := range finalAssignments {
		finalAssignmentByID[assignment.TxID] = assignment
	}
	postExecutionAbandoned := map[string]bool{}
	postExecutionCTxITxConflictCount := 0
	finalNonAbandonedCrossESCConflictPairs := plan.NonAbandonedCrossESCConflictPairs
	finalConflictClosureVerified := plan.ConflictClosureVerified
	postExecutionConflictPreview, _, postExecutionConflictPreviewCount, previewErr := porygonPostExecutionConflictPreview(plan.Assignments, block.TxList)
	if previewErr != nil {
		return BlockExecutionResult{}, previewErr
	}
	sameESCDeferredPreview, _, sameESCDeferredPreviewCount, sameESCPreviewErr := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if sameESCPreviewErr != nil {
		return BlockExecutionResult{}, sameESCPreviewErr
	}
	// Cross-ESC OC conflicts and same-ESC deferred-write hazards are both known
	// before local lane execution. Quarantine any CTx that will be abandoned so
	// its speculative S value can never influence a retained successor.
	speculativeQuarantine := porygonMergeTxBoolSets(postExecutionConflictPreview, sameESCDeferredPreview)
	sameESCDeferredNewAbandonedCount := 0
	sameESCDeferredRemainingPairs := 0
	sameESCDeferredClosureVerified := true
	if distributedOwnership {
		// Porygon ESC members execute their entire assigned batch locally and
		// return one batch result to the OC. Local waves preserve each ESC's
		// deterministic sequential lane; they are not network synchronization
		// points. Cross-ESC conflicts have already been abandoned by the OC plan.
		executionView := copyRegistryStringMap(working)
		localBatchResults := make([]porygonWaveResult, 0)
		requiredSet := map[string]bool{}
		// Paper Figure 6 requires every shard whose subtree is changed by U_i to
		// return enough consistent roots.  This remains mandatory for L-empty
		// maintenance proposals that exist only to drain the final CTx updates.
		if paperProposalMode {
			for sid := range porygonPaperProposalUpdateShardSet(paperProposal, plan.ExecutionShardCount) {
				requiredSet[sid] = true
			}
		}
		localExecutionUS := int64(0)
		for waveIndex, wave := range plan.Waves {
			localWave := make([]string, 0, len(wave))
			for _, txID := range wave {
				assignment, ok := assignmentByID[txID]
				if !ok || assignment.Abandoned {
					continue
				}
				owner := fmt.Sprintf("s%d", assignment.ExecutionShard)
				requiredSet[owner] = true
				if owner == executionShardID {
					localWave = append(localWave, txID)
				}
			}
			started := time.Now()
			localResults, localObserved, err := executePorygonWaveWithPool(ctx, pool, serial, block, localWave, byID, indexByID, executionView, executionShardID, plan.ExecutionShardCount, stateFetch)
			localDuration := time.Since(started)
			executionDuration += localDuration
			localExecutionUS += localDuration.Microseconds()
			if err != nil {
				return BlockExecutionResult{}, err
			}
			distributedObservedByWave[waveIndex] = localObserved
			if localObserved > maximumObserved {
				maximumObserved = localObserved
			}
			for _, txResult := range localResults {
				localExecutedAll[txResult.Item.TxID] = true
				localBatchResults = append(localBatchResults, txResult)
				// A CTx that is deterministically known from the signed AccessList
				// to conflict with an ITx on another ESC still executes and returns S3,
				// but its speculative writes are quarantined from later transactions.
				// OC formally marks it Abandoned only after certified ESC results arrive.
				porygonPublishSpeculativeOverlay(executionView, block.ShardID, txResult, speculativeQuarantine)
			}
		}
		requiredShards := sortedBoolKeys(requiredSet)
		seen := map[string]bool{}
		exchangeStarted := time.Now()
		if batchExchange != nil {
			// Paper2 separates the dynamic ESC role from the fixed Storage Role.
			// The batch root must therefore describe the *logical execution shard*
			// assigned to this ESC, never the physical Storage Role co-located on
			// the current validator.  Storage replicas authenticate the prospective
			// Proposal.T + U + current-ITx root; current CTx remain S candidates.
			certifiedLogicalRoot := ""
			localDeferredPartitions := map[string]bool{}
			if paperProposalMode {
				if multiShardUpdate == nil {
					return BlockExecutionResult{}, fmt.Errorf("Porygon Paper2 logical partition-root projection unavailable")
				}
				rootUpdates := porygonPaperLogicalPartitionUpdates(paperProposal, localBatchResults, assignmentByID, executionShardID, plan.ExecutionShardCount)
				rootStarted := time.Now()
				rootCert, err := multiShardUpdate(ctx, block.BlockHash, block.Height, map[string][]PorygonStateUpdate{executionShardID: rootUpdates})
				exchangeWaitDuration += time.Since(rootStarted)
				if err != nil {
					return BlockExecutionResult{}, err
				}
				localDeferredPartitions = porygonV50DeferredPartitionSet(rootCert)
				for _, part := range rootCert.Partitions {
					if part.PartitionID == executionShardID {
						certifiedLogicalRoot = part.ProspectiveRoot
						break
					}
				}
				if certifiedLogicalRoot == "" {
					return BlockExecutionResult{}, fmt.Errorf("Porygon Paper2 logical partition-root certificate missing %s", executionShardID)
				}
			} else {
				localCertifiedView := copyRegistryStringMap(working)
				for _, localResult := range localBatchResults {
					assignment := assignmentByID[localResult.Item.TxID]
					if assignment.CrossShard || !localResult.Receipt.Success {
						continue
					}
					for key, value := range localResult.Delta.WriteSet {
						homeShard := fmt.Sprintf("s%d", porygonStateShard(key, plan.ExecutionShardCount))
						if strings.TrimSpace(storageShardID) != "" && homeShard != storageShardID {
							continue
						}
						storageKey := qualifyStateKey(homeShard, key)
						localCertifiedView[storageKey] = value
					}
				}
				certifiedLogicalRoot = state.RootOfSnapshot(localCertifiedView)
			}
			localPayload := PorygonESCBatchResult{
				BlockHash: block.BlockHash, Height: block.Height, ExecutionShardID: executionShardID,
				Results: make([]PorygonBatchTxResult, 0, len(localBatchResults)), StateRoot: certifiedLogicalRoot,
				DeferredPartitions: sortedBoolKeys(localDeferredPartitions), BusinessExecutionUS: localExecutionUS,
			}
			for _, item := range localBatchResults {
				localPayload.Results = append(localPayload.Results, PorygonBatchTxResult{TxID: item.Item.TxID, Receipt: item.Receipt, Delta: item.Delta})
			}
			certificate, err := batchExchange(ctx, localPayload, requiredShards)
			exchangeWaitDuration += time.Since(exchangeStarted)
			if err != nil {
				return BlockExecutionResult{}, err
			}
			if certificate.BlockHash != block.BlockHash || certificate.Height != block.Height {
				return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate block identity mismatch")
			}
			for partitionID := range porygonV50DeferredPartitionsFromBatchCertificate(certificate) {
				deferredPartitions[partitionID] = true
			}
			escCertificateCount = 1
			for _, entry := range certificate.Entries {
				certifiedESCResultDigests[entry.ExecutionShardID] = entry.ResultDigest
				if entry.Result.StateRoot == "" {
					return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate missing partition state root for %s", entry.ExecutionShardID)
				}
				certifiedPartitionRoots[entry.ExecutionShardID] = entry.Result.StateRoot
				for _, certified := range entry.Result.Results {
					item, ok := byID[certified.TxID]
					if !ok || seen[certified.TxID] {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate has unknown/duplicate tx %s", certified.TxID)
					}
					assignment := assignmentByID[certified.TxID]
					if assignment.Abandoned || fmt.Sprintf("s%d", assignment.ExecutionShard) != entry.ExecutionShardID {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate ownership mismatch for %s", certified.TxID)
					}
					if err := porygonValidateActualAccess(item, certified.Delta); err != nil {
						return BlockExecutionResult{}, err
					}
					seen[certified.TxID] = true
					distributedCertified[certified.TxID] = porygonWaveResult{Item: item, Receipt: certified.Receipt, Delta: certified.Delta}
				}
			}
		} else {
			localPayload := PorygonESCWaveResult{BlockHash: block.BlockHash, Height: block.Height, Wave: 0, ExecutionShardID: executionShardID, Results: make([]PorygonWaveTxResult, 0, len(localBatchResults)), BusinessExecutionUS: localExecutionUS}
			for _, item := range localBatchResults {
				localPayload.Results = append(localPayload.Results, PorygonWaveTxResult{TxID: item.Item.TxID, Receipt: item.Receipt, Delta: item.Delta})
			}
			certificate, err := waveExchange(ctx, localPayload, requiredShards)
			exchangeWaitDuration += time.Since(exchangeStarted)
			if err != nil {
				return BlockExecutionResult{}, err
			}
			if err := validatePorygonESCWaveCertificate(certificate); err != nil {
				return BlockExecutionResult{}, err
			}
			escCertificateCount = 1
			for _, entry := range certificate.Entries {
				certifiedESCResultDigests[entry.ExecutionShardID] = entry.ResultDigest
				for _, certified := range entry.Result.Results {
					item, ok := byID[certified.TxID]
					if !ok || seen[certified.TxID] {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate has unknown/duplicate tx %s", certified.TxID)
					}
					assignment := assignmentByID[certified.TxID]
					if assignment.Abandoned || fmt.Sprintf("s%d", assignment.ExecutionShard) != entry.ExecutionShardID {
						return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate ownership mismatch for %s", certified.TxID)
					}
					if err := porygonValidateActualAccess(item, certified.Delta); err != nil {
						return BlockExecutionResult{}, err
					}
					seen[certified.TxID] = true
					distributedCertified[certified.TxID] = porygonWaveResult{Item: item, Receipt: certified.Receipt, Delta: certified.Delta}
				}
			}
		}
		if paperProposalMode && len(deferredPartitions) > 0 {
			if err := porygonV50ValidateDeferredCertifiedDisjoint(paperProposal, deferredPartitions, plan.ExecutionShardCount, distributedCertified); err != nil {
				return BlockExecutionResult{}, err
			}
			if strings.TrimSpace(storageShardID) != "" && deferredPartitions[storageShardID] {
				if err := porygonV50RestoreDeferredProposalU(base, working, commitment, paperProposal, storageShardID, plan.ExecutionShardCount); err != nil {
					return BlockExecutionResult{}, err
				}
			}
		}
		expected := 0
		for _, assignment := range plan.Assignments {
			if !assignment.Abandoned {
				expected++
			}
		}
		if len(seen) != expected {
			return BlockExecutionResult{}, fmt.Errorf("porygon ESC batch certificate tx coverage mismatch: got=%d want=%d", len(seen), expected)
		}
		var concurrencyErr error
		finalAssignments, postExecutionAbandoned, postExecutionCTxITxConflictCount, finalNonAbandonedCrossESCConflictPairs, concurrencyErr = porygonPostExecutionConcurrencyControl(plan.Assignments, block.TxList, distributedCertified)
		if concurrencyErr != nil {
			return BlockExecutionResult{}, concurrencyErr
		}
		if postExecutionCTxITxConflictCount != postExecutionConflictPreviewCount {
			return BlockExecutionResult{}, fmt.Errorf("porygon OC CTx/ITx preview mismatch: preview=%d final=%d", postExecutionConflictPreviewCount, postExecutionCTxITxConflictCount)
		}
		var sameESCAbandoned map[string]bool
		var sameESCErr error
		finalAssignments, sameESCAbandoned, sameESCDeferredNewAbandonedCount, sameESCDeferredRemainingPairs, sameESCErr = porygonApplySameESCDeferredWriteHazards(finalAssignments, block.TxList, distributedCertified)
		if sameESCErr != nil {
			return BlockExecutionResult{}, sameESCErr
		}
		if !porygonPreviewAbandonmentClosed(sameESCDeferredPreview, finalAssignments) {
			return BlockExecutionResult{}, fmt.Errorf("porygon same-ESC deferred-write preview did not converge to terminal abandonment")
		}
		for txID := range sameESCAbandoned {
			postExecutionAbandoned[txID] = true
		}
		sameESCDeferredClosureVerified = sameESCDeferredRemainingPairs == 0
		finalConflictClosureVerified = finalNonAbandonedCrossESCConflictPairs == 0
		finalAssignmentByID = map[string]porygonTxAssignment{}
		for _, assignment := range finalAssignments {
			finalAssignmentByID[assignment.TxID] = assignment
		}
		if multiShardUpdate != nil && !paperProposalMode {
			updates, updateCount := porygonPartitionMaterializationUpdates(finalAssignments, plan.ExecutionShardCount, distributedCertified)
			partitionMaterializationUpdateCount += updateCount
			if len(updates) > 0 {
				updateStarted := time.Now()
				certificate, err := multiShardUpdate(ctx, block.BlockHash, block.Height, updates)
				exchangeWaitDuration += time.Since(updateStarted)
				if err != nil {
					return BlockExecutionResult{}, err
				}
				if certificate.RolledBack {
					return BlockExecutionResult{}, fmt.Errorf("porygon multi-shard update rolled back for block %s", block.BlockHash)
				}
				multiShardUpdateCertificateDigest = certificate.CertificateDigest
				protocolGlobalStateRoot = certificate.GlobalStateRoot
				multiShardUpdateAttempt = certificate.Attempt
			}
		}
		criticalPathDuration = executionDuration + exchangeWaitDuration
	}

	for waveIndex, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		snapshot := working
		waveWallStarted := time.Now()
		waveResults := []porygonWaveResult{}
		observed := 0
		if distributedOwnership {
			observed = distributedObservedByWave[waveIndex]
			for _, txID := range wave {
				if item, ok := distributedCertified[txID]; ok {
					waveResults = append(waveResults, item)
				}
			}
		} else {
			started := time.Now()
			var err error
			waveResults, observed, err = executePorygonWaveWithPool(ctx, pool, serial, block, wave, byID, indexByID, snapshot, executionShardID, plan.ExecutionShardCount, stateFetch)
			executionDuration += time.Since(started)
			if err != nil {
				return BlockExecutionResult{}, err
			}
			for _, item := range waveResults {
				localExecutedAll[item.Item.TxID] = true
			}
		}
		if !distributedOwnership {
			criticalPathDuration += time.Since(waveWallStarted)
		}
		if observed > maximumObserved {
			maximumObserved = observed
		}
		sort.SliceStable(waveResults, func(i, j int) bool {
			return indexByID[waveResults[i].Item.TxID] < indexByID[waveResults[j].Item.TxID]
		})
		for _, txResult := range waveResults {
			assignment, ok := finalAssignmentByID[txResult.Item.TxID]
			if !ok {
				return BlockExecutionResult{}, fmt.Errorf("execution plan missing porygon assignment for %s", txResult.Item.TxID)
			}
			if postExecutionAbandoned[txResult.Item.TxID] {
				if localExecutedAll[txResult.Item.TxID] {
					localBusinessExecutionAttemptCount++
				} else if distributedOwnership {
					certifiedRemoteBusinessResultCount++
				}
				receipt := execution.Receipt{
					TxID: txResult.Item.TxID, BlockHash: block.BlockHash, Height: block.Height,
					Success: false, Error: "porygon_ctx_itx_concurrency_abandoned",
					StateKeys: append([]string(nil), txResult.Item.StateKeys...), StateRootAfterTx: commitment.Root(),
				}
				delta := execution.TxDelta{TxID: txResult.Item.TxID, OriginalIndex: indexByID[txResult.Item.TxID], WriteSet: map[string]string{}, Receipt: receipt, Success: false, Error: receipt.Error}
				allReceipts = append(allReceipts, receipt)
				allDeltas = append(allDeltas, delta)
				result.FailedTxs++
				scheduleEvents = append(scheduleEvents, ScheduleEvent{TxID: txResult.Item.TxID, Track: "porygon", QueueName: "oc_concurrency_abandoned", DecisionReason: assignment.ConflictReason, LocalExecution: localExecutedAll[txResult.Item.TxID]})
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: txResult.Item.TxID, Track: "porygon", Attempt: 1, Reason: assignment.ConflictReason, Success: false, FinalCompletion: true})
				continue
			}
			escHistogram[fmt.Sprintf("esc_%d", assignment.ExecutionShard)]++
			if assignment.CrossShard {
				crossShardExecuted++
				actualLockCount += len(assignment.LockedKeys)
			}
			scheduleEvents = append(scheduleEvents, ScheduleEvent{
				TxID: txResult.Item.TxID, Track: "porygon", QueueName: fmt.Sprintf("esc_%d", assignment.ExecutionShard),
				DecisionReason: fmt.Sprintf("porygon_wave_%d_single_esc_execution_cross=%t", waveIndex, assignment.CrossShard),
				LocalExecution: localExecutedAll[txResult.Item.TxID],
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

			// ITx writes are materialized in this EC.  A CTx only produces its S
			// candidate now; its writes are carried in a later proposal U and are
			// applied by the later EC named by that proposal.
			materializeThisRound := !paperProposalMode || !assignment.CrossShard
			if materializeThisRound {
				applyStarted := time.Now()
				for _, key := range keys {
					storageKey := qualifyStateKey(block.ShardID, key)
					if strings.TrimSpace(storageShardID) != "" {
						homeShard := fmt.Sprintf("s%d", porygonStateShard(key, plan.ExecutionShardCount))
						if homeShard != storageShardID {
							continue
						}
						storageKey = qualifyStateKey(homeShard, key)
					}
					working[storageKey] = txResult.Delta.WriteSet[key]
				}
				applyDuration += time.Since(applyStarted)
				commitmentStarted = time.Now()
				for _, key := range keys {
					storageKey := qualifyStateKey(block.ShardID, key)
					if strings.TrimSpace(storageShardID) != "" {
						homeShard := fmt.Sprintf("s%d", porygonStateShard(key, plan.ExecutionShardCount))
						if homeShard != storageShardID {
							continue
						}
						storageKey = qualifyStateKey(homeShard, key)
					}
					commitment.Set(storageKey, txResult.Delta.WriteSet[key])
				}
				stateCommitmentDuration += time.Since(commitmentStarted)
			}
			receipt := txResult.Receipt
			receipt.StateRootAfterTx = commitment.Root()

			delta := txResult.Delta
			delta.Receipt = receipt
			allReceipts = append(allReceipts, receipt)
			allDeltas = append(allDeltas, delta)
			if receipt.Success {
				result.SuccessfulTxs++
			} else {
				result.FailedTxs++
			}
			if localExecutedAll[txResult.Item.TxID] {
				localBusinessExecutionAttemptCount++
				if receipt.Success {
					localBusinessCommittedExecutionCount++
				}
				attempts = append(attempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: txResult.Item.TxID, Track: "porygon", Attempt: 1, Reason: fmt.Sprintf("esc_%d_wave_%d_owned_execution", assignment.ExecutionShard, waveIndex), Success: receipt.Success, FinalCompletion: true})
			} else if distributedOwnership {
				certifiedRemoteBusinessResultCount++
			}
		}
	}

	for _, assignment := range plan.Assignments {
		if !assignment.Abandoned {
			continue
		}
		item := byID[assignment.TxID]
		receipt := execution.Receipt{
			TxID: item.TxID, BlockHash: block.BlockHash, Height: block.Height,
			Success: false, Error: "porygon_cross_shard_conflict_abandoned",
			StateKeys: append([]string(nil), item.StateKeys...), StateRootAfterTx: commitment.Root(),
		}
		delta := execution.TxDelta{
			TxID: item.TxID, OriginalIndex: indexByID[item.TxID], WriteSet: map[string]string{},
			Receipt: receipt, Success: false, Error: receipt.Error,
		}
		allReceipts = append(allReceipts, receipt)
		allDeltas = append(allDeltas, delta)
		result.FailedTxs++
		scheduleEvents = append(scheduleEvents, ScheduleEvent{
			TxID: item.TxID, Track: "porygon", QueueName: "oc_conflict_abandoned",
			DecisionReason: assignment.ConflictReason, LocalExecution: false,
		})
		attempts = append(attempts, BusinessExecutionAttempt{
			BlockHeight: block.Height, TxID: item.TxID, Track: "porygon", Attempt: 0,
			Reason: assignment.ConflictReason, Success: false, FinalCompletion: true,
		})
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
	if paperProposalMode {
		materializedDelta, materializationErr := porygonPaperDurableStateDeltaV50(paperProposal, allDeltas, finalAssignments, block.ShardID, storageShardID, plan.ExecutionShardCount, deferredPartitions)
		if materializationErr != nil {
			return BlockExecutionResult{}, materializationErr
		}
		result.StateDelta = porygonPaperExecutionStateDelta(materializedDelta)
	} else {
		result.StateDelta = executionStateDelta(base, working)
	}

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
	var certifiedExecutionTruth *PorygonCertifiedExecutionTruth
	if distributedOwnership {
		truth, truthErr := porygonBuildCertifiedExecutionTruth(block.BlockHash, block.Height, plan, finalAssignments, distributedCertified, certifiedESCResultDigests)
		if truthErr != nil {
			return BlockExecutionResult{}, truthErr
		}
		certifiedExecutionTruth = &truth
	}
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
	wallClockPipelineOverlapClaimed := plan.PipelineEnabled && plan.CrossBatchWitness && crossBatchWitnessOverlap
	pipelineTimingTruthBoundary := "logical_protocol_slots;wall_clock_overlap_not_claimed"
	if wallClockPipelineOverlapClaimed {
		pipelineTimingTruthBoundary = "real_cross_batch_witness_wall_clock_overlap;shared_pbft_ordering_heights_remain_sequential"
	}
	metrics := map[string]any{
		"porygon_algorithm_id":                                plan.AlgorithmID,
		"porygon_transaction_block_digest":                    plan.TransactionBlockDigest,
		"porygon_transaction_root":                            plan.TransactionRoot,
		"porygon_access_root":                                 plan.AccessRoot,
		"porygon_witness_policy":                              plan.WitnessPolicy,
		"porygon_witness_threshold":                           plan.WitnessThreshold,
		"porygon_witness_threshold_configured":                plan.WitnessThreshold,
		"porygon_witness_threshold_enforced":                  true,
		"porygon_witness_validation_mode":                     "ec_witness_certificate_before_pbft_ordering",
		"porygon_witnessed_block_count":                       1,
		"porygon_validator_witness_verified":                  true,
		"porygon_pipeline_enabled":                            plan.PipelineEnabled,
		"porygon_compact_proposal_lut":                        paperProposalMode,
		"porygon_proposal_u_applied_update_count":             paperProposalUpdateCount,
		"porygon_certified_partition_roots":                   certifiedPartitionRoots,
		"porygon_ctx_preexecution_materializes_state":         !paperProposalMode,
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
		"porygon_logical_state_cross_shard_ratio":             float64(plan.CrossShardTransactionCnt) / float64(max(1, plan.IntraShardTransactionCnt+plan.CrossShardTransactionCnt)),
		"porygon_planned_logical_cross_shard_ratio":           float64(plan.CrossShardTransactionCnt) / float64(max(1, plan.IntraShardTransactionCnt+plan.CrossShardTransactionCnt)),
		"porygon_executed_cross_shard_ratio":                  float64(crossShardExecuted) / float64(max(1, len(plan.Assignments)-plan.IntraShardTransactionCnt-plan.AbandonedCrossShardTransactionCnt-postExecutionCTxITxConflictCount-sameESCDeferredNewAbandonedCount)),
		"porygon_cross_esc_abandon_ratio":                     float64(plan.AbandonedCrossShardTransactionCnt+postExecutionCTxITxConflictCount) / float64(max(1, plan.IntraShardTransactionCnt+plan.CrossShardTransactionCnt)),
		"porygon_cross_shard_metric_truth_scope":              "execution_esc_plus_signed_access_state_shard_ownership;not_workload_source_target_ratio",
		"porygon_single_shard_execution_count":                crossShardExecuted,
		"porygon_multi_shard_update_count":                    actualMultiShardUpdates,
		"porygon_state_lock_count":                            actualLockCount,
		"porygon_pipeline_trace":                              plan.Pipeline,
		"porygon_execution_assignments":                       finalAssignments,
		"porygon_execution_shard_histogram":                   escHistogram,
		"porygon_active_execution_shard_count":                len(escHistogram),
		"porygon_esc_execution_ownership_mode":                map[bool]string{true: "distributed_esc_batch_quorum_result_exchange_v2", false: "legacy_all_replica_business_execution"}[distributedOwnership],
		"porygon_local_execution_shard_id":                    executionShardID,
		"porygon_local_business_execution_count":              localBusinessCommittedExecutionCount,
		"porygon_local_committed_execution_count":             localBusinessCommittedExecutionCount,
		"porygon_local_business_execution_attempt_count":      localBusinessExecutionAttemptCount,
		"porygon_certified_remote_business_result_count":      certifiedRemoteBusinessResultCount,
		"porygon_esc_certificate_count":                       escCertificateCount,
		"porygon_esc_batch_certificate_count":                 escCertificateCount,
		"porygon_state_proof_policy":                          "verified_merkle_treap_inclusion_and_nonmembership_proof",
		"porygon_committee_selection_policy":                  "deterministic_vrf_style_sortition_adapter_over_fixed_pbft_validator_set",
		"porygon_vrf_exact_claimed":                           false,
		"porygon_state_owner_policy":                          "signed_access_account_object_owner_partition_no_metatrack_signal",
		"porygon_cross_shard_conflict_policy":                 porygonCrossESCConflictPolicy,
		"porygon_execution_result_threshold_policy":           "strict_majority_more_than_half_for_sharded_execution_results",
		"porygon_multi_shard_update_threshold_policy":         "strict_majority",
		"porygon_real_cross_execution_shard_network":          distributedOwnership,
		"porygon_generic_relay_finalize_used":                 false,
		"porygon_cross_shard_conflict_abandoned_count":        plan.AbandonedCrossShardTransactionCnt + postExecutionCTxITxConflictCount + sameESCDeferredNewAbandonedCount,
		"porygon_protocol_abandoned_transaction_count":        plan.AbandonedOrderingTransactionCnt + postExecutionCTxITxConflictCount + sameESCDeferredNewAbandonedCount,
		"porygon_post_execution_candidate_quarantine_count":   len(speculativeQuarantine),
		"porygon_abandoned_ctx_itx_conflict_count":            postExecutionCTxITxConflictCount + sameESCDeferredNewAbandonedCount,
		"porygon_abandoned_ctx_ctx_conflict_count":            plan.AbandonedCTxCTxConflictCnt,
		"porygon_rollback_abandoned_ctx_count":                rollbackAbandonedCount,
		"porygon_nonabandoned_cross_esc_conflict_pair_count":  finalNonAbandonedCrossESCConflictPairs,
		"porygon_cross_esc_conflict_closure_verified":         finalConflictClosureVerified,
		"porygon_partition_materialization_update_count":      partitionMaterializationUpdateCount,
		"porygon_multi_shard_update_certificate_digest":       multiShardUpdateCertificateDigest,
		"porygon_protocol_global_state_root":                  protocolGlobalStateRoot,
		"porygon_multi_shard_update_attempt":                  multiShardUpdateAttempt,
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
		"porygon_storage_visibility_policy":                   "signed_access_projection_with_verified_merkle_treap_proof_over_storage_roles",
		"porygon_meta_remote_state_control_plane_used":        false,
		"porygon_state_projection_remote_fetch_used":          stateFetch != nil,
		"porygon_state_storage_identity":                      executionShardID,
		"porygon_state_root_scope":                            map[bool]string{true: "logical_execution_shard_storage_role_strict_majority_root", false: map[bool]string{true: "local_execution_shard_storage_partition", false: "legacy_direct_executor_global_snapshot"}[strings.TrimSpace(executionShardID) != ""]}[paperProposalMode],
		"porygon_paper_storage_root_quorum_enforced":          paperProposalMode && batchExchange != nil && multiShardUpdate != nil,
		"porygon_global_physical_state_root_claimed":          strings.TrimSpace(executionShardID) == "",
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
		"abort_count":                                         plan.AbandonedOrderingTransactionCnt + postExecutionCTxITxConflictCount + sameESCDeferredNewAbandonedCount,
		"reexecution_count":                                   0,
		"serializable":                                        finalConflictClosureVerified && finalNonAbandonedCrossESCConflictPairs == 0 && sameESCDeferredClosureVerified,
		"porygon_mbe_consensus_adaptation":                    "single_global_pbft_ordering_domain_with_esc_quorum_result_exchange",
		"porygon_storage_node_adaptation":                     "paper_storage_role_co_located_on_existing_mbe_nodes;dynamic_esc_identity_separated_from_fixed_storage_role;logical_partition_root_strict_majority_authenticated",
		"porygon_witness_adaptation":                          "real_ec_witness_certificate_with_mbe_pbft_validator_identity",
		"porygon_pipeline_timing_truth_boundary":              pipelineTimingTruthBoundary,
		"porygon_wall_clock_pipeline_overlap_claimed":         wallClockPipelineOverlapClaimed,
		"porygon_full_woec_pipeline_overlap_claimed":          false,
		"porygon_cross_shard_atomicity_truth_boundary":        "single_esc_preexecution_S_then_OC_U_then_following_EC_application_then_later_proposal_commit",
		"porygon_paper_fidelity_version":                       porygonPaperFidelityV50Version,
		"porygon_sharded_execution_result_threshold_rule":      "strict_majority_more_than_half",
		"porygon_v50_ec_slot":                                 porygonV50ECSlotForHeight(block.Height),
		"porygon_v50_deferred_partitions":                     sortedBoolKeys(deferredPartitions),
		"porygon_v50_deferred_partition_count":                len(deferredPartitions),
	}
	metrics["porygon_same_esc_deferred_write_candidate_count"] = sameESCDeferredPreviewCount
	metrics["porygon_same_esc_deferred_write_abandoned_count"] = sameESCDeferredPreviewCount
	metrics["porygon_same_esc_deferred_write_new_abandoned_count"] = sameESCDeferredNewAbandonedCount
	metrics["porygon_same_esc_deferred_write_remaining_pair_count"] = sameESCDeferredRemainingPairs
	metrics["porygon_same_esc_deferred_write_closure_verified"] = sameESCDeferredClosureVerified
	if certifiedExecutionTruth != nil {
		metrics["porygon_certified_execution_truth_version"] = certifiedExecutionTruth.Version
		metrics["porygon_certified_execution_semantic_digest"] = certifiedExecutionTruth.SemanticDigest
		metrics["porygon_certified_execution_future_u_height"] = certifiedExecutionTruth.FutureProposalHeight
		metrics["porygon_certified_execution_future_u_count"] = len(certifiedExecutionTruth.ProposalU)
		metrics["porygon_certified_execution_future_u_digest"] = certifiedExecutionTruth.ProposalUDigest
	}
	return BlockExecutionResult{
		ExecutionResult: result, PorygonCertifiedExecution: certifiedExecutionTruth, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: (poolSetupDuration + criticalPathDuration + applyDuration + stateCommitmentDuration).Milliseconds(),
		TransactionExecutionMS: result.TransactionExecutionMS, DeterministicApplyMS: result.DeterministicMaterializationMS,
		StateCommitmentMS: result.StateCommitmentMS, StateRootVersion: state.CommitmentVersion,
		ScheduleEvents: scheduleEvents, ActualMetrics: metrics, BusinessAttempts: attempts,
	}, nil
}

func porygonPublishSpeculativeOverlay(executionView map[string]string, blockShardID string, result porygonWaveResult, quarantined map[string]bool) bool {
	if quarantined[result.Item.TxID] {
		return false
	}
	for key, value := range result.Delta.WriteSet {
		executionView[qualifyStateKey(blockShardID, key)] = value
	}
	return true
}

func porygonPostExecutionConflictPreview(assignments []porygonTxAssignment, items []tx.SignedTransaction) (map[string]bool, map[string][]string, int, error) {
	byTx := make(map[string]tx.SignedTransaction, len(items))
	for _, item := range items {
		byTx[item.TxID] = item
	}
	preview := map[string]bool{}
	conflicts := map[string][]string{}
	for i := range assignments {
		candidate := assignments[i]
		if !candidate.CrossShard || candidate.Abandoned {
			continue
		}
		candidateItem, ok := byTx[candidate.TxID]
		if !ok {
			return nil, nil, 0, fmt.Errorf("porygon OC concurrency preview missing CTx %s", candidate.TxID)
		}
		for j := range assignments {
			itx := assignments[j]
			if itx.CrossShard || itx.Abandoned || candidate.ExecutionShard == itx.ExecutionShard {
				continue
			}
			itxItem, ok := byTx[itx.TxID]
			if !ok {
				return nil, nil, 0, fmt.Errorf("porygon OC concurrency preview missing ITx %s", itx.TxID)
			}
			if porygonAssignmentsConflict(candidate, itx, candidateItem, itxItem) {
				preview[candidate.TxID] = true
				conflicts[candidate.TxID] = append(conflicts[candidate.TxID], itx.TxID)
			}
		}
	}
	for txID := range conflicts {
		conflicts[txID] = uniqueStrings(conflicts[txID])
	}
	return preview, conflicts, len(preview), nil
}

func porygonPostExecutionConcurrencyControl(assignments []porygonTxAssignment, items []tx.SignedTransaction, certified map[string]porygonWaveResult) ([]porygonTxAssignment, map[string]bool, int, int, error) {
	final := append([]porygonTxAssignment(nil), assignments...)
	preview, conflicts, ctxItxCount, err := porygonPostExecutionConflictPreview(assignments, items)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	postAbandoned := map[string]bool{}
	for i := range final {
		if !preview[final[i].TxID] {
			continue
		}
		if _, ok := certified[final[i].TxID]; !ok {
			return nil, nil, 0, 0, fmt.Errorf("porygon OC concurrency control missing certified CTx %s", final[i].TxID)
		}
		final[i].Abandoned = true
		final[i].ConflictWith = uniqueStrings(append(final[i].ConflictWith, conflicts[final[i].TxID]...))
		final[i].ConflictReason = "oc_post_execution_ctx_itx_conflict_abandoned"
		postAbandoned[final[i].TxID] = true
	}
	pairs := porygonNonAbandonedCrossESCConflictPairs(final, items)
	if pairs != 0 {
		return nil, nil, 0, pairs, fmt.Errorf("porygon OC post-execution conflict closure incomplete: non-abandoned cross-ESC conflict pairs=%d", pairs)
	}
	return final, postAbandoned, ctxItxCount, pairs, nil
}

func porygonPartitionMaterializationUpdates(assignments []porygonTxAssignment, executionShardCount int, certified map[string]porygonWaveResult) (map[string][]PorygonStateUpdate, int) {
	updates := map[string][]PorygonStateUpdate{}
	count := 0
	for _, assignment := range assignments {
		result, ok := certified[assignment.TxID]
		if !ok || assignment.Abandoned || !result.Delta.Success {
			continue
		}
		keys := make([]string, 0, len(result.Delta.WriteSet))
		for key := range result.Delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			partitionID := fmt.Sprintf("s%d", porygonStateShard(key, executionShardCount))
			updates[partitionID] = append(updates[partitionID], PorygonStateUpdate{TxID: assignment.TxID, OriginalIndex: assignment.OriginalIndex, Key: key, Value: result.Delta.WriteSet[key]})
			count++
		}
	}
	for partitionID, items := range updates {
		updates[partitionID] = porygonCanonicalStateUpdates(items)
	}
	return updates, count
}

func executePorygonWaveWithPool(ctx context.Context, pool *fixedBlockWorkerPool, serial *execution.SerialExecutor, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string, executionShardID string, executionShardCount int, stateFetch PorygonStateFetchFunc) ([]porygonWaveResult, int, error) {
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
			txSnapshot, err := porygonTransactionSnapshotWithFetch(ctx, snapshot, block.ShardID, executionShardID, executionShardCount, item, stateFetch)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
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

func porygonTransactionSnapshotWithFetch(ctx context.Context, base map[string]string, orderingDomain, executionShardID string, executionShardCount int, item tx.SignedTransaction, fetch PorygonStateFetchFunc) (map[string]string, error) {
	// Direct executor unit tests and legacy pure-function probes do not carry an
	// execution-shard identity. Preserve that isolated compatibility mode without
	// weakening the real Porygon runtime: production nodes always provide
	// ExecutionShardID, and remote projections remain fail-closed there.
	if strings.TrimSpace(executionShardID) == "" {
		return porygonTransactionSnapshot(base, orderingDomain, item), nil
	}
	out := make(map[string]string, len(item.AccessList))
	for _, access := range item.AccessList {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			continue
		}
		homeShard := fmt.Sprintf("s%d", porygonStateShard(key, executionShardCount))
		qualifiedForExecution := qualifyStateKey(orderingDomain, key)
		// Same-ESC earlier transactions publish their writes into the temporary
		// ordering-domain execution overlay. Prefer that value before consulting
		// a Storage Role so sequential ESC semantics remain correct without
		// introducing per-wave network barriers.
		if value, ok := base[qualifiedForExecution]; ok {
			out[qualifiedForExecution] = value
			continue
		}
		if executionShardID != "" && homeShard == executionShardID {
			if value, ok := base[qualifyStateKey(executionShardID, key)]; ok {
				out[qualifiedForExecution] = value
				continue
			}
			if value, ok := base[key]; ok {
				out[qualifiedForExecution] = value
				continue
			}
		}
		if fetch == nil {
			return nil, fmt.Errorf("porygon state projection fetch unavailable for tx=%s key=%s home=%s execution_shard=%s", item.TxID, key, homeShard, executionShardID)
		}
		event, err := fetch(ctx, item, access)
		if err != nil {
			return nil, fmt.Errorf("porygon state projection fetch tx=%s key=%s home=%s: %w", item.TxID, key, homeShard, err)
		}
		if event.HomeShard != "" && event.HomeShard != homeShard {
			return nil, fmt.Errorf("porygon state projection home mismatch tx=%s key=%s got=%s want=%s", item.TxID, key, event.HomeShard, homeShard)
		}
		out[qualifiedForExecution] = event.Value
	}
	return out, nil
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

// porygonPaperLogicalPartitionUpdates returns the deterministic state changes
// whose root is certified by one logical ESC in the current Paper2 execution
// round. Proposal U is applied first; successful current ITx writes are applied
// in the same round. Current CTx writes are deliberately excluded because they
// remain pre-executed S candidates until a later Proposal U.
func porygonPaperLogicalPartitionUpdates(proposal PorygonProposalBody, localResults []porygonWaveResult, assignmentByID map[string]porygonTxAssignment, executionShardID string, shardCount int) []PorygonStateUpdate {
	updates := make([]PorygonStateUpdate, 0)
	for _, proposalUpdate := range proposal.U {
		for _, row := range proposalUpdate.Updates {
			if row.Key == "" {
				continue
			}
			homeShard := fmt.Sprintf("s%d", porygonStateShard(row.Key, shardCount))
			if homeShard == executionShardID {
				updates = append(updates, row)
			}
		}
	}
	for _, result := range localResults {
		assignment, ok := assignmentByID[result.Item.TxID]
		if !ok || assignment.Abandoned || assignment.CrossShard || !result.Receipt.Success {
			continue
		}
		keys := make([]string, 0, len(result.Delta.WriteSet))
		for key := range result.Delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			homeShard := fmt.Sprintf("s%d", porygonStateShard(key, shardCount))
			if homeShard != executionShardID {
				continue
			}
			updates = append(updates, PorygonStateUpdate{TxID: result.Delta.TxID, OriginalIndex: result.Delta.OriginalIndex, Key: key, Value: result.Delta.WriteSet[key]})
		}
	}
	return porygonPaperCollapseOrderedStateUpdates(updates)
}
