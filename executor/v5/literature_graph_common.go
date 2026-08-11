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

const literatureGraphPlanVersion = "mbe_literature_graph_plan_v1"

type literatureTxAccess struct {
	Item      tx.SignedTransaction
	TxID      string
	Ordinal   int
	ReadKeys  []string
	WriteKeys []string
}

type literatureGraphMetrics struct {
	TransactionCount int `json:"transaction_count"`
	EdgeCount        int `json:"edge_count"`
	WaveCount        int `json:"wave_count"`
	MaximumWaveWidth int `json:"maximum_wave_width"`
	ColorCount       int `json:"color_count,omitempty"`
	PairChecks       int `json:"pair_checks,omitempty"`
}

type literatureGraphPlan struct {
	AlgorithmID             string                 `json:"algorithm_id"`
	Version                 string                 `json:"version"`
	BlockHeight             uint64                 `json:"block_height"`
	CandidateTransactionIDs []string               `json:"candidate_transaction_ids"`
	Waves                   [][]string             `json:"waves"`
	SerializationOrder      []string               `json:"serialization_order"`
	DeclaredAccessSetDigest string                 `json:"declared_access_set_digest"`
	DeclaredReadKeyCount    int                    `json:"declared_read_key_count"`
	DeclaredWriteKeyCount   int                    `json:"declared_write_key_count"`
	Metrics                 literatureGraphMetrics `json:"metrics"`
	PlanDigest              string                 `json:"plan_digest"`
}

type literatureGraphExecutorMetrics struct {
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
}

func literaturePlanDigest(plan literatureGraphPlan) string {
	clone := plan
	clone.PlanDigest = ""
	return stableDigest(clone)
}

func literatureMarshalPlan(plan literatureGraphPlan) ([]byte, error) {
	return literatureJSONMarshal(plan)
}

func literatureParsePlan(raw []byte, algorithmID string) (literatureGraphPlan, error) {
	var plan literatureGraphPlan
	if err := literatureJSONUnmarshal(raw, &plan); err != nil {
		return plan, fmt.Errorf("decode %s plan: %w", algorithmID, err)
	}
	if plan.AlgorithmID != algorithmID || plan.Version != literatureGraphPlanVersion {
		return plan, fmt.Errorf("unsupported literature plan %s/%s", plan.AlgorithmID, plan.Version)
	}
	if plan.PlanDigest == "" || literaturePlanDigest(plan) != plan.PlanDigest {
		return plan, fmt.Errorf("%s plan digest mismatch", algorithmID)
	}
	return plan, nil
}

// Thin wrappers keep JSON ownership in this package without coupling the
// literature baselines to another algorithm implementation.
func literatureJSONMarshal(value any) ([]byte, error)     { return json.Marshal(value) }
func literatureJSONUnmarshal(raw []byte, value any) error { return json.Unmarshal(raw, value) }

func literatureAccessDescriptors(items []tx.SignedTransaction, shardID string) ([]literatureTxAccess, error) {
	out := make([]literatureTxAccess, 0, len(items))
	seen := map[string]bool{}
	for index, item := range items {
		if item.TxID == "" || seen[item.TxID] {
			return nil, fmt.Errorf("literature scheduler requires unique non-empty tx ids")
		}
		seen[item.TxID] = true
		reads, writes := literatureDeclaredAccess(item, shardID)
		out = append(out, literatureTxAccess{Item: item, TxID: item.TxID, Ordinal: index, ReadKeys: reads, WriteKeys: writes})
	}
	return out, nil
}

func literatureDeclaredAccess(item tx.SignedTransaction, shardID string) ([]string, []string) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	if len(item.AccessList) > 0 {
		for _, access := range item.AccessList {
			if access.Key == "" {
				continue
			}
			switch access.Mode {
			case tx.AccessRead:
				reads[access.Key] = true
			case tx.AccessWrite:
				writes[access.Key] = true
			case tx.AccessReadWrite, tx.AccessCommutativeDelta:
				reads[access.Key] = true
				writes[access.Key] = true
			}
		}
		if item.AccessListSource == "legacy_state_keys" {
			for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
				reads[key] = true
				writes[key] = true
			}
		}
	} else {
		for _, key := range item.StateKeys {
			if key != "" {
				reads[key] = true
				writes[key] = true
			}
		}
		for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
			reads[key] = true
			writes[key] = true
		}
	}
	if literatureIsCrossShardTargetCommit(item, shardID) {
		writes["relay_commit:"+item.TxID] = true
	}
	return literatureSortedBoolKeys(reads), literatureSortedBoolKeys(writes)
}

func literatureSortedBoolKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func literatureDeclaredAccessSummary(items []literatureTxAccess) (string, int, int) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	for _, item := range items {
		for _, key := range item.ReadKeys {
			reads[key] = true
		}
		for _, key := range item.WriteKeys {
			writes[key] = true
		}
	}
	payload := struct {
		ReadKeys  []string `json:"read_keys"`
		WriteKeys []string `json:"write_keys"`
	}{ReadKeys: literatureSortedBoolKeys(reads), WriteKeys: literatureSortedBoolKeys(writes)}
	return stableDigest(payload), len(payload.ReadKeys), len(payload.WriteKeys)
}

func literatureConflicts(left, right literatureTxAccess) bool {
	lr, lw := literatureStringSet(left.ReadKeys), literatureStringSet(left.WriteKeys)
	rr, rw := literatureStringSet(right.ReadKeys), literatureStringSet(right.WriteKeys)
	return literatureSetIntersects(lw, rr) || literatureSetIntersects(lr, rw) || literatureSetIntersects(lw, rw)
}

func literatureStringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func literatureSetIntersects(left, right map[string]bool) bool {
	if len(left) > len(right) {
		left, right = right, left
	}
	for key := range left {
		if right[key] {
			return true
		}
	}
	return false
}

func literatureWavesFromEdges(items []literatureTxAccess, edges map[int]map[int]bool) ([][]string, error) {
	indegree := make([]int, len(items))
	for _, children := range edges {
		for child := range children {
			indegree[child]++
		}
	}
	remaining := len(items)
	waves := [][]string{}
	for remaining > 0 {
		ready := make([]int, 0)
		for index := range items {
			if indegree[index] == 0 {
				ready = append(ready, index)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("literature dependency graph contains a cycle")
		}
		sort.Ints(ready)
		wave := make([]string, 0, len(ready))
		for _, index := range ready {
			wave = append(wave, items[index].TxID)
			indegree[index] = -1
			remaining--
		}
		for _, index := range ready {
			for child := range edges[index] {
				indegree[child]--
			}
		}
		waves = append(waves, wave)
	}
	return waves, nil
}

func literatureFinalizePlan(plan literatureGraphPlan) literatureGraphPlan {
	plan.Version = literatureGraphPlanVersion
	plan.SerializationOrder = plan.SerializationOrder[:0]
	maxWidth := 0
	for _, wave := range plan.Waves {
		if len(wave) > maxWidth {
			maxWidth = len(wave)
		}
		plan.SerializationOrder = append(plan.SerializationOrder, wave...)
	}
	plan.Metrics.WaveCount = len(plan.Waves)
	plan.Metrics.MaximumWaveWidth = maxWidth
	plan.PlanDigest = literaturePlanDigest(plan)
	return plan
}

func literatureVerifyPlan(block realblock.Block, plan literatureGraphPlan, algorithmID string, rebuild func(realblock.Block) (literatureGraphPlan, error)) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != algorithmID {
		return fmt.Errorf("%s execution plan is missing", algorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", algorithmID)
	}
	recomputed, err := rebuild(block)
	if err != nil {
		return err
	}
	if recomputed.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("%s deterministic plan mismatch", algorithmID)
	}
	return nil
}

type literatureWaveResult struct {
	Item    tx.SignedTransaction
	Receipt execution.Receipt
	Delta   execution.TxDelta
}

func executeLiteratureGraphPlan(ctx context.Context, block realblock.Block, base map[string]string, plan literatureGraphPlan, workerCount int, executorID string) (BlockExecutionResult, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	working := literatureCopyStringMap(base)
	before := state.RootOfSnapshot(working)
	byID := make(map[string]tx.SignedTransaction, len(block.TxList))
	indexByID := make(map[string]int, len(block.TxList))
	for index, item := range block.TxList {
		byID[item.TxID] = item
		indexByID[item.TxID] = index
	}
	result := execution.Result{
		BlockHash: block.BlockHash, Height: block.Height, StateRootBefore: before,
		Deterministic: true, StateUpdates: map[string]string{}, BlockExecutorID: executorID,
		ExecutorVersion: "1.0.0", WorkerCount: workerCount,
	}
	allDeltas := make([]execution.TxDelta, 0, len(block.TxList))
	allReceipts := make([]execution.Receipt, 0, len(block.TxList))
	maximumObserved := 0
	var executionMS, applyMS int64
	for _, wave := range plan.Waves {
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		// No transaction in a wave mutates working directly. Share the current
		// immutable wave state and keep writes transaction-local until the barrier.
		snapshot := working
		started := time.Now()
		waveResults, observed, err := executeLiteratureWave(ctx, block, wave, byID, indexByID, snapshot, workerCount)
		executionMS += time.Since(started).Milliseconds()
		if err != nil {
			return BlockExecutionResult{}, err
		}
		if observed > maximumObserved {
			maximumObserved = observed
		}
		started = time.Now()
		for _, txResult := range waveResults {
			keys := make([]string, 0, len(txResult.Delta.WriteSet))
			for key := range txResult.Delta.WriteSet {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				working[literatureQualifiedKey(block.ShardID, key)] = txResult.Delta.WriteSet[key]
			}
			receipt := txResult.Receipt
			receipt.StateRootAfterTx = state.RootOfSnapshot(working)
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
		applyMS += time.Since(started).Milliseconds()
	}
	result.Receipts = allReceipts
	result.TxDeltas = allDeltas
	result.StateRootAfter = state.RootOfSnapshot(working)
	result.ReceiptRoot = execution.ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = literatureStateDelta(base, working)
	result.Plan = execution.ExecutionPlan{
		EngineID: executorID, EngineVersion: "1.0.0", BlockHash: block.BlockHash, BlockHeight: block.Height,
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
	metrics := literatureGraphExecutorMetrics{
		AlgorithmID: plan.AlgorithmID, WorkerCount: workerCount, WaveCount: plan.Metrics.WaveCount,
		MaximumWaveWidth: plan.Metrics.MaximumWaveWidth, MaximumObservedParallel: maximumObserved,
		GraphEdgeCount: plan.Metrics.EdgeCount, GraphPairCheckCount: plan.Metrics.PairChecks,
		GraphColorCount: plan.Metrics.ColorCount, TransactionExecutionMS: executionMS,
		DeterministicApplyMS: applyMS,
	}
	actual := map[string]any{
		"literature_graph_metrics":        metrics,
		"maximum_parallel_width":          maximumObserved,
		"dependency_edge_count":           plan.Metrics.EdgeCount,
		"wave_count":                      plan.Metrics.WaveCount,
		"maximum_wave_width":              plan.Metrics.MaximumWaveWidth,
		"graph_color_count":               plan.Metrics.ColorCount,
		"pairwise_conflict_check_count":   plan.Metrics.PairChecks,
		"abort_count":                     0,
		"reexecution_count":               0,
		"serializable":                    true,
		"literature_plan_algorithm_id":    plan.AlgorithmID,
		"literature_plan_digest_verified": true,
	}
	businessAttempts := make([]BusinessExecutionAttempt, 0, len(allDeltas))
	for _, delta := range allDeltas {
		businessAttempts = append(businessAttempts, BusinessExecutionAttempt{BlockHeight: block.Height, TxID: delta.TxID, Track: executorID, Attempt: 1, Reason: "literature_graph_wave_execution", Success: delta.Success, FinalCompletion: true})
	}
	return BlockExecutionResult{
		ExecutionResult: result, StateDelta: stateKVsFromExecutionDelta(result.StateDelta), PlanDigest: plan.PlanDigest,
		WorkerCount: workerCount, BlockExecutionMS: executionMS + applyMS, TransactionExecutionMS: executionMS, DeterministicApplyMS: applyMS,
		ActualMetrics: actual, BusinessAttempts: businessAttempts,
	}, nil
}

func executeLiteratureWave(ctx context.Context, block realblock.Block, wave []string, byID map[string]tx.SignedTransaction, indexByID map[string]int, snapshot map[string]string, workers int) ([]literatureWaveResult, int, error) {
	results := make([]literatureWaveResult, len(wave))
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
				results[task.index] = literatureWaveResult{Item: item, Receipt: receipt, Delta: delta}
				mu.Lock()
				active--
				mu.Unlock()
			}
		}()
	}
	for index := range wave {
		jobs <- job{index: index}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, maximum, firstErr
	}
	return results, maximum, nil
}

func literatureCopyStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func literatureQualifiedKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func literatureIsCrossShardTargetCommit(item tx.SignedTransaction, shardID string) bool {
	if !strings.HasPrefix(item.Payload, "v5_cross:") {
		return false
	}
	target := strings.TrimPrefix(item.Payload, "v5_cross:")
	if colon := strings.Index(target, ":"); colon >= 0 {
		target = target[:colon]
	}
	return strings.TrimSpace(target) != "" && strings.TrimSpace(target) == shardID
}

func literatureStateDelta(before, after map[string]string) []execution.StateUpdate {
	keys := make([]string, 0)
	for key, value := range after {
		if before[key] != value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]execution.StateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, execution.StateUpdate{Key: key, Value: after[key]})
	}
	return out
}
