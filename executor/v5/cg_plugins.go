package v5

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	cgExecutionID     = "cg_execution"
	cgSchedulerID     = "cg_scheduler"
	cgBlockExecutorID = "cg_block_executor"
	cgPlanAlgorithmID = "cg_full_pairwise_dependency_dag_v1"
)

const cgSmartValidatorMode = "paper_smart_validator_address_lists_v1"

type cgExecution struct{ basicPlugin }
type cgScheduler struct{ basicPlugin }
type cgBlockExecutor struct{ basicPlugin }

func (p cgExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "cg", Reason: "full_pairwise_conflict_graph"}
}
func (p cgScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p cgScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}
func (p cgScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildCGPlanWithWorkers(block, cgPlanningWorkerCount(p.config))
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: cgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p cgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("cg execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		return err
	}
	return verifyCGPlanSmart(block, plan, cgPlanningWorkerCount(p.config))
}
func (p cgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("cg execution plan missing")
	}
	parseStarted := time.Now()
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	parseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	verifyStarted := time.Now()
	verifyMode := "full_recompute"
	if input.ExecutionPlanVerified {
		verifyMode = "preverified_projection"
		err = verifyPreverifiedLiteratureGraphProjection(input.Block, plan, cgPlanAlgorithmID)
	} else {
		err = verifyCGPlanSmart(input.Block, plan, cgPlanningWorkerCount(p.config))
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result, err := executeCGPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, configuredWorkerCount(p.config, input.WorkerCount))
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

func cgPlanningWorkerCount(config map[string]any) int {
	if value := intValue(config["worker_count"]); value > 0 {
		if value > 8 {
			return 8
		}
		return value
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	return workers
}

func buildCGPlan(block realblock.Block) (literatureGraphPlan, error) {
	return buildCGPlanWithWorkers(block, cgPlanningWorkerCount(nil))
}

// buildCGPlanWithWorkers follows the paper's parallel CreateDAG structure:
// workers claim transaction i independently and compare only against j>i.
// The RW/WR/WW dependency definition and i->j direction are unchanged.
func buildCGPlanWithWorkers(block realblock.Block, workerCount int) (literatureGraphPlan, error) {
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(items) && len(items) > 0 {
		workerCount = len(items)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	edgeTargets := make([][]int, len(items))
	var next int64 = -1
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1))
				if i >= len(items) {
					return
				}
				children := make([]int, 0)
				for j := i + 1; j < len(items); j++ {
					if literatureConflicts(items[i], items[j]) {
						children = append(children, j)
					}
				}
				edgeTargets[i] = children
			}
		}()
	}
	wg.Wait()

	edges := map[int]map[int]bool{}
	edgeList := make([]literatureGraphEdge, 0)
	for i, children := range edgeTargets {
		if len(children) == 0 {
			continue
		}
		edges[i] = map[int]bool{}
		for _, j := range children {
			edges[i][j] = true
			edgeList = append(edgeList, literatureGraphEdge{From: i, To: j})
		}
	}
	waves, err := literatureWavesFromEdges(items, edges)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	pairChecks := len(items) * (len(items) - 1) / 2
	plan := literatureGraphPlan{
		AlgorithmID: cgPlanAlgorithmID, BlockHeight: block.Height,
		DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount,
		Edges: edgeList, ValidatorMode: cgSmartValidatorMode,
		Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: len(edgeList), PairChecks: pairChecks, PlanningWorkerCount: workerCount},
		Waves:   waves,
	}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}

type cgAddressAccess struct {
	readers []int
	writers []int
}

// verifyCGPlanSmart validates the producer-shared DAG from address-indexed
// read/write lists. It detects missing and extra edges without rerunning the
// producer's full n^2 transaction-pair CreateDAG pass on every validator.
func verifyCGPlanSmart(block realblock.Block, plan literatureGraphPlan, workerCount int) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return fmt.Errorf("%s execution plan is missing", cgPlanAlgorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", cgPlanAlgorithmID)
	}
	if plan.ValidatorMode != cgSmartValidatorMode {
		return fmt.Errorf("cg smart-validator mode mismatch: %s", plan.ValidatorMode)
	}
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return err
	}
	if len(plan.CandidateTransactionIDs) != len(items) {
		return fmt.Errorf("cg candidate transaction count mismatch")
	}
	for i, item := range items {
		if plan.CandidateTransactionIDs[i] != item.TxID {
			return fmt.Errorf("cg candidate transaction mismatch at index %d", i)
		}
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	if plan.DeclaredAccessSetDigest != accessDigest || plan.DeclaredReadKeyCount != readKeyCount || plan.DeclaredWriteKeyCount != writeKeyCount {
		return fmt.Errorf("cg declared access summary mismatch")
	}

	planEdges := map[uint64]bool{}
	edges := map[int]map[int]bool{}
	for _, edge := range plan.Edges {
		if edge.From < 0 || edge.To < 0 || edge.From >= len(items) || edge.To >= len(items) || edge.From >= edge.To {
			return fmt.Errorf("cg plan contains invalid edge %d->%d", edge.From, edge.To)
		}
		code := cgEdgeCode(edge.From, edge.To)
		if planEdges[code] {
			return fmt.Errorf("cg plan contains duplicate edge %d->%d", edge.From, edge.To)
		}
		planEdges[code] = true
		if edges[edge.From] == nil {
			edges[edge.From] = map[int]bool{}
		}
		edges[edge.From][edge.To] = true
	}
	if len(planEdges) != plan.Metrics.EdgeCount {
		return fmt.Errorf("cg plan edge metric mismatch: plan=%d metrics=%d", len(planEdges), plan.Metrics.EdgeCount)
	}
	if plan.Metrics.TransactionCount != len(items) || plan.Metrics.PairChecks != len(items)*(len(items)-1)/2 {
		return fmt.Errorf("cg plan construction metric mismatch")
	}
	if plan.Metrics.PlanningWorkerCount < 1 {
		return fmt.Errorf("cg plan missing paper parallel planning worker evidence")
	}

	byKey := map[string]*cgAddressAccess{}
	for index, item := range items {
		for _, key := range item.ReadKeys {
			entry := byKey[key]
			if entry == nil {
				entry = &cgAddressAccess{}
				byKey[key] = entry
			}
			entry.readers = append(entry.readers, index)
		}
		for _, key := range item.WriteKeys {
			entry := byKey[key]
			if entry == nil {
				entry = &cgAddressAccess{}
				byKey[key] = entry
			}
			entry.writers = append(entry.writers, index)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(keys) && len(keys) > 0 {
		workerCount = len(keys)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	perKey := make([][]uint64, len(keys))
	var nextKey int64 = -1
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ki := int(atomic.AddInt64(&nextKey, 1))
				if ki >= len(keys) {
					return
				}
				entry := byKey[keys[ki]]
				local := map[uint64]bool{}
				for _, writer := range entry.writers {
					for _, reader := range entry.readers {
						if writer == reader {
							continue
						}
						from, to := cgOrderedPair(writer, reader)
						local[cgEdgeCode(from, to)] = true
					}
					for _, otherWriter := range entry.writers {
						if writer == otherWriter {
							continue
						}
						from, to := cgOrderedPair(writer, otherWriter)
						local[cgEdgeCode(from, to)] = true
					}
				}
				row := make([]uint64, 0, len(local))
				for code := range local {
					row = append(row, code)
				}
				perKey[ki] = row
			}
		}()
	}
	wg.Wait()
	expected := map[uint64]bool{}
	for _, row := range perKey {
		for _, code := range row {
			expected[code] = true
		}
	}
	if len(expected) != len(planEdges) {
		return fmt.Errorf("cg smart validator edge count mismatch: expected=%d plan=%d", len(expected), len(planEdges))
	}
	for code := range expected {
		if !planEdges[code] {
			from, to := cgDecodeEdge(code)
			return fmt.Errorf("cg smart validator missing dependency edge %d->%d", from, to)
		}
	}
	for code := range planEdges {
		if !expected[code] {
			from, to := cgDecodeEdge(code)
			return fmt.Errorf("cg smart validator extra dependency edge %d->%d", from, to)
		}
	}

	waves, err := literatureWavesFromEdges(items, edges)
	if err != nil {
		return err
	}
	if !cgSameWaves(waves, plan.Waves) {
		return fmt.Errorf("cg smart validator wave schedule mismatch")
	}
	serialization := make([]string, 0, len(items))
	maxWidth := 0
	for _, wave := range waves {
		serialization = append(serialization, wave...)
		if len(wave) > maxWidth {
			maxWidth = len(wave)
		}
	}
	if !sameStringList(serialization, plan.SerializationOrder) || plan.Metrics.WaveCount != len(waves) || plan.Metrics.MaximumWaveWidth != maxWidth {
		return fmt.Errorf("cg smart validator derived schedule metrics mismatch")
	}
	return nil
}

func cgOrderedPair(left, right int) (int, int) {
	if left < right {
		return left, right
	}
	return right, left
}

func cgEdgeCode(from, to int) uint64 {
	return uint64(uint32(from))<<32 | uint64(uint32(to))
}

func cgDecodeEdge(code uint64) (int, int) {
	return int(uint32(code >> 32)), int(uint32(code))
}

func cgSameWaves(left, right [][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameStringList(left[index], right[index]) {
			return false
		}
	}
	return true
}
