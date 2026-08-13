package v5

import (
	"context"
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	bsxExecutionID     = "bsx_execution"
	bsxSchedulerID     = "bsx_scheduler"
	bsxBlockExecutorID = "bsx_block_executor"
	bsxPlanAlgorithmID = "bsx_homogeneous_conflict_graph_coloring_dsatur_v1"
)

type bsxExecution struct{ basicPlugin }
type bsxScheduler struct{ basicPlugin }
type bsxBlockExecutor struct{ basicPlugin }

func (p bsxExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "bsx", Reason: "undirected_conflict_graph_coloring"}
}
func (p bsxScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p bsxScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}
func (p bsxScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildBSXPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: bsxPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p bsxScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("bsx execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, bsxPlanAlgorithmID)
	if err != nil {
		return err
	}
	return literatureVerifyPlan(block, plan, bsxPlanAlgorithmID, buildBSXPlan)
}
func (p bsxBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("bsx execution plan missing")
	}
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, bsxPlanAlgorithmID)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if err := literatureVerifyPlan(input.Block, plan, bsxPlanAlgorithmID, buildBSXPlan); err != nil {
		return BlockExecutionResult{}, err
	}
	return executeBSXPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, configuredWorkerCount(p.config, input.WorkerCount))
}

func buildBSXPlan(block realblock.Block) (literatureGraphPlan, error) {
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	neighbors := make([]map[int]bool, len(items))
	for i := range neighbors {
		neighbors[i] = map[int]bool{}
	}
	edgeCount, pairChecks := 0, 0
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			pairChecks++
			if !literatureConflicts(items[i], items[j]) {
				continue
			}
			neighbors[i][j] = true
			neighbors[j][i] = true
			edgeCount++
		}
	}
	colors := make([]int, len(items))
	for i := range colors {
		colors[i] = -1
	}
	uncolored := len(items)
	for uncolored > 0 {
		best := -1
		bestSat, bestDegree := -1, -1
		for vertex := range items {
			if colors[vertex] >= 0 {
				continue
			}
			satSet := map[int]bool{}
			for neighbor := range neighbors[vertex] {
				if colors[neighbor] >= 0 {
					satSet[colors[neighbor]] = true
				}
			}
			sat, degree := len(satSet), len(neighbors[vertex])
			if best < 0 || sat > bestSat || (sat == bestSat && degree > bestDegree) || (sat == bestSat && degree == bestDegree && items[vertex].Ordinal < items[best].Ordinal) {
				best, bestSat, bestDegree = vertex, sat, degree
			}
		}
		used := map[int]bool{}
		for neighbor := range neighbors[best] {
			if colors[neighbor] >= 0 {
				used[colors[neighbor]] = true
			}
		}
		color := 0
		for used[color] {
			color++
		}
		colors[best] = color
		uncolored--
	}
	maxColor := -1
	for _, color := range colors {
		if color > maxColor {
			maxColor = color
		}
	}
	waves := make([][]string, maxColor+1)
	for color := 0; color <= maxColor; color++ {
		indexes := []int{}
		for index, assigned := range colors {
			if assigned == color {
				indexes = append(indexes, index)
			}
		}
		sort.Ints(indexes)
		for _, index := range indexes {
			waves[color] = append(waves[color], items[index].TxID)
		}
	}
	plan := literatureGraphPlan{AlgorithmID: bsxPlanAlgorithmID, BlockHeight: block.Height, DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount, Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: edgeCount, PairChecks: pairChecks, ColorCount: maxColor + 1}, Waves: waves}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}
