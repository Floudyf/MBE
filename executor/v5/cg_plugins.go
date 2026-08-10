package v5

import (
	"context"
	"fmt"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	cgExecutionID     = "cg_execution"
	cgSchedulerID     = "cg_scheduler"
	cgBlockExecutorID = "cg_block_executor"
	cgPlanAlgorithmID = "cg_full_pairwise_dependency_dag_v1"
)

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
	plan, err := buildCGPlan(block)
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
	return literatureVerifyPlan(block, plan, cgPlanAlgorithmID, buildCGPlan)
}
func (p cgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("cg execution plan missing")
	}
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if err := literatureVerifyPlan(input.Block, plan, cgPlanAlgorithmID, buildCGPlan); err != nil {
		return BlockExecutionResult{}, err
	}
	return executeLiteratureGraphPlan(ctx, input.Block, input.BaseStateSnapshot, plan, configuredWorkerCount(p.config, input.WorkerCount), cgBlockExecutorID)
}

func buildCGPlan(block realblock.Block) (literatureGraphPlan, error) {
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	edges := map[int]map[int]bool{}
	pairChecks := 0
	edgeCount := 0
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			pairChecks++
			if !literatureConflicts(items[i], items[j]) {
				continue
			}
			if edges[i] == nil {
				edges[i] = map[int]bool{}
			}
			if !edges[i][j] {
				edges[i][j] = true
				edgeCount++
			}
		}
	}
	waves, err := literatureWavesFromEdges(items, edges)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	plan := literatureGraphPlan{AlgorithmID: cgPlanAlgorithmID, BlockHeight: block.Height, DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount, Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: edgeCount, PairChecks: pairChecks}, Waves: waves}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}
