package v5

import (
	"context"
	"fmt"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	acgExecutionID     = "acg_execution"
	acgSchedulerID     = "acg_scheduler"
	acgBlockExecutorID = "acg_block_executor"
	acgPlanAlgorithmID = "nezha_acg_hs_paper_description_v1"
)

type acgExecution struct{ basicPlugin }
type acgScheduler struct{ basicPlugin }
type acgBlockExecutor struct{ basicPlugin }

func (p acgExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "acg", Reason: "address_conflict_graph_hierarchy"}
}
func (p acgScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p acgScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}
func (p acgScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildACGPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: acgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p acgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("acg execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, acgPlanAlgorithmID)
	if err != nil {
		return err
	}
	return literatureVerifyPlan(block, plan, acgPlanAlgorithmID, buildACGPlan)
}
func (p acgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("acg execution plan missing")
	}
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, acgPlanAlgorithmID)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if err := literatureVerifyPlan(input.Block, plan, acgPlanAlgorithmID, buildACGPlan); err != nil {
		return BlockExecutionResult{}, err
	}
	return executeLiteratureGraphPlan(ctx, input.Block, input.BaseStateSnapshot, plan, configuredWorkerCount(p.config, input.WorkerCount), acgBlockExecutorID)
}

// buildACGPlan indexes accesses by address and emits the same original-order
// conflict precedence as the full CG without n(n-1)/2 pair checks.  This is a
// conservative paper-description reimplementation because the Nezha authors did
// not publish source code with the ACG/HS implementation used by the paper.
func buildACGPlan(block realblock.Block) (literatureGraphPlan, error) {
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	type frontier struct {
		writers []int
		readers []int
	}
	byKey := map[string]*frontier{}
	edges := map[int]map[int]bool{}
	edgeCount := 0
	add := func(from, to int) {
		if from == to {
			return
		}
		if edges[from] == nil {
			edges[from] = map[int]bool{}
		}
		if !edges[from][to] {
			edges[from][to] = true
			edgeCount++
		}
	}
	for index, item := range items {
		readSet, writeSet := literatureStringSet(item.ReadKeys), literatureStringSet(item.WriteKeys)
		keys := map[string]bool{}
		for key := range readSet {
			keys[key] = true
		}
		for key := range writeSet {
			keys[key] = true
		}
		for key := range keys {
			f := byKey[key]
			if f == nil {
				f = &frontier{}
				byKey[key] = f
			}
			if writeSet[key] {
				for _, prior := range f.writers {
					add(prior, index)
				}
				for _, prior := range f.readers {
					add(prior, index)
				}
				f.writers = append(f.writers, index)
				f.readers = nil
			} else if readSet[key] {
				for _, prior := range f.writers {
					add(prior, index)
				}
				f.readers = append(f.readers, index)
			}
		}
	}
	waves, err := literatureWavesFromEdges(items, edges)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	plan := literatureGraphPlan{AlgorithmID: acgPlanAlgorithmID, BlockHeight: block.Height, DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount, Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: edgeCount}, Waves: waves}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}
