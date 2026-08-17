package v5

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func fairnessTx(id string, accesses ...tx.AccessItem) tx.SignedTransaction {
	return tx.SignedTransaction{TxID: id, AccessList: accesses, AccessListSchema: "fairness_fixture_v1", AccessListSource: "fairness_fixture"}
}

func TestFixedBlockWorkerPoolReusesBoundedWorkersAcrossBarriers(t *testing.T) {
	ctx := context.Background()
	pool := newFixedBlockWorkerPool(ctx, 2)
	defer pool.Close()
	var executed int64
	for round := 0; round < 3; round++ {
		tasks := make([]func(), 6)
		for i := range tasks {
			tasks[i] = func() {
				time.Sleep(time.Millisecond)
				atomic.AddInt64(&executed, 1)
			}
		}
		maximum, err := pool.Run(tasks)
		if err != nil {
			t.Fatal(err)
		}
		if maximum < 1 || maximum > 2 {
			t.Fatalf("unexpected maximum parallelism %d", maximum)
		}
	}
	if got := atomic.LoadInt64(&executed); got != 18 {
		t.Fatalf("executed=%d want=18", got)
	}
}

func TestPreverifiedLiteratureProjectionAcceptsExactBSXPlanAndRejectsTamper(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 1, TxList: []tx.SignedTransaction{
		fairnessTx("T1", tx.AccessItem{Key: "a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}),
		fairnessTx("T2", tx.AccessItem{Key: "b", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}),
	}}
	plan, err := buildBSXPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: bsxPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	if err := verifyPreverifiedLiteratureGraphProjection(block, plan, bsxPlanAlgorithmID); err != nil {
		t.Fatalf("exact preverified projection rejected: %v", err)
	}
	tampered := plan
	tampered.CandidateTransactionIDs = append([]string(nil), plan.CandidateTransactionIDs...)
	tampered.CandidateTransactionIDs[0] = "tampered"
	if err := verifyPreverifiedLiteratureGraphProjection(block, tampered, bsxPlanAlgorithmID); err == nil {
		t.Fatal("tampered candidate projection unexpectedly accepted")
	}
}

func TestBlockExecutionTimingFieldsKeepExecutorAndDBApplySeparate(t *testing.T) {
	fields := blockExecutionTimingFields(BlockExecutionResult{
		BlockExecutionMS:       101,
		TransactionExecutionMS: 23,
		DeterministicApplyMS:   7,
		StateDBApplyMS:         41,
		StateCommitmentMS:      13,
	})
	if got := fields["deterministic_materialization_ms"]; got != int64(7) {
		t.Fatalf("deterministic materialization=%v want=7", got)
	}
	if got := fields["state_db_apply_ms"]; got != int64(41) {
		t.Fatalf("state db apply=%v want=41", got)
	}
	if got := fields["deterministic_apply_ms"]; got != int64(7) {
		t.Fatalf("legacy deterministic apply alias=%v want=7", got)
	}
}

type fairnessCountingPlanner struct {
	basicPlugin
	verifyCount *int32
}

func (p fairnessCountingPlanner) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}

func (p fairnessCountingPlanner) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}

func (p fairnessCountingPlanner) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	return ConsensusExecutionPlanningResult{Block: block}, nil
}

func (p fairnessCountingPlanner) VerifyBlockPlan(realblock.Block) error {
	atomic.AddInt32(p.verifyCount, 1)
	return nil
}

func TestConsensusPlanSemanticVerificationIsReusedForExactImmutableBlock(t *testing.T) {
	var verifyCount int32
	planner := fairnessCountingPlanner{
		basicPlugin: makeBasic("scheduler", "fairness_counting_planner", map[string]any{}),
		verifyCount: &verifyCount,
	}
	runtime := &NodeRuntime{
		plugins:                RuntimePlugins{Scheduler: planner},
		verifiedExecutionPlans: map[string]verifiedExecutionPlanRecord{},
	}
	block := realblock.Block{ShardID: "s0", Height: 1}
	payload := []byte(`{"fixture":"plan"}`)
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   "fairness_fixture_plan_v1",
		PayloadDigest: stableTextDigest(string(payload)),
		PlanDigest:    stableTextDigest("fairness-plan"),
		Payload:       payload,
	}
	realblock.AssignHash(&block)
	if err := runtime.verifyExecutionPlanEnvelope(block); err != nil {
		t.Fatal(err)
	}
	if err := runtime.verifyExecutionPlanEnvelope(block); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&verifyCount); got != 1 {
		t.Fatalf("semantic verify count=%d want=1", got)
	}
}
