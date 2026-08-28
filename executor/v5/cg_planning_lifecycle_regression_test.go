package v5

import (
	"context"
	"errors"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/tx"
)

func cgPlanningLifecycleFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 1, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessRead}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessRead}, {Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestCGContextPlanningPreservesExactPlan(t *testing.T) {
	scheduler := cgScheduler{}
	block := cgPlanningLifecycleFixture()
	legacy, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	progress := make([]consensusPlanningProgress, 0)
	contextual, err := scheduler.PlanBlockContext(context.Background(), block, func(item consensusPlanningProgress) {
		progress = append(progress, item)
	})
	if err != nil {
		t.Fatal(err)
	}
	left, err := literatureParsePlan(legacy.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		t.Fatal(err)
	}
	right, err := literatureParsePlan(contextual.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		t.Fatal(err)
	}
	if left.PlanDigest != right.PlanDigest || stableDigest(left.Edges) != stableDigest(right.Edges) || stableDigest(left.AbortedTransactionIDs) != stableDigest(right.AbortedTransactionIDs) || stableDigest(left.SerializationOrder) != stableDigest(right.SerializationOrder) || stableDigest(left.Waves) != stableDigest(right.Waves) {
		t.Fatalf("context lifecycle changed Nezha CG semantics: legacy=%+v contextual=%+v", left, right)
	}
	if len(progress) == 0 || progress[len(progress)-1].Phase != "complete" {
		t.Fatalf("expected real planning progress ending in complete, got %+v", progress)
	}
}

func TestCGContextPlanningHonorsCancellationWithoutFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (cgScheduler{}).PlanBlockContext(ctx, cgPlanningLifecycleFixture(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled exact planner returned %v, want context.Canceled", err)
	}
}

func TestProposalPlanningProgressOnlyAdvancesOnRealWork(t *testing.T) {
	r := &NodeRuntime{proposalPlanningInFlight: true, proposalPlanningGeneration: 7, proposalPlanningWorkUnits: 1}
	r.recordProposalPlanningProgress(7, consensusPlanningProgress{AlgorithmID: cgPlanAlgorithmID, Phase: "johnson_cycles", WorkUnits: 100, DetailCount: 4})
	if r.proposalPlanningWorkUnits != 100 || r.proposalPlanningDetailCount != 4 || r.proposalPlanningProgressAt.IsZero() || r.proposalPlanningPhase != "johnson_cycles" {
		t.Fatalf("planning progress not recorded: units=%d detail=%d phase=%s at=%v", r.proposalPlanningWorkUnits, r.proposalPlanningDetailCount, r.proposalPlanningPhase, r.proposalPlanningProgressAt)
	}
	first := r.proposalPlanningProgressAt
	r.recordProposalPlanningProgress(7, consensusPlanningProgress{AlgorithmID: cgPlanAlgorithmID, Phase: "johnson_cycles", WorkUnits: 100, DetailCount: 4})
	if !r.proposalPlanningProgressAt.Equal(first) {
		t.Fatal("non-advancing heartbeat must not manufacture planning progress")
	}
}

func TestStaleLeaderPlanningIsCancelledWithoutPBFTRuleChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &NodeRuntime{
		node:                         NodePlan{NodeID: "n0", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		consensus:                    pbft.NewState("n0", "s0", "n1", []string{"n0", "n1", "n2", "n3"}),
		proposalPlanningInFlight:     true,
		proposalPlanningView:         0,
		proposalPlanningCancel:       cancel,
		proposalPlanningGeneration:   1,
		proposalPlanningAlgorithmID:  cgPlanAlgorithmID,
		proposalPlanningCancelReason: "",
	}
	r.cancelStaleProposalPlanning()
	select {
	case <-ctx.Done():
		if r.proposalPlanningCancelReason == "" {
			t.Fatal("stale planning cancelled without diagnostic reason")
		}
	default:
		t.Fatal("stale leader planning was not cancelled")
	}
}
