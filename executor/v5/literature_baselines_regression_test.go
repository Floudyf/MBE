package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func graphFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 7, BlockHash: "graph-fixture", TxList: []tx.SignedTransaction{
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestCGCycleAwareReferenceDirectionAndACGPreserveAccessEvidence(t *testing.T) {
	block := graphFixture()
	cg, err := buildCGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	acg, err := buildACGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	// Classical CG uses read->writer direction and can therefore point backward
	// in block order. On this acyclic fixture t2 must precede t1.
	wantCG := [][]string{{"t2", "t3"}, {"t1"}}
	if stableDigest(cg.Waves) != stableDigest(wantCG) {
		t.Fatalf("cycle-aware CG schedule=%v want=%v", cg.Waves, wantCG)
	}
	if len(cg.AbortedTransactionIDs) != 0 {
		t.Fatalf("unexpected CG cycle aborts on acyclic fixture: %v", cg.AbortedTransactionIDs)
	}
	if len(acg.AbortedTransactionIDs) != 0 {
		t.Fatalf("unexpected Nezha HS aborts: %v", acg.AbortedTransactionIDs)
	}
	if cg.Metrics.PairChecks != 3 {
		t.Fatalf("CG must preserve full pairwise baseline cost, got %d", cg.Metrics.PairChecks)
	}
	if acg.Metrics.PairChecks != 0 {
		t.Fatalf("ACG must not report pairwise checks")
	}
	if cg.DeclaredAccessSetDigest == "" || cg.DeclaredReadKeyCount == 0 || cg.DeclaredWriteKeyCount == 0 {
		t.Fatalf("CG declared-access evidence missing: %+v", cg)
	}
	if cg.DeclaredAccessSetDigest != acg.DeclaredAccessSetDigest || cg.DeclaredReadKeyCount != acg.DeclaredReadKeyCount || cg.DeclaredWriteKeyCount != acg.DeclaredWriteKeyCount {
		t.Fatalf("CG/ACG declared-access evidence mismatch")
	}
}

func TestBSXColorsContainNoConflictingTransactions(t *testing.T) {
	block := graphFixture()
	plan, err := buildBSXPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	access, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]literatureTxAccess{}
	for _, item := range access {
		byID[item.TxID] = item
	}
	for _, wave := range plan.Waves {
		for i := 0; i < len(wave); i++ {
			for j := i + 1; j < len(wave); j++ {
				if literatureConflicts(byID[wave[i]], byID[wave[j]]) {
					t.Fatalf("BSX color contains conflict: %s %s", wave[i], wave[j])
				}
			}
		}
	}
	if plan.Metrics.ColorCount != len(plan.Waves) {
		t.Fatalf("color count mismatch")
	}
}

func TestLiteratureBaselinePluginsRegistered(t *testing.T) {
	registry := BuiltinRegistry()
	for _, item := range []struct{ category, id string }{{"execution", cgExecutionID}, {"scheduler", cgSchedulerID}, {"block_executor", cgBlockExecutorID}, {"execution", acgExecutionID}, {"scheduler", acgSchedulerID}, {"block_executor", acgBlockExecutorID}, {"execution", bsxExecutionID}, {"scheduler", bsxSchedulerID}, {"block_executor", bsxBlockExecutorID}} {
		if _, err := registry.Create(item.category, item.id, nil); err != nil {
			t.Fatalf("missing %s:%s: %v", item.category, item.id, err)
		}
	}
}

func TestLiteratureGraphExecutionCarriesDeclaredAccessAndBusinessEvidence(t *testing.T) {
	block := graphFixture()
	base := map[string]string{"s0::a": "seed", "s0::b": "old", "s0::c": "old"}
	serial := execution.NewSerialExecutor().ExecuteBlock(block, base)
	builders := []struct {
		name             string
		build            func(realblock.Block) (literatureGraphPlan, error)
		execute          func(context.Context, realblock.Block, map[string]string, literatureGraphPlan, int) (BlockExecutionResult, error)
		serialEquivalent bool
	}{
		{"cg", buildCGPlan, executeCGPlan, false},
		// Nezha ACG/HS owns an address-ranked order and may abort transactions;
		// its oracle is the consensus-bound HS plan, not CG/Serial input order.
		{"acg", buildACGPlan, executeACGPlan, false},
		{"bsx", buildBSXPlan, executeBSXPlan, true},
	}
	for _, tc := range builders {
		plan, err := tc.build(block)
		if err != nil {
			t.Fatalf("%s build: %v", tc.name, err)
		}
		got, err := tc.execute(context.Background(), block, base, plan, 4)
		if err != nil {
			t.Fatalf("%s execute: %v", tc.name, err)
		}
		if tc.serialEquivalent && got.ExecutionResult.StateRootAfter != serial.StateRootAfter {
			t.Fatalf("%s final state changed on serial-equivalent fixture: got=%s serial=%s", tc.name, got.ExecutionResult.StateRootAfter, serial.StateRootAfter)
		}
		if !tc.serialEquivalent {
			again, err := tc.execute(context.Background(), block, base, plan, 4)
			if err != nil {
				t.Fatalf("%s deterministic replay: %v", tc.name, err)
			}
			if got.ExecutionResult.StateRootAfter != again.ExecutionResult.StateRootAfter || got.ExecutionResult.ReceiptRoot != again.ExecutionResult.ReceiptRoot {
				t.Fatalf("%s consensus-bound execution is nondeterministic", tc.name)
			}
			if stableDigest(got.ExecutionResult.Plan.OrderedTransactionIDs) != stableDigest(plan.SerializationOrder) {
				t.Fatalf("%s execution order=%v plan=%v", tc.name, got.ExecutionResult.Plan.OrderedTransactionIDs, plan.SerializationOrder)
			}
		}
		if got.ExecutionResult.Plan.DeclaredAccessSetDigest == "" || got.ExecutionResult.Plan.DeclaredReadKeyCount == 0 || got.ExecutionResult.Plan.DeclaredWriteKeyCount == 0 {
			t.Fatalf("%s execution plan omitted declared-access evidence: %+v", tc.name, got.ExecutionResult.Plan)
		}
		if len(got.BusinessAttempts) != len(block.TxList) {
			t.Fatalf("%s business attempt evidence count=%d want=%d", tc.name, len(got.BusinessAttempts), len(block.TxList))
		}
		if got.BlockExecutionMS < got.TransactionExecutionMS || got.BlockExecutionMS < got.DeterministicApplyMS || got.BlockExecutionMS < got.StateCommitmentMS {
			t.Fatalf("%s block timing does not cover execution/apply/commitment timing", tc.name)
		}
		if got.StateRootVersion != state.CommitmentVersion {
			t.Fatalf("%s state root version=%q want=%q", tc.name, got.StateRootVersion, state.CommitmentVersion)
		}
	}
}
