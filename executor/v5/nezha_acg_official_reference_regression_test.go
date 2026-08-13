package v5

import (
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestNezhaACGOfficialReferenceGoldenOrdering(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 7, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessRead}, {Key: "c", Mode: tx.AccessWrite}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessWrite}}},
	}}
	first, err := buildACGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildACGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"t2"}, {"t0", "t3"}, {"t1"}}
	if !reflect.DeepEqual(first.Waves, want) {
		t.Fatalf("Nezha official-reference HS waves=%v want=%v", first.Waves, want)
	}
	if len(first.AbortedTransactionIDs) != 0 {
		t.Fatalf("unexpected HS aborts: %v", first.AbortedTransactionIDs)
	}
	if first.PlanDigest != second.PlanDigest || !reflect.DeepEqual(first.Waves, second.Waves) {
		t.Fatalf("Nezha official-reference plan is nondeterministic: first=%+v second=%+v", first, second)
	}
	if first.AlgorithmID != "nezha_acg_hs_official_reference_v1" {
		t.Fatalf("algorithm=%q", first.AlgorithmID)
	}
	assertNezhaPlanCoverage(t, first)
}

func TestNezhaACGOfficialReferenceCarriesHSAborts(t *testing.T) {
	// This is a compact cyclic address-dependency case. The translated
	// CGCL-codes/Nezha CreateGraph -> QueuesSort -> DeSS procedure deterministically
	// aborts x and y, while z remains schedulable.
	block := realblock.Block{ShardID: "s0", Height: 8, TxList: []tx.SignedTransaction{
		{TxID: "x", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite}}},
		{TxID: "y", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite}}},
		{TxID: "z", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessRead}, {Key: "a", Mode: tx.AccessWrite}}},
	}}
	plan, err := buildACGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.Waves, [][]string{{"z"}}) {
		t.Fatalf("waves=%v want=[[z]]", plan.Waves)
	}
	if !reflect.DeepEqual(plan.AbortedTransactionIDs, []string{"x", "y"}) {
		t.Fatalf("aborted=%v want=[x y]", plan.AbortedTransactionIDs)
	}
	if plan.Metrics.AbortCount != 2 {
		t.Fatalf("abort_count=%d want=2", plan.Metrics.AbortCount)
	}
	assertNezhaPlanCoverage(t, plan)
}

func assertNezhaPlanCoverage(t *testing.T, plan literatureGraphPlan) {
	t.Helper()
	seen := map[string]bool{}
	for _, wave := range plan.Waves {
		for _, id := range wave {
			if seen[id] {
				t.Fatalf("duplicate scheduled transaction %q", id)
			}
			seen[id] = true
		}
	}
	for _, id := range plan.AbortedTransactionIDs {
		if seen[id] {
			t.Fatalf("transaction %q appears in both waves and abort set", id)
		}
		seen[id] = true
	}
	if len(seen) != len(plan.CandidateTransactionIDs) {
		t.Fatalf("plan coverage=%d candidates=%d plan=%+v", len(seen), len(plan.CandidateTransactionIDs), plan)
	}
	for _, id := range plan.CandidateTransactionIDs {
		if !seen[id] {
			t.Fatalf("candidate %q missing from waves/abort set", id)
		}
	}
}
