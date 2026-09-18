package v5

import (
	"reflect"
	"sort"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func staticEdgeSignatures(edges []dualTrackStaticTopologyEdge) []string {
	if len(edges) == 0 {
		return nil
	}
	out := make([]string, 0, len(edges))
	for _, edge := range edges {
		out = append(out, edge.From+"->"+edge.To+":"+edge.Kind)
	}
	sort.Strings(out)
	return out
}

func staticAccess(id string, mode tx.AccessMode, semantics string) dualTrackConflictAccess {
	return dualTrackConflictAccess{
		TxID:   id,
		Item:   tx.SignedTransaction{TxID: id},
		Access: tx.AccessItem{Key: "k", Mode: mode, UpdateSemantics: semantics, Delta: 1},
	}
}

func TestDualTrackStaticTopologyTruthTable(t *testing.T) {
	cases := []struct {
		name      string
		leftMode  tx.AccessMode
		leftSem   string
		rightMode tx.AccessMode
		rightSem  string
		want      []string
		ambiguous bool
	}{
		{"read-read", tx.AccessRead, "validate", tx.AccessRead, "validate", nil, false},
		{"write-read", tx.AccessWrite, "set", tx.AccessRead, "validate", []string{"a->b:raw"}, false},
		{"read-write", tx.AccessRead, "validate", tx.AccessWrite, "set", []string{"b->a:raw"}, false},
		{"write-rw", tx.AccessWrite, "set", tx.AccessReadWrite, "rmw", []string{"a->b:raw"}, false},
		{"rw-write", tx.AccessReadWrite, "rmw", tx.AccessWrite, "set", []string{"b->a:raw"}, false},
		{"rw-rw", tx.AccessReadWrite, "rmw", tx.AccessReadWrite, "rmw", []string{"a->b:waw"}, false},
		{"write-write", tx.AccessWrite, "set", tx.AccessWrite, "set", []string{"a->b:waw"}, false},
		{"commutative-commutative", tx.AccessCommutativeDelta, "add", tx.AccessCommutativeDelta, "commutative_delta", nil, false},
		{"commutative-read", tx.AccessCommutativeDelta, "add", tx.AccessRead, "validate", []string{"a->b:raw"}, false},
		{"read-commutative", tx.AccessRead, "validate", tx.AccessCommutativeDelta, "add", []string{"b->a:raw"}, false},
		{"unknown-read", tx.AccessUnknown, "unknown", tx.AccessRead, "validate", []string{"a->b:ambiguous", "b->a:ambiguous"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			edges, ambiguous := dualTrackStaticTopologyPair(
				staticAccess("a", tc.leftMode, tc.leftSem),
				staticAccess("b", tc.rightMode, tc.rightSem),
			)
			got := staticEdgeSignatures(edges)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) || ambiguous != tc.ambiguous {
				t.Fatalf("static topology mismatch: edges=%v want=%v ambiguous=%v want=%v", got, want, ambiguous, tc.ambiguous)
			}
		})
	}
}

func TestDualTrackStaticSameKeyRWUsesCanonicalArbitration(t *testing.T) {
	left := staticAccess("a", tx.AccessReadWrite, "rmw")
	right := staticAccess("b", tx.AccessReadWrite, "rmw")
	firstEdges, firstAmbiguous := dualTrackStaticTopologyPair(left, right)
	secondEdges, secondAmbiguous := dualTrackStaticTopologyPair(right, left)
	if firstAmbiguous || secondAmbiguous {
		t.Fatalf("stable TxIDs should be enough to arbitrate same-key RW/RW")
	}
	want := []string{"a->b:waw"}
	if got := staticEdgeSignatures(firstEdges); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected forward arbitration: got=%v want=%v", got, want)
	}
	if got := staticEdgeSignatures(secondEdges); !reflect.DeepEqual(got, want) {
		t.Fatalf("argument order changed canonical RW/RW arbitration: got=%v want=%v", got, want)
	}

	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	a := tx.SignedTransaction{TxID: "a", Sender: "alice", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}}}
	b := tx.SignedTransaction{TxID: "b", Sender: "bob", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}}}
	first := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{a, b}})
	second := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{b, a}})
	for _, result := range []BatchClassificationResult{first, second} {
		if result.SCCCount != 0 {
			t.Fatalf("same-key RW/RW canonical arbitration manufactured an SCC: %#v", result)
		}
		for _, id := range []string{"a", "b"} {
			if result.Decisions[id].Track != "fast" {
				t.Fatalf("same-key RW/RW should remain Fast-eligible after stable serialization: id=%s result=%#v", id, result)
			}
		}
	}
	if first.ConflictEdgeCount != second.ConflictEdgeCount || first.ConflictEdgeCount != 1 {
		t.Fatalf("same-key RW/RW should produce one invariant classification edge: first=%d second=%d", first.ConflictEdgeCount, second.ConflictEdgeCount)
	}
}

func TestDualTrackStaticCrossKeyCycleFallsBackConservative(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{TxID: "t1", Sender: "alice", AccessList: []tx.AccessItem{
			{Key: "c", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
		{TxID: "t2", Sender: "bob", AccessList: []tx.AccessItem{
			{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
		{TxID: "t3", Sender: "carol", AccessList: []tx.AccessItem{
			{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if result.SCCCount != 1 {
		t.Fatalf("static cross-key dependency cycle should form one SCC: %#v", result)
	}
	for _, id := range []string{"t1", "t2", "t3"} {
		if result.Decisions[id].Track != "conservative" {
			t.Fatalf("cycle member must be Conservative: id=%s decision=%#v", id, result.Decisions[id])
		}
	}
}

func TestDualTrackStaticClassificationIsBatchPermutationInvariant(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	makeItems := func() []tx.SignedTransaction {
		return []tx.SignedTransaction{
			{TxID: "writer", Sender: "alice", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
			{TxID: "reader", Sender: "bob", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessRead, UpdateSemantics: "validate"}}},
			{TxID: "independent", Sender: "carol", AccessList: []tx.AccessItem{{Key: "z", Mode: tx.AccessRead, UpdateSemantics: "validate"}}},
		}
	}
	firstItems := makeItems()
	secondItems := []tx.SignedTransaction{firstItems[2], firstItems[1], firstItems[0]}
	first := execution.ClassifyBatch(BatchClassificationInput{Transactions: firstItems})
	second := execution.ClassifyBatch(BatchClassificationInput{Transactions: secondItems})
	for _, id := range []string{"writer", "reader", "independent"} {
		if first.Decisions[id] != second.Decisions[id] {
			t.Fatalf("batch permutation changed classification for %s: first=%#v second=%#v", id, first.Decisions[id], second.Decisions[id])
		}
	}
	if first.SCCCount != second.SCCCount || first.AmbiguousConflictPairCount != second.AmbiguousConflictPairCount || first.ConflictEdgeCount != second.ConflictEdgeCount {
		t.Fatalf("batch permutation changed static topology evidence: first=%#v second=%#v", first, second)
	}
}

func TestDualTrackStaticClassificationIgnoresExactStateVersionMutation(t *testing.T) {
	makeItems := func(leftRequired, leftProduced, rightRequired, rightProduced uint64) []tx.SignedTransaction {
		return []tx.SignedTransaction{
			{
				TxID: "left", Sender: "alice",
				AccessList:             []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
				SchedulingAccessSchema: "alien_worlds_layered_v2_scheduling_v1",
				SchedulingAccessList:   []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
				ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{
					Key: "k", RequiredVersion: leftRequired, ProducedVersion: leftProduced,
				}}},
			},
			{
				TxID: "right", Sender: "bob",
				AccessList:             []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
				SchedulingAccessSchema: "alien_worlds_layered_v2_scheduling_v1",
				SchedulingAccessList:   []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
				ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{
					Key: "k", RequiredVersion: rightRequired, ProducedVersion: rightProduced,
				}}},
			},
		}
	}
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	first := execution.ClassifyBatch(BatchClassificationInput{Transactions: makeItems(0, 1, 1, 2)})
	second := execution.ClassifyBatch(BatchClassificationInput{Transactions: makeItems(900, 901, 3, 4)})
	if !reflect.DeepEqual(first.Decisions, second.Decisions) || first.SCCCount != second.SCCCount || first.ConflictEdgeCount != second.ConflictEdgeCount {
		t.Fatalf("exact StateVersion mutation changed static classification: first=%#v second=%#v", first, second)
	}
	if first.SCCCount != 0 || first.Decisions["left"].Track != "fast" || first.Decisions["right"].Track != "fast" {
		t.Fatalf("same-key RW/RW must use stable canonical arbitration independent of StateVersion: %#v", first)
	}
}

func TestDualTrackStaticPureWAWAndCommutativePairsRemainTopoSafe(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	waw := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{
		{TxID: "a", Sender: "alice", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "b", Sender: "bob", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}})
	if waw.SCCCount != 0 || waw.Decisions["a"].Track != "fast" || waw.Decisions["b"].Track != "fast" {
		t.Fatalf("pure WAW should use deterministic arbitration without a false SCC: %#v", waw)
	}
	commutative := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{
		{TxID: "c1", Sender: "alice", AccessList: []tx.AccessItem{{Key: "counter", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "c2", Sender: "bob", AccessList: []tx.AccessItem{{Key: "counter", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 2}}},
	}})
	if commutative.SCCCount != 0 || commutative.ConflictEdgeCount != 0 || commutative.Decisions["c1"].Track != "fast" || commutative.Decisions["c2"].Track != "fast" {
		t.Fatalf("compatible commutative deltas should not create an ordinary TopoSafe edge: %#v", commutative)
	}
}

func TestDualTrackStaticCanonicalArbitrationDoesNotUseBatchFallbackID(t *testing.T) {
	left := dualTrackConflictAccess{
		TxID:   "tx-0",
		Item:   tx.SignedTransaction{},
		Access: tx.AccessItem{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
	}
	right := dualTrackConflictAccess{
		TxID:   "tx-1",
		Item:   tx.SignedTransaction{},
		Access: tx.AccessItem{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
	}
	edges, ambiguous := dualTrackStaticTopologyPair(left, right)
	want := []string{"tx-0->tx-1:ambiguous", "tx-1->tx-0:ambiguous"}
	if !ambiguous || !reflect.DeepEqual(staticEdgeSignatures(edges), want) {
		t.Fatalf("classifier fallback ids must not become canonical TopoSafe evidence: edges=%v ambiguous=%v", staticEdgeSignatures(edges), ambiguous)
	}
}

func TestDualTrackStaticNonceEvidenceUsesSignedNonceNotBatchOrder(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	low := tx.SignedTransaction{TxID: "low", Sender: "alice", Nonce: 1, AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	high := tx.SignedTransaction{TxID: "high", Sender: "alice", Nonce: 2, AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}}
	first := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{low, high}})
	second := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{high, low}})
	for _, result := range []BatchClassificationResult{first, second} {
		if result.SCCCount != 1 || result.Decisions["low"].Track != "conservative" || result.Decisions["high"].Track != "conservative" {
			t.Fatalf("nonce edge low->high plus static write->read high->low must form a stable SCC: %#v", result)
		}
	}
}
