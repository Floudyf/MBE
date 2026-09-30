package v5

import (
	"sort"
	"testing"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func projectionV655IDs(items []tx.SignedTransaction) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, txIdentifier(item))
	}
	return out
}

func projectionV655Tx(id, sender string, nonce, ordinal, sequence uint64, accesses []tx.AccessItem, deps ...tx.StateVersionDependency) tx.SignedTransaction {
	receiver := "receiver-" + id
	canonicalAccesses := append([]tx.AccessItem(nil), accesses...)
	sort.Slice(canonicalAccesses, func(i, j int) bool {
		if canonicalAccesses[i].Key != canonicalAccesses[j].Key {
			return canonicalAccesses[i].Key < canonicalAccesses[j].Key
		}
		return canonicalAccesses[i].Mode < canonicalAccesses[j].Mode
	})
	canonicalDeps := append([]tx.StateVersionDependency(nil), deps...)
	sort.Slice(canonicalDeps, func(i, j int) bool {
		if canonicalDeps[i].Key != canonicalDeps[j].Key {
			return canonicalDeps[i].Key < canonicalDeps[j].Key
		}
		if canonicalDeps[i].RequiredVersion != canonicalDeps[j].RequiredVersion {
			return canonicalDeps[i].RequiredVersion < canonicalDeps[j].RequiredVersion
		}
		return canonicalDeps[i].ProducedVersion < canonicalDeps[j].ProducedVersion
	})
	item := tx.SignedTransaction{TxID: id, Sender: sender, Receiver: receiver, Nonce: nonce, AccessList: canonicalAccesses}
	routing := tx.ExecutionRoutingMetadata{
		SenderID: sender, ReceiverID: receiver, RoutingOrdinal: ordinal,
		ExecutionShard: "s0", RoutingReason: "v655_test_fixture", RoutePlanDigest: "v655-test-plan",
		RouteBatchSequence: sequence, RouteBatchTransactionCount: 1, RouteBatchShardTransactionCount: 1,
		StateVersions: canonicalDeps,
	}
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		panic(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	if err := tx.ValidateExecutionRouting(item); err != nil {
		panic(err)
	}
	return item
}

func TestMetaTrackProjectionFrontierV655SkipsBlockedMiddleIndependentProjection(t *testing.T) {
	// seq1 -> seq2 is a true RAW/exact execution dependency; seq3 is unrelated.
	// v6.5.3 stops at seq2 and therefore emits only seq1. v6.5.5 must emit the
	// independent ready frontier {seq1, seq3}, preserving sequence order.
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessRead, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1}),
		dependencyIndexV653Tx("c", "carol", 1, 3, 3, tx.AccessWrite, "z", tx.StateVersionDependency{Key: "z", ProducedVersion: 3}),
	}
	legacyPool := &mempool.Mempool{}
	legacy, _, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV653(items, 1000, "s0", legacyPool)
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(legacy); len(got) != 1 || got[0] != "a" {
		t.Fatalf("v6.5.3 control changed unexpectedly: %v", got)
	}

	pool := &mempool.Mempool{}
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", pool)
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(selected); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("ready frontier=%v, want [a c]", got)
	}
	if got := projectionV655IDs(deferred); len(got) != 1 || got[0] != "b" {
		t.Fatalf("deferred=%v, want [b]", got)
	}
	if len(projections) != 2 || projections[0].Sequence != 1 || projections[1].Sequence != 3 {
		t.Fatalf("selected projection identity/order mismatch: %#v", projections)
	}
}

func TestMetaTrackProjectionFrontierV655OrderingOnlyWAWCanShareBlockWhenPredecessorSelected(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2}),
		dependencyIndexV653Tx("c", "carol", 1, 3, 3, tx.AccessWrite, "z", tx.StateVersionDependency{Key: "z", ProducedVersion: 3}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	if index.predecessors["b"]["a"] {
		index.mu.Unlock()
		t.Fatal("blind WAW was incorrectly promoted to transaction execution dependency")
	}
	if !index.projectionOrderPred[2][1] {
		index.mu.Unlock()
		t.Fatal("blind WAW ordering-only projection edge a->b missing")
	}
	index.mu.Unlock()

	selected, deferred, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", &mempool.Mempool{})
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(selected); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("same-block ordering predecessor should preserve parallel execution: %v", got)
	}
	if len(deferred) != 0 {
		t.Fatalf("unexpected deferred WAW projection: %v", projectionV655IDs(deferred))
	}
}

func TestMetaTrackProjectionFrontierV655OrderingOnlyEdgeCannotJumpDeferredPredecessor(t *testing.T) {
	// seq2 is execution-blocked by seq1 through x. seq3 only has an ordering
	// dependency on seq2 through k. seq3 must not jump over deferred seq2.
	items := []tx.SignedTransaction{
		projectionV655Tx("a", "alice", 1, 1, 1,
			[]tx.AccessItem{{Key: "x", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			tx.StateVersionDependency{Key: "x", ProducedVersion: 1}),
		projectionV655Tx("b", "bob", 1, 2, 2,
			[]tx.AccessItem{{Key: "x", Mode: tx.AccessRead, UpdateSemantics: "set"}, {Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			tx.StateVersionDependency{Key: "x", RequiredVersion: 1}, tx.StateVersionDependency{Key: "k", ProducedVersion: 2}),
		projectionV655Tx("c", "carol", 1, 3, 3,
			[]tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			tx.StateVersionDependency{Key: "k", RequiredVersion: 2, ProducedVersion: 3}),
		projectionV655Tx("d", "dave", 1, 4, 4,
			[]tx.AccessItem{{Key: "z", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			tx.StateVersionDependency{Key: "z", ProducedVersion: 4}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	execBlocked := index.projectionPred[2][1]
	orderBlocked := index.projectionOrderPred[3][2]
	index.mu.Unlock()
	if !execBlocked || !orderBlocked {
		t.Fatalf("expected seq1->seq2 execution and seq2->seq3 order edges: exec=%t order=%t", execBlocked, orderBlocked)
	}

	selected, deferred, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", &mempool.Mempool{})
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(selected); len(got) != 2 || got[0] != "a" || got[1] != "d" {
		t.Fatalf("ordering successor jumped deferred predecessor or independent root was lost: %v", got)
	}
	if got := projectionV655IDs(deferred); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("deferred closure mismatch: %v", got)
	}
}

func TestMetaTrackProjectionFrontierV655CommutativePairRemainsParallel(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessCommutativeDelta, "counter", tx.StateVersionDependency{Key: "counter", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessCommutativeDelta, "counter", tx.StateVersionDependency{Key: "counter", ProducedVersion: 2}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	orderBlocked := index.projectionOrderPred[2][1]
	execBlocked := index.projectionPred[2][1]
	index.mu.Unlock()
	if orderBlocked || execBlocked {
		t.Fatalf("compatible commutative pair was serialized: execution=%t ordering=%t", execBlocked, orderBlocked)
	}
	selected, _, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", &mempool.Mempool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 {
		t.Fatalf("commutative independent roots not co-selected: %v", projectionV655IDs(selected))
	}
}

func TestMetaTrackProjectionFrontierV655DeferredProjectionBecomesReadyAfterPredecessorCommit(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessRead, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1}),
		dependencyIndexV653Tx("c", "carol", 1, 3, 3, tx.AccessWrite, "z", tx.StateVersionDependency{Key: "z", ProducedVersion: 3}),
	}
	pool := &mempool.Mempool{}
	selected, deferred, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", pool)
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(selected); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("first frontier=%v", got)
	}
	second, rest, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(deferred, 1000, "s0", pool)
	if err != nil {
		t.Fatal(err)
	}
	if got := projectionV655IDs(second); len(got) != 1 || got[0] != "b" || len(rest) != 0 {
		t.Fatalf("deferred predecessor release failed: second=%v rest=%v", got, projectionV655IDs(rest))
	}
}

func TestMetaTrackProjectionFrontierV655UsesNoEmpiricalProjectionCap(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 12)
	for i := 1; i <= 12; i++ {
		id := string(rune('a' + i - 1))
		key := "key-" + id
		items = append(items, dependencyIndexV653Tx(id, "sender-"+id, 1, uint64(i), uint64(i), tx.AccessWrite, key, tx.StateVersionDependency{Key: key, ProducedVersion: uint64(i)}))
	}
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items, 1000, "s0", &mempool.Mempool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 12 || len(projections) != 12 || len(deferred) != 0 {
		t.Fatalf("independent frontier was arbitrarily capped: selected=%d projections=%d deferred=%d", len(selected), len(projections), len(deferred))
	}
}
