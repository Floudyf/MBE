package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func dependencyIndexV653Tx(id, sender string, nonce, ordinal, sequence uint64, mode tx.AccessMode, key string, deps ...tx.StateVersionDependency) tx.SignedTransaction {
	receiver := "receiver-" + id
	item := tx.SignedTransaction{
		TxID: id, Sender: sender, Receiver: receiver, Nonce: nonce,
		AccessList: []tx.AccessItem{{Key: key, Mode: mode, UpdateSemantics: "set"}},
	}
	routing := tx.ExecutionRoutingMetadata{
		SenderID: sender, ReceiverID: receiver, RoutingOrdinal: ordinal,
		ExecutionShard: "s0", RoutingReason: "v653_test_fixture", RoutePlanDigest: "v653-test-plan",
		RouteBatchSequence: sequence, RouteBatchTransactionCount: 1, RouteBatchShardTransactionCount: 1,
		StateVersions: append([]tx.StateVersionDependency(nil), deps...),
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

func TestMetaTrackDependencyIndexV653ReusesPreviouslyIndexedTransactions(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessReadWrite, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2}),
		dependencyIndexV653Tx("c", "carol", 1, 3, 3, tx.AccessRead, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 2}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	first := index.stats()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	second := index.stats()
	if first.IndexedTransactionCount != 3 || first.NewTransactionCount != 3 {
		t.Fatalf("unexpected first stats: %#v", first)
	}
	if second.IndexedTransactionCount != 3 || second.NewTransactionCount != 3 || second.ReusedTransactionCount != 3 {
		t.Fatalf("repeated candidate was recomputed instead of reused: %#v", second)
	}
	preds, err := index.activeProjectionPredecessors(items)
	if err != nil {
		t.Fatal(err)
	}
	if len(preds[1]) != 0 || len(preds[2]) != 1 || preds[2][0] != 1 || len(preds[3]) != 1 || preds[3][0] != 2 {
		t.Fatalf("unexpected projection predecessors: %#v", preds)
	}
}

func TestMetaTrackDependencyIndexV653MatchesRAWNonceAndExactExecutionBarriers(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("w", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("r", "bob", 1, 2, 2, tx.AccessRead, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1}),
		dependencyIndexV653Tx("n", "alice", 2, 3, 3, tx.AccessWrite, "z", tx.StateVersionDependency{Key: "z", ProducedVersion: 3}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if !index.predecessors["r"]["w"] {
		t.Fatal("RAW/exact predecessor w->r missing")
	}
	if !index.predecessors["n"]["w"] {
		t.Fatal("same-sender predecessor w->n missing")
	}
	if index.txByID["r"].DepthL < 2 || index.txByID["n"].DepthL < 2 {
		t.Fatalf("forward depth was not incrementally reused: r=%d n=%d", index.txByID["r"].DepthL, index.txByID["n"].DepthL)
	}
}

func TestMetaTrackDependencyIndexV653KeepsBlindWAWOrderingOnly(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2}),
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.predecessors["b"]["a"] {
		t.Fatal("blind WAW was incorrectly promoted to execution dependency")
	}
	if len(index.projectionPred[2]) != 0 {
		t.Fatalf("blind WAW incorrectly blocked projection 2: %#v", index.projectionPred[2])
	}
}

func TestMetaTrackDependencyIndexV653RebuildsOnlyForLateLowerOrdinal(t *testing.T) {
	index := newMetaTrackDependencyIndexV653()
	late := dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessRead, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1})
	if err := index.ensure([]tx.SignedTransaction{late}); err != nil {
		t.Fatal(err)
	}
	early := dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1})
	if err := index.ensure([]tx.SignedTransaction{early, late}); err != nil {
		t.Fatal(err)
	}
	stats := index.stats()
	if stats.RebuildCount != 1 || stats.IndexedTransactionCount != 2 {
		t.Fatalf("unexpected rebuild stats: %#v", stats)
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if !index.predecessors["b"]["a"] {
		t.Fatal("late lower ordinal rebuild did not recover exact/RAW edge")
	}
}

func TestMetaTrackDependencyIndexV653ReusesDeferredTransactionsAcrossSuccessiveProposals(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 10)
	for i := 1; i <= 10; i++ {
		deps := []tx.StateVersionDependency{{Key: "chain", ProducedVersion: uint64(i)}}
		mode := tx.AccessWrite
		if i > 1 {
			mode = tx.AccessReadWrite
			deps[0].RequiredVersion = uint64(i - 1)
		}
		items = append(items, dependencyIndexV653Tx(
			string(rune('a'+i-1)), "sender-"+string(rune('a'+i-1)), 1,
			uint64(i), uint64(i), mode, "chain", deps...,
		))
	}
	pool := &mempool.Mempool{}
	remaining := append([]tx.SignedTransaction(nil), items...)
	for round := 0; round < 10; round++ {
		selected, deferred, _, err := selectMetaTrackDependencyClosedPBFTProjectionsIndexedV653(remaining, 1000, "s0", pool)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if len(selected) != 1 {
			t.Fatalf("round %d selected=%d, want exactly one dependency-ready projection", round, len(selected))
		}
		remaining = deferred
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining=%d", len(remaining))
	}
	stats := metaTrackDependencyIndexForPoolV653(pool).stats()
	if stats.NewTransactionCount != 10 || stats.IndexedTransactionCount != 10 {
		t.Fatalf("transactions were re-indexed instead of reused: %#v", stats)
	}
	if stats.ReusedTransactionCount == 0 {
		t.Fatalf("successive proposals did not hit cached dependency facts: %#v", stats)
	}
}

func TestMetaTrackDependencyIndexV653FailedIngestRestoresPreviousCache(t *testing.T) {
	index := newMetaTrackDependencyIndexV653()
	good := dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1})
	if err := index.ensure([]tx.SignedTransaction{good}); err != nil {
		t.Fatal(err)
	}
	bad := dependencyIndexV653Tx("bad", "bob", 1, 2, 2, tx.AccessReadWrite, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2})
	// Deliberately corrupt the otherwise-valid fixture *after* routing-contract
	// validation so this test reaches the index transactional rollback path.
	bad.ExecutionRouting.StateVersions[0].RequiredVersion = 2
	if err := index.ensure([]tx.SignedTransaction{bad}); err == nil {
		t.Fatal("future/non-monotonic dependency should fail closed")
	}
	stats := index.stats()
	if stats.IndexedTransactionCount != 1 || stats.NewTransactionCount != 1 {
		t.Fatalf("failed ingest polluted prior cache: %#v", stats)
	}
	if err := index.ensure([]tx.SignedTransaction{good}); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackDependencyIndexV653MatchesV652CombinedProjectionOracle(t *testing.T) {
	items := []tx.SignedTransaction{
		dependencyIndexV653Tx("a", "alice", 1, 1, 1, tx.AccessWrite, "k", tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		dependencyIndexV653Tx("b", "bob", 1, 2, 2, tx.AccessReadWrite, "k", tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2}),
		dependencyIndexV653Tx("c", "alice", 2, 3, 3, tx.AccessWrite, "z", tx.StateVersionDependency{Key: "z", ProducedVersion: 3}),
		dependencyIndexV653Tx("d", "carol", 1, 4, 4, tx.AccessRead, "z", tx.StateVersionDependency{Key: "z", RequiredVersion: 3}),
	}
	frontier, err := buildMetaTrackProjectionFrontierV612Plan(items)
	if err != nil {
		t.Fatal(err)
	}
	classification := dualTrackExecution{}.ClassifyBatch(BatchClassificationInput{Transactions: items})
	oracle := map[uint64]map[uint64]bool{}
	for _, item := range items {
		oracle[item.ExecutionRouting.RouteBatchSequence] = map[uint64]bool{}
	}
	for consumer, preds := range frontier.PredecessorsBySequence {
		for _, pred := range preds {
			oracle[consumer][pred] = true
		}
	}
	for consumerTxID, preds := range classification.Dependencies {
		consumerSeq := frontier.SequenceByTxID[consumerTxID]
		for _, predTxID := range preds {
			predSeq := frontier.SequenceByTxID[predTxID]
			if predSeq != consumerSeq {
				oracle[consumerSeq][predSeq] = true
			}
		}
	}
	index := newMetaTrackDependencyIndexV653()
	if err := index.ensure(items); err != nil {
		t.Fatal(err)
	}
	got, err := index.activeProjectionPredecessors(items)
	if err != nil {
		t.Fatal(err)
	}
	for seq, expected := range oracle {
		if len(got[seq]) != len(expected) {
			t.Fatalf("sequence %d predecessor count mismatch got=%v expected=%v", seq, got[seq], expected)
		}
		for _, pred := range got[seq] {
			if !expected[pred] {
				t.Fatalf("sequence %d unexpected predecessor %d; oracle=%v", seq, pred, expected)
			}
		}
	}
}
