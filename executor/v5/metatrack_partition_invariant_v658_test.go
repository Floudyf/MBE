package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV658IgnoresRouteBatchAsConsensusAtom(t *testing.T) {
	a := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), 1, nil)
	b := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), 1, nil)
	c := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "c", 3, 3, "p3", "s0", 1, 1), 1, nil)
	selected, deferred, _, err := selectMetaTrackPartitionInvariantFrontierV658([]tx.SignedTransaction{a, b, c}, 3, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 3 || len(deferred) != 0 {
		t.Fatalf("selected=%d deferred=%d", len(selected), len(deferred))
	}
}

func TestMetaTrackV658CapacityStillBoundsPhysicalBlock(t *testing.T) {
	items := []tx.SignedTransaction{
		withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), 1, nil),
		withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), 1, nil),
		withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "c", 3, 3, "p3", "s0", 1, 1), 1, nil),
	}
	selected, deferred, _, err := selectMetaTrackPartitionInvariantFrontierV658(items, 2, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || len(deferred) != 1 {
		t.Fatalf("selected=%d deferred=%d", len(selected), len(deferred))
	}
}

func TestMetaTrackV658ValidatorUsesGlobalRoundNotBatchLocalDepth(t *testing.T) {
	a := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), 1, nil)
	b := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), 2, nil)
	routing := *b.ExecutionRouting
	routing.ConsensusExecutionPredecessorOrdinals = []uint64{1}
	routing.ConsensusExecutionDepth = 1 // deliberately batch-local and too small for a cross-batch chain
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(b, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	b.ExecutionRouting = &routing
	runtime := &NodeRuntime{}
	if _, err := runtime.validateMetaTrackPartitionInvariantFrontierV658(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a, b}}); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackV658RejectsNonDecreasingExactRound(t *testing.T) {
	item := signedMetaTrackBatchTestTx(t, "bad", 2, 2, "p2", "s0", 1, 1)
	item.StateKeys = []string{"remote"}
	item.AccessList = []tx.AccessItem{{Key: "remote", Mode: tx.AccessRead, UpdateSemantics: "validate"}}
	routing := *item.ExecutionRouting
	routing.ConsensusExecutionDepth = 1
	routing.ConsensusExecutionRound = 2
	routing.StateVersions = []tx.StateVersionDependency{{Key: "remote", RequiredVersion: 1, RequiredExecutionRound: 2}}
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	if _, _, _, err := selectMetaTrackPartitionInvariantFrontierV658([]tx.SignedTransaction{item}, 1, "s0", nil); err == nil {
		t.Fatal("non-decreasing exact round must fail")
	}
}
