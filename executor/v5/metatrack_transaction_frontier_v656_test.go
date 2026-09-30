package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func withMetaTrackV656Frontier(t *testing.T, item tx.SignedTransaction, execPred, orderPred []uint64, depth int) tx.SignedTransaction {
	t.Helper()
	routing := *item.ExecutionRouting
	routing.ConsensusExecutionPredecessorOrdinals = append([]uint64(nil), execPred...)
	routing.ConsensusOrderingPredecessorOrdinals = append([]uint64(nil), orderPred...)
	routing.ConsensusExecutionDepth = depth
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	return item
}

func TestMetaTrackV656SkipsCrossBatchExecutionChildButPacksIndependentRoot(t *testing.T) {
	a := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), nil, nil, 1)
	b := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), []uint64{1}, nil, 1)
	c := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "c", 3, 3, "p3", "s0", 1, 1), nil, nil, 1)
	selected, deferred, _, err := selectMetaTrackTransactionFrontierV656([]tx.SignedTransaction{a, b, c}, 10, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].TxID != "a" || selected[1].TxID != "c" {
		t.Fatalf("selected=%v", []string{selected[0].TxID, selected[1].TxID})
	}
	if len(deferred) != 1 || deferred[0].TxID != "b" {
		t.Fatalf("deferred=%v", deferred)
	}
}

func TestMetaTrackV656OrderingEdgeMayShareBlockButCannotBeSkipped(t *testing.T) {
	a := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), nil, nil, 1)
	b := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), nil, []uint64{1}, 1)
	selected, _, _, err := selectMetaTrackTransactionFrontierV656([]tx.SignedTransaction{a, b}, 10, "s0", nil)
	if err != nil || len(selected) != 2 {
		t.Fatalf("same-block ordering edge should be allowed: selected=%d err=%v", len(selected), err)
	}
	selected, deferred2, _, err := selectMetaTrackTransactionFrontierV656([]tx.SignedTransaction{b}, 10, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || len(deferred2) != 0 {
		t.Fatalf("leader assumes absent earlier ordinal committed; backup validator is authoritative")
	}
	r := &NodeRuntime{}
	if _, err := r.validateMetaTrackTransactionFrontierV656(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{b}}); err == nil {
		t.Fatal("validator must reject omitted uncommitted ordering predecessor")
	}
	r.recordMetaTrackCommittedRoutingOrdinalsV656(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a}})
	if _, err := r.validateMetaTrackTransactionFrontierV656(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{b}}); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackV656SignedDepthPreventsCrossBatchChainGrowth(t *testing.T) {
	a := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), nil, nil, 1)
	b := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "b", 2, 2, "p2", "s0", 1, 1), []uint64{1}, nil, 1)
	r := &NodeRuntime{}
	if _, err := r.validateMetaTrackTransactionFrontierV656(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a, b}}); err == nil {
		t.Fatal("cross-batch execution chain must not grow deeper than signed batch-local depth")
	}
}
