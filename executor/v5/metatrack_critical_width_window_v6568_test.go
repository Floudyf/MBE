package v5

import (
	"metaverse-chainlab/executor/realism/tx"
	"testing"
)

func preparedV6568(ord uint64, batch uint64, shard string, preds ...uint64) metaTrackPreparedRecordV6568 {
	return metaTrackPreparedRecordV6568{Record: WorkloadRecord{Index: int(ord - 1), LogicalID: "t", RoutingOrdinal: ord, RouteBatchSequence: batch, ExecutionShard: shard, ConsensusExecutionRound: int(ord), ConsensusExecutionDepth: 1, ConsensusExecutionPredecessorOrdinals: preds}, Route: RoutingDecision{ShardID: shard}}
}

func TestMetaTrackV6568CriticalWidthStrictImprovement(t *testing.T) {
	p := newMetaTrackCriticalWidthWindowPlannerV6568()
	b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)} // N=2,L=2,W=1
	if closed, err := p.PushBatch(b1, 100); err != nil || closed != nil {
		t.Fatalf("start: %v %v", closed, err)
	}
	b2 := []metaTrackPreparedRecordV6568{preparedV6568(3, 2, "s0"), preparedV6568(4, 2, "s1")} // N=4,L=2,W=2 => join
	if closed, err := p.PushBatch(b2, 100); err != nil || closed != nil {
		t.Fatalf("join: %v %v", closed, err)
	}
	b3 := []metaTrackPreparedRecordV6568{preparedV6568(5, 3, "s0", 2), preparedV6568(6, 3, "s1", 5)} // N=6,L=4,W=1.5 => close previous
	closed, err := p.PushBatch(b3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 4 {
		t.Fatalf("closed=%d", len(closed))
	}
	for _, x := range closed {
		if x.Record.ConsensusWindowSequence != 1 || x.Record.ConsensusWindowStartBatchSequence != 1 || x.Record.ConsensusWindowEndBatchSequence != 2 || x.Record.ConsensusWindowTransactionCount != 4 || x.Record.ConsensusWindowCriticalPath != 2 {
			t.Fatalf("bad window %#v", x.Record)
		}
	}
}

func TestMetaTrackV6568CapacityIsOnlyHardCeiling(t *testing.T) {
	p := newMetaTrackCriticalWidthWindowPlannerV6568()
	if _, err := p.PushBatch([]metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s0")}, 1); err == nil {
		t.Fatal("expected block ceiling rejection")
	}
}

func TestMetaTrackV6568WindowSelectorRequiresWholeSignedWindow(t *testing.T) {
	makeTx := func(id string, ord, batch uint64) tx.SignedTransaction {
		r := &tx.ExecutionRoutingMetadata{SenderID: id, ReceiverID: "r", RoutingOrdinal: ord, ExecutionShard: "s0", RoutingReason: "x", RoutePlanDigest: "p", RouteBatchSequence: batch, RouteBatchTransactionCount: 2, RouteBatchShardTransactionCount: 1, ConsensusExecutionDepth: 1, ConsensusExecutionRound: int(ord), ConsensusWindowSequence: 1, ConsensusWindowStartBatchSequence: 1, ConsensusWindowEndBatchSequence: 2, ConsensusWindowRouteBatchCount: 2, ConsensusWindowTransactionCount: 2, ConsensusWindowShardTransactionCount: 2, ConsensusWindowCriticalPath: 1}
		item := tx.SignedTransaction{TxID: id, Sender: id, Receiver: "r"}
		d, _ := tx.ComputeExecutionRoutingDigest(item, *r)
		r.RouteEntryDigest = d
		item.ExecutionRouting = r
		return item
	}
	a := makeTx("a", 1, 1)
	b := makeTx("b", 2, 2)
	if _, _, _, err := selectMetaTrackCriticalWidthWindowV6568([]tx.SignedTransaction{a}, 10, "s0", nil); err == nil {
		t.Fatal("partial window must wait")
	}
	sel, def, _, err := selectMetaTrackCriticalWidthWindowV6568([]tx.SignedTransaction{b, a}, 10, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel) != 2 || len(def) != 0 || sel[0].ExecutionRouting.RouteBatchSequence != 1 {
		t.Fatalf("bad selection")
	}
}
