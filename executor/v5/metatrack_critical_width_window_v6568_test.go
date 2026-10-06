package v5

import (
	"metaverse-chainlab/executor/realism/tx"
	"testing"
)

func preparedV6568(ord uint64, batch uint64, shard string, preds ...uint64) metaTrackPreparedRecordV6568 {
	return metaTrackPreparedRecordV6568{Record: WorkloadRecord{Index: int(ord - 1), LogicalID: "t", RoutingOrdinal: ord, RouteBatchSequence: batch, ExecutionShard: shard, ConsensusExecutionRound: int(ord), ConsensusExecutionDepth: 1, ConsensusExecutionPredecessorOrdinals: preds}, Route: RoutingDecision{ShardID: shard}}
}

func TestMetaTrackV663DependencyClosedIsSafetyNotJoinSufficiency(t *testing.T) {
	current := []metaTrackPreparedRecordV6568{
		preparedV6568(1, 1, "s0"),
		preparedV6568(2, 1, "s1", 1),
	}
	independentNext := []metaTrackPreparedRecordV6568{
		preparedV6568(3, 2, "s0"),
		preparedV6568(4, 2, "s1", 3),
	}
	if !metaTrackDependencyClosedJoinV662(current, independentNext) {
		t.Fatal("candidate should remain dependency-closed")
	}
	if metaTrackPipelineCoupledJoinV663(current, independentNext) {
		t.Fatal("dependency closure alone must not hide an independent next-batch root")
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


func TestMetaTrackV661CriticalPathPreservingJoin(t *testing.T) {
	if !metaTrackCriticalPathPreservingJoinV661(30, 20, 30) {
		t.Fatal("independent/non-extending route batch should aggregate")
	}
	if metaTrackCriticalPathPreservingJoinV661(30, 20, 31) {
		t.Fatal("aggregation must reject a route batch that extends the execution critical path")
	}
	if metaTrackCriticalPathPreservingJoinV661(0, 20, 20) || metaTrackCriticalPathPreservingJoinV661(30, 0, 30) {
		t.Fatal("invalid critical-path evidence must fail closed")
	}
}


func TestMetaTrackV662DependencyClosedJoinAllowsRealCrossBatchChain(t *testing.T) {
	current := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{RoutingOrdinal: 1}},
		{Record: WorkloadRecord{RoutingOrdinal: 2, ConsensusExecutionPredecessorOrdinals: []uint64{1}}},
	}
	batch := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{RoutingOrdinal: 3, ConsensusExecutionPredecessorOrdinals: []uint64{2}}},
	}
	if !metaTrackDependencyClosedJoinV662(current, batch) {
		t.Fatal("real forward cross-batch dependency must not block aggregation")
	}
	broken := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{RoutingOrdinal: 4, ConsensusExecutionPredecessorOrdinals: []uint64{3}}},
	}
	if metaTrackDependencyClosedJoinV662(current, broken) {
		t.Fatal("missing predecessor inside candidate ordinal span must fail closed")
	}
	cycleLike := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{RoutingOrdinal: 3, ConsensusExecutionPredecessorOrdinals: []uint64{3}}},
	}
	if metaTrackDependencyClosedJoinV662(current, cycleLike) {
		t.Fatal("non-backward predecessor must fail closed")
	}
}


func TestMetaTrackV663PipelineBoundaryPreservesIndependentNextBatch(t *testing.T) {
	p:=newMetaTrackCriticalWidthWindowPlannerV6568()
	b1:=[]metaTrackPreparedRecordV6568{preparedV6568(1,1,"s0"),preparedV6568(2,1,"s1",1)}
	if closed,err:=p.PushBatchPipelineCoupledV663(b1,100); err!=nil || closed!=nil { t.Fatalf("start %v %v",closed,err) }
	// b2 has an independent root (ordinal 3); keeping the boundary preserves pipeline overlap.
	b2:=[]metaTrackPreparedRecordV6568{preparedV6568(3,2,"s0"),preparedV6568(4,2,"s1",3)}
	closed,err:=p.PushBatchPipelineCoupledV663(b2,100); if err!=nil {t.Fatal(err)}
	if len(closed)!=2 { t.Fatalf("independent next batch must close prior window, got=%d",len(closed)) }
}

func TestMetaTrackV663PipelineCoupledBatchAggregates(t *testing.T) {
	p:=newMetaTrackCriticalWidthWindowPlannerV6568()
	b1:=[]metaTrackPreparedRecordV6568{preparedV6568(1,1,"s0"),preparedV6568(2,1,"s1",1)}
	if closed,err:=p.PushBatchPipelineCoupledV663(b1,100); err!=nil || closed!=nil { t.Fatalf("start %v %v",closed,err) }
	// The only b2 root depends on the tail RouteBatch, so aggregation does not hide independent work.
	b2:=[]metaTrackPreparedRecordV6568{preparedV6568(3,2,"s0",2),preparedV6568(4,2,"s1",3)}
	if closed,err:=p.PushBatchPipelineCoupledV663(b2,100); err!=nil || closed!=nil { t.Fatalf("coupled batch should aggregate %v %v",closed,err) }
	closed:=p.Flush(); if len(closed)!=4 {t.Fatalf("flush=%d",len(closed))}
	if closed[0].Record.ConsensusWindowRouteBatchCount!=2 {t.Fatalf("window batches=%d",closed[0].Record.ConsensusWindowRouteBatchCount)}
}

func TestMetaTrackV663FixedRouteBatchPlanner(t *testing.T) {
	p:=newMetaTrackCriticalWidthWindowPlannerV6568()
	b1:=[]metaTrackPreparedRecordV6568{preparedV6568(1,1,"s0")}; b2:=[]metaTrackPreparedRecordV6568{preparedV6568(2,2,"s0",1)}
	if closed,err:=p.PushBatchFixedRouteBatchV663(b1,100); err!=nil || closed!=nil {t.Fatalf("first %v %v",closed,err)}
	closed,err:=p.PushBatchFixedRouteBatchV663(b2,100); if err!=nil || len(closed)!=1 {t.Fatalf("second %v %v",closed,err)}
	if closed[0].Record.ConsensusWindowRouteBatchCount!=1 {t.Fatal("fixed planner merged route batches")}
}


func TestMetaTrackV665FullRestoresV661CriticalPathWindow(t *testing.T) {
	p := newMetaTrackCriticalWidthWindowPlannerV6568()
	b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)}
	if closed, err := p.PushBatch(b1, 100); err != nil || closed != nil { t.Fatalf("start: %v %v", closed, err) }
	b2 := []metaTrackPreparedRecordV6568{preparedV6568(3, 2, "s0"), preparedV6568(4, 2, "s1")}
	if closed, err := p.PushBatch(b2, 100); err != nil || closed != nil { t.Fatalf("non-extending batch must aggregate: %v %v", closed, err) }
	b3 := []metaTrackPreparedRecordV6568{preparedV6568(5, 3, "s0", 2), preparedV6568(6, 3, "s1", 5)}
	closed, err := p.PushBatch(b3, 100)
	if err != nil { t.Fatal(err) }
	if len(closed) != 4 { t.Fatalf("critical-path extension must close prior window, got=%d", len(closed)) }
	if closed[0].Record.ConsensusWindowStartBatchSequence != 1 || closed[0].Record.ConsensusWindowEndBatchSequence != 2 || closed[0].Record.ConsensusWindowCriticalPath != 2 { t.Fatalf("unexpected restored Full window: %#v", closed[0].Record) }
}


func TestMetaTrackV668ExperimentalNLWindow(t *testing.T) {
    p := newMetaTrackCriticalWidthWindowPlannerV6568()
    b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)} // N=2,L=2
    if closed, err := p.PushBatchAdaptiveNLV668(b1, 100); err != nil || closed != nil { t.Fatalf("start: %v %v", closed, err) }
    b2 := []metaTrackPreparedRecordV6568{preparedV6568(3, 2, "s0"), preparedV6568(4, 2, "s1")} // N=4,L=2 => N/L improves
    if closed, err := p.PushBatchAdaptiveNLV668(b2, 100); err != nil || closed != nil { t.Fatalf("join: %v %v", closed, err) }
    b3 := []metaTrackPreparedRecordV6568{preparedV6568(5, 3, "s0", 2), preparedV6568(6, 3, "s1", 5)} // N=6,L=4 => N/L falls
    closed, err := p.PushBatchAdaptiveNLV668(b3, 100)
    if err != nil { t.Fatal(err) }
    if len(closed) != 4 { t.Fatalf("closed=%d", len(closed)) }
    for _, x := range closed {
        if x.Record.ConsensusWindowStartBatchSequence != 1 || x.Record.ConsensusWindowEndBatchSequence != 2 || x.Record.ConsensusWindowRouteBatchCount != 2 || x.Record.ConsensusWindowTransactionCount != 4 || x.Record.ConsensusWindowCriticalPath != 2 { t.Fatalf("bad adaptive window %#v", x.Record) }
    }
}


func TestMetaTrackV669FormalAdaptiveAllowsCriticalPathGrowthWhenNLImproves(t *testing.T) {
	p := newMetaTrackCriticalWidthWindowPlannerV6568()
	b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)}
	if closed, err := p.PushBatchAdaptiveNLV669(b1, 100); err != nil || closed != nil { t.Fatalf("start: %v %v", closed, err) }
	// N=4,L=3: V661 rejects the 2->3 critical-path growth, while N/L improves 1 -> 4/3.
	b2 := []metaTrackPreparedRecordV6568{preparedV6568(3, 2, "s0", 2), preparedV6568(4, 2, "s1")}
	if closed, err := p.PushBatchAdaptiveNLV669(b2, 100); err != nil || closed != nil { t.Fatalf("formal adaptive should aggregate: %v %v", closed, err) }
	closed := p.Flush()
	if len(closed) != 4 || closed[0].Record.ConsensusWindowRouteBatchCount != 2 || closed[0].Record.ConsensusWindowCriticalPath != 3 { t.Fatalf("unexpected adaptive window: %#v", closed) }
}

func TestMetaTrackV669FormalAdaptiveRequiresDependencyClosure(t *testing.T) {
	p := newMetaTrackCriticalWidthWindowPlannerV6568()
	b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)}
	if closed, err := p.PushBatchAdaptiveNLV669(b1, 100); err != nil || closed != nil { t.Fatalf("start: %v %v", closed, err) }
	broken := []metaTrackPreparedRecordV6568{preparedV6568(4, 2, "s0", 3), preparedV6568(5, 2, "s1")}
	closed, err := p.PushBatchAdaptiveNLV669(broken, 100)
	if err != nil { t.Fatal(err) }
	if len(closed) != 2 { t.Fatalf("dependency-open candidate must close prior window, closed=%d", len(closed)) }
}

func TestMetaTrackV669DoesNotRewriteHistoricalV668Rule(t *testing.T) {
	b1 := []metaTrackPreparedRecordV6568{preparedV6568(1, 1, "s0"), preparedV6568(2, 1, "s1", 1)}
	broken := []metaTrackPreparedRecordV6568{preparedV6568(4, 2, "s0", 3), preparedV6568(5, 2, "s1")}
	p668 := newMetaTrackCriticalWidthWindowPlannerV6568()
	if closed, err := p668.PushBatchAdaptiveNLV668(b1, 100); err != nil || closed != nil { t.Fatalf("v668 start: %v %v", closed, err) }
	closed668, err := p668.PushBatchAdaptiveNLV668(broken, 100)
	if err != nil { t.Fatal(err) }
	if closed668 != nil { t.Fatal("historical V668 N/L-only rule unexpectedly rejected attractive candidate") }
	p669 := newMetaTrackCriticalWidthWindowPlannerV6568()
	if closed, err := p669.PushBatchAdaptiveNLV669(b1, 100); err != nil || closed != nil { t.Fatalf("v669 start: %v %v", closed, err) }
	closed669, err := p669.PushBatchAdaptiveNLV669(broken, 100)
	if err != nil { t.Fatal(err) }
	if len(closed669) != 2 { t.Fatal("formal V669 must reject dependency-open candidate") }
}
