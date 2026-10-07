package v5

import (
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackStreamingV669ReturnsEachRouteBatchImmediately(t *testing.T) {
	planner := newMetaTrackCriticalWidthWindowPlannerV6568()
	first := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{Index: 0, RoutingOrdinal: 1, RouteBatchSequence: 1, ExecutionShard: "s0", ConsensusExecutionRound: 1}},
		{Record: WorkloadRecord{Index: 1, RoutingOrdinal: 2, RouteBatchSequence: 1, ExecutionShard: "s1", ConsensusExecutionRound: 1}},
	}
	got, err := planner.PushBatchAdaptiveNLV669StreamingV2(first, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("first batch held: got=%d", len(got))
	}
	if got[0].Record.ConsensusWindowSequence != 1 || got[0].Record.ConsensusWindowRouteBatchCount != 1 {
		t.Fatalf("unexpected first prefix: %+v", got[0].Record)
	}

	second := []metaTrackPreparedRecordV6568{
		{Record: WorkloadRecord{Index: 2, RoutingOrdinal: 3, RouteBatchSequence: 2, ExecutionShard: "s0", ConsensusExecutionRound: 1}},
		{Record: WorkloadRecord{Index: 3, RoutingOrdinal: 4, RouteBatchSequence: 2, ExecutionShard: "s1", ConsensusExecutionRound: 1}},
	}
	got, err = planner.PushBatchAdaptiveNLV669StreamingV2(second, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("second batch held: got=%d", len(got))
	}
	if got[0].Record.ConsensusWindowSequence != 1 || got[0].Record.ConsensusWindowRouteBatchCount != 2 {
		t.Fatalf("second batch did not join V669 prefix: %+v", got[0].Record)
	}
	if got[0].Record.ConsensusWindowTransactionCount != 4 {
		t.Fatalf("unexpected cumulative N: %d", got[0].Record.ConsensusWindowTransactionCount)
	}
}

func TestMetaTrackStreamingPrefixJoinUsesStrictNOverL(t *testing.T) {
	prev := metaTrackStreamingProjectionV2{
		identity: metaTrackBatchProjectionIdentity{Sequence: 4, TransactionCount: 100, ShardTransactionCount: 50},
		first:    &structExecutionRoutingV2A,
	}
	next := metaTrackStreamingProjectionV2{
		identity: metaTrackBatchProjectionIdentity{Sequence: 5, TransactionCount: 100, ShardTransactionCount: 50},
		first:    &structExecutionRoutingV2B,
	}
	if !metaTrackStreamingPrefixJoinValidV2(prev, next) {
		t.Fatal("strict N/L improvement should admit the second prefix")
	}
	bad := structExecutionRoutingV2B
	bad.ConsensusWindowCriticalPath = 20
	next.first = &bad
	if metaTrackStreamingPrefixJoinValidV2(prev, next) {
		t.Fatal("non-improving N/L prefix must be rejected")
	}
}

var structExecutionRoutingV2A = makeStreamingRoutingV2(9, 4, 4, 1, 100, 50, 10)
var structExecutionRoutingV2B = makeStreamingRoutingV2(9, 4, 5, 2, 200, 100, 15)

func makeStreamingRoutingV2(window, start, end uint64, batches, n, shardN, critical int) tx.ExecutionRoutingMetadata {
	return tx.ExecutionRoutingMetadata{
		ConsensusWindowSequence:              window,
		ConsensusWindowStartBatchSequence:    start,
		ConsensusWindowEndBatchSequence:      end,
		ConsensusWindowRouteBatchCount:       batches,
		ConsensusWindowTransactionCount:      n,
		ConsensusWindowShardTransactionCount: shardN,
		ConsensusWindowCriticalPath:          critical,
	}
}

// MBE_METATRACK_MECHPACK_V24_RUNTIME_REGRESSION
func withMetaTrackStreamingV24(
	t *testing.T,
	item tx.SignedTransaction,
	execPred, orderPred []uint64,
	depth, round int,
	window, start, end uint64,
	batches, n, shardN, critical int,
) tx.SignedTransaction {
	t.Helper()
	routing := *item.ExecutionRouting
	routing.ConsensusExecutionPredecessorOrdinals = append([]uint64(nil), execPred...)
	routing.ConsensusOrderingPredecessorOrdinals = append([]uint64(nil), orderPred...)
	routing.ConsensusExecutionDepth = depth
	routing.ConsensusExecutionRound = round
	routing.ConsensusWindowSequence = window
	routing.ConsensusWindowStartBatchSequence = start
	routing.ConsensusWindowEndBatchSequence = end
	routing.ConsensusWindowRouteBatchCount = batches
	routing.ConsensusWindowTransactionCount = n
	routing.ConsensusWindowShardTransactionCount = shardN
	routing.ConsensusWindowCriticalPath = critical
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	if err := tx.ValidateExecutionRouting(item); err != nil {
		t.Fatal(err)
	}
	return item
}

func TestMetaTrackStreamingV24CrossBatchDepthUsesBatchLocalScope(t *testing.T) {
	// Batch 1 has local depth 2. Batch 2 contains a transaction that depends on
	// Batch 1, so the aggregate block-local chain reaches depth 3, while that
	// Batch-2 transaction correctly signs batch-local depth 1. This reproduces
	// the artifacts(92) failure where the old validator compared 24/12 against 1.
	a1 := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "a1", 1, 1, "p1", "s0", 2, 2), nil, nil, 1, 1, 1, 1, 1, 1, 2, 2, 2)
	a2 := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "a2", 2, 1, "p1", "s0", 2, 2), []uint64{1}, nil, 2, 2, 1, 1, 1, 1, 2, 2, 2)
	b1 := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "b1", 3, 2, "p2", "s0", 3, 3), []uint64{2}, nil, 1, 3, 1, 1, 2, 2, 5, 5, 3)
	b2 := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "b2", 4, 2, "p2", "s0", 3, 3), nil, nil, 1, 1, 1, 1, 2, 2, 5, 5, 3)
	b3 := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "b3", 5, 2, "p2", "s0", 3, 3), nil, nil, 1, 1, 1, 1, 2, 2, 5, 5, 3)
	block := realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a1, a2, b1, b2, b3}}

	legacy := &NodeRuntime{}
	if _, err := legacy.validateMetaTrackTransactionFrontierV656(block); err == nil || !strings.Contains(err.Error(), "exceeds signed batch-local depth") {
		t.Fatalf("fixture must reproduce legacy cross-batch depth rejection, got %v", err)
	}
	streaming := &NodeRuntime{}
	if _, err := streaming.validateMetaTrackStreamingWindowV2(block); err != nil {
		t.Fatalf("streaming validator rejected valid cross-RouteBatch dependency: %v", err)
	}
}

func TestMetaTrackStreamingV24StillRejectsUnderSignedSameBatchDepth(t *testing.T) {
	a := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "same-a", 1, 1, "same", "s0", 2, 2), nil, nil, 1, 1, 3, 1, 1, 1, 2, 2, 2)
	b := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "same-b", 2, 1, "same", "s0", 2, 2), []uint64{1}, nil, 1, 2, 3, 1, 1, 1, 2, 2, 2)
	block := realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a, b}}
	if _, err := (&NodeRuntime{}).validateMetaTrackStreamingWindowV2(block); err == nil || !strings.Contains(err.Error(), "batch-local dependency depth 2 exceeds signed batch-local depth 1") {
		t.Fatalf("under-signed same-batch depth must fail closed, got %v", err)
	}
}

func TestMetaTrackStreamingV24StillRejectsOmittedUncommittedPredecessor(t *testing.T) {
	item := withMetaTrackStreamingV24(t, signedMetaTrackBatchTestTx(t, "missing", 2, 1, "missing", "s0", 1, 1), []uint64{1}, nil, 1, 2, 4, 1, 1, 1, 1, 1, 1)
	block := realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{item}}
	if _, err := (&NodeRuntime{}).validateMetaTrackStreamingWindowV2(block); err == nil || !strings.Contains(err.Error(), "omitted execution predecessor ordinal=1") {
		t.Fatalf("omitted uncommitted predecessor must fail closed, got %v", err)
	}
}

func TestMetaTrackStreamingV24AllowsZeroLocalProjectionGap(t *testing.T) {
	prevRouting := makeStreamingRoutingV2(7, 4, 9, 6, 600, 80, 60)
	currRouting := makeStreamingRoutingV2(7, 4, 11, 8, 800, 100, 70)
	prev := metaTrackStreamingProjectionV2{
		identity: metaTrackBatchProjectionIdentity{Sequence: 9, TransactionCount: 100, ShardTransactionCount: 20},
		first:    &prevRouting,
	}
	current := metaTrackStreamingProjectionV2{
		identity: metaTrackBatchProjectionIdentity{Sequence: 11, TransactionCount: 100, ShardTransactionCount: 20},
		first:    &currRouting,
	}
	if !metaTrackStreamingPrefixJoinValidV2(prev, current) {
		t.Fatal("global RouteBatch 10 has zero local s0 projection; sequence 9 -> 11 must remain joinable")
	}
	bad := currRouting
	bad.ConsensusWindowShardTransactionCount = 120 // skipped batch would have contributed local transactions
	current.first = &bad
	if metaTrackStreamingPrefixJoinValidV2(prev, current) {
		t.Fatal("projection gap with unexplained local shard-count growth must fail closed")
	}
}
