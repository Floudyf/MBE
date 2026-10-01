package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackAblationV659ForceAllConservativeOnlyChangesTrack(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", map[string]any{metaTrackAblationForceAllConservativeV659: true})}
	items := []tx.SignedTransaction{
		{TxID: "a", Sender: "alice", Nonce: 1, AccessList: []tx.AccessItem{{Key: "k1", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "b", Sender: "bob", Nonce: 1, AccessList: []tx.AccessItem{{Key: "k2", Mode: tx.AccessRead}}},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	applyMetaTrackForceAllConservativeV659(execution, &result)
	if len(result.Decisions) != 2 {
		t.Fatalf("decisions=%d", len(result.Decisions))
	}
	for id, decision := range result.Decisions {
		if decision.Track != "conservative" {
			t.Fatalf("%s track=%s", id, decision.Track)
		}
	}
}

func TestMetaTrackAblationV659FlagsDefaultOff(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	if metaTrackForceAllConservativeEnabledV659(execution) {
		t.Fatal("force-conservative ablation must default off")
	}
	if metaTrackCriticalWidthDisabledV659(nil) {
		t.Fatal("critical-width ablation must default off")
	}
}


func TestMetaTrackAblationV660CleanSingleVariables(t *testing.T) {
	state := &metaTrackIncrementalRoutingStateV650{ProducerByVersion: map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650{{Key: "k", Version: 7}: {Shard: "s0", Ordinal: 1, FinishRank: 5}}}
	record := WorkloadRecord{Index: 1, AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite}}, StateVersions: []tx.StateVersionDependency{{Key: "k", RequiredVersion: 7}}}
	if got := metaTrackAblationReadyRankV660(true, record, "s1", state); got != 0 {
		t.Fatalf("routing ablation ready rank=%d want=0", got)
	}
	if got := metaTrackAblationReadyRankV660(false, record, "s1", state); got != 6 {
		t.Fatalf("full routing ready rank=%d want=6", got)
	}

	planner := newMetaTrackCriticalWidthWindowPlannerV6568()
	batch1 := []metaTrackPreparedRecordV6568{{Record: WorkloadRecord{RouteBatchSequence: 1, ConsensusExecutionRound: 1, ExecutionShard: "s0"}}}
	batch2 := []metaTrackPreparedRecordV6568{{Record: WorkloadRecord{RouteBatchSequence: 2, ConsensusExecutionRound: 2, ExecutionShard: "s0"}}}
	if closed, err := planner.PushBatchSingleRouteBatchV660(batch1, 100); err != nil || len(closed) != 0 {
		t.Fatalf("first single-batch push closed=%d err=%v", len(closed), err)
	}
	closed, err := planner.PushBatchSingleRouteBatchV660(batch2, 100)
	if err != nil || len(closed) != 1 || closed[0].Record.ConsensusWindowRouteBatchCount != 1 {
		t.Fatalf("second single-batch push=%#v err=%v", closed, err)
	}
	last := planner.Flush()
	if len(last) != 1 || last[0].Record.ConsensusWindowRouteBatchCount != 1 || last[0].Record.ConsensusWindowSequence == closed[0].Record.ConsensusWindowSequence {
		t.Fatalf("final single-batch window=%#v previous=%#v", last, closed)
	}

	if got := metaTrackAblationHomePublicationClassV660(true, 1, metaTrackVersionClassLocalTransient); got != metaTrackVersionClassRemoteLive {
		t.Fatalf("forced Home class=%s", got)
	}
	if got := metaTrackAblationHomePublicationClassV660(false, 1, metaTrackVersionClassLocalTransient); got != metaTrackVersionClassLocalTransient {
		t.Fatalf("full class changed=%s", got)
	}
	if got := metaTrackAblationHomePublicationClassV660(true, 0, metaTrackVersionClassLocalTransient); got != metaTrackVersionClassLocalTransient {
		t.Fatalf("ordering-only local transient should not publish Home: %s", got)
	}
}


func TestMetaTrackAblationV661AlignedMechanisms(t *testing.T) {
	state := &metaTrackIncrementalRoutingStateV650{PairShardSupport: map[string]map[string]int{keyPair("a", "b"): {"s0": 7}}}
	if got := metaTrackAblationCoaccessLocalityV661(true, []string{"a", "b"}, "s0", state); got != 0 {
		t.Fatalf("coaccess ablation=%d want=0", got)
	}
	if got := metaTrackAblationCoaccessLocalityV661(false, []string{"a", "b"}, "s0", state); got != 7 {
		t.Fatalf("full coaccess=%d want=7", got)
	}
	if metaTrackAblationSingleReadyQueueV661 == "" || metaTrackAblationOnDemandStateFetchV661 == "" {
		t.Fatal("v661 runtime ablation controls missing")
	}
}
