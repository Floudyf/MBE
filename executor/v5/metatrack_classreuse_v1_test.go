package v5

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

// The positive gate compares all classified fields, not just the fast/slow labels.
func TestMTDualV1SchedulerReuseExactClassification(t *testing.T) {
	types := []struct {
		name       string
		classifier ExecutionPlugin
	}{
		{"full", dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}},
		{"no_dual", metaTrackSingleExecution{makeBasic("execution", metaTrackSingleExecutionID, nil)}},
	}
	items := []tx.SignedTransaction{
		{TxID: "a", Sender: "alice", AccessList: []tx.AccessItem{{Key: "asset:x", Mode: tx.AccessWrite}, {Key: "remote:y", Mode: tx.AccessRead}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{ControlPolicy: metaTrackDeclaredAccessFrontierPolicy, RoutingOrdinal: 1, RouteBatchSequence: 1, StateVersions: []tx.StateVersionDependency{{Key: "asset:x", ProducedVersion: 1}}}},
		{TxID: "b", Sender: "bob", AccessList: []tx.AccessItem{{Key: "asset:x", Mode: tx.AccessReadWrite}, {Key: "remote:y", Mode: tx.AccessRead}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{ControlPolicy: metaTrackDeclaredAccessFrontierPolicy, RoutingOrdinal: 2, RouteBatchSequence: 1, StateVersions: []tx.StateVersionDependency{{Key: "asset:x", RequiredVersion: 1, ProducedVersion: 2}}}},
		{TxID: "c", Sender: "bob", AccessList: []tx.AccessItem{{Key: "remote:z", Mode: tx.AccessRead}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{ControlPolicy: metaTrackDeclaredAccessFrontierPolicy, RoutingOrdinal: 3, RouteBatchSequence: 2}},
		{TxID: "d", Sender: "carol", AccessList: []tx.AccessItem{{Key: "counter:k", Mode: tx.AccessCommutativeDelta}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{ControlPolicy: metaTrackDeclaredAccessFrontierPolicy, RoutingOrdinal: 4, RouteBatchSequence: 2}},
	}
	readinessCases := []struct {
		name  string
		ready map[string]bool
	}{
		{"nil", nil},
		{"empty", map[string]bool{}},
		{"some_not_ready", map[string]bool{"k:remote:y": false, "k:remote:z": false, "k:counter:k": true}},
		{"exact_version", map[string]bool{"v:00000000000000000001:asset:x": false}},
		{"mixed", map[string]bool{"v:00000000000000000001:asset:x": false, "k:remote:y": false, "k:remote:z": true}},
	}
	for _, entry := range types {
		for _, test := range readinessCases {
			t.Run(entry.name+"/"+test.name, func(t *testing.T) {
				sched := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}.Schedule(items, entry.classifier)
				if sched.MetaTrackPreparedClassification == nil {
					t.Fatal("scheduler did not preserve structural classification")
				}
				got, ok := metaTrackReuseScheduleClassificationV1(sched.MetaTrackPreparedClassification, items, test.ready)
				if !ok {
					t.Fatal("expected per-call reuse")
				}
				want := batchClassificationWithReadiness(items, entry.classifier, test.ready)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("classification divergence\nreuse=%#v\nfresh=%#v", got, want)
				}
				if len(sched.MetaTrackPreparedClassification.StateWaitKeys) != 0 {
					t.Fatal("original scheduler classification mutated")
				}
				initial := batchClassification(items, entry.classifier)
				bindMetaTrackInBlockVersionHandoffs(items, &got)
				if !reflect.DeepEqual(*sched.MetaTrackPreparedClassification, initial) {
					t.Fatal("execution handoff mutated prepared scheduler classification")
				}
				bindMetaTrackInBlockVersionHandoffs(items, &want)
				if !reflect.DeepEqual(got.Dependencies, want.Dependencies) || !reflect.DeepEqual(got.StateWaitKeys, want.StateWaitKeys) {
					t.Fatal("bound DAG or StateReady differed")
				}
			})
		}
	}
}

func TestMTDualV1ReuseFailsClosedOnInputMismatch(t *testing.T) {
	items := []tx.SignedTransaction{{TxID: "a", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessRead}}}}
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	sched := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}.Schedule(items, execution)
	if _, ok := metaTrackReuseScheduleClassificationV1(nil, items, nil); ok {
		t.Fatal("nil must fall back")
	}
	if _, ok := metaTrackReuseScheduleClassificationV1(sched.MetaTrackPreparedClassification, append(items, tx.SignedTransaction{TxID: "b"}), nil); ok {
		t.Fatal("length mismatch must fall back")
	}
	if _, ok := metaTrackReuseScheduleClassificationV1(sched.MetaTrackPreparedClassification, []tx.SignedTransaction{{TxID: "DIFFERENT"}}, nil); ok {
		t.Fatal("transaction mismatch must fall back")
	}
}

func TestMTDualV1ExecutionDAGSameAcrossTracks(t *testing.T) {
	cases := [][]tx.SignedTransaction{
		{
			{TxID: "w", Sender: "a", AccessList: []tx.AccessItem{{Key: "key", Mode: tx.AccessWrite}}},
			{TxID: "r", Sender: "b", AccessList: []tx.AccessItem{{Key: "key", Mode: tx.AccessRead}}},
			{TxID: "rmw", Sender: "b", AccessList: []tx.AccessItem{{Key: "key", Mode: tx.AccessReadWrite}}},
			{TxID: "w2", Sender: "c", AccessList: []tx.AccessItem{{Key: "key", Mode: tx.AccessWrite}}},
		},
		{
			{TxID: "delta1", AccessList: []tx.AccessItem{{Key: "counter", Mode: tx.AccessCommutativeDelta}}},
			{TxID: "delta2", AccessList: []tx.AccessItem{{Key: "counter", Mode: tx.AccessCommutativeDelta}}},
			{TxID: "read", AccessList: []tx.AccessItem{{Key: "counter", Mode: tx.AccessRead}}},
		},
	}
	full := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	single := metaTrackSingleExecution{makeBasic("execution", metaTrackSingleExecutionID, nil)}
	for idx, items := range cases {
		a := batchClassificationWithReadiness(items, full, nil)
		b := batchClassificationWithReadiness(items, single, nil)
		bindMetaTrackInBlockVersionHandoffs(items, &a)
		bindMetaTrackInBlockVersionHandoffs(items, &b)
		if !reflect.DeepEqual(a.Dependencies, b.Dependencies) {
			t.Fatalf("case %d: Full and no-dual DAG differ: full=%#v single=%#v", idx, a.Dependencies, b.Dependencies)
		}
	}
}

func TestMTDualV1ObservedDecisionReuse(t *testing.T) {
	items := []tx.SignedTransaction{
		{TxID: "first", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite}}},
		{TxID: "second", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite}}},
	}
	full := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	baseline := batchClassification(items, full)
	existing := baseline
	bindMetaTrackInBlockVersionHandoffs(items, &existing)
	observed, ok := metaTrackReuseObservedClassificationV1(&existing, items)
	if !ok || !reflect.DeepEqual(observed.Decisions, baseline.Decisions) {
		t.Fatal("post-commit observation changed track identity")
	}
	if _, ok := metaTrackReuseObservedClassificationV1(nil, items); ok {
		t.Fatal("nil observation must fall back")
	}
	if _, ok := metaTrackReuseObservedClassificationV1(&existing, items[:1]); ok {
		t.Fatal("incomplete observation must fall back")
	}
}
