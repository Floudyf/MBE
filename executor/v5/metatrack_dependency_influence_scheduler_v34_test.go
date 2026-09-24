package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func v34Item(id string, ordinal uint64, accesses []tx.AccessItem, versions []tx.StateVersionDependency) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID:       id,
		AccessList: accesses,
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ControlPolicy:      metaTrackDeclaredAccessFrontierPolicy,
			RouteBatchSequence: 1,
			RoutingOrdinal:     ordinal,
			StateVersions:      versions,
		},
	}
}

func TestMetaTrackV34InfluencePriorityAppliesAtReadySelectionOnly(t *testing.T) {
	items := []tx.SignedTransaction{
		v34Item("short", 1, []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}, []tx.StateVersionDependency{{Key: "b", ProducedVersion: 1}}),
		v34Item("root", 2, []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 2}}),
		v34Item("mid", 3, []tx.AccessItem{{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}, []tx.StateVersionDependency{{Key: "a", RequiredVersion: 2}, {Key: "c", ProducedVersion: 3}}),
		v34Item("leaf", 4, []tx.AccessItem{{Key: "c", Mode: tx.AccessRead, UpdateSemantics: "validate"}}, []tx.StateVersionDependency{{Key: "c", RequiredVersion: 3}}),
	}
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	scheduler := builtinScheduler{makeBasic("scheduler", metaTrackDependencyInfluenceSchedulerID, nil)}
	got := scheduler.Schedule(items, execution)
	if !got.DependencyInfluenceValid || got.ReadyPriorityPolicy != metaTrackDependencyInfluencePriorityPolicy {
		t.Fatalf("invalid influence evidence: %#v", got)
	}
	if len(got.Ordered) != len(items) || txIdentifier(got.Ordered[0]) != "short" || txIdentifier(got.Ordered[1]) != "root" {
		t.Fatalf("candidate scheduler must preserve canonical pre-dispatch order, got=%v", txIDsV34(got.Ordered))
	}
	if got.DependencyInfluenceTailDepth["root"] <= got.DependencyInfluenceTailDepth["short"] {
		t.Fatalf("expected root longer tail: %#v", got.DependencyInfluenceTailDepth)
	}
	fast := []string{"short", "root"}
	conservative := []string{}
	if selected := metaTrackPopHighestDependencyInfluence(&fast, &conservative, got); selected != "root" {
		t.Fatalf("ready selector must choose larger-H root, got=%s", selected)
	}
}

func TestMetaTrackV34PopIgnoresFastConservativeTrackWhenInfluenceHigher(t *testing.T) {
	schedule := ScheduleResult{
		ReadyPriorityPolicy:          metaTrackDependencyInfluencePriorityPolicy,
		DependencyInfluenceTailDepth: map[string]int{"fast": 1, "conservative": 5},
		DependencyInfluenceDescCount: map[string]int{"fast": 0, "conservative": 9},
		DependencyInfluenceOrdinal:   map[string]uint64{"fast": 1, "conservative": 2},
		DependencyInfluenceValid:     true,
	}
	fast := []string{"fast"}
	conservative := []string{"conservative"}
	got := metaTrackPopHighestDependencyInfluence(&fast, &conservative, schedule)
	if got != "conservative" {
		t.Fatalf("expected influence priority to choose conservative-labeled critical tx, got=%s", got)
	}
}

func TestMetaTrackV34InfluenceTieBreaksByCanonicalOrdinal(t *testing.T) {
	schedule := ScheduleResult{
		DependencyInfluenceTailDepth: map[string]int{"a": 3, "b": 3},
		DependencyInfluenceDescCount: map[string]int{"a": 4, "b": 4},
		DependencyInfluenceOrdinal:   map[string]uint64{"a": 9, "b": 7},
	}
	if !metaTrackDependencyInfluenceBefore("b", "a", schedule) {
		t.Fatal("lower canonical ordinal must win exact H/D tie")
	}
}

func txIDsV34(items []tx.SignedTransaction) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, txIdentifier(item))
	}
	return out
}
