package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV35OldReadyRoundCannotBeBypassedByNewCriticalTransaction(t *testing.T) {
	schedule := ScheduleResult{
		DependencyInfluenceTailDepth: map[string]int{"old": 1, "new-critical": 99},
		DependencyInfluenceDescCount: map[string]int{"old": 0, "new-critical": 999},
		DependencyInfluenceOrdinal:   map[string]uint64{"old": 1, "new-critical": 2},
		DependencyInfluenceValid:     true,
	}
	fast := []string{"old", "new-critical"}
	conservative := []string{}
	rounds := map[string]uint64{"old": 1, "new-critical": 2}
	if got := metaTrackPeekReadyRoundChoice(fast, conservative, rounds, schedule, true); got != "old" {
		t.Fatalf("newer high-H transaction bypassed older Ready Round: got=%s", got)
	}
}

func TestMetaTrackV35CandidateDiffersFromControlOnlyWithinSameReadyRound(t *testing.T) {
	schedule := ScheduleResult{
		DependencyInfluenceTailDepth: map[string]int{"fast-short": 1, "cons-critical": 20},
		DependencyInfluenceDescCount: map[string]int{"fast-short": 0, "cons-critical": 30},
		DependencyInfluenceOrdinal:   map[string]uint64{"fast-short": 1, "cons-critical": 2},
		DependencyInfluenceValid:     true,
	}
	fast := []string{"fast-short"}
	conservative := []string{"cons-critical"}
	rounds := map[string]uint64{"fast-short": 7, "cons-critical": 7}
	if got := metaTrackPeekReadyRoundChoice(fast, conservative, rounds, schedule, false); got != "fast-short" {
		t.Fatalf("control should preserve fast-first within the same round, got=%s", got)
	}
	if got := metaTrackPeekReadyRoundChoice(fast, conservative, rounds, schedule, true); got != "cons-critical" {
		t.Fatalf("candidate should choose higher H/D within the same round, got=%s", got)
	}
}

func TestMetaTrackV35ControlSchedulerUsesSameStaticEvidenceAndCanonicalOrder(t *testing.T) {
	items := []tx.SignedTransaction{
		v34Item("a", 1, []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 1}}),
		v34Item("b", 2, []tx.AccessItem{{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"}}, []tx.StateVersionDependency{{Key: "a", RequiredVersion: 1}}),
	}
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	scheduler := builtinScheduler{makeBasic("scheduler", metaTrackReadyRoundControlSchedulerID, nil)}
	got := scheduler.Schedule(items, execution)
	if got.ReadyPriorityPolicy != metaTrackReadyRoundControlPriorityPolicy || !got.DependencyInfluenceValid {
		t.Fatalf("ready-round control evidence invalid: %#v", got)
	}
	if len(got.Ordered) != 2 || txIdentifier(got.Ordered[0]) != "a" || txIdentifier(got.Ordered[1]) != "b" {
		t.Fatalf("control scheduler must preserve canonical order: %v", txIDsV34(got.Ordered))
	}
	if got.DependencyInfluenceOrdinal["a"] != 1 || got.DependencyInfluenceOrdinal["b"] != 2 {
		t.Fatalf("control/candidate evidence parity lost: %#v", got.DependencyInfluenceOrdinal)
	}
}
