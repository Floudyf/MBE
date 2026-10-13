package v5

import "testing"

func TestMetaTrackUnlockReadyFairnessAndContention(t *testing.T) {
	base := ScheduleResult{DependencyInfluenceOrdinal: map[string]uint64{"f1": 1, "c1": 2, "f2": 3, "c2": 4}}
	rounds := map[string]uint64{"f1": 1, "c1": 1, "f2": 1, "c2": 2}
	unlock := map[string]int{"f1": 0, "c1": 3, "f2": 1, "c2": 99}
	fast := []string{"f1", "f2"}
	conservative := []string{"c1", "c2"}
	if got := metaTrackPeekUnlockReady(fast, conservative, rounds, base, unlock); got != "c1" {
		t.Fatalf("only oldest round, highest immediate unlock first: %q", got)
	}
	if got := metaTrackPopUnlockReady(&fast, &conservative, rounds, base, unlock); got != "c1" {
		t.Fatalf("pop selected wrong tx %q", got)
	}
	if got := metaTrackPopUnlockReady(&fast, &conservative, rounds, base, unlock); got != "f2" {
		t.Fatalf("second oldest round wrong tx %q", got)
	}
	if got := metaTrackPopUnlockReady(&fast, &conservative, rounds, base, unlock); got != "f1" {
		t.Fatalf("canonical tie/fairness wrong tx %q", got)
	}
	if got := metaTrackPopUnlockReady(&fast, &conservative, rounds, base, unlock); got != "c2" {
		t.Fatalf("newer round wrong tx %q", got)
	}
	if got := metaTrackPopUnlockReady(&fast, &conservative, rounds, base, unlock); got != "" {
		t.Fatalf("empty queue should return empty, got %q", got)
	}
}

func TestMetaTrackUnlockReadyTiesIgnoreOldFastPriority(t *testing.T) {
	s := ScheduleResult{DependencyInfluenceOrdinal: map[string]uint64{"conservative": 1, "fast": 2}}
	got := metaTrackPeekUnlockReady([]string{"fast"}, []string{"conservative"}, map[string]uint64{"fast": 1, "conservative": 1}, s, map[string]int{})
	if got != "conservative" {
		t.Fatalf("equal priority should follow canonical order rather than permanently demote conservative: %q", got)
	}
}
