package v5

import (
	"context"
	"testing"
	"time"
)

func TestSubmissionPacerValidatesReplayConfiguration(t *testing.T) {
	if _, err := newSubmissionPacer(WorkloadPlan{ReplayMode: "fixed_rate"}); err == nil {
		t.Fatal("fixed_rate without target_submission_tps must fail")
	}
	if _, err := newSubmissionPacer(WorkloadPlan{ReplayMode: "max_throughput", TargetSubmissionTPS: 100}); err == nil {
		t.Fatal("max_throughput with target_submission_tps must fail closed")
	}
	if _, err := newSubmissionPacer(WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 200}); err != nil {
		t.Fatalf("valid fixed_rate rejected: %v", err)
	}
}

func TestSubmissionPacerUsesAbsoluteReleaseTimeline(t *testing.T) {
	pacer, err := newSubmissionPacer(WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 200})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := pacer.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if pacer.startedAt.IsZero() || pacer.nextOrdinal != 1 {
		t.Fatalf("first release did not initialize absolute schedule: %+v", pacer)
	}

	// Simulate client/network work that makes the next slot late. The next due
	// time must remain anchored to the original schedule; it must not move to
	// "now + interval" as the old maximum-rate cap did.
	pacer.startedAt = time.Now().Add(-20 * time.Millisecond)
	pacer.nextOrdinal = 1
	started := time.Now()
	if err := pacer.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Millisecond {
		t.Fatalf("late absolute release was incorrectly delayed by prior send time: %s", elapsed)
	}
	if pacer.lateReleaseCount != 1 || pacer.maxScheduleLag <= 0 {
		t.Fatalf("late-release evidence missing: count=%d lag=%s", pacer.lateReleaseCount, pacer.maxScheduleLag)
	}
	if pacer.nextOrdinal != 1 {
		t.Fatalf("late release must rebase the next slot: ordinal=%d want=1", pacer.nextOrdinal)
	}
	// A late slot must not cause a catch-up burst. The next Wait should sleep for
	// approximately one full interval from the rebased release time.
	started = time.Now()
	if err := pacer.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 3*time.Millisecond {
		t.Fatalf("late release was followed by catch-up burst: next wait=%s", elapsed)
	}
}

func TestSubmissionPacerFixedRateIntervalIsAbsoluteTPS(t *testing.T) {
	pacer, err := newSubmissionPacer(WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 500})
	if err != nil {
		t.Fatal(err)
	}
	if pacer.interval != 2*time.Millisecond {
		t.Fatalf("500 TPS interval=%s want=2ms", pacer.interval)
	}
}
