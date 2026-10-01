package v5

import (
	"testing"
	"time"
)

func TestMetaTrackNaturalWindowV657UsesProducerIntervalForFixedRate(t *testing.T) {
	plan := WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 500}
	current := make([]WorkloadRecord, 500)
	for i := range current {
		current[i].Index = i
	}
	close, reason := metaTrackNaturalProducerWindowShouldCloseV657(plan, time.Second, 1000, current, WorkloadRecord{Index: 500})
	if !close || reason != "producer_interval" {
		t.Fatalf("expected producer interval close, got close=%v reason=%q", close, reason)
	}
}

func TestMetaTrackNaturalWindowV657CapacityWinsWithoutArtificialClock(t *testing.T) {
	plan := WorkloadPlan{ReplayMode: "max_throughput"}
	current := make([]WorkloadRecord, 1000)
	close, reason := metaTrackNaturalProducerWindowShouldCloseV657(plan, time.Second, 1000, current, WorkloadRecord{Index: 1000})
	if !close || reason != "block_capacity" {
		t.Fatalf("expected capacity close, got close=%v reason=%q", close, reason)
	}
}

func TestMetaTrackNaturalWindowV657DoesNotCloseEarly(t *testing.T) {
	plan := WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 500}
	current := make([]WorkloadRecord, 250)
	for i := range current {
		current[i].Index = i
	}
	close, reason := metaTrackNaturalProducerWindowShouldCloseV657(plan, time.Second, 1000, current, WorkloadRecord{Index: 250})
	if close || reason != "" {
		t.Fatalf("unexpected early close=%v reason=%q", close, reason)
	}
}

func TestMetaTrackNaturalWindowV657Stats(t *testing.T) {
	stats := newMetaTrackNaturalWindowStatsV657(WorkloadPlan{ReplayMode: "fixed_rate", TargetSubmissionTPS: 500}, time.Second, 1000)
	stats.NoteWindow(500, "producer_interval")
	stats.NoteWindow(1000, "block_capacity")
	stats.NoteWindow(250, "end_of_stream")
	if stats.FixedMicroBatchEnabled || stats.WindowCount != 3 || stats.TimeBoundaryCount != 1 || stats.CapacityBoundaryCount != 1 || stats.EndOfStreamBoundaryCount != 1 || stats.MinWindowSize != 250 || stats.MaxWindowSize != 1000 || stats.TotalTransactions != 1750 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}
