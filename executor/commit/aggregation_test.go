package commit

import "testing"

func ptr(v float64) *float64 { return &v }
func TestAggregateOnlyFastCommutativeDeltas(t *testing.T) {
	r := Aggregate([]Update{{ID: "a", PrimaryKey: "balance:u", Fast: true, Commutative: true, Delta: ptr(3)}, {ID: "b", PrimaryKey: "balance:u", Fast: true, Commutative: true, Delta: ptr(4)}, {ID: "c", PrimaryKey: "x", Fast: false, Commutative: true, Delta: ptr(1)}, {ID: "d", PrimaryKey: "y", Fast: true, Commutative: false, Delta: ptr(1)}}, Config{Enabled: true, MinHotCount: 2, RequireFastTrack: true, ConservativeOnConstraintFailure: true})
	if r.Aggregates["balance:u"] != 7 || r.Metrics.AggregatedCommitCount != 1 || r.Metrics.SavedCommitCount != 1 || r.Metrics.ConservativeCommitCount != 2 {
		t.Fatalf("bad result %+v", r)
	}
}
func TestAggregateRejectsMissingDeltaAndConstraintFailure(t *testing.T) {
	r := Aggregate([]Update{{PrimaryKey: "p", Fast: true, Commutative: true}, {PrimaryKey: "n", Fast: true, Commutative: true, Delta: ptr(-2)}, {PrimaryKey: "n", Fast: true, Commutative: true, Delta: ptr(-1)}}, Config{Enabled: true, MinHotCount: 2, RequireFastTrack: true, ConservativeOnConstraintFailure: true})
	if r.Metrics.MissingDeltaCount != 1 || r.Metrics.ConstraintFailureCount != 1 {
		t.Fatalf("bad constraints %+v", r.Metrics)
	}
}
