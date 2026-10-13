package v5

import "testing"

func TestMetaTrackDynamicRiskReadyNoFalseProtection(t *testing.T) {
	order := ScheduleResult{DependencyInfluenceOrdinal: map[string]uint64{"normal": 2, "risky": 1}}
	for _, tc := range []struct {
		name       string
		riskFlight int
		workers    int
		unlock     map[string]int
		expected   string
	}{
		{"protect_ready_normal_when_risk_fills_other_slots", 3, 4, map[string]int{"normal": 0, "risky": 5}, "normal"},
		{"release_important_predecessor_when_budget_free", 0, 4, map[string]int{"normal": 0, "risky": 5}, "risky"},
		{"normal_wins_tied_unlock_score", 0, 4, map[string]int{"normal": 0, "risky": 0}, "normal"},
		{"single_worker_protects_normal", 0, 1, map[string]int{"normal": 0, "risky": 4}, "normal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			choice := metaTrackPeekDynamicRiskReady([]string{"normal"}, []string{"risky"}, map[string]uint64{"normal": 1, "risky": 1}, order, tc.unlock, tc.riskFlight, tc.workers)
			if choice != tc.expected {
				t.Fatalf("got=%q want=%q", choice, tc.expected)
			}
		})
	}
}

func TestMetaTrackDynamicRiskReadyOldestRoundAndWorkConserving(t *testing.T) {
	order := ScheduleResult{DependencyInfluenceOrdinal: map[string]uint64{"riskold": 9, "normalnew": 1}}
	choice := metaTrackPeekDynamicRiskReady([]string{"normalnew"}, []string{"riskold"}, map[string]uint64{"normalnew": 2, "riskold": 1}, order, map[string]int{}, 7, 8)
	if choice != "riskold" {
		t.Fatalf("oldest round starved: %s", choice)
	}
	choice = metaTrackPeekDynamicRiskReady(nil, []string{"riskold"}, map[string]uint64{"riskold": 1}, order, map[string]int{}, 7, 8)
	if choice != "riskold" {
		t.Fatalf("risk-only work was not runnable: %s", choice)
	}
}
