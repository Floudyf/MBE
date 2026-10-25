package main

import "testing"

// MBE_PORYGON_DRAIN_GATE_V1 is a supervisor-only regression. The actual
// paper maintenance decision stays inside the existing Porygon producer.
func TestPorygonDrainGateV1(t *testing.T) {
	porygonPending := map[string]any{"porygon_pending_protocol_work": true}
	if !hasPendingInfrastructureWork(porygonPending) {
		t.Fatal("Porygon certified future work must prevent drain quiescence")
	}
	porygonCleared := map[string]any{"porygon_pending_protocol_work": false}
	if hasPendingInfrastructureWork(porygonCleared) {
		t.Fatal("Porygon cleared protocol obligations must permit quiescence")
	}
	otherMethod := map[string]any{"pending_commit_count": float64(1)}
	if !hasPendingInfrastructureWork(otherMethod) {
		t.Fatal("ordinary pending commit must retain its existing drain behavior")
	}
	noWork := map[string]any{}
	if hasPendingInfrastructureWork(noWork) {
		t.Fatal("unchanged non-Porygon empty status should remain quiescent")
	}
}
