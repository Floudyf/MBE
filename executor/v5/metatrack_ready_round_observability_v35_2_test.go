package v5

import (
	"os"
	"strings"
	"testing"
)

func TestMetaTrackV352ReadyRoundObservabilityFieldsBoundToRuntime(t *testing.T) {
	raw, err := os.ReadFile("registry.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, needle := range []string{
		"multiCandidateReadyRoundCount",
		"influenceArbitrationDecisionCount",
		`"multi_candidate_ready_round_count"`,
		`"influence_arbitration_decision_count"`,
		`"influence_changed_choice_count"`,
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("missing v35.2 observability binding: %s", needle)
		}
	}
}
