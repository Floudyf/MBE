package v5

import (
	"errors"
	"testing"
)

func TestPorygonV532ProposalPhaseErrorProjection(t *testing.T) {
	if got := porygonProposalPhaseErrV532(nil); got != "" {
		t.Fatalf("nil error projection=%q", got)
	}
	if got := porygonProposalPhaseErrV532(errors.New("phase failed")); got != "phase failed" {
		t.Fatalf("error projection=%q", got)
	}
}
