package v5

import (
	"context"
	"testing"
)

func TestPorygonV36DistributedESCReadinessFailsClosed(t *testing.T) {
	exchange := func(context.Context, PorygonESCWaveResult, []string) (PorygonESCWaveCertificate, error) {
		return PorygonESCWaveCertificate{}, nil
	}
	if porygonDistributedESCReady("", exchange) {
		t.Fatal("empty execution shard unexpectedly considered distributed-ESC ready")
	}
	if porygonDistributedESCReady("esc-0", nil) {
		t.Fatal("nil wave exchange unexpectedly considered distributed-ESC ready")
	}
	if !porygonDistributedESCReady("esc-0", exchange) {
		t.Fatal("complete distributed ESC wiring was rejected")
	}
}
