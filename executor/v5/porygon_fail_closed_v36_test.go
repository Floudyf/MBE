package v5

import (
	"context"
	"testing"
)

func TestPorygonV36DistributedESCReadinessFailsClosed(t *testing.T) {
	exchange := func(context.Context, PorygonESCWaveResult, []string) (PorygonESCWaveCertificate, error) {
		return PorygonESCWaveCertificate{}, nil
	}
	if porygonDistributedESCReady("", exchange, nil) {
		t.Fatal("empty execution shard unexpectedly considered distributed-ESC ready")
	}
	if porygonDistributedESCReady("esc-0", nil, nil) {
		t.Fatal("nil ESC exchange unexpectedly considered distributed-ESC ready")
	}
	if !porygonDistributedESCReady("esc-0", exchange, nil) {
		t.Fatal("complete wave-exchange distributed ESC wiring was rejected")
	}
	batchExchange := func(context.Context, PorygonESCBatchResult, []string) (PorygonESCBatchCertificate, error) {
		return PorygonESCBatchCertificate{}, nil
	}
	if !porygonDistributedESCReady("esc-0", nil, batchExchange) {
		t.Fatal("complete batch-exchange distributed ESC wiring was rejected")
	}
}
