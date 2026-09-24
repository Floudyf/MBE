package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestV30MetaTrackTrackAttemptMetrics(t *testing.T) {
	metrics := metaTrackTrackMetricsFromAttempts([]BusinessExecutionAttempt{
		{TxID: "f1", Track: "fast", Attempt: 1, FinalCompletion: true, DurationUS: 10_000},
		{TxID: "f2", Track: "fast", Attempt: 1, FinalCompletion: false, DurationUS: 5_000},
		{TxID: "f2", Track: "conservative", Attempt: 2, FinalCompletion: true, DurationUS: 20_000},
		{TxID: "c1", Track: "conservative", Attempt: 1, FinalCompletion: true, DurationUS: 30_000},
	})
	if metrics["metatrack_fast_initial_tx_count"] != 2 || metrics["metatrack_conservative_initial_tx_count"] != 1 {
		t.Fatalf("unexpected initial track counts: %#v", metrics)
	}
	if metrics["metatrack_fast_fallback_count"] != 1 || metrics["metatrack_conservative_reexecution_count"] != 1 {
		t.Fatalf("unexpected fallback/reexecution counts: %#v", metrics)
	}
	if got := metrics["metatrack_fast_business_execution_sum_ms"].(float64); got != 15.0 {
		t.Fatalf("fast execution sum=%v", got)
	}
	if got := metrics["metatrack_conservative_business_execution_sum_ms"].(float64); got != 50.0 {
		t.Fatalf("conservative execution sum=%v", got)
	}
	if got := metrics["metatrack_fast_fallback_rate"].(float64); got != 0.5 {
		t.Fatalf("fast fallback rate=%v", got)
	}
	if got := metrics["metatrack_conservative_reexecution_share"].(float64); got != 0.5 {
		t.Fatalf("conservative reexecution share=%v", got)
	}
	if got := metrics["metatrack_fast_business_execution_p95_ms"].(float64); got <= 0 {
		t.Fatalf("fast p95=%v", got)
	}
	if got := metrics["metatrack_conservative_business_execution_p99_ms"].(float64); got <= 0 {
		t.Fatalf("conservative p99=%v", got)
	}
}

func TestV30StatelessAdmissionRecordsNarrowExternalFrontier(t *testing.T) {
	_, runtime, key := statelessVersionAdmissionTestRuntimes(t)
	consumer := statelessVersionAdmissionItem("tx2", "s1", key, 2, 1)
	block := realblock.Block{BlockHash: "v30-frontier", Height: 1, ShardID: "s1", Timestamp: time.Now().UnixMilli(), TxIDs: []string{consumer.TxID}, TxList: []tx.SignedTransaction{consumer}}
	admitted, deferred, err := runtime.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.TxList) != 0 || len(deferred) != 1 {
		t.Fatalf("expected deferred external exact-version tx: admitted=%d deferred=%d", len(admitted.TxList), len(deferred))
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	got := runtime.runtimeMetricCounts
	for key, want := range map[string]int64{
		"stateless_version_admission_candidate_event_count":                    1,
		"stateless_version_admission_candidate_tx_count":                       1,
		"stateless_version_admission_external_exact_dependency_tx_count":       1,
		"stateless_version_admission_external_exact_dependency_edge_count":     1,
		"stateless_version_admission_external_exact_dependency_token_count":    1,
		"stateless_version_admission_external_version_not_ready_token_count":   1,
		"stateless_version_admission_deferred_direct_external_not_ready_count": 1,
		"stateless_version_admission_deferred_internal_propagation_count":      0,
	} {
		if got[key] != want {
			t.Fatalf("%s=%d want=%d metrics=%#v", key, got[key], want, got)
		}
	}
}
