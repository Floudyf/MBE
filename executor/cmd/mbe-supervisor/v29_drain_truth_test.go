package main

import "testing"

func TestV29DrainCountsExactVersionPendingWork(t *testing.T) {
	status := map[string]any{
		"committed_height":                         float64(2),
		"mempool_depth":                            float64(0),
		"reserved_tx_count":                        float64(0),
		"pending_commit_count":                     float64(0),
		"pending_future_block_count":               float64(0),
		"pending_cross_shard_count":                float64(0),
		"pending_state_delta_count":                float64(0),
		"pending_state_delta_key_count":            float64(0),
		"ready_state_delta_count":                  float64(0),
		"pending_state_fetch_count":                float64(2),
		"pending_state_version_subscription_count": float64(3),
		"pending_version_admission_watch_count":    float64(4),
		"proposal_in_flight":                       false,
	}
	if !hasPendingInfrastructureWork(status) {
		t.Fatal("exact-version pending work was ignored by drain gate")
	}
	snapshot := makeProgressSnapshot(0, []map[string]any{status}, nil)
	if snapshot.Pending != 9 {
		t.Fatalf("pending exact-version work=%d, want 9", snapshot.Pending)
	}
}
