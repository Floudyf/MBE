package v5

import "testing"

func TestFaultPolicyFromPlanPreservesRuntimePolicy(t *testing.T) {
	policy := faultPolicyFromPlan(map[string]any{
		"mode":               "network_delay_drop",
		"delay_ms":           7,
		"drop_rate":          0.25,
		"seed":               11,
		"drop_message_types": []any{"PBFT_PREPARE"},
		"target_peer_ids":    []any{"n3"},
	})
	if !policy.Enabled || policy.DelayMS != 7 || policy.DropRate != 0.25 || policy.Seed != 11 {
		t.Fatalf("fault plan was not applied: %+v", policy)
	}
	if len(policy.DropMessageTypes) != 1 || policy.DropMessageTypes[0] != "PBFT_PREPARE" {
		t.Fatalf("message filter was not applied: %+v", policy.DropMessageTypes)
	}
	if len(policy.TargetPeerIDs) != 1 || policy.TargetPeerIDs[0] != "n3" {
		t.Fatalf("peer filter was not applied: %+v", policy.TargetPeerIDs)
	}
}
