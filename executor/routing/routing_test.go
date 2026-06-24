package routing

import "testing"

func TestHashRoutingBuildsDeterministicStateMap(t *testing.T) {
	router := NewHashRouting(4)
	result := router.BuildRouting([]Transaction{{ID: "tx-1", AccessKeys: []string{"asset:1", "asset:2"}}}, nil)
	if len(result.StateToExecution) != 2 {
		t.Fatalf("M_t has %d keys, want 2", len(result.StateToExecution))
	}
	if result.StateToExecution["asset:1"] != router.BuildRouting([]Transaction{{AccessKeys: []string{"asset:1"}}}, nil).StateToExecution["asset:1"] {
		t.Fatal("hash route is not deterministic")
	}
}
