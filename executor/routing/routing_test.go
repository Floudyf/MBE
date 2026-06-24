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

func TestCoAccessRoutingKeepsHighAffinityKeysTogetherAndIsDeterministic(t *testing.T) {
	batch := []Transaction{{ID: "a", AccessKeys: []string{"hot:a", "hot:b"}}, {ID: "b", AccessKeys: []string{"hot:a", "hot:b"}}, {ID: "c", AccessKeys: []string{"cold:c"}}}
	router := NewCoAccessRouting(3, 2, 8, 1)
	first, second := router.BuildRouting(batch, nil), router.BuildRouting(batch, nil)
	if first.StateToExecution["hot:a"] != first.StateToExecution["hot:b"] {
		t.Fatal("high co-access keys were separated")
	}
	if first.StateToExecution["hot:a"] != second.StateToExecution["hot:a"] || first.TxToExecution["a"] != second.TxToExecution["a"] {
		t.Fatal("co-access routing is not deterministic")
	}
	if first.TxToExecution["a"] != first.StateToExecution["hot:a"] {
		t.Fatal("psi_t did not select M_t for transaction access")
	}
	if first.Metrics.RoutingPolicy != "co_access" || first.Metrics.CoAccessGroupCount != 2 || first.Metrics.CrossShardTxCount != 0 {
		t.Fatalf("unexpected metrics: %+v", first.Metrics)
	}
}

func TestRoutingHandlesEmptySingleAndMultiKeyTransactions(t *testing.T) {
	result := NewCoAccessRouting(2, 1, 4, 1).BuildRouting([]Transaction{{ID: "empty"}, {ID: "one", AccessKeys: []string{"k"}}, {ID: "many", AccessKeys: []string{"k", "z"}}}, nil)
	if _, ok := result.TxToExecution["empty"]; !ok {
		t.Fatal("empty transaction has no deterministic psi_t")
	}
	if result.Metrics.RemoteKeyCount < 0 || result.Metrics.RoutingTimeMS < 0 {
		t.Fatal("invalid routing metrics")
	}
}
