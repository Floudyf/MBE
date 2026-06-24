package execution_sharding

import "testing"

func TestHashExecutionShardingUsesBatchRoutingMap(t *testing.T) {
	assigner := NewHashExecutionSharding(4)
	got := assigner.Assign(Transaction{ID: "tx-1", AccessKeys: []string{"asset:1"}}, Context{StateToExecution: map[string]int{"asset:1": 3}})
	if got != 3 {
		t.Fatalf("assigned shard %d, want 3", got)
	}
}
