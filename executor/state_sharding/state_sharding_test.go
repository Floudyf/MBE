package state_sharding

import "testing"

func TestHashStateShardingIsStableAndBounded(t *testing.T) {
	sharding := NewHashStateSharding(4)
	first := sharding.LocateState("asset:42")
	if first != sharding.LocateState("asset:42") || first < 0 || first >= 4 {
		t.Fatalf("unexpected state shard %d", first)
	}
}
