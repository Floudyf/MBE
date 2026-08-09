package v5

import (
	"fmt"
	"testing"
)

func TestLogicalSourceShardIsIndependentOfExecutionPlacement(t *testing.T) {
	sharding := builtinSharding{basicPlugin: basicPlugin{id: "hash_sharding", config: map[string]any{}}}
	shards := []string{"s0", "s1"}
	record := WorkloadRecord{
		Index:     2,
		StateKeys: []string{"shard:account", "coaccess:hot-update"},
	}

	home := shardFor(sharding, record.StateKeys, shards)
	if home == "" {
		t.Fatal("stable state-key home must be available")
	}
	other := "s0"
	if other == home {
		other = "s1"
	}

	fromHash := logicalSourceShardForRecord(record, RoutingDecision{ShardID: home, Reason: "state_key_hash"}, sharding, shards)
	fromMetaTrack := logicalSourceShardForRecord(record, RoutingDecision{ShardID: other, Reason: "coaccess_placement"}, sharding, shards)
	if fromHash != home || fromMetaTrack != home {
		t.Fatalf("logical source changed with execution placement: home=%s hash=%s metatrack=%s", home, fromHash, fromMetaTrack)
	}

	hashSender := fmt.Sprintf("client_%s_%d", fromHash, record.Index)
	metaSender := fmt.Sprintf("client_%s_%d", fromMetaTrack, record.Index)
	if hashSender != metaSender {
		t.Fatalf("synthetic sender identity changed with execution placement: %s != %s", hashSender, metaSender)
	}
	if "shard:"+fromHash+":account" != "shard:"+fromMetaTrack+":account" {
		t.Fatal("semantic account state key changed with execution placement")
	}
}

func TestLogicalSourceShardPreservesExplicitWorkloadSource(t *testing.T) {
	sharding := builtinSharding{basicPlugin: basicPlugin{id: "hash_sharding", config: map[string]any{}}}
	record := WorkloadRecord{SourceShard: "s1", StateKeys: []string{"k"}}
	got := logicalSourceShardForRecord(record, RoutingDecision{ShardID: "s0"}, sharding, []string{"s0", "s1"})
	if got != "s1" {
		t.Fatalf("explicit source shard must win, got %q", got)
	}
}
