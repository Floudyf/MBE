package v5

import "testing"

// The fixed-hash control must use the same declared total-capacity budget as
// Full, even when transactions arrive in several signed RouteBatches.
func TestMetaTrackFlowAblationHashHasGlobalCapacityAcrossBatches(t *testing.T) {
	shards := []string{"s0", "s1"}
	p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{metaTrackAblationHashRoutingV662: true})}
	counts := map[string]int{}
	forced := 0
	for batch := 0; batch < 4; batch++ {
		records := []WorkloadRecord{}
		for j := 0; j < 5; j++ {
			index := batch*5 + j
			records = append(records, WorkloadRecord{Index: index, LogicalID: "fixed", RoutingOrdinal: uint64(index + 1), SourceShard: "s0", StateKeys: []string{"state"}})
		}
		plan := p.PlanBatch(BatchRoutingInput{BatchIndex: batch, Records: records, ShardIDs: shards, ExpectedTransactionCount: 20})
		if plan.PlacementPolicy != "metatrack_deterministic_hash_ablation_v662" || !plan.IncrementalBatchPartitionInvariant {
			t.Fatalf("incorrect fixed hash carrier: %s", plan.PlacementPolicy)
		}
		if plan.IncrementalExecutionShardCapacity != 10 {
			t.Fatalf("capacity=%d want=10", plan.IncrementalExecutionShardCapacity)
		}
		for _, item := range plan.TransactionPlacements {
			counts[item.ExecutionShard]++
			if item.ExecutionShard != "s0" && item.ExecutionShard != "s1" {
				t.Fatalf("unknown placement: %q", item.ExecutionShard)
			}
		}
		forced += plan.IncrementalCapacityForcedChoiceCount
	}
	if counts["s0"] != 10 || counts["s1"] != 10 || forced != 10 {
		t.Fatalf("counts=%v forced=%d", counts, forced)
	}
}

// The replacement is deterministic across changes in RouteBatch boundaries.
func TestMetaTrackFlowAblationHashPartitionInvariant(t *testing.T) {
	get := func(batchSize int) []string {
		p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{metaTrackAblationHashRoutingV662: true})}
		out := []string{}
		for base := 0; base < 20; base += batchSize {
			records := make([]WorkloadRecord, 0, batchSize)
			for i := base; i < base+batchSize; i++ {
				records = append(records, WorkloadRecord{Index: i, LogicalID: "tx", RoutingOrdinal: uint64(i + 1), SourceShard: "s0", StateKeys: []string{"state"}})
			}
			plan := p.PlanBatch(BatchRoutingInput{BatchIndex: base / batchSize, Records: records, ShardIDs: []string{"s0", "s1"}, ExpectedTransactionCount: 20})
			for _, item := range plan.TransactionPlacements {
				out = append(out, item.ExecutionShard)
			}
		}
		return out
	}
	x, y := get(5), get(10)
	if len(x) != 20 || len(y) != 20 {
		t.Fatalf("incorrect placement lengths")
	}
	for i := range x {
		if x[i] != y[i] {
			t.Fatalf("batch partition changed placement for tx %d: %q != %q", i, x[i], y[i])
		}
	}
}
