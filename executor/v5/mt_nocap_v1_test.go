package v5

import (
	"fmt"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

// MBE_MT_NOCAP_V1: isolated admission experiment. Full capacity still enforced.
func TestMTNoCapV1KeepsOriginalFullAndDropsOnlyCumulativeCap(t *testing.T) {
	shards := []string{"s0", "s1"}
	hot := metaTrackV650TestKeyForShard(t, "s0", shards)
	records := make([]WorkloadRecord, 0, 4)
	for i := 0; i < 4; i++ {
		records = append(records, WorkloadRecord{Index: i, LogicalID: fmt.Sprintf("nocap-%d", i), AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}}})
	}
	records = metaTrackV650AssignVersions(records)
	shard := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	input := BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: shards, Sharding: shard, ExpectedTransactionCount: len(records)}
	full := metaTrackV650Planner()
	baseline := full.PlanBatch(input)
	if baseline.IncrementalExecutionShardCapacity != 2 || full.incrementalV650.ShardLoad["s0"] != 2 || full.incrementalV650.ShardLoad["s1"] != 2 {
		t.Fatalf("original Full cap/placement changed: capacity=%d loads=%v", baseline.IncrementalExecutionShardCapacity, full.incrementalV650.ShardLoad)
	}
	noCap := metaTrackV650Planner()
	noCap.config["experimental_no_global_capacity_v1"] = true
	result := noCap.PlanBatch(input)
	if result.IncrementalExecutionShardCapacity != 0 || noCap.incrementalV650.Capacity != 0 {
		t.Fatalf("NoCap must disable cumulative count admission: capacity=%d", result.IncrementalExecutionShardCapacity)
	}
	if len(result.TransactionPlacements) != 4 || result.IncrementalCapacityForcedChoiceCount != 0 {
		t.Fatalf("NoCap must route all records without N/shards forced choice: placements=%d forced=%d", len(result.TransactionPlacements), result.IncrementalCapacityForcedChoiceCount)
	}
	if noCap.incrementalV650.ShardLoad["s0"]+noCap.incrementalV650.ShardLoad["s1"] != 4 {
		t.Fatalf("NoCap lost transaction: %v", noCap.incrementalV650.ShardLoad)
	}
}

func TestMTNoCapV1PreservesCapacityAcrossRouteBatches(t *testing.T) {
	shards := []string{"s0", "s1"}
	key := metaTrackV650TestKeyForShard(t, "s0", shards)
	records := make([]WorkloadRecord, 0, 10)
	for i := 0; i < 10; i++ {
		records = append(records, WorkloadRecord{Index: i, LogicalID: fmt.Sprintf("nocap-batch-%d", i), AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite}}})
	}
	records = metaTrackV650AssignVersions(records)
	p := metaTrackV650Planner()
	p.config["experimental_no_global_capacity_v1"] = true
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for b := 0; b < 2; b++ {
		plan := p.PlanBatch(BatchRoutingInput{BatchIndex: b, Records: records[b*5 : (b+1)*5], ShardIDs: shards, Sharding: sharding, ExpectedTransactionCount: 10})
		if plan.IncrementalExecutionShardCapacity != 0 || plan.IncrementalCapacityForcedChoiceCount != 0 || len(plan.TransactionPlacements) != 5 {
			t.Fatalf("batch %d: cap=%d forced=%d items=%d", b, plan.IncrementalExecutionShardCapacity, plan.IncrementalCapacityForcedChoiceCount, len(plan.TransactionPlacements))
		}
	}
	if p.incrementalV650.Processed != 10 {
		t.Fatalf("lost routed records: %d", p.incrementalV650.Processed)
	}
}
