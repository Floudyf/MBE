package v5

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func metaTrackV650TestKeyForShard(t *testing.T, want string, shards []string) string {
	t.Helper()
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("v650-key-%s-%d", want, index)
		if shardFor(sharding, []string{key}, shards) == want {
			return key
		}
	}
	t.Fatalf("unable to find key for shard %s", want)
	return ""
}

func metaTrackV650AssignVersions(records []WorkloadRecord) []WorkloadRecord {
	out := append([]WorkloadRecord(nil), records...)
	lastWriter := map[string]uint64{}
	for index := range out {
		out[index].RoutingOrdinal = uint64(out[index].Index + 1)
		out[index].StateVersions = stateVersionDependenciesForRecord(out[index], out[index].RoutingOrdinal, lastWriter)
	}
	return out
}

func metaTrackV650Planner() *metaTrackRouting {
	return &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{
		"control_policy": metaTrackDeclaredAccessFrontierPolicy,
		"incremental_exact_continuity_routing_v65": true,
	})}
}

func metaTrackV650RunPartitions(t *testing.T, records []WorkloadRecord, partitions []int) (map[int]string, *metaTrackRouting) {
	t.Helper()
	planner := metaTrackV650Planner()
	shards := []string{"s0", "s1"}
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	placements := map[int]string{}
	cursor := 0
	for batchIndex, width := range partitions {
		if cursor >= len(records) {
			break
		}
		end := cursor + width
		if end > len(records) {
			end = len(records)
		}
		plan := planner.PlanBatch(BatchRoutingInput{
			BatchIndex:               batchIndex,
			Records:                  records[cursor:end],
			ShardIDs:                 shards,
			Sharding:                 sharding,
			ExpectedTransactionCount: len(records),
		})
		if plan.IncrementalRoutingPolicy != metaTrackIncrementalExactContinuityPolicyV650 || !plan.IncrementalBatchPartitionInvariant {
			t.Fatalf("v650 policy evidence missing: %#v", plan)
		}
		for _, placement := range plan.TransactionPlacements {
			placements[placement.TxIndex] = placement.ExecutionShard
		}
		cursor = end
	}
	if cursor != len(records) {
		t.Fatalf("partition coverage=%d want=%d", cursor, len(records))
	}
	return placements, planner
}

func TestMetaTrackV650RoutingIsRouteBatchPartitionInvariant(t *testing.T) {
	shards := []string{"s0", "s1"}
	hot := metaTrackV650TestKeyForShard(t, "s0", shards)
	cold1 := metaTrackV650TestKeyForShard(t, "s1", shards)
	cold2 := ""
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for index := 10000; index < 20000; index++ {
		candidate := fmt.Sprintf("v650-partition-s1-%d", index)
		if shardFor(sharding, []string{candidate}, shards) == "s1" && candidate != cold1 {
			cold2 = candidate
			break
		}
	}
	if cold2 == "" {
		t.Fatal("missing second s1 key")
	}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "a", AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}, {Key: cold1, Mode: tx.AccessReadWrite}, {Key: cold2, Mode: tx.AccessReadWrite}}},
		{Index: 1, LogicalID: "b", AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}}},
		{Index: 2, LogicalID: "c", AccessList: []tx.AccessItem{{Key: cold1, Mode: tx.AccessReadWrite}}},
		{Index: 3, LogicalID: "d", AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}, {Key: cold1, Mode: tx.AccessRead}}},
		{Index: 4, LogicalID: "e", AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}}},
		{Index: 5, LogicalID: "f", AccessList: []tx.AccessItem{{Key: cold2, Mode: tx.AccessReadWrite}}},
	}
	records = metaTrackV650AssignVersions(records)
	all, allPlanner := metaTrackV650RunPartitions(t, records, []int{len(records)})
	chunked, chunkedPlanner := metaTrackV650RunPartitions(t, records, []int{1, 2, 1, 2})
	if !reflect.DeepEqual(all, chunked) {
		t.Fatalf("route-batch boundary changed placement:\nall=%v\nchunked=%v", all, chunked)
	}
	if allPlanner.incrementalV650.HistoryDigest != chunkedPlanner.incrementalV650.HistoryDigest {
		t.Fatalf("history digest changed with partitioning: %s != %s", allPlanner.incrementalV650.HistoryDigest, chunkedPlanner.incrementalV650.HistoryDigest)
	}
}

func TestMetaTrackV651ExactContinuityCannotOverrideThreeOtherCriteria(t *testing.T) {
	candidates := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", ReadyRank: 0, ExactCross: 0, CoaccessLocality: 0, RemoteCost: 9, Load: 9, Admissible: true},
		{Shard: "s1", ReadyRank: 1, ExactCross: 1, CoaccessLocality: 10, RemoteCost: 1, Load: 1, Admissible: true},
	}
	ranked := metaTrackIncrementalRankCandidatesV651(candidates)
	sort.Slice(ranked, func(i, j int) bool { return metaTrackIncrementalCandidateLessV650(ranked[i], ranked[j]) })
	if ranked[0].Shard != "s1" {
		t.Fatalf("rank-balanced selector kept old exact-first behavior: %#v", ranked)
	}
}

func TestMetaTrackV651CoaccessMagnitudeCannotDominateOtherSignals(t *testing.T) {
	candidates := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", ReadyRank: 4, ExactCross: 1, CoaccessLocality: 20000, RemoteCost: 12, Load: 20, Admissible: true},
		{Shard: "s1", ReadyRank: 3, ExactCross: 1, CoaccessLocality: 1, RemoteCost: 4, Load: 10, Admissible: true},
	}
	ranked := metaTrackIncrementalRankCandidatesV651(candidates)
	sort.Slice(ranked, func(i, j int) bool { return metaTrackIncrementalCandidateLessV650(ranked[i], ranked[j]) })
	if ranked[0].Shard != "s1" {
		t.Fatalf("raw coaccess magnitude dominated ordinal balance: %#v", ranked)
	}
}

func TestMetaTrackV651ReadyRankChargesOnlyStructuralCrossShardStage(t *testing.T) {
	state := &metaTrackIncrementalRoutingStateV650{
		ProducerByVersion: map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650{
			{Key: "k", Version: 7}: {Shard: "s0", Ordinal: 1, FinishRank: 5},
		},
	}
	record := WorkloadRecord{
		Index:         1,
		AccessList:    []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite}},
		StateVersions: []tx.StateVersionDependency{{Key: "k", RequiredVersion: 7}},
	}
	if got := metaTrackIncrementalReadyRankV651(record, "s0", state); got != 5 {
		t.Fatalf("local ready rank=%d want=5", got)
	}
	if got := metaTrackIncrementalReadyRankV651(record, "s1", state); got != 6 {
		t.Fatalf("cross-shard ready rank=%d want=6 structural stages", got)
	}
}

func TestMetaTrackV650CapacityIsDerivedAndBalanced(t *testing.T) {
	shards := []string{"s0", "s1"}
	hot := metaTrackV650TestKeyForShard(t, "s0", shards)
	records := []WorkloadRecord{}
	for index := 0; index < 4; index++ {
		records = append(records, WorkloadRecord{Index: index, LogicalID: fmt.Sprintf("t%d", index), AccessList: []tx.AccessItem{{Key: hot, Mode: tx.AccessReadWrite}}})
	}
	records = metaTrackV650AssignVersions(records)
	planner := metaTrackV650Planner()
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	plan := planner.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: shards, Sharding: sharding, ExpectedTransactionCount: 4})
	if plan.IncrementalExecutionShardCapacity != 2 {
		t.Fatalf("capacity=%d want ceil(4/2)=2", plan.IncrementalExecutionShardCapacity)
	}
	if got := planner.incrementalV650.ShardLoad; got["s0"] != 2 || got["s1"] != 2 {
		t.Fatalf("derived capacity did not balance final load: %#v", got)
	}
	if plan.IncrementalCapacityForcedChoiceCount == 0 {
		t.Fatalf("expected capacity to force at least one hot-chain move: %#v", plan)
	}
}

func TestMetaTrackV650NewRunResetsIncrementalHistory(t *testing.T) {
	shards := []string{"s0", "s1"}
	key := metaTrackV650TestKeyForShard(t, "s0", shards)
	planner := metaTrackV650Planner()
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	first := metaTrackV650AssignVersions([]WorkloadRecord{
		{Index: 0, LogicalID: "run1-a", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite}}},
		{Index: 1, LogicalID: "run1-b", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite}}},
	})
	planner.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: first, ShardIDs: shards, Sharding: sharding, ExpectedTransactionCount: 2})
	if planner.incrementalV650 == nil || planner.incrementalV650.Processed != 2 {
		t.Fatalf("first run history missing: %#v", planner.incrementalV650)
	}

	second := metaTrackV650AssignVersions([]WorkloadRecord{
		{Index: 0, LogicalID: "run2-a", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite}}},
	})
	plan := planner.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: second, ShardIDs: shards, Sharding: sharding, ExpectedTransactionCount: 1})
	if plan.IncrementalHistoryTransactionCountBefore != 0 {
		t.Fatalf("new run inherited previous history count=%d", plan.IncrementalHistoryTransactionCountBefore)
	}
	if planner.incrementalV650.Processed != 1 {
		t.Fatalf("new run processed=%d want=1", planner.incrementalV650.Processed)
	}
}

func TestMetaTrackV650DisabledPreservesLegacyPlanner(t *testing.T) {
	shards := []string{"s0", "s1"}
	key := metaTrackV650TestKeyForShard(t, "s0", shards)
	records := metaTrackV650AssignVersions([]WorkloadRecord{{Index: 0, LogicalID: "legacy", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite}}}})
	planner := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"control_policy": metaTrackDeclaredAccessFrontierPolicy})}
	plan := planner.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: shards, Sharding: builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}, ExpectedTransactionCount: 1})
	if plan.IncrementalRoutingPolicy != "" {
		t.Fatalf("legacy planner unexpectedly activated v650: %#v", plan)
	}
	if plan.PlacementPolicy != "frequency_coaccess_admissible_v2" || plan.TransactionPolicy != "majority_place_queue_tie_v1" {
		t.Fatalf("legacy policy changed: %#v", plan)
	}
}
