package v5

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackLogicalStateUnitsSeparateStateOwnershipFromExecutionShards(t *testing.T) {
	routing := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{
		"state_storage_unit_count": 8,
		"placement_mu":             2.5,
		"placement_min_budget":     7,
	})}
	shards := []string{"s0", "s1"}
	keys := []string{"asset:a", "asset:b", "asset:c", "asset:d"}
	units := map[string]bool{}
	for _, key := range keys {
		home := routing.LogicalStateHome(key, builtinSharding{}, shards)
		if home.UnitCount != 8 || home.StateUnitID == "" {
			t.Fatalf("logical state home missing for %s: %#v", key, home)
		}
		if home.ServingShard != "s0" && home.ServingShard != "s1" {
			t.Fatalf("logical state unit is not co-located on a real serving shard: %#v", home)
		}
		units[home.StateUnitID] = true
	}
	if len(units) <= len(shards) {
		t.Fatalf("explicit S/P separation did not expose more logical state units than execution shards: units=%v shards=%v", units, shards)
	}

	records := make([]WorkloadRecord, 0, len(keys))
	for index, key := range keys {
		records = append(records, WorkloadRecord{Index: index, LogicalID: key, AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}})
	}
	plan := routing.PlanBatch(BatchRoutingInput{BatchIndex: 9, Records: records, ShardIDs: shards, Sharding: builtinSharding{}})
	if plan.StateStorageUnitCount != 8 || plan.PlacementMinBudget != 7 || plan.PlacementMu != "2.5" {
		t.Fatalf("paper-model routing parameters were not propagated: %#v", plan)
	}
	for _, placement := range plan.StatePlacements {
		home := routing.LogicalStateHome(placement.Key, builtinSharding{}, shards)
		if placement.HomeStateUnit != home.StateUnitID || placement.HomeShard != home.ServingShard {
			t.Fatalf("state placement lost persistent logical ownership: placement=%#v home=%#v", placement, home)
		}
	}
}

func TestMetaTrackLogicalStateUnitCompatibilityModePreservesLegacyPhysicalHome(t *testing.T) {
	routing := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", nil)}
	shards := []string{"s0", "s1", "s2"}
	for _, key := range []string{"asset:a", "nonce:alice", "contract:market"} {
		home := routing.LogicalStateHome(key, builtinSharding{}, shards)
		legacy := shardFor(builtinSharding{}, []string{key}, shards)
		if home.ServingShard != legacy {
			t.Fatalf("default paper-model compatibility changed legacy physical state home for %s: got=%s want=%s", key, home.ServingShard, legacy)
		}
		if home.UnitCount != len(shards) || home.StateUnitID == "" {
			t.Fatalf("default logical state-unit evidence missing for %s: %#v", key, home)
		}
	}
}

func TestDualTrackKnownDirectionOrdinaryWritesRemainFastWhenTopoSafe(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{TxID: "writer", AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "reader", AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessRead, UpdateSemantics: "validate"}}},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if got := result.Decisions["writer"]; got.Track != "fast" || got.Reason != "known_direction_write_access" {
		t.Fatalf("known-direction writer was incorrectly forced Conservative: %#v", got)
	}
	if got := result.Decisions["reader"]; got.Track != "fast" || !strings.Contains(got.Reason, "topo_safe_dependency") || !strings.Contains(got.Reason, "raw_dependency") {
		t.Fatalf("topologically safe dependent reader should remain Fast: %#v", got)
	}
	if len(result.Dependencies["reader"]) != 1 || result.Dependencies["reader"][0] != "writer" {
		t.Fatalf("ordinary write dependency semantics changed unexpectedly: %#v", result.Dependencies)
	}
}

func TestLogicalStateUnitProviderDoesNotChangeBaselineHomeRouting(t *testing.T) {
	shards := []string{"s0", "s1"}
	baseline := statelessHashRouting{basicPlugin: makeBasic("routing", "stateless_hash_routing", nil)}
	key := "asset:baseline"
	home := logicalStateHomeForRouting(baseline, builtinSharding{}, key, shards)
	want := shardFor(builtinSharding{}, []string{key}, shards)
	if home.StateUnitID != "" || home.ServingShard != want {
		t.Fatalf("baseline routing was changed by optional MetaTrack logical-state-unit support: %#v want=%s", home, want)
	}
}
