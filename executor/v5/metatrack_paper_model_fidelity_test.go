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
		{
			TxID:       "writer",
			AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{
				RoutingOrdinal: 1,
				StateVersions:  []tx.StateVersionDependency{{Key: "object:x", RequiredVersion: 0, ProducedVersion: 1}},
			},
		},
		{
			TxID:       "reader",
			AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{
				RoutingOrdinal: 2,
				StateVersions:  []tx.StateVersionDependency{{Key: "object:x", RequiredVersion: 1}},
			},
		},
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
	if result.SCCCount != 0 || result.AmbiguousConflictPairCount != 0 {
		t.Fatalf("exact-version direction was incorrectly treated as ambiguous: %#v", result)
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

func TestDualTrackAmbiguousSignedBusinessConflictFallsBackConservative(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{
			TxID:       "writer",
			AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{
				RoutingOrdinal: 1,
			},
		},
		{
			TxID:       "reader",
			AccessList: []tx.AccessItem{{Key: "object:x", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
			ExecutionRouting: &tx.ExecutionRoutingMetadata{
				RoutingOrdinal: 2,
			},
		},
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	for _, id := range []string{"writer", "reader"} {
		got := result.Decisions[id]
		if got.Track != "conservative" || !strings.Contains(got.Reason, "ambiguous_conflict_direction:object:x") || !strings.Contains(got.Reason, "nontrivial_scc") {
			t.Fatalf("ambiguous signed business conflict must be Conservative: id=%s got=%#v result=%#v", id, got, result)
		}
	}
	if result.SCCCount != 1 || result.AmbiguousConflictPairCount != 1 {
		t.Fatalf("ambiguous topology evidence mismatch: %#v", result)
	}
	if len(result.Dependencies["reader"]) != 1 || result.Dependencies["reader"][0] != "writer" {
		t.Fatalf("classification SCC polluted execution scheduler dependencies: %#v", result.Dependencies)
	}
}

func TestDualTrackUnsupportedWriteSemanticsFallsBackConservative(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	item := tx.SignedTransaction{
		TxID: "unsupported",
		AccessList: []tx.AccessItem{{
			Key:             "object:dynamic",
			Mode:            tx.AccessReadWrite,
			UpdateSemantics: "runtime_discovered_cross_key_constraint",
		}},
	}
	got := execution.Classify(item)
	if got.Track != "conservative" || !strings.Contains(got.Reason, "unsupported_update_semantics:object:dynamic") {
		t.Fatalf("unsupported write semantics must be Conservative: %#v", got)
	}
	result := execution.ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{item}})
	if result.Decisions[item.TxID].Track != "conservative" || result.SemanticUnsafeCount != 1 {
		t.Fatalf("batch SemanticSafe gate did not reject unsupported operation: %#v", result)
	}
}

func TestDualTrackStateReadinessDoesNotChangeTrackAssignment(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	item := tx.SignedTransaction{
		TxID:       "remote-reader",
		AccessList: []tx.AccessItem{{Key: "object:remote", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 3,
			StateVersions:  []tx.StateVersionDependency{{Key: "object:remote", RequiredVersion: 2}},
		},
	}
	token := stateReadinessToken(item, item.AccessList[0])
	result := execution.ClassifyBatch(BatchClassificationInput{
		Transactions:         []tx.SignedTransaction{item},
		RemoteStateReadiness: map[string]bool{token: false},
	})
	if got := result.Decisions[item.TxID]; got.Track != "fast" {
		t.Fatalf("StateReady must remain orthogonal to Fast/Conservative assignment: %#v", got)
	}
	if len(result.StateWaitKeys[item.TxID]) != 1 || result.StateWaitKeys[item.TxID][0] != token {
		t.Fatalf("not-ready state was not recorded independently: %#v", result.StateWaitKeys)
	}
}

func TestDualTrackModeSemanticPairsAreValidated(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	cases := []struct {
		name   string
		access tx.AccessItem
	}{
		{"ordinary-write-cannot-use-validate", tx.AccessItem{Key: "k1", Mode: tx.AccessWrite, UpdateSemantics: "validate"}},
		{"ordinary-rmw-cannot-use-add", tx.AccessItem{Key: "k2", Mode: tx.AccessReadWrite, UpdateSemantics: "add", Delta: 1}},
		{"ordinary-write-cannot-claim-commutative", tx.AccessItem{Key: "k3", Mode: tx.AccessWrite, UpdateSemantics: "commutative_delta", Delta: 1}},
		{"commutative-cannot-use-set", tx.AccessItem{Key: "k4", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "set", Delta: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := execution.Classify(tx.SignedTransaction{TxID: tc.name, AccessList: []tx.AccessItem{tc.access}})
			if got.Track != "conservative" {
				t.Fatalf("invalid mode/semantic pair must be Conservative: %#v", got)
			}
		})
	}
	for _, access := range []tx.AccessItem{
		{Key: "set", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		{Key: "alien", Mode: tx.AccessReadWrite, UpdateSemantics: "alien_worlds_contract_semantic_state"},
		{Key: "delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1},
		{Key: "read", Mode: tx.AccessRead, UpdateSemantics: "validate"},
	} {
		if got := execution.Classify(tx.SignedTransaction{TxID: access.Key, AccessList: []tx.AccessItem{access}}); got.Track != "fast" {
			t.Fatalf("valid stable pair was rejected: access=%#v decision=%#v", access, got)
		}
	}
}

func TestRemoteVersionAdmissionProbeNormalizesAsFetch(t *testing.T) {
	if got := NormalizeRemoteOperationKind(statelessVersionAdmissionProbeAccessKind); got != "fetch" {
		t.Fatalf("version admission probe kind=%q want fetch", got)
	}
	if got := NormalizeRemoteOperationKind("write"); got != "fetch" {
		t.Fatalf("remote write access kind=%q want fetch", got)
	}
}

func TestMetaTrackClassificationEvidenceSummaryAggregatesBlocks(t *testing.T) {
	blocks := []map[string]any{
		{"metatrack_classification_conflict_edge_count": 4, "metatrack_classification_dependency_chain_max": 3, "metatrack_classification_nontrivial_scc_count": 1, "metatrack_classification_ambiguous_conflict_pair_count": 2, "metatrack_classification_semantic_unsafe_unique_count": 5},
		{"metatrack_classification_conflict_edge_count": 7, "metatrack_classification_dependency_chain_max": 2, "metatrack_classification_nontrivial_scc_count": 2, "metatrack_classification_ambiguous_conflict_pair_count": 1, "metatrack_classification_semantic_unsafe_unique_count": 3},
	}
	got := summarizeMetaTrackClassificationEvidence(blocks)
	if got.conflictEdgeCount != 11 || got.dependencyChainMax != 3 || got.nontrivialSCCCount != 3 || got.ambiguousConflictPairCount != 3 || got.semanticUnsafeUniqueCount != 8 {
		t.Fatalf("classification evidence aggregation mismatch: %#v", got)
	}
}
