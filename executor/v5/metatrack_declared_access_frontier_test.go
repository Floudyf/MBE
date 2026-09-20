package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackDeclaredAccessFrontierV2PreservesOriginalCoaccessPlacement(t *testing.T) {
	shards := []string{"s0", "s1"}
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	records := []WorkloadRecord{
		{
			Index: 0, LogicalID: "tx0", RoutingOrdinal: 1,
			AccessList: []tx.AccessItem{
				{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
				{Key: "k2", Mode: tx.AccessRead, UpdateSemantics: "none"},
			},
			SchedulingAccessList: []tx.AccessItem{
				{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
				{Key: "k2", Mode: tx.AccessRead, UpdateSemantics: "none"},
			},
			StateVersions: []tx.StateVersionDependency{
				{Key: "k1", ProducedVersion: 1},
				{Key: "k2"},
			},
		},
		{
			Index: 1, LogicalID: "tx1", RoutingOrdinal: 2,
			AccessList: []tx.AccessItem{
				{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
				{Key: "k3", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
			},
			SchedulingAccessList: []tx.AccessItem{
				{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
				{Key: "k3", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
			},
			StateVersions: []tx.StateVersionDependency{
				{Key: "k1", RequiredVersion: 1, ProducedVersion: 2},
				{Key: "k3", ProducedVersion: 2},
			},
		},
	}

	legacy := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"routing_epoch": 0})}
	strict := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"routing_epoch": 0, "control_policy": metaTrackDeclaredAccessFrontierPolicy})}

	legacyPlan := legacy.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: shards, Sharding: sharding})
	strictPlan := strict.PlanBatch(BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: shards, Sharding: sharding})

	if strictPlan.ControlPolicy != metaTrackDeclaredAccessFrontierPolicy {
		t.Fatalf("strict control policy = %q", strictPlan.ControlPolicy)
	}
	if len(legacyPlan.StatePlacements) != len(strictPlan.StatePlacements) {
		t.Fatalf("state placement count changed: legacy=%d strict=%d", len(legacyPlan.StatePlacements), len(strictPlan.StatePlacements))
	}
	for i := range legacyPlan.StatePlacements {
		a, b := legacyPlan.StatePlacements[i], strictPlan.StatePlacements[i]
		if a.Key != b.Key || a.ExecutionShard != b.ExecutionShard || a.HomeShard != b.HomeShard || a.Reason != b.Reason {
			t.Fatalf("state placement changed at %d: legacy=%+v strict=%+v", i, a, b)
		}
	}
	if len(legacyPlan.TransactionPlacements) != len(strictPlan.TransactionPlacements) {
		t.Fatalf("transaction placement count changed")
	}
	for i := range legacyPlan.TransactionPlacements {
		a, b := legacyPlan.TransactionPlacements[i], strictPlan.TransactionPlacements[i]
		if a.ExecutionShard != b.ExecutionShard || a.PredictedRemoteReads != b.PredictedRemoteReads || a.PredictedRemoteWrites != b.PredictedRemoteWrites {
			t.Fatalf("transaction routing changed at %d: legacy=%+v strict=%+v", i, a, b)
		}
		if b.FrontierDigest == "" {
			t.Fatalf("strict frontier digest missing at %d", i)
		}
		if len(b.LogicalDomains) != 0 || b.Local || b.Bridge {
			t.Fatalf("declared-access frontier leaked logical-domain admission state: %+v", b)
		}
	}
}

func TestDualTrackDeclaredAccessFrontierV2DoesNotUseLocationAsAdmissionGate(t *testing.T) {
	item := tx.SignedTransaction{
		TxID: "tx-fast",
		AccessList: []tx.AccessItem{
			{Key: "remote:k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
		},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
			ExecutionShard: "s0",
			FrontierDigest: "signed-frontier",
		},
	}
	decision := (dualTrackExecution{}).Classify(item)
	if decision.Track != "fast" {
		t.Fatalf("location/remote access must not force conservative track: %+v", decision)
	}
}

func TestMetaTrackStrictAdmissionChecksRuntimeAndSchedulingDeclarations(t *testing.T) {
	p := metaTrackStrictAdmission{}
	good := tx.SignedTransaction{
		AccessList: []tx.AccessItem{
			{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
			{Key: "k2", Mode: tx.AccessRead, UpdateSemantics: "none"},
		},
		SchedulingAccessList: []tx.AccessItem{
			{Key: "k1", Mode: tx.AccessUnknown, UpdateSemantics: "unknown"},
			{Key: "k2", Mode: tx.AccessRead, UpdateSemantics: "none"},
		},
	}
	if err := p.validateDeclaredAccess(good); err != nil {
		t.Fatalf("valid runtime/scheduling declaration rejected: %v", err)
	}
	bad := good
	bad.SchedulingAccessList = []tx.AccessItem{
		{Key: "k1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
		{Key: "k3", Mode: tx.AccessRead, UpdateSemantics: "none"},
	}
	if err := p.validateDeclaredAccess(bad); err == nil {
		t.Fatal("mismatched runtime/scheduling key sets must be rejected")
	}
}

func TestMetaTrackDeclaredAccessFrontierBindingRoundTrip(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:        "tx-bind",
		LogicalTxID: "logical-bind",
		AccessList: []tx.AccessItem{
			{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
		},
		SchedulingAccessList: []tx.AccessItem{
			{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"},
		},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 1,
			ExecutionShard: "s0",
			ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
			StateVersions:  []tx.StateVersionDependency{{Key: "k", ProducedVersion: 1}},
		},
	}
	record := WorkloadRecord{
		Index: 0, LogicalID: item.LogicalTxID, RoutingOrdinal: 1,
		AccessList: item.AccessList, SchedulingAccessList: item.SchedulingAccessList,
		StateVersions: item.ExecutionRouting.StateVersions,
	}
	item.ExecutionRouting.FrontierDigest = metaTrackDeclaredAccessFrontierDigest(record, "s0")
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err != nil {
		t.Fatalf("binding validation failed: %v", err)
	}
}

func TestMetaTrackRequiredVersionTicketCountCountsActualRequirements(t *testing.T) {
	item := tx.SignedTransaction{
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			StateVersions: []tx.StateVersionDependency{
				{Key: "a", RequiredVersion: 0, ProducedVersion: 1},
				{Key: "b", RequiredVersion: 1, ProducedVersion: 2},
				{Key: "c", RequiredVersion: 2},
			},
		},
	}
	if got := metaTrackRequiredVersionTicketCount(item); got != 2 {
		t.Fatalf("required ticket count=%d want=2", got)
	}
}
