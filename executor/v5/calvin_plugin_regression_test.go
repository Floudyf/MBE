package v5

import (
	"errors"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestCalvinRoutingCapabilitiesStayMethodScoped(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("routing", calvinStatefulRoutingID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	stateful := plugin.(RoutingPlugin)
	if _, ok := stateful.(RoutingRuntimeCapabilities); ok {
		t.Fatal("Stateful Calvin must not activate generic stateless routing capabilities")
	}
	if mode := crossShardFinalityMode(stateful); mode != CrossShardFinalityCalvinGlobalCommit {
		t.Fatalf("stateful Calvin finality mode=%q", mode)
	}

	plugin, err = registry.Create("routing", calvinStatelessRoutingID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	stateless := plugin.(RoutingPlugin)
	if _, ok := stateless.(BatchRoutingPlugin); !ok {
		t.Fatal("Stateless Calvin must bind deterministic execution/state-home metadata")
	}
	capability, ok := stateless.(RoutingRuntimeCapabilities)
	if !ok {
		t.Fatal("Stateless Calvin routing capabilities missing")
	}
	if !capability.StatelessDirectExecution() || !capability.BindExecutionRoutingMetadata() {
		t.Fatal("Stateless Calvin must enable generic remote-home metadata")
	}
	if capability.NativeVersionedStateReady() || capability.StatelessVersionAdmission() || capability.BindBatchProjectionMetadata() {
		t.Fatal("Stateless Calvin must not activate MetaTrack native StateReady/admission/projection semantics")
	}
	if mode := crossShardFinalityMode(stateless); mode != CrossShardFinalityCalvinGlobalCommit {
		t.Fatalf("stateless Calvin finality mode=%q", mode)
	}
}

func TestStatelessCalvinRoutingUsesSignedAccessListNotSchedulingAccessList(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("routing", calvinStatelessRoutingID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	planner := plugin.(BatchRoutingPlugin)
	plan := planner.PlanBatch(BatchRoutingInput{
		Records: []WorkloadRecord{{
			Index:                0,
			SourceShard:          "s0",
			StateKeys:            []string{"signed-key"},
			AccessList:           []tx.AccessItem{{Key: "signed-key", Mode: tx.AccessReadWrite}},
			SchedulingAccessList: []tx.AccessItem{{Key: "scheduling-only-decoy", Mode: tx.AccessReadWrite}},
		}},
		ShardIDs: []string{"s0", "s1"},
		Sharding: builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)},
	})
	seenSigned, seenDecoy := false, false
	for _, row := range plan.AccessMatrix {
		seenSigned = seenSigned || row.Key == "signed-key"
		seenDecoy = seenDecoy || row.Key == "scheduling-only-decoy"
	}
	if !seenSigned || seenDecoy {
		t.Fatalf("Stateless Calvin state planning must use signed AccessList only: signed=%v decoy=%v", seenSigned, seenDecoy)
	}
}

func TestCalvinStateStorageUsesExecutionShardIdentity(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("state_storage", calvinStateStorageID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := plugin.(ExecutionShardStorageIdentityCapability)
	if !ok || !capability.UseExecutionShardStorageIdentity() {
		t.Fatal("Calvin state storage must persist by execution-shard identity")
	}
}

func TestCalvinNo2PCCoordinatorDisablesLegacyRelay(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("cross_shard", calvinCrossPartitionID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if plugin.(CrossShardPlugin).IsCrossShard(calvinTestTx("x")) {
		t.Fatal("Calvin must not activate MBE Relay/Finalize")
	}
}

func TestCalvinStorageCapabilityDoesNotChangeDefaultStateStore(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("state_storage", "persistent_local_state_store", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plugin.(ExecutionShardStorageIdentityCapability); ok {
		t.Fatal("default persistent state store must retain historical ShardID storage semantics")
	}
}

func TestCalvinFinalityCapabilityDoesNotChangeHashRouting(t *testing.T) {
	registry := BuiltinRegistry()
	plugin, err := registry.Create("routing", "hash_routing_baseline", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if got := crossShardFinalityMode(plugin.(RoutingPlugin)); got != CrossShardFinalityLegacyRelay {
		t.Fatalf("hash routing finality changed: %q", got)
	}
}

func TestCalvinRoutingPreservesLogicalSourceShard(t *testing.T) {
	registry := BuiltinRegistry()
	for _, id := range []string{calvinStatefulRoutingID, calvinStatelessRoutingID} {
		plugin, err := registry.Create("routing", id, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		decision := plugin.(RoutingPlugin).Route(RoutingInput{SourceShard: "s1", ShardIDs: []string{"s0", "s1"}})
		if decision.ShardID != "s1" {
			t.Fatalf("%s collapsed logical workload route: got=%q want=s1", id, decision.ShardID)
		}
	}
}

func TestCalvinCorrectnessErrorsAreDeterministicExecutionFailures(t *testing.T) {
	for _, message := range []string{
		"calvin_access_violation: undeclared read",
		"calvin active-participant outcome mismatch",
	} {
		if !isDeterministicExecutionError(errors.New(message)) {
			t.Fatalf("Calvin correctness error was not classified deterministic: %s", message)
		}
	}
}
