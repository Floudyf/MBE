package v5

import "testing"

func TestRoutingRuntimeCapabilitiesReplaceCorePluginIDDispatch(t *testing.T) {
	registry := BuiltinRegistry()
	statelessPlugin, err := registry.Create("routing", "stateless_hash_routing", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	stateless, ok := statelessPlugin.(RoutingPlugin)
	if !ok || !usesStatelessDirectExecution(stateless) {
		t.Fatalf("stateless capability missing: %T", statelessPlugin)
	}
	if !routingBindsExecutionMetadata(stateless) || routingBindsBatchProjectionMetadata(stateless) {
		t.Fatal("stateless metadata capability mismatch")
	}
	if got := batchExecutionPlanAlgorithmIDForRouting(stateless); got != "stateless_hash_batch_execution_plan_v1" {
		t.Fatalf("unexpected stateless plan id: %s", got)
	}
	if !routingUsesStatelessVersionAdmission(stateless) || routingUsesNativeVersionedStateReady(stateless) {
		t.Fatal("stateless version capability mismatch")
	}

	metaPlugin, err := registry.Create("routing", "metatrack_coaccess_routing", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	meta, ok := metaPlugin.(RoutingPlugin)
	if !ok || !usesStatelessDirectExecution(meta) {
		t.Fatalf("MetaTrack capability missing: %T", metaPlugin)
	}
	if !routingBindsExecutionMetadata(meta) || !routingBindsBatchProjectionMetadata(meta) {
		t.Fatal("MetaTrack metadata capability mismatch")
	}
	if got := batchExecutionPlanAlgorithmIDForRouting(meta); got != "metatrack_batch_execution_plan_v1" {
		t.Fatalf("unexpected MetaTrack plan id: %s", got)
	}
	if !routingUsesSignedBatchExecutionPlan(meta) || !routingUsesNativeVersionedStateReady(meta) {
		t.Fatal("MetaTrack signed-plan/state-ready capability mismatch")
	}
	if routingUsesStatelessVersionAdmission(meta) {
		t.Fatal("MetaTrack must not use stateless-hash version admission")
	}
}
