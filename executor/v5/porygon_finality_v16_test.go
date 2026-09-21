package v5

import "testing"

func TestPorygonV16FinalityCapabilityIsIndependentFromRuntimeControlPlane(t *testing.T) {
	routing := porygonStatelessRouting{makeBasic("routing", porygonRoutingID, nil)}
	var plugin RoutingPlugin = routing
	if got := crossShardFinalityMode(plugin); got != CrossShardFinalityPorygonGlobalCommit {
		t.Fatalf("finality mode=%q", got)
	}
	if !usesDirectCommitFinality(plugin) {
		t.Fatal("Porygon durable global commit must be terminal")
	}
	if usesStatelessDirectExecution(plugin) {
		t.Fatal("Porygon must not regain RoutingRuntimeCapabilities")
	}
	if _, ok := plugin.(RoutingRuntimeCapabilities); ok {
		t.Fatal("Porygon must not implement RoutingRuntimeCapabilities")
	}
	if _, ok := plugin.(BatchRoutingPlugin); ok {
		t.Fatal("Porygon must not implement BatchRoutingPlugin")
	}
}

func TestPorygonV16PlanFinalityModeUsesGlobalDurableCommit(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{{PluginProfile: map[string]PluginConfig{
		"routing": {PluginID: porygonRoutingID, Config: map[string]any{}},
	}}}}
	if got := PlanCrossShardFinalityMode(plan); got != CrossShardFinalityPorygonGlobalCommit {
		t.Fatalf("plan finality mode=%q", got)
	}
	if !PlanUsesDirectCommitFinality(plan) {
		t.Fatal("Porygon plan must use direct durable-commit finality")
	}
}

func TestPorygonV16PreservesExistingFinalityModes(t *testing.T) {
	stateless := statelessHashRouting{basicPlugin: makeBasic("routing", "stateless_hash_routing", nil)}
	if got := crossShardFinalityMode(stateless); got != CrossShardFinalityStatelessDirect {
		t.Fatalf("stateless mode=%q", got)
	}
	legacy := hashRouting{basicPlugin: makeBasic("routing", "hash_routing_baseline", nil)}
	if got := crossShardFinalityMode(legacy); got != CrossShardFinalityLegacyRelay {
		t.Fatalf("legacy mode=%q", got)
	}
}
