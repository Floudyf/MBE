package v5

import "testing"

func TestPorygonV10NodePlanSeparatesExecutionShardFromConsensusDomain(t *testing.T) {
	node := NodePlan{NodeID: "n4", ShardID: "porygon-global", ExecutionShardID: "s1", ConsensusDomainID: "porygon-global"}
	if got := effectiveExecutionShardID(node); got != "s1" {
		t.Fatalf("execution shard=%s", got)
	}
	if got := effectiveConsensusDomainID(node); got != "porygon-global" {
		t.Fatalf("consensus domain=%s", got)
	}
	legacy := NodePlan{NodeID: "n0", ShardID: "s0"}
	if effectiveExecutionShardID(legacy) != "s0" || effectiveConsensusDomainID(legacy) != "s0" {
		t.Fatal("legacy fallback changed")
	}
}

func TestPorygonV10RoutingUsesFrontendExecutionShardSet(t *testing.T) {
	p := porygonStatelessRouting{makeBasic("routing", porygonRoutingID, nil)}
	decision := p.Route(RoutingInput{Index: 1, StateKeys: []string{"key-a"}, ShardIDs: []string{"s0", "s1"}, Sharding: builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}})
	if decision.ShardID != "s0" && decision.ShardID != "s1" {
		t.Fatalf("route=%q", decision.ShardID)
	}
	if decision.Reason != "porygon_execution_shard_route" {
		t.Fatalf("reason=%q", decision.Reason)
	}
}

func TestPorygonV10PlanAlgorithmVersion(t *testing.T) {
	if porygonPlanAlgorithmID != "porygon_3d_parallelism_plan_v3" {
		t.Fatalf("plan id=%s", porygonPlanAlgorithmID)
	}
}
