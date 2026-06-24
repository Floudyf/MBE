package core

import (
	"metaverse-chainlab/executor/execution_sharding"
	"metaverse-chainlab/executor/routing"
	"metaverse-chainlab/executor/state_sharding"
)

// ModuleSet is the V1.2 single-chain executor composition. Only default hash
// plugins are instantiated; later routing and execution mechanisms remain out of scope.
type ModuleSet struct {
	StateSharding     state_sharding.Locator
	ExecutionSharding execution_sharding.Assigner
	Routing           routing.Builder
}

// DefaultModuleSet constructs V0-compatible hash routing or the V1.5 co-access planner.
func DefaultModuleSet(config ReplayConfig) ModuleSet {
	return ModuleSet{
		StateSharding:     state_sharding.NewHashStateSharding(config.StateShardCount),
		ExecutionSharding: execution_sharding.NewHashExecutionSharding(config.ExecutionShardCount),
		Routing:           selectRouting(config),
	}
}
func selectRouting(config ReplayConfig) routing.Builder {
	if config.RoutingPolicy == "co_access" {
		return routing.NewCoAccessRouting(config.ExecutionShardCount, config.CoAccessMinWeight, config.CoAccessMaxGroupSize, config.CoAccessBalanceWeight)
	}
	return routing.NewHashRouting(config.ExecutionShardCount)
}
