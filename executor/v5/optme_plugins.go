package v5

import (
	"fmt"

	"metaverse-chainlab/executor/realism/tx"
)

const (
	optmeExecutionID         = "optme_execution"
	optmeSchedulerID         = "optme_scheduler"
	optmeStatefulExecutorID  = "optme_block_executor"
	optmeStatelessExecutorID = "stateless_optme_block_executor"
	optmeStatelessRoutingID  = "stateless_optme_routing"
	optmeStatefulMode        = "stateful"
	optmeStatelessMode       = "stateless"
	optmeEngineVersion       = "1.1.0"
)

type optmeExecution struct{ basicPlugin }

func (p optmeExecution) Classify(item tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "optme", Reason: "post_consensus_simulate_schedule_commit"}
}

type optmeScheduler struct{ basicPlugin }

func (p optmeScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p optmeScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	out := ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
	for _, item := range items {
		out.Events = append(out.Events, ScheduleEvent{TxID: item.TxID, Track: "optme", QueueName: "consensus_output", DecisionReason: "optme_schedule_is_built_from_observed_post_consensus_rw_sets", LocalExecution: true})
	}
	return out
}

// MBE_OPTME_V20_PLUGIN_PROFILE: the scheduler owns the paper algorithm; MBE
// supplies worker_count as the execution-resource dimension.
func (p optmeScheduler) BuildObservedSchedule(observed []optmeObservedTx, workers int) optmeSchedule {
	return buildOptmeScheduleWithWorkers(observed, workers)
}

type optmeObservedScheduleProvider interface {
	BuildObservedSchedule([]optmeObservedTx, int) optmeSchedule
}

type statelessOptmeRouting struct{ basicPlugin }

func (p statelessOptmeRouting) Route(input RoutingInput) RoutingDecision {
	d := hashRouting{p.basicPlugin}.Route(input)
	d.Reason = "stateless_optme_source_or_state_hash_execution"
	return d
}
func (p statelessOptmeRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	plan := statelessHashRouting{basicPlugin: p.basicPlugin}.PlanBatch(input)
	plan.PlacementPolicy = "stateless_optme_state_home_hash_v1"
	plan.TransactionPolicy = "stateless_optme_source_or_state_hash_v1"
	for i := range plan.TransactionPlacements {
		plan.TransactionPlacements[i].Reason = "stateless_optme_execution_home"
	}
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}
func (p statelessOptmeRouting) StatelessDirectExecution() bool     { return true }
func (p statelessOptmeRouting) BatchRoutingArtifactFamily() string { return "generic_stateless" }

// Stateless-OptME v22 keeps execution routing metadata for placement/projection
// identity only. Client/source order never creates transaction StateVersions;
// execution uses one consensus-bound H-1 block-start projection and the
// author-source OptME scheduler remains authoritative.
func (p statelessOptmeRouting) BindExecutionRoutingMetadata() bool { return true }
func (p statelessOptmeRouting) BindBatchProjectionMetadata() bool  { return false }
func (p statelessOptmeRouting) BatchExecutionPlanAlgorithmID() string {
	return "stateless_optme_projection_v1"
}
func (p statelessOptmeRouting) SignedBatchExecutionPlan() bool  { return false }
func (p statelessOptmeRouting) NativeVersionedStateReady() bool { return false }

// MBE_OPTME_V22_GLOBAL_ORDER_PROJECTION
func (p statelessOptmeRouting) StatelessVersionAdmission() bool { return false }

func registerOptMEPlugins(register func(string, string, Factory)) {
	registerOptMEV22Plugins(register)
	register("execution", optmeExecutionID, func(c map[string]any) (Plugin, error) {
		return optmeExecution{makeBasic("execution", optmeExecutionID, c)}, nil
	})
	register("scheduler", optmeSchedulerID, func(c map[string]any) (Plugin, error) {
		return optmeScheduler{makeBasic("scheduler", optmeSchedulerID, c)}, nil
	})
	register("block_executor", optmeStatefulExecutorID, func(c map[string]any) (Plugin, error) {
		return optmeBlockExecutor{basicPlugin: makeBasic("block_executor", optmeStatefulExecutorID, c), mode: optmeStatefulMode}, nil
	})
	register("block_executor", optmeStatelessExecutorID, func(c map[string]any) (Plugin, error) {
		return optmeBlockExecutor{basicPlugin: makeBasic("block_executor", optmeStatelessExecutorID, c), mode: optmeStatelessMode}, nil
	})
	register("routing", optmeStatelessRoutingID, func(c map[string]any) (Plugin, error) {
		return statelessOptmeRouting{makeBasic("routing", optmeStatelessRoutingID, c)}, nil
	})
}

func validateOptMEPluginCombination(p RuntimePlugins) error {
	executionID, schedulerID, executorID, routingID := "", "", "", ""
	if p.Execution != nil {
		executionID = p.Execution.ID()
	}
	if p.Scheduler != nil {
		schedulerID = p.Scheduler.ID()
	}
	if p.BlockExecutor != nil {
		executorID = p.BlockExecutor.ID()
	}
	if p.Routing != nil {
		routingID = p.Routing.ID()
	}
	selected := executionID == optmeExecutionID || schedulerID == optmeSchedulerID ||
		executorID == optmeStatefulExecutorID || executorID == optmeStatelessExecutorID ||
		routingID == optmeStatefulRoutingID || routingID == optmeStatelessRoutingID
	if !selected {
		return nil
	}
	if executionID != optmeExecutionID {
		return fmt.Errorf("OptME requires execution:%s", optmeExecutionID)
	}
	if schedulerID != optmeSchedulerID {
		return fmt.Errorf("OptME requires scheduler:%s", optmeSchedulerID)
	}
	stateless := executorID == optmeStatelessExecutorID || routingID == optmeStatelessRoutingID
	if stateless {
		if executorID != optmeStatelessExecutorID || routingID != optmeStatelessRoutingID {
			return fmt.Errorf("Stateless-OptME requires routing:%s and block_executor:%s", optmeStatelessRoutingID, optmeStatelessExecutorID)
		}
	} else if executorID != optmeStatefulExecutorID || routingID != optmeStatefulRoutingID {
		return fmt.Errorf("OptME requires routing:%s and block_executor:%s", optmeStatefulRoutingID, optmeStatefulExecutorID)
	}
	storageID := "persistent_local_state_store"
	if stateless {
		storageID = optmePartitionStateStorageID
	}
	required := []struct {
		category string
		actual   Plugin
		id       string
	}{
		{"transaction_admission", p.Admission, "signature_nonce_admission"},
		{"txpool", p.TxPool, "fifo_per_node_mempool"},
		{"sharding", p.Sharding, "deterministic_state_key_sharding"},
		{"block_producer", p.BlockProducer, "time_or_count_block_producer"},
		{"consensus", p.Consensus, "pbft_style_consensus"},
		{"network", p.Network, "localhost_tcp_typed_network"},
		{"state_access", p.StateAccess, "direct_state_access"},
		{"state_storage", p.StateStorage, storageID},
		{"cross_shard", p.CrossShard, optmeGlobalCrossShardID},
		{"commit", p.Commit, "normal_commit"},
		{"metrics", p.Metrics, "runtime_core_metrics"},
		{"observability", p.Observability, "node_network_consensus_observer"},
	}
	for _, item := range required {
		if item.actual == nil || item.actual.ID() != item.id {
			actual := "<nil>"
			if item.actual != nil {
				actual = item.actual.ID()
			}
			return fmt.Errorf("OptME requires %s:%s, got %s", item.category, item.id, actual)
		}
	}
	return nil
}
