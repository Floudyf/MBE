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
	optmeEngineVersion       = "1.0.0"
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
func (p optmeScheduler) BuildObservedSchedule(observed []optmeObservedTx) optmeSchedule {
	return buildOptmeSchedule(observed)
}

type optmeObservedScheduleProvider interface {
	BuildObservedSchedule([]optmeObservedTx) optmeSchedule
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

// Stateless-OptME keeps OptME scheduling post-consensus, but the MBE stateless
// substrate binds an algorithm-agnostic global exact-version chain for state
// transport. Runtime must not replace OptME with the generic versioned-wave
// executor; the metadata is consumed only by remote fetch/writeback/admission.
func (p statelessOptmeRouting) BindExecutionRoutingMetadata() bool { return true }
func (p statelessOptmeRouting) BindBatchProjectionMetadata() bool  { return false }
func (p statelessOptmeRouting) BatchExecutionPlanAlgorithmID() string {
	return "stateless_optme_projection_v1"
}
func (p statelessOptmeRouting) SignedBatchExecutionPlan() bool  { return false }
func (p statelessOptmeRouting) NativeVersionedStateReady() bool { return false }
func (p statelessOptmeRouting) StatelessVersionAdmission() bool { return true }

func registerOptMEPlugins(register func(string, string, Factory)) {
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
	selected := executionID == optmeExecutionID || schedulerID == optmeSchedulerID || executorID == optmeStatefulExecutorID || executorID == optmeStatelessExecutorID || routingID == optmeStatelessRoutingID
	if !selected {
		return nil
	}
	stateless := executorID == optmeStatelessExecutorID || routingID == optmeStatelessRoutingID
	stateful := executorID == optmeStatefulExecutorID || (selected && routingID == "hash_routing_baseline")
	if stateless && stateful && executorID == optmeStatefulExecutorID {
		return fmt.Errorf("OptME stateful and stateless profiles may not be mixed")
	}
	if executionID != optmeExecutionID {
		return fmt.Errorf("OptME requires execution:%s", optmeExecutionID)
	}
	if schedulerID != optmeSchedulerID {
		return fmt.Errorf("OptME requires scheduler:%s", optmeSchedulerID)
	}
	if stateless {
		if executorID != optmeStatelessExecutorID || routingID != optmeStatelessRoutingID {
			return fmt.Errorf("Stateless-OptME requires routing:%s and block_executor:%s", optmeStatelessRoutingID, optmeStatelessExecutorID)
		}
	} else {
		if executorID != optmeStatefulExecutorID || routingID != "hash_routing_baseline" {
			return fmt.Errorf("OptME requires routing:hash_routing_baseline and block_executor:%s", optmeStatefulExecutorID)
		}
	}
	if p.Consensus == nil || p.Consensus.ID() != "pbft_style_consensus" {
		return fmt.Errorf("OptME requires the shared PBFT consensus plugin")
	}
	if p.BlockProducer == nil || p.BlockProducer.ID() != "time_or_count_block_producer" {
		return fmt.Errorf("OptME is post-consensus and requires the shared time_or_count_block_producer")
	}
	if p.Commit == nil || p.Commit.ID() != "normal_commit" {
		return fmt.Errorf("OptME requires normal_commit")
	}
	return nil
}
