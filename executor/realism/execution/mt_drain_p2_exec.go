package execution

import (
	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
)

// MTDrainP2EmptyResult is MBE's *zero-user-transaction* execution adapter.
// It never runs for a block with a user transaction and does not modify any
// Serial or Block-STM scheduling, validation, or materialization algorithm.
// The caller must supply the already authenticated post-system-delta view.
// A private StateUpdates copy preserves the original execution.Result contract.
func MTDrainP2EmptyResult(b block.Block, view map[string]string, certified *state.Commitment, engineID string, workers int, schedulerMode, dependencyWaitMode string) (Result, bool) {
	if certified == nil || len(b.TxList) != 0 || len(b.SystemStateDeltas) == 0 {
		return Result{}, false
	}
	if engineID != SerialBlockExecutorID && engineID != BlockSTMExecutorID {
		return Result{}, false
	}
	updates := make(map[string]string, len(view))
	for key, value := range view {
		updates[key] = value
	}
	root := certified.Root()
	result := Result{
		BlockHash:        b.BlockHash,
		Height:           b.Height,
		StateRootBefore:  root,
		StateRootAfter:   root,
		ReceiptRoot:      ReceiptRoot(nil),
		StateUpdates:     updates,
		StateDelta:       make([]StateUpdate, 0),
		Deterministic:    true,
		EVMExecution:     false,
		FabricExecution:  false,
		StateRootVersion: state.CommitmentVersion,
	}
	declared := declaredAccessSet(b.TxList)
	if engineID == SerialBlockExecutorID {
		plan := buildSerialPlan(b, declared)
		result.BlockExecutorID = SerialBlockExecutorID
		result.ExecutorVersion = SerialBlockExecutorVersion
		result.WorkerCount = 1
		result.Plan = plan
		result.PlanDigest = plan.PlanDigest
		return result, true
	}
	if workers < 1 {
		workers = 1
	}
	plan := buildBlockSTMPlan(b, declared, workers)
	result.BlockExecutorID = BlockSTMExecutorID
	result.ExecutorVersion = BlockSTMExecutorVersion
	result.WorkerCount = workers
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	result.SerialEquivalent = true
	result.BlockSTMMetrics = BlockSTMMetrics{
		WorkerCount:          workers,
		SchedulerMode:        schedulerMode,
		DependencyWaitMode:   dependencyWaitMode,
		IncarnationHistogram: map[int]int{},
	}
	return result, true
}
