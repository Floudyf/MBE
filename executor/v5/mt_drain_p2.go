package v5

import (
	"context"
	"fmt"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
)

// mtDrainP2Eligible is only for the original serial/Block-STM block-executor
// adapters, not their normal user transaction processing paths. Exact-version
// watches, MetaTrack StateReady, TxAllo projections, OptME, Calvin and Porygon
// retain their original execution behavior.
func (r *NodeRuntime) mtDrainP2Eligible(b realblock.Block, certified *state.Commitment, historical bool) bool {
	if r == nil || r.plugins.BlockExecutor == nil ||
		r.txalloStatefulReplicaEnabled() || r.optmeStatelessProjectionEnabled() ||
		r.statelessCalvinRemoteStateEnabled() || r.nativeMetaTrackStateReadyEnabled() ||
		!mtSystemDrainCertifiedCommitmentV1(b, r.plugins.BlockExecutor.ID(), historical, certified) {
		return false
	}
	switch adapter := r.plugins.BlockExecutor.(type) {
	case serialBlockExecutor:
		return true
	case blockSTMBlockExecutor:
		// Correctness and serial-oracle modes keep the existing full executor.
		return strings.TrimSpace(fmt.Sprint(adapter.config["execution_mode"])) == "performance" &&
			strings.TrimSpace(fmt.Sprint(adapter.config["oracle_mode"])) == "off"
	default:
		return false
	}
}

func (r *NodeRuntime) mtDrainP2Execute(ctx context.Context, b realblock.Block, view map[string]string, certified *state.Commitment) (BlockExecutionResult, error) {
	id := r.plugins.BlockExecutor.ID()
	workers := 1
	schedulerMode := ""
	waitMode := ""
	switch adapter := r.plugins.BlockExecutor.(type) {
	case serialBlockExecutor:
		workers = configuredWorkerCount(adapter.config, blockExecutorWorkerCountFromProfile(r.pluginSnapshot))
	case blockSTMBlockExecutor:
		if err := ctx.Err(); err != nil {
			return BlockExecutionResult{}, err
		}
		workers = configuredWorkerCount(adapter.config, blockExecutorWorkerCountFromProfile(r.pluginSnapshot))
		schedulerMode = "legacy_scan_v1"
		waitMode = "abort_new_incarnation_v1"
		if s := strings.TrimSpace(fmt.Sprint(adapter.config["scheduler_mode"])); s == "priority_heap_v1" || s == "legacy_scan_v1" {
			schedulerMode = s
		}
		if s := strings.TrimSpace(fmt.Sprint(adapter.config["dependency_wait_mode"])); s == "suspend_same_incarnation_v1" || s == "abort_new_incarnation_v1" {
			waitMode = s
		}
	default:
		return BlockExecutionResult{}, fmt.Errorf("MTDrainP2 unsupported block executor %s", id)
	}
	result, ok := execution.MTDrainP2EmptyResult(b, view, certified, id, workers, schedulerMode, waitMode)
	if !ok {
		return BlockExecutionResult{}, fmt.Errorf("MTDrainP2 unexpected ineligible system-only block at height %d", b.Height)
	}
	out := BlockExecutionResult{
		ExecutionResult:        result,
		StateDelta:             make([]state.StateKV, 0),
		PlanDigest:             result.PlanDigest,
		WorkerCount:            workers,
		TransactionExecutionMS: result.TransactionExecutionMS,
		DeterministicApplyMS:   result.DeterministicMaterializationMS,
		StateCommitmentMS:      result.StateCommitmentMS,
		StateRootVersion:       result.StateRootVersion,
	}
	if _, isSTM := r.plugins.BlockExecutor.(blockSTMBlockExecutor); isSTM {
		out.ActualMetrics = map[string]any{}
	}
	return out, nil
}
