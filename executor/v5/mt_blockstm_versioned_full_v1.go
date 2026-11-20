package v5

// MT_STM_SINGLE_CONTEXT_V1: one paper-compatible Block-STM instance owns the
// entire consensus block. StateReady gates individual transactions, not waves.
// External exact versions are bound to each transaction's base reads; internal
// dependencies remain inside the shared MVMemory and validation scheduler.

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
)

func (r *NodeRuntime) executeVersionedBlockSTMFullV1(ctx context.Context, block realblock.Block, base map[string]string, commitment *state.Commitment) (BlockExecutionResult, error) {
	if r.plugins.BlockExecutor == nil || r.plugins.BlockExecutor.ID() != "block_stm_block_executor" {
		return BlockExecutionResult{}, fmt.Errorf("single-context Block-STM requires the original block-stm executor")
	}
	started := time.Now()
	shards := r.shardIDs()
	ordinalOwner := map[uint64]int{}
	for index, item := range block.TxList {
		if item.ExecutionRouting != nil && item.ExecutionRouting.RoutingOrdinal != 0 {
			ordinal := item.ExecutionRouting.RoutingOrdinal
			if _, exists := ordinalOwner[ordinal]; exists {
				return BlockExecutionResult{}, fmt.Errorf("Block-STM duplicate routing ordinal %d", ordinal)
			}
			ordinalOwner[ordinal] = index
		}
	}
	probesByIndex := make([][]versionedStateProbe, len(block.TxList))
	internalDelegated := 0
	for index, item := range block.TxList {
		probes := versionedStateAccesses(item, shards, r.stateHomeShardForKey, r.node.ShardID)
		for _, probe := range probes {
			dep, ok := stateVersionDependencyForKey(item, probe.access.Key)
			if ok && dep.RequiredVersion != 0 {
				if producer, inside := ordinalOwner[dep.RequiredVersion]; inside {
					if producer >= index {
						return BlockExecutionResult{}, fmt.Errorf("Block-STM invalid in-block exact predecessor: consumer=%d producer=%d key=%s", index, producer, probe.access.Key)
					}
					// Internal dependencies also require the exact signed value.
					// The shared Block-STM scheduler owns MV conflicts, while the
					// stable-prefix publication supplies immutable exact aliases
					// (including successful no-op producers and historical values).
					internalDelegated++
				}
			}
			probesByIndex[index] = append(probesByIndex[index], probe)
		}
	}
	var probeCount, waitCount, readyCount, probeLatencyNS int64
	readiness := func(readyCtx context.Context, index int) (map[string]string, error) {
		if index < 0 || index >= len(probesByIndex) {
			return nil, fmt.Errorf("Block-STM readiness index out of range: %d", index)
		}
		overrides := make(map[string]string, len(probesByIndex[index]))
		// Emit one real blocked-admission event per transaction, not one
		// event per 20ms retry. The native late-ready test uses this causal
		// event to release the exact version without a timing-based sleep.
		blockedNotified := false
		for _, probe := range probesByIndex[index] {
			for {
				if err := readyCtx.Err(); err != nil {
					return nil, err
				}
				value, ready, latency, err := r.probeStateAccessOnce(readyCtx, block, probe.item, probe.access, probe.homeShard)
				atomic.AddInt64(&probeCount, 1)
				atomic.AddInt64(&probeLatencyNS, latency.Nanoseconds())
				if err != nil {
					return nil, fmt.Errorf("Block-STM external StateReady tx=%s key=%s: %w", probe.item.TxID, probe.access.Key, err)
				}
				if ready {
					// Per-transaction value, not a common block snapshot:
					// different exact versions of one key may coexist.
					overrides[probe.access.Key] = value
					atomic.AddInt64(&readyCount, 1)
					break
				}
				atomic.AddInt64(&waitCount, 1)
				if !blockedNotified {
					blockedNotified = true
					r.addRuntimeMetric("block_stm_tx_state_ready_wait_observed", 1)
				}
				select {
				case <-readyCtx.Done():
					return nil, readyCtx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		return overrides, nil
	}
	// Finalization is stable only once every lower index has validated. The
	// Block-STM engine invokes this callback for its monotonic stable prefix,
	// so exact versions become visible to other shards without speculative
	// or reverted values escaping.
	publish := func(publishCtx context.Context, index int, delta execution.TxDelta) error {
		if index < 0 || index >= len(block.TxList) || delta.TxID != block.TxList[index].TxID {
			return fmt.Errorf("Block-STM invalid stable-prefix publication at index %d", index)
		}
		return r.publishTransactionStateVersions(publishCtx, block, block.TxList[index], delta, base)
	}
	input := BlockExecutionInput{
		Block: block, BaseStateSnapshot: base, BaseStateCommitment: commitment,
		NodeID: r.node.NodeID, ShardID: r.node.ShardID,
		WorkerCount: blockExecutorWorkerCountFromProfile(r.pluginSnapshot),
		Execution:   r.plugins.Execution, Scheduler: r.plugins.Scheduler,
		MetaTrackExactAccessPolicy: metaTrackExactAccessPolicyID(r.plugins.StateAccess),
		Progress:                   r.updateBlockExecutionProgress,
		BlockSTMTxReadiness:        readiness, BlockSTMTxFinalized: publish,
	}
	result, err := r.plugins.BlockExecutor.ExecuteBlock(ctx, input)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if result.ExecutionResult.BlockExecutorID != execution.BlockSTMExecutorID {
		return BlockExecutionResult{}, fmt.Errorf("single-context Block-STM unexpectedly changed executor identity")
	}
	if result.ActualMetrics == nil {
		result.ActualMetrics = map[string]any{}
	}
	metrics := result.ActualMetrics
	metrics["versioned_state_ready_wave_count"] = 1 // one shared block execution context
	metrics["versioned_state_ready_wait_observation_count"] = atomic.LoadInt64(&waitCount)
	metrics["versioned_state_ready_resolved_token_count"] = atomic.LoadInt64(&readyCount)
	metrics["versioned_state_probe_count"] = atomic.LoadInt64(&probeCount)
	metrics["versioned_state_probe_latency_ms"] = atomic.LoadInt64(&probeLatencyNS) / int64(time.Millisecond)
	// No artificial "ready width" is reported: ready arrivals are asynchronous,
	// and executor transaction count is not a measure of simultaneous readiness.
	metrics["block_stm_shared_execution_tx_count"] = len(block.TxList)
	metrics["versioned_state_ready_wave_policy"] = "shared_blockstm_single_context_v1"
	metrics["versioned_state_ready_execution_scope"] = "whole_consensus_block_shared_MVMemory"
	// The established public metric describes the per-tx/per-key readiness
	// frontier, not the internal worker scheduler. Preserve this existing
	// contract; expose the new implementation mode separately.
	metrics["versioned_state_ready_scheduler_mode"] = "per_transaction_per_key_version_frontier"
	metrics["block_stm_state_ready_admission_mode"] = "blockstm_transaction_level_state_ready"
	metrics["versioned_wave_execution_policy"] = "whole_block_state_ready_v1"
	metrics["versioned_wave_delta_only_count"] = 0
	metrics["versioned_wave_full_fallback_count"] = 0
	metrics["block_stm_internal_version_dependency_delegated_count"] = internalDelegated
	metrics["versioned_state_ready_execution_ms"] = time.Since(started).Milliseconds()
	return result, nil
}
