package v5

import (
	"context"
	"fmt"
	"time"
)

// Porygon Paper2 executes E(h) concurrently with earlier rounds, but its
// prospective Storage Role root must use the *certified* canonical h-1 base.
// Proposal.T remains the independent h-2 read anchor. Waiting for h-1 is
// therefore a local readiness dependency, not a PBFT proposal timeout.
// Never substitute a stale base, invent a root, or alter the paper's U policy.
func (r *NodeRuntime) porygonV55AwaitCanonicalBase(ctx context.Context, height uint64, partitionID string, missingPredecessorTimeout time.Duration) (uint64, string, error) {
	if r == nil || height == 0 || partitionID == "" {
		return 0, "", fmt.Errorf("Porygon Paper2 canonical-base request incomplete: height=%d partition=%s", height, partitionID)
	}
	if missingPredecessorTimeout <= 0 {
		missingPredecessorTimeout = 5 * time.Second
	}
	started := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// This deadline detects a missing predecessor, not a slow *known* executing
	// predecessor. In the latter case E(h-1) must be allowed to finish without
	// reclassifying valid paper pipeline overlap as a fatal execution error.
	missingSince := time.Now()
	for {
		baseHeight, baseRoot, ready := r.porygonPaperCanonicalPartitionBase(height, partitionID)
		if ready {
			r.addPorygonRuntimeMetric("porygon_v55_local_base_ready_wait_us", time.Since(started).Microseconds())
			return baseHeight, baseRoot, nil
		}
		active, terminal, detail := r.porygonV55PredecessorCondition(baseHeight)
		if terminal {
			r.addPorygonRuntimeMetric("porygon_v55_predecessor_failure_propagated_count", 1)
			return 0, "", fmt.Errorf("Porygon Paper2 canonical predecessor unavailable: height=%d partition=%s required_base=%d: %s", height, partitionID, baseHeight, detail)
		}
		if active {
			// Reset only the missing-predecessor alarm. No timeout is added to
			// the actual ordered E(h-1)->root(h-1)->E(h) dependency.
			missingSince = time.Now()
			r.addPorygonRuntimeMetric("porygon_v55_active_predecessor_wait_poll_count", 1)
		} else if time.Since(missingSince) >= missingPredecessorTimeout {
			r.addPorygonRuntimeMetric("porygon_partition_root_timeout_count", 1)
			return 0, "", fmt.Errorf("Porygon Paper2 partition-root local base timeout: height=%d partition=%s requester=%s required_base=%d predecessor=%s", height, partitionID, r.node.NodeID, baseHeight, detail)
		}
		r.addPorygonRuntimeMetric("porygon_partition_root_local_base_not_ready_count", 1)
		select {
		case <-ctx.Done():
			return 0, "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// The pipeline is the source of truth for whether the *immediately preceding*
// height can still certify its state. Lock neither runtime.mu nor paper state.mu
// while inspecting pipeline.mu: no new cross-subsystem lock order is introduced.
func (r *NodeRuntime) porygonV55PredecessorCondition(requiredHeight uint64) (active, terminal bool, detail string) {
	r.mu.Lock()
	fatal := r.fatalExecutionError
	r.mu.Unlock()
	if fatal != "" {
		return false, true, "prior fatal execution: " + fatal
	}
	state := r.porygonPipelineRuntime()
	state.mu.Lock()
	item := state.blocks[requiredHeight]
	frontier := state.executedHeight
	phase := porygonPipelineOrdered
	if item != nil {
		phase = item.Phase
	}
	state.mu.Unlock()
	if item == nil {
		if requiredHeight <= frontier {
			return false, true, fmt.Sprintf("executed frontier %d already passed an uncertified base", frontier)
		}
		return false, false, "preceding execution not yet registered"
	}
	switch phase {
	case porygonPipelineFailed:
		return false, true, "preceding execution failed"
	case porygonPipelineExecuted, porygonPipelineProtocolCommitted, porygonPipelineDurable:
		return false, true, "preceding execution finished but required certified root is missing"
	case porygonPipelineOrdered:
		return true, false, "preceding ordered execution still in progress"
	default:
		return false, false, "preceding execution not ready"
	}
}
