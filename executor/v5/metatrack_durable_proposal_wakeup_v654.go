package v5

import "context"

// MBE_METATRACK_DURABLE_PROPOSAL_WAKEUP_V654
//
// The newest MetaTrack profile already enforces one PBFT height in flight at a
// time. v6.5.4 does not change that rule. It only removes the quantization gap
// between a durable commit becoming fully installed and the next periodic
// block-production tick. A size-one channel coalesces duplicate wakeups while
// the ticker remains as the liveness fallback.
func newMetaTrackDurableProposalWakeupV654() chan struct{} {
	return make(chan struct{}, 1)
}

func (r *NodeRuntime) metaTrackDurableProposalWakeupEnabledV654() bool {
	return r != nil && r.metaTrackBlockExecutorFlag("dependency_closed_consensus")
}

func (r *NodeRuntime) signalMetaTrackDurableProposalWakeupV654() {
	if !r.metaTrackDurableProposalWakeupEnabledV654() || r.proposalWakeup == nil {
		return
	}
	// Only the current primary can create the next proposal. Backups retain the
	// unchanged periodic replay/catch-up path and therefore never create wakeup
	// traffic on every durable replica commit.
	if !r.isCurrentLeader() {
		return
	}
	select {
	case r.proposalWakeup <- struct{}{}:
		r.addRuntimeMetric("metatrack_durable_proposal_wakeup_signal_count", 1)
	default:
		// A pending wakeup already represents the same desired state: ask the main
		// loop to retry proposal creation after the current durable commit. Never
		// queue an unbounded number of redundant retries.
		r.addRuntimeMetric("metatrack_durable_proposal_wakeup_coalesced_count", 1)
	}
}

func (r *NodeRuntime) handleMetaTrackDurableProposalWakeupV654(ctx context.Context) {
	if !r.metaTrackDurableProposalWakeupEnabledV654() {
		return
	}
	r.addRuntimeMetric("metatrack_durable_proposal_wakeup_consumed_count", 1)
	if !r.isCurrentLeader() {
		r.addRuntimeMetric("metatrack_durable_proposal_wakeup_not_leader_count", 1)
		return
	}
	if r.catchupNeeded() {
		r.addRuntimeMetric("metatrack_durable_proposal_wakeup_catchup_count", 1)
		r.requestCatchup(ctx)
		return
	}
	r.addRuntimeMetric("metatrack_durable_proposal_wakeup_proposal_attempt_count", 1)
	// propose() keeps the existing single-height proposalInFlight /
	// proposalPlanningInFlight / PBFT view-change guards. No new PBFT slot can be
	// opened while another height is active.
	r.propose(ctx)
}
