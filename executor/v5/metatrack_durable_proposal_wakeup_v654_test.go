package v5

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/consensus/pbft"
)

func metaTrackDurableProposalWakeupRuntimeV654(nodeID, leaderID string, enabled bool) *NodeRuntime {
	validators := []string{"n0", "n1", "n2", "n3"}
	return &NodeRuntime{
		node:           NodePlan{NodeID: nodeID, ShardID: "s0", Validators: validators},
		consensus:      pbft.NewState(nodeID, "s0", leaderID, validators),
		proposalWakeup: newMetaTrackDurableProposalWakeupV654(),
		pluginSnapshot: map[string]PluginConfig{
			"block_executor": {
				PluginID: metaTrackBlockExecutorID,
				Config: map[string]any{
					"dependency_closed_consensus": enabled,
				},
			},
		},
		runtimeMetricCounts: map[string]int64{},
	}
}

func TestMetaTrackDurableProposalWakeupV654IsLatestOnly(t *testing.T) {
	runtime := metaTrackDurableProposalWakeupRuntimeV654("n0", "n0", false)
	runtime.signalMetaTrackDurableProposalWakeupV654()
	if got := len(runtime.proposalWakeup); got != 0 {
		t.Fatalf("disabled profile queued wakeup: got=%d want=0", got)
	}
}

func TestMetaTrackDurableProposalWakeupV654LeaderSignalCoalesces(t *testing.T) {
	runtime := metaTrackDurableProposalWakeupRuntimeV654("n0", "n0", true)
	runtime.signalMetaTrackDurableProposalWakeupV654()
	runtime.signalMetaTrackDurableProposalWakeupV654()
	if got := cap(runtime.proposalWakeup); got != 1 {
		t.Fatalf("wakeup channel capacity=%d want=1", got)
	}
	if got := len(runtime.proposalWakeup); got != 1 {
		t.Fatalf("duplicate durable wakeups were not coalesced: got=%d want=1", got)
	}
	if got := runtime.runtimeMetricCounts["metatrack_durable_proposal_wakeup_signal_count"]; got != 1 {
		t.Fatalf("signal metric=%d want=1", got)
	}
	if got := runtime.runtimeMetricCounts["metatrack_durable_proposal_wakeup_coalesced_count"]; got != 1 {
		t.Fatalf("coalesced metric=%d want=1", got)
	}
}

func TestMetaTrackDurableProposalWakeupV654BackupDoesNotSignal(t *testing.T) {
	runtime := metaTrackDurableProposalWakeupRuntimeV654("n1", "n0", true)
	runtime.signalMetaTrackDurableProposalWakeupV654()
	if got := len(runtime.proposalWakeup); got != 0 {
		t.Fatalf("backup queued proposal wakeup: got=%d want=0", got)
	}
}

func TestMetaTrackDurableProposalWakeupV654StaleSignalCannotProposeAsBackup(t *testing.T) {
	runtime := metaTrackDurableProposalWakeupRuntimeV654("n1", "n0", true)
	// The handler must re-check leadership even if a stale signal was queued
	// before a view change. As a backup it returns before touching pool/proposer.
	runtime.handleMetaTrackDurableProposalWakeupV654(context.Background())
	if got := runtime.runtimeMetricCounts["metatrack_durable_proposal_wakeup_not_leader_count"]; got != 1 {
		t.Fatalf("not-leader metric=%d want=1", got)
	}
	if got := runtime.runtimeMetricCounts["metatrack_durable_proposal_wakeup_proposal_attempt_count"]; got != 0 {
		t.Fatalf("backup attempted proposal: got=%d want=0", got)
	}
}
