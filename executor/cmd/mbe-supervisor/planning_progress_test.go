package main

import "testing"

func TestProposalPlanningProgressCountsAsRealDrainProgress(t *testing.T) {
	previous := progressSnapshot{Terminal: 0, MinHeight: 0, MaxHeight: 0, Mempool: 1000, Reserved: 100, ProposalPlanningInFlight: true, ProposalPlanningProgressAtMS: 1000, ProposalPlanningWorkUnits: 100}
	current := previous
	current.ProposalPlanningProgressAtMS = 2000
	current.ProposalPlanningWorkUnits = 200
	observation := observeDrainProgress(previous, true, current, true)
	if !observation.AnyProgress {
		t.Fatal("advancing exact planner work must reset drain no-progress accounting")
	}
}

func TestProposalPlanningProgressRequiresFreshCompleteStatus(t *testing.T) {
	previous := progressSnapshot{ProposalPlanningInFlight: true, ProposalPlanningProgressAtMS: 1000, ProposalPlanningWorkUnits: 100}
	current := previous
	current.ProposalPlanningProgressAtMS = 2000
	current.ProposalPlanningWorkUnits = 200
	observation := observeDrainProgress(previous, true, current, false)
	if observation.AnyProgress {
		t.Fatal("incomplete node-status snapshots must not manufacture infrastructure progress")
	}
}

func TestMakeProgressSnapshotCarriesPlanningEvidence(t *testing.T) {
	statuses := []map[string]any{
		{"committed_height": 0, "mempool_depth": 900, "reserved_tx_count": 100, "proposal_planning_in_flight": true, "proposal_planning_progress_at_ms": 12345, "proposal_planning_work_units": 6789, "proposal_planning_detail_count": 42},
		{"committed_height": 0, "mempool_depth": 1000, "reserved_tx_count": 0, "proposal_planning_in_flight": false, "proposal_planning_progress_at_ms": 0, "proposal_planning_work_units": 0, "proposal_planning_detail_count": 0},
	}
	snapshot := makeProgressSnapshot(0, statuses, nil)
	if !snapshot.ProposalPlanningInFlight || snapshot.ProposalPlanningProgressAtMS != 12345 || snapshot.ProposalPlanningWorkUnits != 6789 || snapshot.ProposalPlanningDetailCount != 42 {
		t.Fatalf("planning evidence not preserved: %+v", snapshot)
	}
}
