package main

import "testing"

func TestTerminalReservationAloneDoesNotCountAsInfrastructureWork(t *testing.T) {
	status := map[string]any{
		"reserved_tx_count":             float64(23),
		"pending_commit_count":          float64(0),
		"pending_future_block_count":    float64(0),
		"pending_cross_shard_count":     float64(0),
		"pending_state_delta_count":     float64(0),
		"pending_state_delta_key_count": float64(0),
		"ready_state_delta_count":       float64(0),
	}
	if hasPendingInfrastructureWork(status) {
		t.Fatal("terminal-only reservation must not keep the drain loop alive")
	}
	status["pending_commit_count"] = float64(1)
	if !hasPendingInfrastructureWork(status) {
		t.Fatal("pending commit must keep the drain loop alive")
	}
}

func TestTerminalOnlyMempoolAndProposalWorkDoNotBlockDrain(t *testing.T) {
	terminal := map[string]bool{"tx-terminal": true}
	statuses := []map[string]any{{
		"mempool_depth":                     float64(1),
		"mempool_logical_tx_ids":            []any{"tx-terminal"},
		"proposal_in_flight":                true,
		"proposal_work_details_available":   true,
		"proposal_system_state_delta_count": float64(0),
		"proposal_logical_tx_ids":           []any{"tx-terminal"},
	}}
	if hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("terminal-only mempool residue must not block drain")
	}
	if hasPendingProposalWork(statuses, terminal) {
		t.Fatal("terminal-only proposal residue must not block drain")
	}

	statuses[0]["mempool_logical_tx_ids"] = []any{"tx-terminal", "tx-live"}
	statuses[0]["mempool_depth"] = float64(2)
	if !hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("non-terminal mempool transaction must keep drain alive")
	}
	statuses[0]["mempool_logical_tx_ids"] = []any{"tx-terminal"}
	statuses[0]["mempool_depth"] = float64(1)
	statuses[0]["proposal_logical_tx_ids"] = []any{"tx-terminal", "tx-live"}
	if !hasPendingProposalWork(statuses, terminal) {
		t.Fatal("non-terminal proposal transaction must keep drain alive")
	}
}
