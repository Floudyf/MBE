package v5

import (
	"context"
	"encoding/json"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
)

func TestCatchupPeerCandidatesPreferLeaderWithoutBroadcastFanout(t *testing.T) {
	runtime := &NodeRuntime{
		node:      NodePlan{NodeID: "n2", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		consensus: pbft.NewState("n2", "s0", "n0", []string{"n0", "n1", "n2", "n3"}),
	}
	peers := runtime.catchupPeerCandidates()
	if len(peers) != 3 || peers[0] != "n0" || peers[1] != "n1" || peers[2] != "n3" {
		t.Fatalf("unexpected catch-up peer order: %#v", peers)
	}
}

func TestRecordProposalEvidenceCompactsAriaCandidatePayloadOnlyInArtifactCopy(t *testing.T) {
	evidence := map[string]any{
		"candidate_count":          2,
		"candidate_transactions":   []any{map[string]any{"tx_id": "a"}, map[string]any{"tx_id": "b"}},
		"candidate_tx_ids":         []string{"a", "b"},
		"candidate_payload_digest": "digest",
		"selected_tx_ids":          []string{"a"},
		"deferred_tx_ids":          []string{"b"},
	}
	payload, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0"}}
	block := realblock.Block{Height: 1, BlockHash: "h1", ProposalEvidence: &realblock.ProposalEvidenceEnvelope{AlgorithmID: ariaCandidateSelectionEvidenceID, Payload: payload, PayloadDigest: "digest"}}
	runtime.recordProposalEvidence(block)
	if len(runtime.proposalEvidence) != 1 {
		t.Fatalf("rows=%d", len(runtime.proposalEvidence))
	}
	object, ok := runtime.proposalEvidence[0]["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload type=%T", runtime.proposalEvidence[0]["payload"])
	}
	if _, exists := object["candidate_transactions"]; exists {
		t.Fatal("artifact copy retained full candidate transaction payload")
	}
	if object["candidate_transactions_retained"] != false {
		t.Fatalf("retained=%v", object["candidate_transactions_retained"])
	}
	if object["candidate_payload_digest"] != "digest" {
		t.Fatalf("candidate digest lost: %#v", object)
	}
	// Consensus evidence on the block remains unchanged.
	if len(block.ProposalEvidence.Payload) != len(payload) {
		t.Fatal("consensus-bound proposal evidence was mutated")
	}
}

func TestSchedulerTraceRetentionPreservesExactAggregate(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0"}}
	block := realblock.Block{Height: 1}
	events := make([]ScheduleEvent, schedulerTraceRetentionLimit+7)
	for i := range events {
		events[i] = ScheduleEvent{TxID: "tx", Blocked: true, LocalExecution: true}
	}
	runtime.recordScheduleEvents(block, events, true)
	if len(runtime.schedulerRows) != schedulerTraceRetentionLimit {
		t.Fatalf("retained=%d", len(runtime.schedulerRows))
	}
	if runtime.schedulerRowsDropped != 7 {
		t.Fatalf("dropped=%d", runtime.schedulerRowsDropped)
	}
	if runtime.schedulerAggregate.total != len(events) || runtime.schedulerAggregate.blocked != len(events) {
		t.Fatalf("aggregate=%+v", runtime.schedulerAggregate)
	}
}

func TestCatchupResponseConcurrencyGuardRejectsExcessWorkWithoutError(t *testing.T) {
	runtime := &NodeRuntime{
		node:                     NodePlan{NodeID: "n0", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		consensus:                pbft.NewState("n0", "s0", "n0", []string{"n0", "n1", "n2", "n3"}),
		runtimeMetricCounts:      map[string]int64{},
		catchupResponsesInFlight: maxConcurrentCatchupResponses,
	}
	err := runtime.handleCertifiedCatchupRequest(context.Background(), "n1", CatchupRequest{ShardID: "s0", FromHeight: 1, ToHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.runtimeMetricCounts["pbft_catchup_response_busy_count"] != 1 {
		t.Fatalf("busy metric=%d", runtime.runtimeMetricCounts["pbft_catchup_response_busy_count"])
	}
}
