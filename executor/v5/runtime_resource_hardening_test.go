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
		"wire_version":             ariaCandidateSelectionConsensusWireVersion,
		"candidate_count":          2,
		"candidate_transactions":   []any{map[string]any{"tx_id": "legacy-a"}, map[string]any{"tx_id": "legacy-b"}},
		"candidate_tx_ids":         []string{"a", "b"},
		"candidate_payload_digest": "digest",
		"selected_tx_ids":          []string{"a"},
		"deferred_tx_ids":          []string{"b"},
		"deferred_transactions":    []any{map[string]any{"tx_id": "b", "payload": "large-signed-payload"}},
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
	for _, key := range []string{"candidate_transactions", "deferred_transactions"} {
		if _, exists := object[key]; exists {
			t.Fatalf("artifact copy retained full %s payload", key)
		}
		if object[key+"_retained"] != false {
			t.Fatalf("%s retained marker=%v", key, object[key+"_retained"])
		}
		if object[key+"_audit_digest"] == "" || object[key+"_audit_digest"] == nil {
			t.Fatalf("%s audit digest missing: %#v", key, object)
		}
	}
	// Preserve the pre-existing v2 candidate audit sample contract. Only the new
	// deferred signed-transaction vector is sample-suppressed to stop shutdown bloat.
	if _, exists := object["candidate_transactions_audit_sample"]; !exists {
		t.Fatal("legacy candidate transaction audit sample was removed")
	}
	if _, exists := object["deferred_transactions_audit_sample"]; exists {
		t.Fatal("artifact copy retained deferred signed-transaction audit sample")
	}
	if object["deferred_transactions_omitted_count"] != float64(1) && object["deferred_transactions_omitted_count"] != 1 {
		t.Fatalf("deferred omitted count=%v", object["deferred_transactions_omitted_count"])
	}
	if object["candidate_payload_digest"] != "digest" {
		t.Fatalf("candidate digest lost: %#v", object)
	}
	if object["audit_payload_compaction_version"] != "proposal_selection_audit_digest_v2" {
		t.Fatalf("compaction version=%v", object["audit_payload_compaction_version"])
	}
	// Consensus evidence on the block remains byte-for-byte unchanged.
	if string(block.ProposalEvidence.Payload) != string(payload) {
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
