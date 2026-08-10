package v5

import "testing"

func TestCompactProposalAuditPayloadPreservesV6ContractAndDirectAuditVectors(t *testing.T) {
	payload := map[string]any{
		"candidate_count":           3,
		"candidate_transactions":    []any{map[string]any{"tx_id": "a"}, map[string]any{"tx_id": "b"}, map[string]any{"tx_id": "c"}},
		"candidate_tx_ids":          []any{"a", "b", "c"},
		"candidate_logical_ids":     []any{"la", "lb", "lc"},
		"selected_tx_ids":           []any{"a", "c"},
		"selected_logical_ids":      []any{"la", "lc"},
		"deferred_tx_ids":           []any{"b"},
		"deferred_logical_ids":      []any{"lb"},
		"deferred_reasons":          map[string]any{"b": "conflict"},
		"trace":                     []any{map[string]any{"tx_id": "a"}, map[string]any{"tx_id": "b"}},
		"selection_semantic_digest": "semantic",
	}
	compactProposalAuditPayload(payload)

	if _, ok := payload["candidate_transactions"]; ok {
		t.Fatal("full candidate transaction vector was retained")
	}
	if _, ok := payload["trace"]; ok {
		t.Fatal("full trace vector was retained")
	}
	for _, key := range []string{"candidate_transactions", "trace"} {
		if payload[key+"_audit_digest"] == nil || payload[key+"_audit_count"] == nil {
			t.Fatalf("missing audit commitment for %s", key)
		}
	}
	if payload["candidate_transactions_retained"] != false {
		t.Fatalf("v6 retained marker changed: %v", payload["candidate_transactions_retained"])
	}
	if payload["candidate_transactions_omitted_count"] != 3 {
		t.Fatalf("v6 omitted-count marker changed: %v", payload["candidate_transactions_omitted_count"])
	}

	// These compact vectors are deliberately retained for direct evidence review.
	for _, key := range []string{"candidate_tx_ids", "candidate_logical_ids", "selected_tx_ids", "selected_logical_ids", "deferred_tx_ids", "deferred_logical_ids", "deferred_reasons"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("direct audit field was removed: %s", key)
		}
	}
	if payload["selection_semantic_digest"] != "semantic" {
		t.Fatalf("semantic commitment changed")
	}
	if payload["audit_payload_compacted"] != true {
		t.Fatalf("compaction marker missing")
	}
	if payload["audit_payload_compaction_version"] != "proposal_selection_audit_digest_v2" {
		t.Fatalf("unexpected compaction version: %v", payload["audit_payload_compaction_version"])
	}
}
