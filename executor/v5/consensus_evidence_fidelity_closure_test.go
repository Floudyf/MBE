package v5

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func consensusEvidenceWireTestPlan(edgeCount int) literatureGraphPlan {
	edges := make([]literatureGraphEdge, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges = append(edges, literatureGraphEdge{From: i % 7, To: (i + 1) % 7})
	}
	plan := literatureGraphPlan{
		AlgorithmID:             "wire_test",
		Version:                 literatureGraphPlanVersion,
		BlockHeight:             1,
		CandidateTransactionIDs: []string{"a", "b"},
		Waves:                   [][]string{{"a"}, {"b"}},
		SerializationOrder:      []string{"a", "b"},
		DeclaredAccessSetDigest: "access",
		DeclaredReadKeyCount:    1,
		DeclaredWriteKeyCount:   1,
		Edges:                   edges,
		Metrics:                 literatureGraphMetrics{TransactionCount: 2, EdgeCount: edgeCount, WaveCount: 2, MaximumWaveWidth: 1},
	}
	plan.PlanDigest = literaturePlanDigest(plan)
	return plan
}

func TestConsensusEvidenceGraphWireNeverExpandsSparsePlan(t *testing.T) {
	plan := consensusEvidenceWireTestPlan(2)
	full, err := literatureMarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := literatureMarshalConsensusPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > len(full) {
		t.Fatalf("adaptive consensus wire expanded sparse plan: wire=%d full=%d", len(wire), len(full))
	}
	parsed, err := literatureParsePlan(wire, "wire_test")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.PlanDigest != plan.PlanDigest {
		t.Fatalf("sparse plan semantic digest changed: got=%s want=%s", parsed.PlanDigest, plan.PlanDigest)
	}
}

func TestConsensusEvidenceGraphWireThresholdNeverExpandsPayload(t *testing.T) {
	for edgeCount := 0; edgeCount <= 64; edgeCount++ {
		plan := consensusEvidenceWireTestPlan(edgeCount)
		full, err := literatureMarshalPlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := literatureMarshalConsensusPlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) > len(full) {
			t.Fatalf("edge_count=%d expanded payload: wire=%d full=%d", edgeCount, len(wire), len(full))
		}
		if edgeCount < literatureGraphPlanCompactEdgeThreshold && string(wire) != string(full) {
			t.Fatalf("edge_count=%d below threshold did not preserve exact legacy encoding", edgeCount)
		}
	}
}

func TestConsensusEvidenceGraphWireOmitsDenseEdgesAndPreservesSemanticDigest(t *testing.T) {
	plan := consensusEvidenceWireTestPlan(256)
	full, err := literatureMarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := literatureMarshalConsensusPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(compact), `"edges":`) {
		t.Fatalf("dense compact graph wire still carries full edges")
	}
	if !strings.Contains(string(compact), `"edges_digest":`) {
		t.Fatalf("dense compact graph wire is missing edge commitment")
	}
	if len(compact) >= len(full) {
		t.Fatalf("dense compact graph wire did not shrink: compact=%d full=%d", len(compact), len(full))
	}
	if len(compact)*2 >= len(full) {
		t.Fatalf("dense compact graph wire savings are too small to justify compact mode: compact=%d full=%d", len(compact), len(full))
	}
	parsed, err := literatureParsePlan(compact, "wire_test")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConsensusWireVersion != literatureGraphPlanCompactWireVersion || len(parsed.Edges) != 0 || parsed.EdgesDigest == "" || parsed.PlanDigest != plan.PlanDigest {
		t.Fatalf("unexpected dense compact parse: %#v", parsed)
	}
	if err := literatureVerifyCompactEdgeCommitment(parsed, plan.Edges); err != nil {
		t.Fatalf("edge commitment failed: %v", err)
	}
	tampered := append([]literatureGraphEdge(nil), plan.Edges...)
	tampered[0].To = 99
	if err := literatureVerifyCompactEdgeCommitment(parsed, tampered); err == nil {
		t.Fatal("tampered edge projection was accepted")
	}
}

func TestConsensusEvidenceAriaWireOmitsAcceptedDuplicateAndTrace(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 3, Sender: "aria-wire-sender", Receiver: "aria-wire-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-wire", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("admit failed: %#v", admitted)
		}
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size": 3, "candidate_scan_multiplier": 1,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 3,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)
	raw := string(candidate.ProposalEvidence.Payload)
	if strings.Contains(raw, `"candidate_transactions"`) || strings.Contains(raw, `"trace"`) {
		t.Fatalf("Aria compact evidence carries duplicate/diagnostic O(n) data: %s", raw)
	}
	var wire ariaCandidateSelectionConsensusWire
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.WireVersion != ariaCandidateSelectionConsensusWireVersion {
		t.Fatalf("wrong Aria wire version: %q", wire.WireVersion)
	}
	decoded, err := decodeAriaCandidateSelectionEvidence(candidate)
	if err != nil {
		t.Fatalf("decode/reconstruct failed: %v", err)
	}
	if len(decoded.CandidateTransactions) != wire.CandidateCount {
		t.Fatalf("candidate reconstruction mismatch: got=%d want=%d", len(decoded.CandidateTransactions), wire.CandidateCount)
	}
	if _, _, err := recomputeAriaCandidateSelection(context.Background(), candidate, map[string]string{}, 3, producer.config); err != nil {
		t.Fatalf("validator recomputation failed: %v", err)
	}
}

func TestConsensusEvidenceAriaSelectedAndDeferredTamperingRemainRejected(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 3, Sender: "aria-tamper-sender", Receiver: "aria-tamper-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-tamper", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("admit failed: %#v", admitted)
		}
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size": 3, "candidate_scan_multiplier": 1,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 3,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)

	selectedTampered := candidate
	selectedTampered.TxList = append([]tx.SignedTransaction(nil), candidate.TxList...)
	selectedTampered.TxList[0].Payload += "-tampered"
	if _, err := decodeAriaCandidateSelectionEvidence(selectedTampered); err == nil || !strings.Contains(err.Error(), "candidate payload mismatch") {
		t.Fatalf("selected payload tampering was not rejected by the candidate commitment: %v", err)
	}

	var wire ariaCandidateSelectionConsensusWire
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.DeferredTransactions) > 0 {
		deferredTampered := candidate
		wire2 := wire
		wire2.DeferredTransactions = append([]tx.SignedTransaction(nil), wire.DeferredTransactions...)
		wire2.DeferredTransactions[0].Payload += "-tampered"
		encoded, err := json.Marshal(wire2)
		if err != nil {
			t.Fatal(err)
		}
		deferredTampered.ProposalEvidence = &realblock.ProposalEvidenceEnvelope{
			AlgorithmID:   ariaCandidateSelectionEvidenceID,
			PayloadDigest: stableTextDigest(string(encoded)),
			Payload:       encoded,
		}
		if _, err := decodeAriaCandidateSelectionEvidence(deferredTampered); err == nil || !strings.Contains(err.Error(), "candidate payload mismatch") {
			t.Fatalf("deferred payload tampering was not rejected by full candidate commitment: %v", err)
		}
	}
}

func TestConsensusEvidenceGroundhogWireOmitsTransactionTrace(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 2, Sender: "groundhog-wire-sender", Receiver: "groundhog-wire-receiver", StartNonce: 0,
		Value: 600000, Seed: "groundhog-wire", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("admit failed: %#v", admitted)
		}
	}
	producer := groundhogBlockProducer{makeBasic("block_producer", groundhogBlockProducerID, map[string]any{
		"block_size": 2, "candidate_scan_multiplier": 1, "ordered_set_limit": 32,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 2,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)
	raw := string(candidate.ProposalEvidence.Payload)
	if strings.Contains(raw, `"trace"`) {
		t.Fatalf("Groundhog proposal evidence still carries transaction trace: %s", raw)
	}
	runtime := &NodeRuntime{plugins: RuntimePlugins{BlockProducer: producer}}
	if err := runtime.verifyProposalEvidenceEnvelope(candidate); err != nil {
		t.Fatalf("compact Groundhog evidence rejected: %v", err)
	}
}
