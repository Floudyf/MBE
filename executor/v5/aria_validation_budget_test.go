package v5

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestAriaSelectionLimitClampsToCandidateCount(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 3, Sender: "aria-limit-sender", Receiver: "aria-limit-receiver", StartNonce: 0,
		Value: 10, Seed: "aria-limit-clamp", StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}

	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size": 90, "candidate_scan_multiplier": 4,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 90,
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)

	var evidence ariaCandidateSelectionEvidence
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.CandidateCount != len(items) {
		t.Fatalf("candidate count = %d, want %d", evidence.CandidateCount, len(items))
	}
	if evidence.SelectionLimit != len(items) {
		t.Fatalf("selection limit = %d, want candidate count %d", evidence.SelectionLimit, len(items))
	}
	if _, err := decodeAriaCandidateSelectionEvidence(candidate); err != nil {
		t.Fatalf("clamped evidence failed validation: %v", err)
	}
	if _, _, err := recomputeAriaCandidateSelection(
		context.Background(),
		candidate,
		map[string]string{},
		4,
		producer.config,
	); err != nil {
		t.Fatalf("validator recomputation failed: %v", err)
	}
	if got := producer.EstimateProposalValidationWork(candidate); got != len(items) {
		t.Fatalf("validation work estimate = %d, want %d", got, len(items))
	}
}

func TestProposalTimeoutUsesValidationWorkEstimate(t *testing.T) {
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{
		"block_size":  90,
		"interval_ms": 75,
	})}
	runtime := &NodeRuntime{
		plugins: RuntimePlugins{
			BlockProducer: producer,
			Consensus: builtinConsensus{makeBasic("consensus", "pbft_style_consensus", map[string]any{
				"timeout_ms": 2000,
			})},
		},
	}

	if got, want := runtime.proposalTimeout(), 14300*time.Millisecond; got != want {
		t.Fatalf("baseline proposal timeout = %s, want %s", got, want)
	}

	runtime.proposalWorkUnits.Store(360)
	if got, want := runtime.proposalTimeout(), 41300*time.Millisecond; got != want {
		t.Fatalf("candidate-aware proposal timeout = %s, want %s", got, want)
	}

	runtime.proposalWorkUnits.Store(1000)
	if got, want := runtime.proposalTimeout(), 60*time.Second; got != want {
		t.Fatalf("proposal timeout cap = %s, want %s", got, want)
	}
}
