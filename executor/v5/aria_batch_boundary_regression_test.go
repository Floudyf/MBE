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

func TestAriaCanonicalDefaultUsesOneFormalBatchAsConflictWindow(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 8, Sender: "aria-batch-sender", Receiver: "aria-batch-receiver", StartNonce: 0, Value: 1, Seed: "aria-canonical-batch-boundary", StartTimeMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, map[string]any{"block_size": 2, "reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 2, Now: time.UnixMilli(20), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)
	var evidence ariaCandidateSelectionEvidence
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.ScanLimit != 2 || evidence.CandidateCount != 2 {
		t.Fatalf("canonical Aria conflict window = scan:%d candidates:%d, want one formal batch of 2", evidence.ScanLimit, evidence.CandidateCount)
	}
}
