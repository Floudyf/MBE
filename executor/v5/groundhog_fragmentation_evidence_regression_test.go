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

func TestGroundhogProposalEvidenceSeparatesPoolDepthCandidateAndSelectionCounts(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 8, Sender: "groundhog-evidence-sender", Receiver: "groundhog-evidence-receiver",
		StartNonce: 0, Value: 1, Seed: "groundhog-fragmentation-evidence", StartTimeMS: 1,
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
	producer := groundhogBlockProducer{makeBasic("block_producer", groundhogBlockProducerID, map[string]any{
		"block_size": 2, "candidate_scan_multiplier": 4, "ordered_set_limit": 64,
	})}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: 2,
		Now: time.UnixMilli(20), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.ReleaseReserved(candidate.TxList)
	if candidate.ProposalEvidence == nil {
		t.Fatal("missing Groundhog proposal evidence")
	}
	var evidence groundhogCandidateSelectionEvidence
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.PoolDepthBefore != 8 {
		t.Fatalf("pool_depth_before=%d want=8", evidence.PoolDepthBefore)
	}
	if evidence.ScanLimit != 8 || evidence.CandidateCount != 8 {
		t.Fatalf("scan/candidate=%d/%d want=8/8", evidence.ScanLimit, evidence.CandidateCount)
	}
	if evidence.SelectedCount != len(evidence.SelectedTxIDs) || evidence.DeferredCount != len(evidence.DeferredTxIDs) {
		t.Fatalf("selection counters disagree with ids: selected=%d/%d deferred=%d/%d", evidence.SelectedCount, len(evidence.SelectedTxIDs), evidence.DeferredCount, len(evidence.DeferredTxIDs))
	}
	if evidence.SelectedCount != 2 || evidence.DeferredCount != 6 {
		t.Fatalf("selected/deferred=%d/%d want=2/6", evidence.SelectedCount, evidence.DeferredCount)
	}
}
