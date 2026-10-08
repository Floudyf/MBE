package v5

import (
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_COMPLETE_WINDOW_V671_REGRESSION
//
// Unlike signedMetaTrackBatchTestTx, this fixture deliberately passes through
// the real mempool admission path. It therefore constructs a fully valid,
// Ed25519-signed transaction with positive Value and a TxID bound to the final
// ExecutionRouting metadata.
func completeWindowV671Tx(t *testing.T, id string, ordinal, batch uint64) tx.SignedTransaction {
	t.Helper()

	publicKey, privateKey := tx.DeterministicKeyPair("metatrack-v671-" + id)
	item := tx.SignedTransaction{
		Sender:    tx.AddressFromPublicKey(publicKey),
		Receiver:  "receiver-" + id,
		Nonce:     0,
		Value:     1,
		StateKeys: []string{"state:" + id},
		AccessList: []tx.AccessItem{{
			Key:             "state:" + id,
			Mode:            tx.AccessReadWrite,
			UpdateSemantics: "set",
		}},
		Payload:   "metatrack-v671-complete-window-regression",
		Timestamp: int64(ordinal),
	}

	routing := tx.ExecutionRoutingMetadata{
		SenderID:                             item.Sender,
		ReceiverID:                           item.Receiver,
		RoutingOrdinal:                       ordinal,
		ExecutionShard:                       "s0",
		RoutingReason:                        "test_complete_window_v671",
		RoutePlanDigest:                      "v671-plan",
		RouteBatchSequence:                   batch,
		RouteBatchTransactionCount:           1,
		RouteBatchShardTransactionCount:      1,
		ConsensusExecutionDepth:              1,
		ConsensusExecutionRound:              int(ordinal),
		ConsensusWindowSequence:              1,
		ConsensusWindowStartBatchSequence:    1,
		ConsensusWindowEndBatchSequence:      2,
		ConsensusWindowRouteBatchCount:       2,
		ConsensusWindowTransactionCount:      2,
		ConsensusWindowShardTransactionCount: 2,
		ConsensusWindowCriticalPath:          1,
	}
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing

	if err := tx.Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := tx.Verify(item); err != nil {
		t.Fatalf("fixture must pass real transaction verification: %v", err)
	}
	return item
}

func newCompleteWindowV671Producer() metaTrackNLWindowProducerV669 {
	// Match the real BuiltinRegistry factory. metaTrackProducerConfigV663()
	// forces dependency_closed_consensus=true for the official V669 producer.
	return metaTrackNLWindowProducerV669{
		builtinBlockProducer{
			makeBasic("block_producer", metaTrackNLWindowProducerV669ID, map[string]any{
				"block_size":                  1000,
				"interval_ms":                 100,
				"dependency_closed_consensus": true,
			}),
		},
	}
}

func TestMetaTrackV671BuildCandidateWaitsForWholeWindowAndUsesNoProposalEvidence(t *testing.T) {
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	first := completeWindowV671Tx(t, "v671-a", 1, 1)
	second := completeWindowV671Tx(t, "v671-b", 2, 2)

	if result := pool.AdmitAt(first, time.UnixMilli(1)); !result.Accepted {
		t.Fatalf("first admit failed: %+v", result)
	}

	producer := newCompleteWindowV671Producer()
	input := BlockProductionInput{
		Pool:            pool,
		Proposer:        realblock.NewProposer("n0", "s0"),
		Limit:           1000,
		Now:             time.UnixMilli(10),
		RoutingPluginID: "metatrack_coaccess_routing",
	}

	if _, err := producer.BuildCandidate(input); err == nil || !strings.Contains(err.Error(), "consensus window incomplete") {
		t.Fatalf("partial signed V669 window must wait, got %v", err)
	}
	if pool.Len() != 1 {
		t.Fatalf("partial-window failure must release reservation, pool=%d", pool.Len())
	}

	if result := pool.AdmitAt(second, time.UnixMilli(2)); !result.Accepted {
		t.Fatalf("second admit failed: %+v", result)
	}
	candidate, err := producer.BuildCandidate(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.TxList) != 2 {
		t.Fatalf("candidate tx count=%d want=2", len(candidate.TxList))
	}
	if candidate.TxList[0].ExecutionRouting.RouteBatchSequence != 1 ||
		candidate.TxList[1].ExecutionRouting.RouteBatchSequence != 2 {
		t.Fatalf("candidate did not preserve complete V669 RouteBatch order")
	}
	if candidate.ProposalEvidence != nil {
		t.Fatalf("MetaTrack V669 must not occupy ProposalEvidence: %+v", candidate.ProposalEvidence)
	}
	if metaTrackStreamingWindowMetadataV2(candidate) {
		t.Fatal("multi-RouteBatch complete V669 window must not be treated as streaming-prefix metadata")
	}
	if _, err := (&NodeRuntime{}).validateMetaTrackCriticalWidthWindowV6568(candidate); err != nil {
		t.Fatalf("existing complete-window consensus validator rejected candidate: %v", err)
	}
}
