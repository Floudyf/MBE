package v5

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestSuccessfulRelayDefersNextAckRetry(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n0", "s0")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n1" {
			t.Fatalf(
				"relay destination = %q, want n1",
				nodeID,
			)
		}

		if envelope.MessageType != p2p.MessageXShardRelay {
			t.Fatalf(
				"message type = %q, want relay",
				envelope.MessageType,
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-pacing-relay",
		},
		LogicalTxID: "logical-pacing-relay",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueOutboundRelay(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial relay send count = %d, want 1",
			sendCount,
		)
	}

	if len(runtime.pendingOutboundRelays) != 1 {
		t.Fatalf(
			"pending relay count = %d, want 1",
			len(runtime.pendingOutboundRelays),
		)
	}

	// Force the global retry scheduler to become eligible.
	// A per-message ACK retry deadline should still suppress
	// another send immediately after a successful delivery.
	runtime.lastCrossShardRetry = time.Time{}

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 1 {
		t.Fatalf(
			"relay was retried before ACK retry delay elapsed: send count = %d, want 1",
			sendCount,
		)
	}
}

func TestSuccessfulFinalizeDefersNextAckRetry(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n1", "s1")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n0" {
			t.Fatalf(
				"Finalize destination = %q, want n0",
				nodeID,
			)
		}

		if envelope.MessageType != finalizeMessage {
			t.Fatalf(
				"message type = %q, want Finalize",
				envelope.MessageType,
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-pacing-finalize",
		},
		LogicalTxID: "logical-pacing-finalize",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueFinalize(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial Finalize send count = %d, want 1",
			sendCount,
		)
	}

	if len(runtime.pendingFinalizeMessages) != 1 {
		t.Fatalf(
			"pending Finalize count = %d, want 1",
			len(runtime.pendingFinalizeMessages),
		)
	}

	// The global scheduler is eligible, while the successful
	// Finalize message should still be inside its ACK wait period.
	runtime.lastCrossShardRetry = time.Time{}

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 1 {
		t.Fatalf(
			"Finalize was retried before ACK retry delay elapsed: send count = %d, want 1",
			sendCount,
		)
	}
}

func TestRelayRetriesAfterAckRetryDeadline(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n0", "s0")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n1" {
			t.Fatalf(
				"relay destination = %q, want n1",
				nodeID,
			)
		}

		if envelope.MessageType != p2p.MessageXShardRelay {
			t.Fatalf(
				"message type = %q, want relay",
				envelope.MessageType,
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-pacing-relay-expired",
		},
		LogicalTxID: "logical-pacing-relay-expired",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueOutboundRelay(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial relay send count = %d, want 1",
			sendCount,
		)
	}

	logicalID := relayLogicalID(relay)

	runtime.mu.Lock()
	runtime.outboundRelayRetryAfter[logicalID] =
		time.Now().Add(-time.Millisecond)
	runtime.lastCrossShardRetry = time.Time{}
	runtime.mu.Unlock()

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 2 {
		t.Fatalf(
			"relay send count after expired deadline = %d, want 2",
			sendCount,
		)
	}
}

func TestFinalizeRetriesAfterAckRetryDeadline(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n1", "s1")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n0" {
			t.Fatalf(
				"Finalize destination = %q, want n0",
				nodeID,
			)
		}

		if envelope.MessageType != finalizeMessage {
			t.Fatalf(
				"message type = %q, want Finalize",
				envelope.MessageType,
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-pacing-finalize-expired",
		},
		LogicalTxID: "logical-pacing-finalize-expired",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueFinalize(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial Finalize send count = %d, want 1",
			sendCount,
		)
	}

	logicalID := finalizeLogicalID(
		runtime.pendingFinalizeMessages[relay.LogicalTxID],
	)

	runtime.mu.Lock()
	runtime.finalizeRetryAfter[logicalID] =
		time.Now().Add(-time.Millisecond)
	runtime.lastCrossShardRetry = time.Time{}
	runtime.mu.Unlock()

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 2 {
		t.Fatalf(
			"Finalize send count after expired deadline = %d, want 2",
			sendCount,
		)
	}
}
