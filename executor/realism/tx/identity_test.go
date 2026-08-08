package tx

import (
	"strings"
	"testing"
)

func TestLogicalTransactionIdentityIsSignedButStableAcrossExecutionRouting(t *testing.T) {
	publicKey, privateKey := DeterministicKeyPair("logical-identity")
	base := SignedTransaction{
		LogicalTxID: "logical-event-42",
		Sender:      AddressFromPublicKey(publicKey),
		Receiver:    "receiver-logical",
		Nonce:       1,
		Value:       1,
		StateKeys:   []string{"state:hot"},
		AccessList:  []AccessItem{{Key: "state:hot", Mode: AccessWrite, UpdateSemantics: "set"}},
		Payload:     "direct-access",
		Timestamp:   42,
	}

	makeSigned := func(shard string) SignedTransaction {
		item := base
		routing := ExecutionRoutingMetadata{
			SenderID:        item.Sender,
			ReceiverID:      item.Receiver,
			RoutingEpoch:    3,
			RoutingOrdinal:  1,
			ExecutionShard:  shard,
			RoutingReason:   "test-route",
			RoutePlanDigest: strings.Repeat("a", 64),
		}
		digest, err := ComputeExecutionRoutingDigest(item, routing)
		if err != nil {
			t.Fatal(err)
		}
		routing.RouteEntryDigest = digest
		item.ExecutionRouting = &routing
		if err := Sign(&item, privateKey); err != nil {
			t.Fatal(err)
		}
		if err := Verify(item); err != nil {
			t.Fatal(err)
		}
		return item
	}

	left := makeSigned("s0")
	right := makeSigned("s1")
	if left.TxID == right.TxID {
		t.Fatal("method-specific routing must remain covered by the physical transaction id")
	}
	if SemanticID(left) != "logical-event-42" || SemanticID(right) != "logical-event-42" {
		t.Fatalf("semantic identity changed: %q %q", SemanticID(left), SemanticID(right))
	}

	tampered := left
	tampered.LogicalTxID = "other-logical-event"
	if err := Verify(tampered); err == nil {
		t.Fatal("logical transaction identity tampering must be rejected")
	}
}

func TestSemanticIDLegacyFallbacks(t *testing.T) {
	if got := SemanticID(SignedTransaction{TraceSourceID: "trace-1", TxID: "physical"}); got != "trace-1" {
		t.Fatalf("trace fallback mismatch: %s", got)
	}
	if got := SemanticID(SignedTransaction{TxID: "physical"}); got != "physical" {
		t.Fatalf("physical fallback mismatch: %s", got)
	}
}
