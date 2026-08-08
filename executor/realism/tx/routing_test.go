package tx

import (
	"strings"
	"testing"
)

func TestExecutionRoutingMetadataIsCoveredByTransactionIDAndSignature(t *testing.T) {
	publicKey, privateKey := DeterministicKeyPair("execution-routing-metadata")
	item := SignedTransaction{
		Sender:     AddressFromPublicKey(publicKey),
		Receiver:   "receiver-routing",
		Nonce:      3,
		Value:      1,
		StateKeys:  []string{"nonce:sender", "asset:hot"},
		AccessList: []AccessItem{{Key: "nonce:sender", Mode: AccessReadWrite, UpdateSemantics: "set"}, {Key: "asset:hot", Mode: AccessRead, UpdateSemantics: "validate"}},
		Payload:    "v5_stateless",
		Timestamp:  42,
	}
	routing := ExecutionRoutingMetadata{
		SenderID:              item.Sender,
		ReceiverID:            item.Receiver,
		RoutingEpoch:          7,
		RoutingOrdinal:        1,
		ExecutionShard:        "s2",
		RoutingReason:         "sender_group_epoch_affinity:predicted_reads=1:predicted_writes=1",
		RoutePlanDigest:       strings.Repeat("a", 64),
		PredictedRemoteReads:  1,
		PredictedRemoteWrites: 1,
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
		t.Fatalf("signed routing metadata should verify: %v", err)
	}

	tampered := item
	copyRouting := *item.ExecutionRouting
	copyRouting.ExecutionShard = "s3"
	tampered.ExecutionRouting = &copyRouting
	if err := Verify(tampered); err == nil {
		t.Fatal("execution shard tampering must be rejected")
	}
}

func TestExecutionRoutingMetadataRejectsEntryDigestMismatchBeforeAdmission(t *testing.T) {
	item := SignedTransaction{Sender: "sender", Receiver: "receiver", Nonce: 1, Value: 1, StateKeys: []string{"asset:1"}}
	item.ExecutionRouting = &ExecutionRoutingMetadata{
		SenderID:         item.Sender,
		ReceiverID:       item.Receiver,
		RoutingOrdinal:   1,
		ExecutionShard:   "s0",
		RoutingReason:    "sender_group_epoch_affinity",
		RoutePlanDigest:  strings.Repeat("b", 64),
		RouteEntryDigest: "not-the-canonical-digest",
	}
	if err := ValidateExecutionRouting(item); err == nil || !strings.Contains(err.Error(), "route_entry_digest") {
		t.Fatalf("expected route entry digest rejection, got %v", err)
	}
}

func TestExecutionRoutingStateVersionsAreSignedAndValidated(t *testing.T) {
	publicKey, privateKey := DeterministicKeyPair("execution-routing-state-versions")
	item := SignedTransaction{
		Sender:     AddressFromPublicKey(publicKey),
		Receiver:   "receiver-versioned",
		Nonce:      1,
		Value:      1,
		StateKeys:  []string{"asset:k"},
		AccessList: []AccessItem{{Key: "asset:k", Mode: AccessReadWrite, UpdateSemantics: "set"}},
		Payload:    "v5_stateless",
		Timestamp:  1,
	}
	routing := ExecutionRoutingMetadata{
		SenderID:        item.Sender,
		ReceiverID:      item.Receiver,
		RoutingOrdinal:  7,
		ExecutionShard:  "s1",
		RoutingReason:   "hash_state_home",
		RoutePlanDigest: strings.Repeat("c", 64),
		StateVersions:   []StateVersionDependency{{Key: "asset:k", RequiredVersion: 3, ProducedVersion: 7}},
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
		t.Fatalf("version-bound transaction should verify: %v", err)
	}

	tampered := item
	copyRouting := *item.ExecutionRouting
	copyRouting.StateVersions = append([]StateVersionDependency(nil), item.ExecutionRouting.StateVersions...)
	copyRouting.StateVersions[0].RequiredVersion = 4
	tampered.ExecutionRouting = &copyRouting
	if err := Verify(tampered); err == nil {
		t.Fatal("required state-version tampering must invalidate transaction identity/signature")
	}

	bad := routing
	bad.StateVersions = []StateVersionDependency{{Key: "asset:k", RequiredVersion: 7, ProducedVersion: 7}}
	bad.RouteEntryDigest, err = ComputeExecutionRoutingDigest(item, bad)
	if err != nil {
		t.Fatal(err)
	}
	item.ExecutionRouting = &bad
	if err := ValidateExecutionRouting(item); err == nil || !strings.Contains(err.Error(), "state version ordering") {
		t.Fatalf("expected invalid version ordering rejection, got %v", err)
	}
}
