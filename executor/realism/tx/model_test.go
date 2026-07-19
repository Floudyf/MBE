package tx

import "testing"

func TestSignedTxVerifySuccessAndDeterministicID(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:alice")
	item := SignedTransaction{
		Receiver:   "bob",
		Nonce:      0,
		Value:      7,
		StateKeys:  []string{"acct:pending", "acct:bob"},
		Payload:    "hello",
		Timestamp:  123,
		SourceKind: "test",
	}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	item.StateKeys[0] = "acct:" + item.Sender
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	id := item.TxID
	if err := Verify(item); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	copy := item
	if err := AssignID(&copy); err != nil {
		t.Fatal(err)
	}
	if copy.TxID != id {
		t.Fatalf("deterministic tx id mismatch: %s != %s", copy.TxID, id)
	}
}

func TestSignatureReject(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:alice")
	item := SignedTransaction{
		Receiver:   "bob",
		Nonce:      0,
		Value:      1,
		StateKeys:  []string{"acct:alice"},
		Payload:    "hello",
		Timestamp:  1,
		SourceKind: "test",
	}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	item.Payload = "tampered"
	if err := Verify(item); err == nil || err.Error() != ErrInvalidTxID {
		t.Fatalf("expected invalid tx id after tamper, got %v", err)
	}
	if err := AssignID(&item); err != nil {
		t.Fatal(err)
	}
	if err := Verify(item); err == nil || err.Error() != ErrInvalidSignature {
		t.Fatalf("expected invalid signature, got %v", err)
	}
}

func TestAccessListCoveredByTxIDAndSignature(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:access")
	item := SignedTransaction{
		Receiver:   "bob",
		Nonce:      0,
		Value:      1,
		StateKeys:  []string{"acct:alice", "acct:bob"},
		AccessList: []AccessItem{{Key: "balance:alice", Mode: AccessReadWrite, UpdateSemantics: "set"}},
		Payload:    "hello",
		Timestamp:  1,
		SourceKind: "test",
	}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	item.AccessList[0].Key = "balance:mallory"
	if err := Verify(item); err == nil || err.Error() != ErrInvalidTxID {
		t.Fatalf("expected invalid tx id after access-list tamper, got %v", err)
	}
	if err := AssignID(&item); err != nil {
		t.Fatal(err)
	}
	if err := Verify(item); err == nil || err.Error() != ErrInvalidSignature {
		t.Fatalf("expected invalid signature after access-list tx id reassignment, got %v", err)
	}
}

func TestAddressFromPublicKeyDeterministic(t *testing.T) {
	publicKey, _ := DeterministicKeyPair("seed:address")
	first := AddressFromPublicKey(publicKey)
	second := AddressFromPublicKey(publicKey)
	if first != second {
		t.Fatalf("address not deterministic: %s != %s", first, second)
	}
	if len(first) != 42 || first[:2] != "0x" {
		t.Fatalf("unexpected address format: %s", first)
	}
}

func TestSignedTxSenderPublicKeyBindingPasses(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:bound")
	item := SignedTransaction{Receiver: "bob", Nonce: 0, Value: 1, StateKeys: []string{"acct:sender", "acct:bob"}, Payload: "hello", Timestamp: 1, SourceKind: "test"}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	if !IsBoundSender(item.Sender, item.PublicKey) {
		t.Fatalf("sender is not bound to public key")
	}
	if err := Verify(item); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestSignedTxTamperedSenderRejected(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:bound")
	item := SignedTransaction{Receiver: "bob", Nonce: 0, Value: 1, StateKeys: []string{"acct:sender", "acct:bob"}, Payload: "hello", Timestamp: 1, SourceKind: "test"}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	item.Sender = "0x1111111111111111111111111111111111111111"
	if err := AssignID(&item); err != nil {
		t.Fatal(err)
	}
	if err := Verify(item); err == nil || err.Error() != ErrSenderPublicKeyMismatch {
		t.Fatalf("expected sender/public key mismatch, got %v", err)
	}
}

func TestSignedTxTamperedPublicKeyRejected(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:bound")
	otherPublicKey, _ := DeterministicKeyPair("seed:other")
	item := SignedTransaction{Receiver: "bob", Nonce: 0, Value: 1, StateKeys: []string{"acct:sender", "acct:bob"}, Payload: "hello", Timestamp: 1, SourceKind: "test"}
	if err := Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	item.PublicKey = encodeKey(otherPublicKey)
	if err := AssignID(&item); err != nil {
		t.Fatal(err)
	}
	if err := Verify(item); err == nil || err.Error() != ErrSenderPublicKeyMismatch {
		t.Fatalf("expected sender/public key mismatch, got %v", err)
	}
}
