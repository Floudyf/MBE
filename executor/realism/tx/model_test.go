package tx

import "testing"

func TestSignedTxVerifySuccessAndDeterministicID(t *testing.T) {
	_, privateKey := DeterministicKeyPair("seed:alice")
	item := SignedTransaction{
		Sender:     "alice",
		Receiver:   "bob",
		Nonce:      0,
		Value:      7,
		StateKeys:  []string{"acct:alice", "acct:bob"},
		Payload:    "hello",
		Timestamp:  123,
		SourceKind: "test",
	}
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
		Sender:     "alice",
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
