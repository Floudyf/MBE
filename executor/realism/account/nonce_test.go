package account

import "testing"

func TestNonceManagerRejectsStaleAndFutureNonce(t *testing.T) {
	m := NewNonceManager()
	if err := m.Accept("alice", 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate("alice", 0); err == nil {
		t.Fatalf("expected stale nonce reject")
	}
	if err := m.Validate("alice", 2); err == nil {
		t.Fatalf("expected future nonce reject")
	}
	if err := m.Accept("alice", 1); err != nil {
		t.Fatal(err)
	}
	if got := m.Expected("alice"); got != 2 {
		t.Fatalf("expected nonce 2, got %d", got)
	}
}
