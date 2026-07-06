package mempool

import (
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/tx"
)

func signed(t *testing.T, sender string, nonce uint64) tx.SignedTransaction {
	t.Helper()
	_, privateKey := tx.DeterministicKeyPair("seed:" + sender)
	item := tx.SignedTransaction{
		Sender:     sender,
		Receiver:   "bob",
		Nonce:      nonce,
		Value:      1,
		StateKeys:  []string{"acct:" + sender, "acct:bob"},
		Payload:    "payload",
		Timestamp:  int64(nonce),
		SourceKind: "test",
	}
	if err := tx.Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	return item
}

func TestMempoolAdmissionAndRejects(t *testing.T) {
	m := New("n0", "s0", Policy{Capacity: 2, TTL: time.Minute}, account.NewNonceManager())
	tx0 := signed(t, "alice", 0)
	if result := m.Admit(tx0); !result.Accepted {
		t.Fatalf("expected accept, got %s", result.RejectReason)
	}
	if result := m.Admit(tx0); result.Accepted || result.RejectReason != ReasonDuplicateTx {
		t.Fatalf("expected duplicate reject, got %+v", result)
	}
	bad := signed(t, "mallory", 0)
	bad.Signature = "bad"
	if result := m.Admit(bad); result.Accepted || result.RejectReason != tx.ErrInvalidSignature {
		t.Fatalf("expected signature reject, got %+v", result)
	}
	stale := signed(t, "alice", 0)
	stale.Payload = "stale-nonce"
	_, staleKey := tx.DeterministicKeyPair("seed:alice")
	if err := tx.Sign(&stale, staleKey); err != nil {
		t.Fatal(err)
	}
	if result := m.Admit(stale); result.Accepted || result.RejectReason != account.ReasonStaleNonce {
		t.Fatalf("expected stale nonce reject, got %+v", result)
	}
	future := signed(t, "carol", 2)
	if result := m.Admit(future); result.Accepted || result.RejectReason != account.ReasonFutureNonceNotSupported {
		t.Fatalf("expected future nonce reject, got %+v", result)
	}
	if result := m.Admit(signed(t, "alice", 1)); !result.Accepted {
		t.Fatalf("expected second alice accept, got %+v", result)
	}
	if result := m.Admit(signed(t, "dave", 0)); result.Accepted || result.RejectReason != ReasonCapacity {
		t.Fatalf("expected capacity reject, got %+v", result)
	}
}

func TestPopReadyDeterministicAndNodeIsolation(t *testing.T) {
	a := New("n0", "s0", Policy{Capacity: 10, TTL: time.Minute}, account.NewNonceManager())
	b := New("n1", "s0", Policy{Capacity: 10, TTL: time.Minute}, account.NewNonceManager())
	tx0 := signed(t, "alice", 0)
	tx1 := signed(t, "alice", 1)
	if !a.Admit(tx0).Accepted || !a.Admit(tx1).Accepted {
		t.Fatal("expected node A admits")
	}
	if !b.Admit(tx0).Accepted {
		t.Fatal("expected node B isolated mempool admits same tx")
	}
	popped := a.PopReady(1)
	if len(popped) != 1 || popped[0].TxID != tx0.TxID {
		t.Fatalf("unexpected pop order")
	}
	if a.Len() != 1 || b.Len() != 1 {
		t.Fatalf("expected isolated sizes, got a=%d b=%d", a.Len(), b.Len())
	}
}
