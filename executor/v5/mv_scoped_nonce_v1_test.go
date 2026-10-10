package v5

import (
	"context"
	"fmt"
	"path/filepath"
	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
	"testing"
	"time"
)

// MBE_MV_COMPAT_V1 regression: one source signer, two physical ingress domains.
func TestMVScopedNonceAcrossPBFTAdmissionDomainsV1(t *testing.T) {
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = canonicalDirectRecord(i, "")
		rows[i]["sender_id"] = "axie-zero-mint-origin"
	}
	root := t.TempDir()
	relative, sum := writeCanonicalFixture(t, filepath.Join(root, ".cache", "workloads"), rows)
	plan := canonicalPlan(relative, sum, len(rows))
	iterator, err := NewCanonicalTraceIterator(plan, 2, root)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	pools := map[string]*mempool.Mempool{
		"s0": mempool.New("n0", "s0", mempool.Policy{Capacity: 100, TTL: time.Minute}, account.NewNonceManager()),
		"s1": mempool.New("n1", "s1", mempool.Policy{Capacity: 100, TTL: time.Minute}, account.NewNonceManager()),
	}
	sender := ""
	for i := range rows {
		rec, err := iterator.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		domain := fmt.Sprintf("s%d", i%2)
		signed, err := iterator.SignedTransactionForAdmissionDomain(rec, domain)
		if err != nil {
			t.Fatal(err)
		}
		if signed.Nonce != uint64(i/2) {
			t.Fatalf("nonce[%d] got=%d want=%d", i, signed.Nonce, i/2)
		}
		if sender == "" {
			sender = signed.Sender
		} else if sender != signed.Sender {
			t.Fatalf("signature identity changed across domains")
		}
		if err := tx.Verify(signed); err != nil {
			t.Fatal(err)
		}
		if result := pools[domain].Admit(signed); !result.Accepted {
			t.Fatalf("scoped replay admission failed at index %d: %s", i, result.RejectReason)
		}
	}
}

func TestMVScopedNonceRequiresExplicitIngressDomainV1(t *testing.T) {
	root := t.TempDir()
	relative, sum := writeCanonicalFixture(t, filepath.Join(root, ".cache", "workloads"), []map[string]any{canonicalDirectRecord(0, "")})
	iterator, err := NewCanonicalTraceIterator(canonicalPlan(relative, sum, 1), 2, root)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	rec, err := iterator.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := iterator.SignedTransactionForAdmissionDomain(rec, ""); err == nil {
		t.Fatal("empty ingress domain must fail closed")
	}
}
