package v5

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestHeneGuardNonceSemanticV1(t *testing.T) {
	key := "henesys_nonce:0x" + strings.Repeat("a", 40)
	item := tx.SignedTransaction{
		SourceKind: "canonical_trace_replay",
		SchedulingAccessList: []tx.AccessItem{{
			Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "henesys_sender_nonce_sequence",
		}},
	}
	if safe, reason := dualTrackSemanticSafety(item); !safe {
		t.Fatalf("evidence-scoped nonce rejected: %s", reason)
	}
	unsupported := []string{"henesys_erc20_transfer_intent", "henesys_erc1155_transfer_intent"}
	for _, semantic := range unsupported {
		item.SchedulingAccessList = []tx.AccessItem{{Key: "henesys_erc20_balance:0x" + strings.Repeat("1", 40), Mode: tx.AccessReadWrite, UpdateSemantics: semantic}}
		if safe, reason := dualTrackSemanticSafety(item); safe || !strings.Contains(reason, "unsupported_update_semantics") {
			t.Fatalf("intent semantics should remain conservative: %q %v %s", semantic, safe, reason)
		}
	}
	item.SchedulingAccessList = []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "henesys_sender_nonce_sequence"}}
	item.SourceKind = "synthetic"
	if safe, _ := dualTrackSemanticSafety(item); safe {
		t.Fatal("noncanonical nonce accepted")
	}
	item.SourceKind = "canonical_trace_replay"
	for _, fakeKey := range []string{"henesys_nonce:other", "henesys_nonce:0x" + strings.Repeat("g", 40), "other_nonce:0x" + strings.Repeat("1", 40)} {
		item.SchedulingAccessList[0].Key = fakeKey
		if safe, _ := dualTrackSemanticSafety(item); safe {
			t.Fatalf("unverified nonce accepted: %q", fakeKey)
		}
	}
	item.SchedulingAccessList = nil
	item.AccessList = []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "henesys_sender_nonce_sequence"}}
	if safe, _ := dualTrackSemanticSafety(item); safe {
		t.Fatal("non-scheduling nonce accepted")
	}
}
