package v5

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackLogicalDomainFrontierPlannerDerivesLocalAndBridge(t *testing.T) {
	routing := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{
		"control_policy":       metaTrackLogicalDomainFrontierPolicy,
		"logical_domain_count": 2,
	})}
	records := []WorkloadRecord{
		{Index: 0, LogicalID: "t0", AccessList: []tx.AccessItem{{Key: "k1", Mode: tx.AccessRead}, {Key: "k2", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{Index: 1, LogicalID: "t1", AccessList: []tx.AccessItem{{Key: "k1", Mode: tx.AccessRead}, {Key: "k2", Mode: tx.AccessRead}}},
		{Index: 2, LogicalID: "t2", AccessList: []tx.AccessItem{{Key: "k4", Mode: tx.AccessRead}, {Key: "k5", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{Index: 3, LogicalID: "t3", AccessList: []tx.AccessItem{{Key: "k4", Mode: tx.AccessRead}, {Key: "k5", Mode: tx.AccessRead}}},
		{Index: 4, LogicalID: "bridge", AccessList: []tx.AccessItem{{Key: "k2", Mode: tx.AccessRead}, {Key: "k4", Mode: tx.AccessRead}}},
	}
	plan := routing.PlanBatch(BatchRoutingInput{
		BatchIndex: 0,
		Records:    records,
		ShardIDs:   []string{"s0", "s1"},
		Sharding:   builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)},
	})
	if plan.ControlPolicy != metaTrackLogicalDomainFrontierPolicy {
		t.Fatalf("control policy mismatch: %#v", plan)
	}
	if plan.LogicalDomainCount != 2 {
		t.Fatalf("logical domain count mismatch: %d", plan.LogicalDomainCount)
	}
	localCount := 0
	bridgeCount := 0
	for _, placement := range plan.TransactionPlacements {
		if placement.FrontierDigest == "" {
			t.Fatalf("frontier digest must be present for %s", placement.LogicalID)
		}
		switch len(placement.LogicalDomains) {
		case 1:
			if !placement.Local || placement.Bridge {
				t.Fatalf("single-domain placement must be local: %#v", placement)
			}
			localCount++
		default:
			if len(placement.LogicalDomains) < 2 || placement.Local || !placement.Bridge {
				t.Fatalf("multi-domain placement must be bridge: %#v", placement)
			}
			bridgeCount++
		}
	}
	if localCount == 0 || bridgeCount == 0 {
		t.Fatalf("expected both local and bridge transactions: local=%d bridge=%d placements=%#v", localCount, bridgeCount, plan.TransactionPlacements)
	}
}

func TestDualTrackLogicalDomainBridgeIsNeverFast(t *testing.T) {
	item := tx.SignedTransaction{
		TxID: "bridge",
		AccessList: []tx.AccessItem{
			{Key: "k1", Mode: tx.AccessRead},
			{Key: "k2", Mode: tx.AccessRead},
		},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ControlPolicy:  metaTrackLogicalDomainFrontierPolicy,
			LogicalDomains: []string{"d0000", "d0001"},
			Bridge:         true,
			FrontierDigest: "frontier",
		},
	}
	result := (dualTrackExecution{}).ClassifyBatch(BatchClassificationInput{Transactions: []tx.SignedTransaction{item}})
	decision := result.Decisions[item.TxID]
	if decision.Track != "conservative" {
		t.Fatalf("bridge must be conservative: %#v", decision)
	}
	if !strings.Contains(decision.Reason, "bridge_transaction") {
		t.Fatalf("bridge reason missing: %#v", decision)
	}
	if result.BridgeTransactionCount != 1 || result.ConservativeBridgeCount != 1 {
		t.Fatalf("bridge counters mismatch: %#v", result)
	}
}

func TestMetaTrackStrictAdmissionRequiresDeclaredAccess(t *testing.T) {
	p := metaTrackStrictAdmission{basicPlugin: makeBasic("transaction_admission", "metatrack_strict_admission_v1", nil)}
	if err := p.validateDeclaredAccess(tx.SignedTransaction{}); err == nil {
		t.Fatalf("missing declared access must be rejected")
	}
}
