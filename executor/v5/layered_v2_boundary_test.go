package v5

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestLayeredV2NormalizedAccessPrefersSchedulingEvidence(t *testing.T) {
	record := WorkloadRecord{
		AccessList:           []tx.AccessItem{{Key: "runtime:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		SchedulingAccessList: []tx.AccessItem{{Key: "declared:k", Mode: tx.AccessRead, UpdateSemantics: "none"}},
	}
	got := normalizedAccessItems(record)
	if len(got) != 1 || got[0].Key != "declared:k" {
		t.Fatalf("routing/planning did not prefer scheduling evidence: %#v", got)
	}
}

func TestLayeredV2ClassificationDoesNotDependOnExactStateVersions(t *testing.T) {
	makeItems := func(leftVersion, rightVersion uint64) []tx.SignedTransaction {
		left := tx.SignedTransaction{
			TxID: "left", Sender: "alice",
			AccessList:             []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			SchedulingAccessSchema: "alien_worlds_layered_v2_scheduling_v1",
			SchedulingAccessList:   []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			ExecutionRouting:       &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: leftVersion, ProducedVersion: leftVersion + 1}}},
		}
		right := tx.SignedTransaction{
			TxID: "right", Sender: "bob",
			AccessList:             []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			SchedulingAccessSchema: "alien_worlds_layered_v2_scheduling_v1",
			SchedulingAccessList:   []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			ExecutionRouting:       &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: rightVersion, ProducedVersion: rightVersion + 1}}},
		}
		return []tx.SignedTransaction{left, right}
	}
	execution := dualTrackExecution{}
	first := execution.ClassifyBatch(BatchClassificationInput{Transactions: makeItems(0, 100)})
	second := execution.ClassifyBatch(BatchClassificationInput{Transactions: makeItems(500, 1)})
	if !reflect.DeepEqual(first.Decisions, second.Decisions) || !reflect.DeepEqual(first.Dependencies, second.Dependencies) {
		t.Fatalf("layered V2 classification changed when exact StateVersions changed:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.AmbiguousConflictPairCount != 0 || second.AmbiguousConflictPairCount != 0 {
		t.Fatalf("static TopoSafe graph should not use StateVersion direction evidence: %d %d", first.AmbiguousConflictPairCount, second.AmbiguousConflictPairCount)
	}
	if first.SCCCount != 0 || second.SCCCount != 0 {
		t.Fatalf("same-key RW/RW canonical arbitration must not manufacture an SCC: %d %d", first.SCCCount, second.SCCCount)
	}
	for _, id := range []string{"left", "right"} {
		if first.Decisions[id].Track != "fast" || second.Decisions[id].Track != "fast" {
			t.Fatalf("same-key RW/RW with stable canonical arbitration should remain Fast-eligible: id=%s first=%#v second=%#v", id, first.Decisions[id], second.Decisions[id])
		}
	}
}

func TestLayeredV2ControlledRMWCanPassSemanticSafety(t *testing.T) {
	item := tx.SignedTransaction{
		TxID: "controlled-rmw", Sender: "alice",
		AccessList:             []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
		SchedulingAccessSchema: "alien_worlds_layered_v2_scheduling_v1",
		SchedulingAccessList:   []tx.AccessItem{{Key: "k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
	}
	decision := (dualTrackExecution{}).Classify(item)
	if decision.Track != "fast" {
		t.Fatalf("controlled deterministic RMW should pass SemanticSafe for an otherwise safe transaction: %#v", decision)
	}
}
