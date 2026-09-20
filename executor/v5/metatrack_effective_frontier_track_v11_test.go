package v5

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func v11Routing(ordinal uint64, versions []tx.StateVersionDependency) *tx.ExecutionRoutingMetadata {
	return &tx.ExecutionRoutingMetadata{
		ControlPolicy:      metaTrackDeclaredAccessFrontierPolicy,
		RouteBatchSequence: 1,
		RoutingOrdinal:     ordinal,
		StateVersions:      versions,
	}
}

func TestMetaTrackV11TransitiveProducerChainStaysFast(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{
			TxID: "p1",
			AccessList: []tx.AccessItem{
				{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"},
				{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			},
			ExecutionRouting: v11Routing(1, []tx.StateVersionDependency{
				{Key: "a", ProducedVersion: 1},
				{Key: "c", ProducedVersion: 1},
			}),
		},
		{
			TxID: "p2",
			AccessList: []tx.AccessItem{
				{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
				{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			},
			ExecutionRouting: v11Routing(2, []tx.StateVersionDependency{
				{Key: "a", RequiredVersion: 1},
				{Key: "b", ProducedVersion: 2},
			}),
		},
		{
			TxID: "consumer",
			AccessList: []tx.AccessItem{
				{Key: "c", Mode: tx.AccessRead, UpdateSemantics: "validate"},
				{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			},
			ExecutionRouting: v11Routing(3, []tx.StateVersionDependency{
				{Key: "c", RequiredVersion: 1},
				{Key: "b", RequiredVersion: 2},
			}),
		},
	}
	result := batchClassificationWithReadiness(items, execution, nil)
	if got := result.Decisions["consumer"]; got.Track != "fast" {
		t.Fatalf("transitively reducible two-producer chain must remain Fast: %#v result=%#v", got, result)
	}
	if result.EffectiveFrontierWidthOneCount < 1 || result.EffectiveFrontierWidthMultiCount != 0 {
		t.Fatalf("unexpected effective frontier counts: %#v", result)
	}
	if result.EffectiveFrontierReducedProducerCount < 1 {
		t.Fatalf("expected p1 to be transitively reduced behind p2: %#v", result)
	}
}

func TestMetaTrackV11IndependentValueFrontiersJoinConservative(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{
			TxID:             "left",
			AccessList:       []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: v11Routing(1, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 1}}),
		},
		{
			TxID:             "right",
			AccessList:       []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: v11Routing(2, []tx.StateVersionDependency{{Key: "b", ProducedVersion: 2}}),
		},
		{
			TxID: "join",
			AccessList: []tx.AccessItem{
				{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
				{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			},
			ExecutionRouting: v11Routing(3, []tx.StateVersionDependency{
				{Key: "a", RequiredVersion: 1},
				{Key: "b", RequiredVersion: 2},
			}),
		},
	}
	result := batchClassificationWithReadiness(items, execution, nil)
	got := result.Decisions["join"]
	if got.Track != "conservative" || !strings.Contains(got.Reason, "multi_frontier_value_join") {
		t.Fatalf("independent value-frontier join must be Conservative: %#v result=%#v", got, result)
	}
	if result.EffectiveFrontierWidthMultiCount != 1 || result.EffectiveFrontierTrackDemotionCount != 1 || result.EffectiveFrontierWidthMax != 2 {
		t.Fatalf("unexpected effective frontier evidence: %#v", result)
	}
}

func TestMetaTrackV11ExternalCompletedVersionsDoNotCreateCurrentWindowJoin(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	item := tx.SignedTransaction{
		TxID: "consumer",
		AccessList: []tx.AccessItem{
			{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
		},
		ExecutionRouting: v11Routing(20, []tx.StateVersionDependency{
			{Key: "a", RequiredVersion: 11},
			{Key: "b", RequiredVersion: 12},
		}),
	}
	result := batchClassificationWithReadiness([]tx.SignedTransaction{item}, execution, nil)
	if got := result.Decisions[item.TxID]; got.Track != "fast" {
		t.Fatalf("already-completed external versions are StateReady inputs, not an in-flight join: %#v", got)
	}
	if result.EffectiveFrontierWidthZeroCount != 1 || result.EffectiveFrontierWidthMultiCount != 0 {
		t.Fatalf("external versions must not enter current-window frontier width: %#v", result)
	}
}

func TestMetaTrackV11ReadinessDoesNotChangeJoinTrack(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	left := tx.SignedTransaction{
		TxID:             "left",
		AccessList:       []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: v11Routing(1, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 1}}),
	}
	right := tx.SignedTransaction{
		TxID:             "right",
		AccessList:       []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: v11Routing(2, []tx.StateVersionDependency{{Key: "b", ProducedVersion: 2}}),
	}
	join := tx.SignedTransaction{
		TxID: "join",
		AccessList: []tx.AccessItem{
			{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
			{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
		},
		ExecutionRouting: v11Routing(3, []tx.StateVersionDependency{
			{Key: "a", RequiredVersion: 1},
			{Key: "b", RequiredVersion: 2},
		}),
	}
	tokenA := stateReadinessToken(join, join.AccessList[0])
	tokenB := stateReadinessToken(join, join.AccessList[1])
	notReady := batchClassificationWithReadiness(
		[]tx.SignedTransaction{left, right, join},
		execution,
		map[string]bool{tokenA: false, tokenB: false},
	)
	ready := batchClassificationWithReadiness(
		[]tx.SignedTransaction{left, right, join},
		execution,
		map[string]bool{tokenA: true, tokenB: true},
	)
	if notReady.Decisions["join"].Track != "conservative" || ready.Decisions["join"].Track != "conservative" {
		t.Fatalf("StateReady must remain orthogonal to structural track classification: notReady=%#v ready=%#v", notReady, ready)
	}
	if len(notReady.StateWaitKeys["join"]) != 2 || len(ready.StateWaitKeys["join"]) != 0 {
		t.Fatalf("StateReady evidence changed unexpectedly: notReady=%#v ready=%#v", notReady.StateWaitKeys, ready.StateWaitKeys)
	}
}
