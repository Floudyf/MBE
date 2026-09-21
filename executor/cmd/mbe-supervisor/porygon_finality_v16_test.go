package main

import (
	"fmt"
	"testing"

	v5 "metaverse-chainlab/executor/v5"
)

func TestPorygonFinalityV18PreservesHistoricalFinalityAPI(t *testing.T) {
	var legacy func(string, []v5.NodePlan, bool) (map[string]any, error) = deriveFinalityArtifacts
	var protocolAware func(string, []v5.NodePlan, string) (map[string]any, error) = deriveFinalityArtifactsWithMode
	if legacy == nil || protocolAware == nil {
		t.Fatal("finality API compatibility entry points must both exist")
	}
}

func TestPorygonFinalityV18ReproducesAndCloses473IncompleteCase(t *testing.T) {
	classification := map[string]bool{}
	allIDs := make([]any, 0, 1000)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("tx-%04d", i)
		classification[id] = i < 473
		allIDs = append(allIDs, id)
	}
	statuses := []map[string]any{{
		"shard_id":                          "porygon-global",
		"durable_committed_logical_tx_ids":  allIDs,
		"execution_failed_logical_tx_ids":   []any{},
		"admission_rejected_logical_tx_ids": []any{},
	}}

	legacy, counts, err := deriveLiveTerminalWithExpected(classification, statuses, false, map[string]int{"porygon-global": 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 527 || counts["incomplete"] != 473 {
		t.Fatalf("legacy reproduction terminal=%d incomplete=%d", len(legacy), counts["incomplete"])
	}

	direct, counts, err := deriveLiveTerminalWithExpected(classification, statuses, true, map[string]int{"porygon-global": 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(direct) != 1000 || counts["incomplete"] != 0 {
		t.Fatalf("direct finality terminal=%d incomplete=%d", len(direct), counts["incomplete"])
	}
}
