package v5

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func v33Routing(ordinal uint64, versions []tx.StateVersionDependency) *tx.ExecutionRoutingMetadata {
	return &tx.ExecutionRoutingMetadata{
		ControlPolicy:      metaTrackDeclaredAccessFrontierPolicy,
		RouteBatchSequence: 1,
		RoutingOrdinal:     ordinal,
		StateVersions:      versions,
	}
}

func v33ForkJoinItems() []tx.SignedTransaction {
	return []tx.SignedTransaction{
		{
			TxID:             "left",
			AccessList:       []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: v33Routing(1, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 1}}),
		},
		{
			TxID:             "right",
			AccessList:       []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: v33Routing(2, []tx.StateVersionDependency{{Key: "b", ProducedVersion: 2}}),
		},
		{
			TxID: "join",
			AccessList: []tx.AccessItem{
				{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"},
				{Key: "b", Mode: tx.AccessRead, UpdateSemantics: "validate"},
				{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			},
			ExecutionRouting: v33Routing(3, []tx.StateVersionDependency{
				{Key: "a", RequiredVersion: 1},
				{Key: "b", RequiredVersion: 2},
				{Key: "c", ProducedVersion: 3},
			}),
		},
		{
			TxID:             "tail",
			AccessList:       []tx.AccessItem{{Key: "c", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
			ExecutionRouting: v33Routing(4, []tx.StateVersionDependency{{Key: "c", RequiredVersion: 3}}),
		},
	}
}

func TestMetaTrackV33BitsetMatrixMatchesLegacyFrontierAndComputesStructure(t *testing.T) {
	items := v33ForkJoinItems()
	analysis := analyzeMetaTrackDependencyStructure(items)
	if !analysis.MatrixAnalysisValid || analysis.MatrixAnalysisError != "" {
		t.Fatalf("matrix analysis invalid: %#v", analysis)
	}
	if analysis.MatrixCheckedCount != len(items) || analysis.FrontierWidthMismatch != 0 || analysis.FrontierIDMismatch != 0 {
		t.Fatalf("matrix/legacy frontier mismatch: %#v", analysis)
	}
	join := analysis.ByTx["join"]
	if join.LegacyFrontierWidth != 2 || join.MatrixFrontierWidth != 2 || !join.FrontierWidthEqual || !join.FrontierIDsEqual {
		t.Fatalf("join frontier mismatch: %#v", join)
	}
	if strings.Join(join.LegacyFrontierIDs, "|") != "left|right" || strings.Join(join.MatrixFrontierIDs, "|") != "left|right" {
		t.Fatalf("join frontier ids mismatch: %#v", join)
	}
	if join.DependencyDepthL != 2 || join.TailDepthH != 2 || join.DescendantCountD != 1 || join.DirectChildCount != 1 || !join.CriticalPath || join.CriticalPathLength != 3 {
		t.Fatalf("join structure mismatch: %#v", join)
	}
	left := analysis.ByTx["left"]
	if left.DependencyDepthL != 1 || left.TailDepthH != 3 || left.DescendantCountD != 2 || left.DirectChildCount != 1 || !left.CriticalPath {
		t.Fatalf("left structure mismatch: %#v", left)
	}

	// The production classifier is intentionally untouched by V33. It must
	// still classify the same V11 join as Conservative and the width-one tail
	// as Fast.
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	classification := batchClassification(items, execution)
	if got := classification.Decisions["join"]; got.Track != "conservative" || !strings.Contains(got.Reason, "multi_frontier_value_join") {
		t.Fatalf("legacy join track changed: %#v", got)
	}
	if got := classification.Decisions["tail"]; got.Track != "fast" {
		t.Fatalf("legacy width-one track changed: %#v", got)
	}
}

func TestMetaTrackV33BitsetTransitiveReductionMatchesLegacy(t *testing.T) {
	items := []tx.SignedTransaction{
		{
			TxID: "p1",
			AccessList: []tx.AccessItem{
				{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"},
				{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			},
			ExecutionRouting: v33Routing(1, []tx.StateVersionDependency{
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
			ExecutionRouting: v33Routing(2, []tx.StateVersionDependency{
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
			ExecutionRouting: v33Routing(3, []tx.StateVersionDependency{
				{Key: "c", RequiredVersion: 1},
				{Key: "b", RequiredVersion: 2},
			}),
		},
	}

	analysis := analyzeMetaTrackDependencyStructure(items)
	consumer := analysis.ByTx["consumer"]
	if !consumer.MatrixAnalysisValid || !consumer.FrontierWidthEqual || !consumer.FrontierIDsEqual {
		t.Fatalf("matrix/legacy mismatch: %#v analysis=%#v", consumer, analysis)
	}
	if consumer.LegacyFrontierWidth != 1 || consumer.MatrixFrontierWidth != 1 || strings.Join(consumer.LegacyFrontierIDs, "|") != "p2" || strings.Join(consumer.MatrixFrontierIDs, "|") != "p2" {
		t.Fatalf("transitive reduction mismatch: %#v", consumer)
	}
	if consumer.DependencyDepthL != 3 || consumer.TailDepthH != 1 || consumer.DescendantCountD != 0 || !consumer.CriticalPath || consumer.CriticalPathLength != 3 {
		t.Fatalf("consumer structure mismatch: %#v", consumer)
	}

	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	classification := batchClassification(items, execution)
	if got := classification.Decisions["consumer"]; got.Track != "fast" {
		t.Fatalf("legacy transitive-reduction track changed: %#v", got)
	}
}

func TestMetaTrackV33MatrixFailureIsObservationOnly(t *testing.T) {
	items := []tx.SignedTransaction{
		{
			TxID:             "consumer",
			AccessList:       []tx.AccessItem{{Key: "a", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
			ExecutionRouting: v33Routing(1, []tx.StateVersionDependency{{Key: "a", RequiredVersion: 2}}),
		},
		{
			TxID:             "future-producer",
			AccessList:       []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
			ExecutionRouting: v33Routing(2, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 2}}),
		},
	}

	analysis := analyzeMetaTrackDependencyStructure(items)
	if analysis.MatrixAnalysisValid || analysis.MatrixAnalysisError == "" {
		t.Fatalf("expected observation to reject non-forward edge: %#v", analysis)
	}
	if analysis.MatrixCheckedCount != 0 || analysis.FrontierWidthMismatch != 0 || analysis.FrontierIDMismatch != 0 {
		t.Fatalf("invalid observation must not manufacture equivalence checks: %#v", analysis)
	}

	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	classification := batchClassification(items, execution)
	if got := classification.Decisions["consumer"]; got.Track != "fast" {
		t.Fatalf("observation failure changed production classification: %#v", got)
	}
}

func TestMetaTrackV33DependencyStructureCSV(t *testing.T) {
	dir := t.TempDir()
	items := v33ForkJoinItems()
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	classification := batchClassification(items, execution)
	decisions := make(map[string]ExecutionDecision, len(items))
	for _, item := range items {
		decisions[item.TxID] = decisionForTx(item, classification.Decisions, execution)
	}
	capture := metaTrackDependencyStructureBlockCapture{
		Height:       7,
		BlockHash:    "block-v33",
		Transactions: items,
		Decisions:    decisions,
	}
	runtime := &NodeRuntime{node: NodePlan{DataDir: dir}}
	runtime.dependencyStructureBlocks = []metaTrackDependencyStructureBlockCapture{capture}
	if err := runtime.writeMetaTrackNodeArtifacts(nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(filepath.Join(dir, "metatrack_dependency_structure.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(file).ReadAll()
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(items)+1 {
		t.Fatalf("rows=%d want %d", len(rows), len(items)+1)
	}
	if len(rows[0]) != 27 {
		t.Fatalf("unexpected dependency-structure header width=%d: %#v", len(rows[0]), rows[0])
	}
	wantHeaders := map[string]bool{
		"raw_producer_ids":      false,
		"legacy_frontier_ids":   false,
		"matrix_frontier_width": false,
		"dependency_depth_l":    false,
		"tail_depth_h":          false,
		"descendant_count_d":    false,
		"critical_path":         false,
		"matrix_analysis_valid": false,
		"matrix_analysis_us":    false,
	}
	for _, header := range rows[0] {
		if _, ok := wantHeaders[header]; ok {
			wantHeaders[header] = true
		}
	}
	for header, found := range wantHeaders {
		if !found {
			t.Fatalf("missing dependency structure column %q: %#v", header, rows[0])
		}
	}

	summaryFile, err := os.Open(filepath.Join(dir, "metatrack_dependency_structure_summary.csv"))
	if err != nil {
		t.Fatal(err)
	}
	summaryRows, err := csv.NewReader(summaryFile).ReadAll()
	summaryFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(summaryRows) != 2 || len(summaryRows[0]) != 15 || len(summaryRows[1]) != 15 {
		t.Fatalf("unexpected dependency structure summary: %#v", summaryRows)
	}
	if summaryRows[1][6] != "0" || summaryRows[1][7] != "0" || summaryRows[1][8] != "true" {
		t.Fatalf("matrix equivalence summary failed: %#v", summaryRows[1])
	}
}
