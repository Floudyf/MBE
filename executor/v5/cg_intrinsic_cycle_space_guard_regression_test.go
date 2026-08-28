package v5

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func cgTestCompleteBidirectedAdjacency(n int) [][]int {
	adjacency := make([][]int, n)
	for from := 0; from < n; from++ {
		for to := 0; to < n; to++ {
			if from != to {
				adjacency[from] = append(adjacency[from], to)
			}
		}
	}
	return adjacency
}

func cgTestComponent(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

func cgTestItems(n int, readWriteKey string) []literatureTxAccess {
	items := make([]literatureTxAccess, n)
	for i := range items {
		items[i] = literatureTxAccess{TxID: "T" + string(rune('A'+i)), Ordinal: i}
		if readWriteKey != "" {
			items[i].ReadKeys = []string{readWriteKey}
			items[i].WriteKeys = []string{readWriteKey}
		}
	}
	return items
}

func TestCGCompleteBidirectedSCCExactFastPathMatchesRawJohnson(t *testing.T) {
	for n := 2; n <= 8; n++ {
		adjacency := cgTestCompleteBidirectedAdjacency(n)
		component := cgTestComponent(n)
		if !cgUnitCompleteBidirectedComponent(component, adjacency) {
			t.Fatalf("n=%d complete bidirected component was not recognized", n)
		}
		cycleSet, err := cgNezhaFindCycles(component, adjacency)
		if err != nil {
			t.Fatalf("n=%d raw Johnson: %v", n, err)
		}
		rawVictims, err := cgNezhaBreakCycles(cycleSet)
		if err != nil {
			t.Fatalf("n=%d raw BreakCycles: %v", n, err)
		}
		fastVictims := cgCompleteBidirectedVictims(component)
		if !reflect.DeepEqual(fastVictims, rawVictims) {
			t.Fatalf("n=%d victim sequence mismatch: fast=%v raw=%v", n, fastVictims, rawVictims)
		}
	}
}

func TestCGCompleteBidirectedSCCFastPathRejectsNonUnitMultigraph(t *testing.T) {
	adjacency := cgTestCompleteBidirectedAdjacency(4)
	adjacency[0] = append(adjacency[0], 1)
	if cgUnitCompleteBidirectedComponent(cgTestComponent(4), adjacency) {
		t.Fatal("parallel arc must disable the unit-multiplicity complete-SCC shortcut")
	}
	adjacency = cgTestCompleteBidirectedAdjacency(4)
	adjacency[2] = adjacency[2][1:]
	if cgUnitCompleteBidirectedComponent(cgTestComponent(4), adjacency) {
		t.Fatal("missing arc must disable the complete-SCC shortcut")
	}
}

func TestCGRMWCliqueCycleLowerBoundMatchesSmallExactCounts(t *testing.T) {
	want := map[int]uint64{
		2: 1,
		3: 5,
		4: 20,
		5: 84,
		6: 409,
		7: 2365,
		8: 16064,
	}
	for n, expected := range want {
		if got := cgRMWCliqueCycleLowerBound(n).Uint64(); got != expected {
			t.Fatalf("n=%d cycle count mismatch: got=%d want=%d", n, got, expected)
		}
	}
	if got := cgRMWCliqueCycleLowerBound(16).Uint64(); got != 3_809_950_976_992 {
		t.Fatalf("K16 lower bound mismatch: %d", got)
	}
	if got := cgRMWCliqueCycleLowerBound(102).String(); len(got) != 161 {
		t.Fatalf("K102 exact lower-bound evidence should have 161 digits, got %d: %s", len(got), got)
	}
}

func TestCGRMWCliqueScalableClosureCompletesPreviouslyFatalMixedSCC(t *testing.T) {
	const cliqueSize = cgIntrinsicRMWCliqueGuardSize
	items := cgTestItems(cliqueSize+1, "")
	for i := 0; i < cliqueSize; i++ {
		items[i].ReadKeys = []string{"hot"}
		items[i].WriteKeys = []string{"hot"}
	}
	items[cliqueSize].ReadKeys = []string{"outside"}
	items[cliqueSize].WriteKeys = []string{"outside"}

	adjacency := make([][]int, cliqueSize+1)
	for from := 0; from < cliqueSize; from++ {
		for to := 0; to < cliqueSize; to++ {
			if from != to {
				adjacency[from] = append(adjacency[from], to)
			}
		}
	}
	// Add a two-way bridge through a clique member. The original graph is one
	// mixed SCC and is not itself complete bidirected, so the scalable closure
	// must use the large-RMW-clique reduction rather than the pure-SCC shortcut.
	adjacency[0] = append(adjacency[0], cliqueSize)
	adjacency[cliqueSize] = append(adjacency[cliqueSize], 0)

	frontiers, order, victims, err := cgResolveOfficialAdjacency(items, adjacency)
	if err != nil {
		t.Fatalf("previously fatal large-RMW mixed SCC must now complete deterministically: %v", err)
	}
	wantVictims := make([]int, cliqueSize-1)
	for i := range wantVictims {
		wantVictims[i] = i
	}
	if !reflect.DeepEqual(victims, wantVictims) {
		t.Fatalf("mandatory clique victim set drifted: got=%v want=%v", victims, wantVictims)
	}
	if len(order) != 2 || len(frontiers) == 0 {
		t.Fatalf("expected clique survivor plus fringe transaction: order=%v frontiers=%v", order, frontiers)
	}

	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}
	for _, victim := range victims {
		active[victim] = false
	}
	if residual := cgCyclicSCCs(len(items), cgSimpleEdgesFromAdjacency(adjacency), active); len(residual) != 0 {
		t.Fatalf("scalable closure left a cyclic residual graph: %v", residual)
	}

	frontiers2, order2, victims2, err := cgResolveOfficialAdjacency(items, adjacency)
	if err != nil {
		t.Fatalf("second deterministic scalable resolution failed: %v", err)
	}
	if !reflect.DeepEqual(victims2, victims) ||
		!reflect.DeepEqual(order2, order) ||
		!reflect.DeepEqual(frontiers2, frontiers) {
		t.Fatalf("scalable closure is not deterministic: first=(%v,%v,%v) second=(%v,%v,%v)",
			victims, order, frontiers, victims2, order2, frontiers2)
	}
}

func TestCGTractableMixedSCCStillMatchesExactJohnsonBreakCycles(t *testing.T) {
	const cliqueSize = 5
	items := cgTestItems(cliqueSize+1, "")
	for i := 0; i < cliqueSize; i++ {
		items[i].ReadKeys = []string{"hot"}
		items[i].WriteKeys = []string{"hot"}
	}
	items[cliqueSize].ReadKeys = []string{"outside"}
	items[cliqueSize].WriteKeys = []string{"outside"}

	adjacency := make([][]int, cliqueSize+1)
	for from := 0; from < cliqueSize; from++ {
		for to := 0; to < cliqueSize; to++ {
			if from != to {
				adjacency[from] = append(adjacency[from], to)
			}
		}
	}
	adjacency[0] = append(adjacency[0], cliqueSize)
	adjacency[cliqueSize] = append(adjacency[cliqueSize], 0)

	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}
	components := cgCyclicSCCs(len(items), cgSimpleEdgesFromAdjacency(adjacency), active)
	if len(components) != 1 {
		t.Fatalf("expected one tractable mixed SCC, got %v", components)
	}
	if _, size := cgMaxRMWCliqueInComponent(items, components[0]); size >= cgIntrinsicRMWCliqueGuardSize {
		t.Fatalf("test fixture unexpectedly crosses scalable threshold: %d", size)
	}

	cycleSet, err := cgNezhaFindCycles(components[0], adjacency)
	if err != nil {
		t.Fatalf("raw Johnson enumeration failed: %v", err)
	}
	exactVictims, err := cgNezhaBreakCycles(cycleSet)
	if err != nil {
		t.Fatalf("raw BreakCycles failed: %v", err)
	}
	_, _, resolvedVictims, err := cgResolveOfficialAdjacency(items, adjacency)
	if err != nil {
		t.Fatalf("normal resolver failed: %v", err)
	}
	if !reflect.DeepEqual(resolvedVictims, exactVictims) {
		t.Fatalf("tractable mixed SCC must remain exact Johnson+BreakCycles: resolver=%v exact=%v",
			resolvedVictims, exactVictims)
	}
}

func TestCGRMWCliqueGuardThresholdIsEvidenceOnly(t *testing.T) {
	items := cgTestItems(cgIntrinsicRMWCliqueGuardSize-1, "hot")
	component := cgTestComponent(len(items))
	key, size := cgMaxRMWCliqueInComponent(items, component)
	if key != "hot" || size != cgIntrinsicRMWCliqueGuardSize-1 {
		t.Fatalf("unexpected clique evidence: key=%q size=%d", key, size)
	}
	if size >= cgIntrinsicRMWCliqueGuardSize {
		t.Fatal("below-threshold evidence fixture unexpectedly crosses guard")
	}
}

func TestCGCompleteBidirectedResolveMatchesRawReferenceProjection(t *testing.T) {
	const n = 7
	items := cgTestItems(n, "hot")
	adjacency := cgTestCompleteBidirectedAdjacency(n)

	frontiers, order, victims, err := cgResolveOfficialAdjacency(items, adjacency)
	if err != nil {
		t.Fatal(err)
	}
	cycleSet, err := cgNezhaFindCycles(cgTestComponent(n), adjacency)
	if err != nil {
		t.Fatal(err)
	}
	rawVictims, err := cgNezhaBreakCycles(cycleSet)
	if err != nil {
		t.Fatal(err)
	}
	active := make([]bool, n)
	for i := range active {
		active[i] = true
	}
	for _, victim := range rawVictims {
		active[victim] = false
	}
	rawOrder, err := cgBasicTopologicalOrder(items, adjacency, active)
	if err != nil {
		t.Fatal(err)
	}
	rawFrontiers, err := cgExecutionFrontiersForActive(items, adjacency, active)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(victims, rawVictims) || !reflect.DeepEqual(order, rawOrder) || !reflect.DeepEqual(frontiers, rawFrontiers) {
		t.Fatalf("exact shortcut projection drifted: victims=%v/%v order=%v/%v frontiers=%v/%v", victims, rawVictims, order, rawOrder, frontiers, rawFrontiers)
	}
}

func TestCGIntrinsicCycleSpaceErrorUsesFatalPlanningHandoff(t *testing.T) {
	err := cgIntrinsicCycleSpaceError{ComponentSize: 100, CliqueSize: 16, Key: "hot", LowerBound: cgRMWCliqueCycleLowerBound(16).String()}
	if !isFatalConsensusPlanningError(err) {
		t.Fatal("CG intrinsic cycle-space evidence error must opt into fatal planning handoff")
	}
	if isFatalConsensusPlanningError(nil) {
		t.Fatal("nil planning error must not be fatal")
	}
	if isFatalConsensusPlanningError(errors.New("ordinary planner error")) {
		t.Fatal("ordinary planner errors must remain retryable")
	}
	if isFatalConsensusPlanningError(context.Canceled) || isFatalConsensusPlanningError(context.DeadlineExceeded) {
		t.Fatal("lifecycle cancellation/deadline errors must not become fatal planning errors")
	}
	r := &NodeRuntime{}
	r.markFatalPlanningError(err)
	if r.fatalPlanningError == "" || r.lastProposalError == "" {
		t.Fatalf("fatal planning evidence was not recorded: fatal=%q last=%q", r.fatalPlanningError, r.lastProposalError)
	}
}
