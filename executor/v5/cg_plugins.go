package v5

import (
	"context"
	"fmt"
	"sort"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	cgExecutionID     = "cg_execution"
	cgSchedulerID     = "cg_scheduler"
	cgBlockExecutorID = "cg_block_executor"
	cgPlanAlgorithmID = "nezha_cg_johnson_conflict_graph_v2"
)

const cgSmartValidatorMode = "nezha_cg_reference_full_recompute_v2"

type cgExecution struct{ basicPlugin }
type cgScheduler struct{ basicPlugin }
type cgBlockExecutor struct{ basicPlugin }

func (p cgExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "cg", Reason: "full_pairwise_conflict_graph"}
}
func (p cgScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p cgScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}
func (p cgScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildCGPlanWithWorkers(block, cgPlanningWorkerCount(p.config))
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: cgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p cgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("cg execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		return err
	}
	return verifyCGPlanSmart(block, plan, cgPlanningWorkerCount(p.config))
}
func (p cgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("cg execution plan missing")
	}
	parseStarted := time.Now()
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	parseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	verifyStarted := time.Now()
	verifyMode := "full_recompute"
	if input.ExecutionPlanVerified {
		verifyMode = "preverified_projection"
		err = verifyPreverifiedLiteratureGraphProjection(input.Block, plan, cgPlanAlgorithmID)
	} else {
		err = verifyCGPlanSmart(input.Block, plan, cgPlanningWorkerCount(p.config))
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result, err := executeCGPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, configuredWorkerCount(p.config, input.WorkerCount))
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if result.ActualMetrics == nil {
		result.ActualMetrics = map[string]any{}
	}
	result.ActualMetrics["literature_plan_parse_ms"] = parseMS
	result.ActualMetrics["literature_plan_verify_ms"] = verifyMS
	result.ActualMetrics["literature_plan_verify_mode"] = verifyMode
	result.ActualMetrics["literature_plan_preverified"] = input.ExecutionPlanVerified
	return result, nil
}

// Nezha's published CG testing path constructs the conflict graph sequentially.
// Worker-count experiments therefore vary only the executor parallelism, not the
// reference CG planner itself.
func cgPlanningWorkerCount(_ map[string]any) int {
	return 1
}

func buildCGPlan(block realblock.Block) (literatureGraphPlan, error) {
	return buildCGPlanWithWorkers(block, cgPlanningWorkerCount(nil))
}

// buildCGPlanWithWorkers is a semantic port of the conventional CG baseline
// shipped with the official Nezha artifact (CGCL-codes/Nezha):
// core/classical_graph.go::NewBuildConflictGraph + test.go::TestConflictGraph.
//
// The reference constructs WW edges from the earlier writer to the later writer
// and RW edges from every reader to every other writer of the same address. RW
// edges may therefore point either forward or backward in block order, so the
// graph can contain directed cycles. The requested worker count is deliberately
// ignored here because the published CG graph-construction path is sequential;
// execution worker count still controls parallel execution of residual DAG waves.
func buildCGPlanWithWorkers(block realblock.Block, _ int) (literatureGraphPlan, error) {
	constructionStarted := time.Now()
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)

	reads := make([]map[string]bool, len(items))
	writes := make([]map[string]bool, len(items))
	writeOwners := map[string][]int{}
	for index, item := range items {
		reads[index] = literatureStringSet(item.ReadKeys)
		writes[index] = literatureStringSet(item.WriteKeys)
		for key := range writes[index] {
			writeOwners[key] = append(writeOwners[key], index)
		}
	}

	edges := map[int]map[int]bool{}
	edgeList := make([]literatureGraphEdge, 0)
	seen := map[uint64]bool{}
	addEdge := func(from, to int) {
		if from == to {
			return
		}
		code := cgEdgeCode(from, to)
		if seen[code] {
			return
		}
		seen[code] = true
		if edges[from] == nil {
			edges[from] = map[int]bool{}
		}
		edges[from][to] = true
		edgeList = append(edgeList, literatureGraphEdge{From: from, To: to})
	}

	// Official NewBuildConflictGraph: preserve block order for write/write edges.
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if literatureSetIntersects(writes[i], writes[j]) {
				addEdge(i, j)
			}
		}
	}
	// Official NewBuildConflictGraph: each reader precedes every other writer of
	// the value it observed, regardless of the writer's original block position.
	for reader := range items {
		for key := range reads[reader] {
			for _, writer := range writeOwners[key] {
				if writer != reader {
					addEdge(reader, writer)
				}
			}
		}
	}

	sort.Slice(edgeList, func(i, j int) bool {
		if edgeList[i].From != edgeList[j].From {
			return edgeList[i].From < edgeList[j].From
		}
		return edgeList[i].To < edgeList[j].To
	})
	constructionMS := time.Since(constructionStarted).Milliseconds()

	sortingStarted := time.Now()
	waves, abortedIndexes, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	sortingMS := time.Since(sortingStarted).Milliseconds()
	abortedIDs := make([]string, 0, len(abortedIndexes))
	for _, index := range abortedIndexes {
		abortedIDs = append(abortedIDs, items[index].TxID)
	}
	pairChecks := len(items) * (len(items) - 1) / 2
	plan := literatureGraphPlan{
		AlgorithmID:             cgPlanAlgorithmID,
		BlockHeight:             block.Height,
		DeclaredAccessSetDigest: accessDigest,
		DeclaredReadKeyCount:    readKeyCount,
		DeclaredWriteKeyCount:   writeKeyCount,
		Edges:                   edgeList,
		ValidatorMode:           cgSmartValidatorMode,
		AbortedTransactionIDs:   abortedIDs,
		Metrics: literatureGraphMetrics{
			TransactionCount:     len(items),
			EdgeCount:            len(edgeList),
			PairChecks:           pairChecks,
			PlanningWorkerCount:  1,
			AbortCount:           len(abortedIDs),
			CycleResolutionCount: len(abortedIDs),
			GraphConstructionMS:  constructionMS,
			SortingMS:            sortingMS,
		},
		Waves: waves,
	}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}

// cgResolveCyclesAndWaves follows the Nezha reference path:
// Tarjan SCC -> one Johnson elementary-cycle enumeration per original SCC ->
// BreakCycles maximum-membership victim selection -> residual topological DAG.
//
// V1 re-enumerated all remaining elementary cycles after every selected victim.
// Although that kept the victim set equivalent, it multiplied the expensive
// Johnson work and caused the 10k/theta=0.6 real-cluster smoke to stall PBFT
// validation. V2 enumerates each SCC exactly once, as graph/johnsonce.go does.
func cgResolveCyclesAndWaves(items []literatureTxAccess, edges map[int]map[int]bool) ([][]string, []int, error) {
	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}

	adjacency := cgNezhaAdjacency(len(items), edges)
	components := cgCyclicSCCs(len(items), edges, active)
	aborted := make([]int, 0)
	for _, component := range components {
		cycleSet, err := cgNezhaFindCycles(component, adjacency)
		if err != nil {
			return nil, nil, err
		}
		victims, err := cgNezhaBreakCycles(cycleSet)
		if err != nil {
			return nil, nil, err
		}
		for _, victim := range victims {
			active[victim] = false
			aborted = append(aborted, victim)
		}
	}

	sort.Ints(aborted)
	waves, err := cgWavesForActive(items, edges, active)
	if err != nil {
		return nil, nil, err
	}
	return waves, aborted, nil
}

// cgNezhaCycleSet is a sparse representation of the official JohnsonCE
// boolMap. The reference allocates one n-vertex membership row per cycle. MBE
// stores only the vertices actually present in each cycle plus a per-vertex
// index of containing cycles. This changes representation only: cycle identity,
// membership counts, victim choice, and residual commit set are identical.
type cgNezhaCycleSet struct {
	offsets             []int
	members             []int
	membershipCount     []int
	cyclesByVertex      [][]int
	activeCycle         []bool
	remainingMembership int
}

func cgNewNezhaCycleSet(vertexCount int) *cgNezhaCycleSet {
	return &cgNezhaCycleSet{
		offsets:         []int{0},
		membershipCount: make([]int, vertexCount),
	}
}

func (set *cgNezhaCycleSet) addCycle(path []int) {
	if len(path) == 0 {
		return
	}
	set.members = append(set.members, path...)
	set.offsets = append(set.offsets, len(set.members))
	for _, vertex := range path {
		set.membershipCount[vertex]++
		set.remainingMembership++
	}
}

func (set *cgNezhaCycleSet) cycleCount() int {
	if len(set.offsets) == 0 {
		return 0
	}
	return len(set.offsets) - 1
}

func (set *cgNezhaCycleSet) cycleMembers(cycleIndex int) []int {
	return set.members[set.offsets[cycleIndex]:set.offsets[cycleIndex+1]]
}

func (set *cgNezhaCycleSet) buildSparseIndex() {
	set.cyclesByVertex = make([][]int, len(set.membershipCount))
	set.activeCycle = make([]bool, set.cycleCount())
	for cycleIndex := 0; cycleIndex < set.cycleCount(); cycleIndex++ {
		set.activeCycle[cycleIndex] = true
		for _, vertex := range set.cycleMembers(cycleIndex) {
			set.cyclesByVertex[vertex] = append(set.cyclesByVertex[vertex], cycleIndex)
		}
	}
}

func cgNezhaAdjacency(vertexCount int, edges map[int]map[int]bool) [][]int {
	adjacency := make([][]int, vertexCount)
	for from, children := range edges {
		if from < 0 || from >= vertexCount {
			continue
		}
		for child := range children {
			if child >= 0 && child < vertexCount {
				adjacency[from] = append(adjacency[from], child)
			}
		}
		sort.Ints(adjacency[from])
	}
	return adjacency
}

// cgNezhaFindCycles is a direct semantic port of
// graph/johnsonce.go::FindCycles/FindCyclesRecur/Unblock at official commit
// 85eaf541591e5f3020dd520cf3b8ee35009d296a. Each elementary directed cycle
// is emitted once. No victim is removed and no SCC is re-enumerated here.
func cgNezhaFindCycles(component []int, adjacency [][]int) (*cgNezhaCycleSet, error) {
	vertexCount := len(adjacency)
	cycleSet := cgNewNezhaCycleSet(vertexCount)
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	if len(vertices) < 2 {
		cycleSet.buildSparseIndex()
		return cycleSet, nil
	}
	for _, vertex := range vertices {
		if vertex < 0 || vertex >= vertexCount {
			return nil, fmt.Errorf("nezha cg SCC vertex out of range: %d", vertex)
		}
	}

	// The reference has an explicit two-vertex SCC fast path and records one
	// two-vertex cycle. A two-vertex SCC in this simple graph necessarily has
	// both directions, because duplicate/self edges are excluded by construction.
	if len(vertices) == 2 {
		cycleSet.addCycle([]int{vertices[0], vertices[1]})
		cycleSet.buildSparseIndex()
		return cycleSet, nil
	}

	explore := make([]bool, vertexCount)
	for _, vertex := range vertices {
		explore[vertex] = true
	}

	for _, start := range vertices {
		blocked := make([]bool, vertexCount)
		blockedMap := make([][]int, vertexCount)
		stack := make([]int, 0, len(vertices))

		var unblock func(int)
		unblock = func(vertex int) {
			blocked[vertex] = false
			for _, dependency := range blockedMap[vertex] {
				if blocked[dependency] {
					unblock(dependency)
				}
			}
			blockedMap[vertex] = nil
		}

		var findCyclesRecur func(int) bool
		findCyclesRecur = func(current int) bool {
			foundCycle := false
			stack = append(stack, current)
			blocked[current] = true

			for _, next := range adjacency[current] {
				if !explore[next] {
					continue
				}
				if next == start {
					foundCycle = true
					cycleSet.addCycle(stack)
				} else if !blocked[next] {
					if findCyclesRecur(next) {
						foundCycle = true
					}
				}
			}

			if foundCycle {
				unblock(current)
			} else {
				for _, next := range adjacency[current] {
					if explore[next] {
						blockedMap[next] = append(blockedMap[next], current)
					}
				}
			}

			stack = stack[:len(stack)-1]
			return foundCycle
		}

		findCyclesRecur(start)
		explore[start] = false
	}

	cycleSet.buildSparseIndex()
	return cycleSet, nil
}

// cgNezhaBreakCycles ports graph/johnsonce.go::BreakCycles. The reference scans
// vertex ordinals with strict '>' when choosing the maximum membership count,
// so the lowest original ordinal wins ties. Clearing a selected victim removes
// every still-active cycle that contains it and decrements all membership counts.
func cgNezhaBreakCycles(cycleSet *cgNezhaCycleSet) ([]int, error) {
	if cycleSet == nil {
		return nil, fmt.Errorf("nezha cg cycle set is nil")
	}
	invalid := make([]bool, len(cycleSet.membershipCount))
	for cycleSet.remainingMembership != 0 {
		victim := 0
		for vertex := 1; vertex < len(cycleSet.membershipCount); vertex++ {
			if cycleSet.membershipCount[vertex] > cycleSet.membershipCount[victim] {
				victim = vertex
			}
		}
		if len(cycleSet.membershipCount) == 0 || cycleSet.membershipCount[victim] <= 0 {
			return nil, fmt.Errorf("nezha cg BreakCycles has remaining membership without a selectable victim")
		}

		for _, cycleIndex := range cycleSet.cyclesByVertex[victim] {
			if !cycleSet.activeCycle[cycleIndex] {
				continue
			}
			cycleSet.activeCycle[cycleIndex] = false
			for _, member := range cycleSet.cycleMembers(cycleIndex) {
				cycleSet.remainingMembership--
				cycleSet.membershipCount[member]--
				if cycleSet.membershipCount[member] < 0 {
					return nil, fmt.Errorf("nezha cg cycle membership underflow for vertex %d", member)
				}
			}
		}
		invalid[victim] = true
	}

	victims := make([]int, 0)
	for vertex, aborted := range invalid {
		if aborted {
			victims = append(victims, vertex)
		}
	}
	return victims, nil
}

func cgCyclicSCCs(vertexCount int, edges map[int]map[int]bool, active []bool) [][]int {
	indices := make([]int, vertexCount)
	lowlink := make([]int, vertexCount)
	onStack := make([]bool, vertexCount)
	for i := range indices {
		indices[i] = -1
	}
	stack := make([]int, 0, vertexCount)
	index := 0
	components := make([][]int, 0)
	var strongConnect func(int)
	strongConnect = func(v int) {
		indices[v] = index
		lowlink[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		children := make([]int, 0, len(edges[v]))
		for child := range edges[v] {
			if active[child] {
				children = append(children, child)
			}
		}
		sort.Ints(children)
		for _, child := range children {
			if indices[child] < 0 {
				strongConnect(child)
				if lowlink[child] < lowlink[v] {
					lowlink[v] = lowlink[child]
				}
			} else if onStack[child] && indices[child] < lowlink[v] {
				lowlink[v] = indices[child]
			}
		}
		if lowlink[v] != indices[v] {
			return
		}
		component := []int{}
		for len(stack) > 0 {
			last := len(stack) - 1
			w := stack[last]
			stack = stack[:last]
			onStack[w] = false
			component = append(component, w)
			if w == v {
				break
			}
		}
		sort.Ints(component)
		cyclic := len(component) > 1
		if len(component) == 1 && edges[component[0]][component[0]] {
			cyclic = true
		}
		if cyclic {
			components = append(components, component)
		}
	}
	for v := 0; v < vertexCount; v++ {
		if active[v] && indices[v] < 0 {
			strongConnect(v)
		}
	}
	sort.Slice(components, func(i, j int) bool { return components[i][0] < components[j][0] })
	return components
}

func cgWavesForActive(items []literatureTxAccess, edges map[int]map[int]bool, active []bool) ([][]string, error) {
	indegree := make([]int, len(items))
	remaining := 0
	for i := range items {
		if active[i] {
			remaining++
		}
	}
	for from, children := range edges {
		if !active[from] {
			continue
		}
		for child := range children {
			if active[child] {
				indegree[child]++
			}
		}
	}
	done := make([]bool, len(items))
	waves := make([][]string, 0)
	for remaining > 0 {
		ready := make([]int, 0)
		for index := range items {
			if active[index] && !done[index] && indegree[index] == 0 {
				ready = append(ready, index)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("cg cycle resolution left a cyclic dependency graph")
		}
		sort.Ints(ready)
		wave := make([]string, 0, len(ready))
		for _, index := range ready {
			done[index] = true
			remaining--
			wave = append(wave, items[index].TxID)
		}
		for _, index := range ready {
			for child := range edges[index] {
				if active[child] && !done[child] {
					indegree[child]--
				}
			}
		}
		waves = append(waves, wave)
	}
	return waves, nil
}

type cgAddressAccess struct {
	readers []int
	writers []int
}

// verifyCGPlanSmart validates the producer-shared DAG from address-indexed
// read/write lists. It detects missing and extra edges without rerunning the
// producer's full n^2 transaction-pair CreateDAG pass on every validator.
func verifyCGPlanSmart(block realblock.Block, plan literatureGraphPlan, workerCount int) error {
	// The producer's planning-worker count is observability evidence, not a
	// scheduling semantic. Validators may use a different local worker count.
	// Rebuild with the producer-recorded count so semantic verification remains
	// independent from validator-local parallelism.
	if plan.ValidatorMode != cgSmartValidatorMode {
		return fmt.Errorf("cg cycle-aware validator mode mismatch: %s", plan.ValidatorMode)
	}
	recordedWorkers := plan.Metrics.PlanningWorkerCount
	if recordedWorkers < 1 {
		return fmt.Errorf("cg plan missing planning worker evidence")
	}
	_ = workerCount
	rebuild := func(candidate realblock.Block) (literatureGraphPlan, error) {
		return buildCGPlanWithWorkers(candidate, recordedWorkers)
	}
	return literatureVerifyPlan(block, plan, cgPlanAlgorithmID, rebuild)
}

func cgOrderedPair(left, right int) (int, int) {
	if left < right {
		return left, right
	}
	return right, left
}

func cgEdgeCode(from, to int) uint64 {
	return uint64(uint32(from))<<32 | uint64(uint32(to))
}

func cgDecodeEdge(code uint64) (int, int) {
	return int(uint32(code >> 32)), int(uint32(code))
}

func cgSameWaves(left, right [][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameStringList(left[index], right[index]) {
			return false
		}
	}
	return true
}
