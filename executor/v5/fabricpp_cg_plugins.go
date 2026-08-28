package v5

import (
	"fmt"
	"sort"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	fabricPPCGExecutionID     = "fabricpp_cg_execution"
	fabricPPCGSchedulerID     = "fabricpp_cg_scheduler"
	fabricPPCGBlockExecutorID = "fabricpp_cg_block_executor"
	fabricPPCGPlanAlgorithmID = "fabricpp_sharma_sigmod2019_traditional_cg_v1"
	fabricPPCGValidatorMode   = "fabricpp_sigmod2019_full_recompute_v1"
)

type fabricPPCGExecution struct{ basicPlugin }
type fabricPPCGScheduler struct{ basicPlugin }
type fabricPPCGBlockExecutor struct{ basicPlugin }

func registerFabricPPCGPlugins(register func(string, string, Factory)) {
	register("execution", fabricPPCGExecutionID, func(config map[string]any) (Plugin, error) {
		return fabricPPCGExecution{makeBasic("execution", fabricPPCGExecutionID, config)}, nil
	})
	register("scheduler", fabricPPCGSchedulerID, func(config map[string]any) (Plugin, error) {
		return fabricPPCGScheduler{makeBasic("scheduler", fabricPPCGSchedulerID, config)}, nil
	})
	register("block_executor", fabricPPCGBlockExecutorID, func(config map[string]any) (Plugin, error) {
		return fabricPPCGBlockExecutor{makeBasic("block_executor", fabricPPCGBlockExecutorID, config)}, nil
	})
}

func validateFabricPPCGCombination(plugins RuntimePlugins) error {
	execSelected := plugins.Execution != nil && plugins.Execution.ID() == fabricPPCGExecutionID
	schedSelected := plugins.Scheduler != nil && plugins.Scheduler.ID() == fabricPPCGSchedulerID
	blockSelected := plugins.BlockExecutor != nil && plugins.BlockExecutor.ID() == fabricPPCGBlockExecutorID
	if !execSelected && !schedSelected && !blockSelected {
		return nil
	}
	if !(execSelected && schedSelected && blockSelected) {
		return fmt.Errorf("Fabric++ CG execution, scheduler, and block executor must be selected together")
	}
	required := []struct {
		category, id string
		plugin       Plugin
	}{
		{"routing", "hash_routing_baseline", plugins.Routing},
		{"block_producer", "time_or_count_block_producer", plugins.BlockProducer},
		{"state_access", "direct_state_access", plugins.StateAccess},
		{"state_storage", "persistent_local_state_store", plugins.StateStorage},
		{"commit", "normal_commit", plugins.Commit},
	}
	for _, item := range required {
		if item.plugin == nil || item.plugin.ID() != item.id {
			actual := "<nil>"
			if item.plugin != nil {
				actual = item.plugin.ID()
			}
			return fmt.Errorf("Fabric++ CG requires %s:%s, got %s", item.category, item.id, actual)
		}
	}
	return nil
}

func (p fabricPPCGExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "fabricpp_cg", Reason: "fabricpp_sigmod2019_read_write_conflict_graph"}
}

func (p fabricPPCGScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}

func (p fabricPPCGScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}

func (p fabricPPCGScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	plan, err := buildFabricPPCGPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalConsensusPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   fabricPPCGPlanAlgorithmID,
		PayloadDigest: stableTextDigest(string(raw)),
		PlanDigest:    plan.PlanDigest,
		Payload:       raw,
	}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}

func (p fabricPPCGScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("Fabric++ CG execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, fabricPPCGPlanAlgorithmID)
	if err != nil {
		return err
	}
	return verifyFabricPPCGPlan(block, plan)
}

// buildFabricPPCGPlan implements the transaction-reordering conflict relation
// from Sharma et al., SIGMOD 2019, Algorithm 1. A paper conflict Ti -> Tj
// exists iff Ti writes a key read by Tj: WS(Ti) intersect RS(Tj) != empty.
// There is deliberately no standalone WW edge in this graph.
//
// The paper states that a conflict Ti -> Tj must be serialized as Tj before Ti.
// Therefore Edges records the paper conflict orientation for audit evidence,
// while residual execution waves are built from the reversed dependency
// relation (reader before writer). Reversing every edge preserves SCCs/cycles
// and hence does not alter Tarjan/Johnson/BreakCycles victim semantics.
func buildFabricPPCGPlan(block realblock.Block) (literatureGraphPlan, error) {
	constructionStarted := time.Now()
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)

	reads := make([]map[string]bool, len(items))
	writes := make([]map[string]bool, len(items))
	for index, item := range items {
		reads[index] = literatureStringSet(item.ReadKeys)
		writes[index] = literatureStringSet(item.WriteKeys)
	}

	paperEdges := map[int]map[int]bool{}
	edgeList := make([]literatureGraphEdge, 0)
	pairChecks := 0
	for reader := 0; reader < len(items); reader++ {
		for writer := 0; writer < len(items); writer++ {
			if reader == writer {
				continue
			}
			pairChecks++
			if !literatureSetIntersects(reads[reader], writes[writer]) {
				continue
			}
			// Paper conflict orientation: writer Ti -> reader Tj.
			if paperEdges[writer] == nil {
				paperEdges[writer] = map[int]bool{}
			}
			if paperEdges[writer][reader] {
				continue
			}
			paperEdges[writer][reader] = true
			edgeList = append(edgeList, literatureGraphEdge{From: writer, To: reader})
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
	waves, victimSequence, err := resolveFabricPPCGCyclesAndSchedule(items, paperEdges)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	sortingMS := time.Since(sortingStarted).Milliseconds()

	abortedIDs := make([]string, 0, len(victimSequence))
	for _, index := range victimSequence {
		abortedIDs = append(abortedIDs, items[index].TxID)
	}
	plan := literatureGraphPlan{
		AlgorithmID:             fabricPPCGPlanAlgorithmID,
		BlockHeight:             block.Height,
		DeclaredAccessSetDigest: accessDigest,
		DeclaredReadKeyCount:    readKeyCount,
		DeclaredWriteKeyCount:   writeKeyCount,
		Edges:                   edgeList,
		ValidatorMode:           fabricPPCGValidatorMode,
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

func verifyFabricPPCGPlan(block realblock.Block, plan literatureGraphPlan) error {
	if plan.ValidatorMode != fabricPPCGValidatorMode {
		return fmt.Errorf("Fabric++ CG validator mode mismatch: %s", plan.ValidatorMode)
	}
	return literatureVerifyPlan(block, plan, fabricPPCGPlanAlgorithmID, buildFabricPPCGPlan)
}

// fabricPPCGCycleSet is an exact sparse representation of Algorithm 1's
// transaction-by-cycle membership table. It stores every elementary cycle;
// there is no cycle cap, approximate counter, heuristic victim, or fallback.
type fabricPPCGCycleSet struct {
	offsets             []int
	members             []int
	membershipCount     []int
	cyclesByVertex      [][]int
	activeCycle         []bool
	remainingMembership int
}

func newFabricPPCGCycleSet(vertexCount int) *fabricPPCGCycleSet {
	return &fabricPPCGCycleSet{offsets: []int{0}, membershipCount: make([]int, vertexCount)}
}

func (set *fabricPPCGCycleSet) addCycle(path []int) {
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

func (set *fabricPPCGCycleSet) cycleCount() int { return len(set.offsets) - 1 }

func (set *fabricPPCGCycleSet) cycleMembers(index int) []int {
	return set.members[set.offsets[index]:set.offsets[index+1]]
}

func (set *fabricPPCGCycleSet) buildIndex() {
	set.cyclesByVertex = make([][]int, len(set.membershipCount))
	set.activeCycle = make([]bool, set.cycleCount())
	for cycleIndex := 0; cycleIndex < set.cycleCount(); cycleIndex++ {
		set.activeCycle[cycleIndex] = true
		for _, vertex := range set.cycleMembers(cycleIndex) {
			set.cyclesByVertex[vertex] = append(set.cyclesByVertex[vertex], cycleIndex)
		}
	}
}

func fabricPPCGAdjacency(vertexCount int, edges map[int]map[int]bool) [][]int {
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

func fabricPPCGCyclicSCCs(vertexCount int, edges map[int]map[int]bool) [][]int {
	indices := make([]int, vertexCount)
	lowlink := make([]int, vertexCount)
	onStack := make([]bool, vertexCount)
	for index := range indices {
		indices[index] = -1
	}
	stack := make([]int, 0, vertexCount)
	nextIndex := 0
	components := make([][]int, 0)
	var strongConnect func(int)
	strongConnect = func(vertex int) {
		indices[vertex] = nextIndex
		lowlink[vertex] = nextIndex
		nextIndex++
		stack = append(stack, vertex)
		onStack[vertex] = true

		children := make([]int, 0, len(edges[vertex]))
		for child := range edges[vertex] {
			children = append(children, child)
		}
		sort.Ints(children)
		for _, child := range children {
			if indices[child] < 0 {
				strongConnect(child)
				if lowlink[child] < lowlink[vertex] {
					lowlink[vertex] = lowlink[child]
				}
			} else if onStack[child] && indices[child] < lowlink[vertex] {
				lowlink[vertex] = indices[child]
			}
		}
		if lowlink[vertex] != indices[vertex] {
			return
		}
		component := make([]int, 0)
		for len(stack) > 0 {
			last := len(stack) - 1
			member := stack[last]
			stack = stack[:last]
			onStack[member] = false
			component = append(component, member)
			if member == vertex {
				break
			}
		}
		if len(component) > 1 {
			sort.Ints(component)
			components = append(components, component)
		}
	}
	for vertex := 0; vertex < vertexCount; vertex++ {
		if indices[vertex] < 0 {
			strongConnect(vertex)
		}
	}
	sort.Slice(components, func(i, j int) bool { return components[i][0] < components[j][0] })
	return components
}

// fabricPPCGEnumerateComponentCycles is a direct Johnson-style elementary-cycle
// enumeration. After each start vertex it recomputes SCCs in the remaining
// induced subgraph, selects the cyclic SCC with the least vertex, and runs the
// blocked/unblock circuit procedure. This preserves Johnson's output-sensitive
// behavior and materializes every directed elementary cycle exactly once.
func fabricPPCGEnumerateComponentCycles(component []int, adjacency [][]int, set *fabricPPCGCycleSet) error {
	if len(component) < 2 {
		return nil
	}
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	vertexCount := len(adjacency)
	for _, vertex := range vertices {
		if vertex < 0 || vertex >= vertexCount {
			return fmt.Errorf("Fabric++ CG SCC vertex out of range: %d", vertex)
		}
	}

	nextLowerBound := vertices[0]
	for {
		allowed := make([]bool, vertexCount)
		for _, vertex := range vertices {
			if vertex >= nextLowerBound {
				allowed[vertex] = true
			}
		}
		components := fabricPPCGJohnsonCyclicSCCs(adjacency, allowed)
		if len(components) == 0 {
			return nil
		}
		// Johnson chooses the cyclic SCC whose least vertex is smallest.
		scc := components[0]
		start := scc[0]
		inSCC := make([]bool, vertexCount)
		for _, vertex := range scc {
			inSCC[vertex] = true
		}

		blocked := make([]bool, vertexCount)
		blockedBy := make([]map[int]bool, vertexCount)
		stack := make([]int, 0, len(scc))
		var unblock func(int)
		unblock = func(vertex int) {
			blocked[vertex] = false
			dependencies := blockedBy[vertex]
			blockedBy[vertex] = nil
			for dependency := range dependencies {
				if blocked[dependency] {
					unblock(dependency)
				}
			}
		}

		var circuit func(int) bool
		circuit = func(current int) bool {
			found := false
			stack = append(stack, current)
			blocked[current] = true
			for _, next := range adjacency[current] {
				if !inSCC[next] {
					continue
				}
				if next == start {
					set.addCycle(append([]int(nil), stack...))
					found = true
				} else if !blocked[next] && circuit(next) {
					found = true
				}
			}
			if found {
				unblock(current)
			} else {
				for _, next := range adjacency[current] {
					if !inSCC[next] {
						continue
					}
					if blockedBy[next] == nil {
						blockedBy[next] = map[int]bool{}
					}
					blockedBy[next][current] = true
				}
			}
			stack = stack[:len(stack)-1]
			return found
		}
		circuit(start)
		nextLowerBound = start + 1
	}
}

func fabricPPCGJohnsonCyclicSCCs(adjacency [][]int, allowed []bool) [][]int {
	vertexCount := len(adjacency)
	indices := make([]int, vertexCount)
	lowlink := make([]int, vertexCount)
	onStack := make([]bool, vertexCount)
	for index := range indices {
		indices[index] = -1
	}
	stack := make([]int, 0, vertexCount)
	nextIndex := 0
	components := make([][]int, 0)
	var strongConnect func(int)
	strongConnect = func(vertex int) {
		indices[vertex] = nextIndex
		lowlink[vertex] = nextIndex
		nextIndex++
		stack = append(stack, vertex)
		onStack[vertex] = true
		for _, child := range adjacency[vertex] {
			if child < 0 || child >= vertexCount || !allowed[child] {
				continue
			}
			if indices[child] < 0 {
				strongConnect(child)
				if lowlink[child] < lowlink[vertex] {
					lowlink[vertex] = lowlink[child]
				}
			} else if onStack[child] && indices[child] < lowlink[vertex] {
				lowlink[vertex] = indices[child]
			}
		}
		if lowlink[vertex] != indices[vertex] {
			return
		}
		component := make([]int, 0)
		for len(stack) > 0 {
			last := len(stack) - 1
			member := stack[last]
			stack = stack[:last]
			onStack[member] = false
			component = append(component, member)
			if member == vertex {
				break
			}
		}
		if len(component) > 1 {
			sort.Ints(component)
			components = append(components, component)
		}
	}
	for vertex := 0; vertex < vertexCount; vertex++ {
		if allowed[vertex] && indices[vertex] < 0 {
			strongConnect(vertex)
		}
	}
	sort.Slice(components, func(i, j int) bool { return components[i][0] < components[j][0] })
	return components
}

// fabricPPCGBreakCycles implements Algorithm 1 step (4) globally over all
// cycles: choose the transaction in the most still-active cycles; on a tie,
// choose the smaller original transaction ordinal; remove every cycle that
// contains that victim; update participation counts; repeat until no cycle is
// left. Johnson is not rerun after each victim.
func fabricPPCGBreakCycles(set *fabricPPCGCycleSet) ([]int, error) {
	if set == nil {
		return nil, fmt.Errorf("Fabric++ CG cycle set is nil")
	}
	set.buildIndex()
	victims := make([]int, 0)
	for set.remainingMembership != 0 {
		if len(set.membershipCount) == 0 {
			return nil, fmt.Errorf("Fabric++ CG has cycle membership but no vertices")
		}
		victim := 0
		for vertex := 1; vertex < len(set.membershipCount); vertex++ {
			// Strict > preserves the smaller ordinal on equal participation.
			if set.membershipCount[vertex] > set.membershipCount[victim] {
				victim = vertex
			}
		}
		if set.membershipCount[victim] <= 0 {
			return nil, fmt.Errorf("Fabric++ CG has remaining cycle membership without a selectable victim")
		}
		victims = append(victims, victim)
		for _, cycleIndex := range set.cyclesByVertex[victim] {
			if !set.activeCycle[cycleIndex] {
				continue
			}
			set.activeCycle[cycleIndex] = false
			for _, member := range set.cycleMembers(cycleIndex) {
				set.remainingMembership--
				set.membershipCount[member]--
				if set.membershipCount[member] < 0 {
					return nil, fmt.Errorf("Fabric++ CG cycle membership underflow for vertex %d", member)
				}
			}
		}
	}
	return victims, nil
}

func resolveFabricPPCGCyclesAndSchedule(items []literatureTxAccess, paperEdges map[int]map[int]bool) ([][]string, []int, error) {
	adjacency := fabricPPCGAdjacency(len(items), paperEdges)
	cycleSet := newFabricPPCGCycleSet(len(items))
	for _, component := range fabricPPCGCyclicSCCs(len(items), paperEdges) {
		if err := fabricPPCGEnumerateComponentCycles(component, adjacency, cycleSet); err != nil {
			return nil, nil, err
		}
	}
	victimSequence, err := fabricPPCGBreakCycles(cycleSet)
	if err != nil {
		return nil, nil, err
	}
	active := make([]bool, len(items))
	for index := range active {
		active[index] = true
	}
	for _, victim := range victimSequence {
		active[victim] = false
	}
	order, err := fabricPPCGSerializableSchedule(items, paperEdges, active)
	if err != nil {
		return nil, nil, err
	}
	// Fabric++ returns one total serializable order. MBE stores a literature plan
	// as waves, so each scheduled transaction is represented as a singleton wave.
	// This is a framework encoding only; it does not add post-reordering parallelism.
	waves := make([][]string, 0, len(order))
	for _, txID := range order {
		waves = append(waves, []string{txID})
	}
	return waves, victimSequence, nil
}

// fabricPPCGSerializableSchedule is a deterministic realization of Algorithm 1
// lines 43-71. The paper rebuilds the residual conflict graph, repeatedly climbs
// from an unscheduled node through unscheduled parents until it reaches a source,
// schedules that source, descends through an unscheduled child, and finally
// inverts the collected order. The pseudo-code leaves getNextNode/iteration order
// unspecified; MBE uses the smaller original transaction ordinal whenever such a
// choice is required so every validator obtains the same plan.
func fabricPPCGSerializableSchedule(items []literatureTxAccess, paperEdges map[int]map[int]bool, active []bool) ([]string, error) {
	parents := make([][]int, len(items))
	children := make([][]int, len(items))
	remaining := 0
	for index := range items {
		if active[index] {
			remaining++
		}
	}
	for from, outgoing := range paperEdges {
		if from < 0 || from >= len(items) || !active[from] {
			continue
		}
		for to := range outgoing {
			if to < 0 || to >= len(items) || !active[to] {
				continue
			}
			children[from] = append(children[from], to)
			parents[to] = append(parents[to], from)
		}
	}
	for index := range items {
		sort.Ints(parents[index])
		sort.Ints(children[index])
	}

	scheduled := make([]bool, len(items))
	nextUnscheduled := func() int {
		for index := range items {
			if active[index] && !scheduled[index] {
				return index
			}
		}
		return -1
	}
	startNode := nextUnscheduled()
	order := make([]int, 0, remaining)
	for len(order) < remaining {
		if startNode < 0 || startNode >= len(items) || !active[startNode] {
			return nil, fmt.Errorf("Fabric++ CG schedule traversal lost an active start node")
		}
		if scheduled[startNode] {
			startNode = nextUnscheduled()
			continue
		}

		addNode := true
		for _, parent := range parents[startNode] {
			if active[parent] && !scheduled[parent] {
				startNode = parent
				addNode = false
				break
			}
		}
		if !addNode {
			continue
		}

		scheduled[startNode] = true
		order = append(order, startNode)
		next := -1
		for _, child := range children[startNode] {
			if active[child] && !scheduled[child] {
				next = child
				break
			}
		}
		if next >= 0 {
			startNode = next
		} else {
			startNode = nextUnscheduled()
		}
	}

	// Algorithm 1 line 71: return order.invert(). Since a paper conflict
	// writer->reader means the reader must precede the writer in the serializable
	// block order, inversion converts the source-first conflict-graph traversal
	// into the required reader-before-writer schedule.
	serialized := make([]string, len(order))
	for index := range order {
		vertex := order[len(order)-1-index]
		serialized[index] = items[vertex].TxID
	}
	return serialized, nil
}
