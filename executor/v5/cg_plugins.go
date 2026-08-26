package v5

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	cgExecutionID     = "cg_execution"
	cgSchedulerID     = "cg_scheduler"
	cgBlockExecutorID = "cg_block_executor"
	cgPlanAlgorithmID = "cg_cycle_aware_conflict_graph_v4"
)

const cgSmartValidatorMode = "cycle_aware_full_recompute_v1"

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

func cgPlanningWorkerCount(config map[string]any) int {
	if value := intValue(config["worker_count"]); value > 0 {
		if value > 8 {
			return 8
		}
		return value
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	return workers
}

func buildCGPlan(block realblock.Block) (literatureGraphPlan, error) {
	return buildCGPlanWithWorkers(block, cgPlanningWorkerCount(nil))
}

// buildCGPlanWithWorkers follows the paper's parallel CreateDAG structure:
// workers claim transaction i independently and compare only against j>i.
// The RW/WR/WW dependency definition and i->j direction are unchanged.
func buildCGPlanWithWorkers(block realblock.Block, workerCount int) (literatureGraphPlan, error) {
	constructionStarted := time.Now()
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(items) && len(items) > 0 {
		workerCount = len(items)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	// Batch-SI's experimental CG baseline requires a cycle-capable conflict
	// graph.  The direction below follows the public classical-CG construction
	// used by the Nezha/MorphDAG reference: read->writer dependencies may point
	// either forward or backward in block order, while WW dependencies preserve
	// the original block order.  Unlike the old i<j-only DAG, this graph can
	// contain cycles and therefore needs deterministic cycle resolution.
	reads := make([]map[string]bool, len(items))
	writes := make([]map[string]bool, len(items))
	for index, item := range items {
		reads[index] = literatureStringSet(item.ReadKeys)
		writes[index] = literatureStringSet(item.WriteKeys)
	}
	pairEdges := make([][]literatureGraphEdge, len(items))
	var next int64 = -1
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1))
				if i >= len(items) {
					return
				}
				local := make([]literatureGraphEdge, 0)
				for j := i + 1; j < len(items); j++ {
					// Classical r-w dependency: the reader must precede the
					// transaction that writes the value it observed.
					if literatureSetIntersects(reads[i], writes[j]) || literatureSetIntersects(writes[i], writes[j]) {
						local = append(local, literatureGraphEdge{From: i, To: j})
					}
					if literatureSetIntersects(reads[j], writes[i]) {
						local = append(local, literatureGraphEdge{From: j, To: i})
					}
				}
				pairEdges[i] = local
			}
		}()
	}
	wg.Wait()

	edges := map[int]map[int]bool{}
	edgeList := make([]literatureGraphEdge, 0)
	seen := map[uint64]bool{}
	for _, row := range pairEdges {
		for _, edge := range row {
			code := cgEdgeCode(edge.From, edge.To)
			if seen[code] {
				continue
			}
			seen[code] = true
			if edges[edge.From] == nil {
				edges[edge.From] = map[int]bool{}
			}
			edges[edge.From][edge.To] = true
			edgeList = append(edgeList, edge)
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
			PlanningWorkerCount:  workerCount,
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

// cgResolveCyclesAndWaves implements the Batch-SI paper-level CG cycle-abort
// semantics without elementary-cycle enumeration. The paper defines CG as a DAG
// in Definition 2, while Section 5.2/5.7 also reports dependency-cycle handling
// and CG aborts; it does not publish a cycle-victim rule. The cited Piduguralla
// ConcSawtooth implementation constructs conflict edges from earlier to later
// transactions and therefore supplies no cycle-victim policy either.
//
// MBE therefore uses a deliberately non-performance-tuned deterministic closure:
// detect cyclic strongly connected components, abort the lowest original
// transaction ordinal that is actually inside a cyclic SCC, and repeat until
// the remaining graph is acyclic. This keeps victim choice consensus-safe and
// bounded by graph traversal; it does not enumerate elementary cycles and does
// not consult TPS, latency, worker count, or any runtime feedback.
func cgResolveCyclesAndWaves(items []literatureTxAccess, edges map[int]map[int]bool) ([][]string, []int, error) {
	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}

	aborted := make([]int, 0)
	for {
		components := cgCyclicSCCs(len(items), edges, active)
		if len(components) == 0 {
			break
		}

		// cgCyclicSCCs sorts each component and then sorts components by their
		// lowest original ordinal. Selecting components[0][0] is therefore a
		// deterministic rule restricted to vertices proven to be cyclic.
		victim := components[0][0]
		active[victim] = false
		aborted = append(aborted, victim)
	}

	sort.Ints(aborted)
	waves, err := cgWavesForActive(items, edges, active)
	if err != nil {
		return nil, nil, err
	}
	return waves, aborted, nil
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
