package v5

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	cgExecutionID          = "cg_execution"
	cgSchedulerID          = "cg_scheduler"
	cgBlockExecutorID      = "cg_block_executor"
	cgPlanAlgorithmID      = "nezha_cg_johnson_conflict_graph_v4"
	cgConsensusPlanVersion = "mbe_nezha_cg_retry_metadata_v5"
)

const cgSmartValidatorMode = "nezha_cg_reference_projection_validator_v4"
const cgExecutionAdaptationMode = "mbe_dependency_ready_worker_adaptation_v1"
const cgCycleStorageMode = "nezha_cycle_membership_multiplicity_storage_v1"
const cgIntrinsicCycleGuardMode = "nezha_cg_intrinsic_cycle_space_guard_v1"
const cgExactCompleteBidirectedSCCMode = "nezha_cg_complete_bidirected_scc_exact_v1"
const cgLargeRMWCliqueReductionMode = "nezha_cg_mandatory_rmw_clique_reduction_then_bounded_johnson_v2"
const cgBoundedJohnsonResolutionMode = "nezha_cg_exact_johnson_with_deterministic_prefix_fallback_v1"
const cgRetryLifecycleMode = "fifo_deferred_to_later_block"
const cgIntrinsicRMWCliqueGuardSize = 16
const cgJohnsonCycleOccurrenceBudget int64 = 10000
const cgJohnsonTraversalWorkBudget int64 = 250000
const cgJohnsonPlanTraversalWorkBudget int64 = 1000000

type cgIntrinsicCycleSpaceError struct {
	ComponentSize int
	CliqueSize    int
	Key           string
	LowerBound    string
}

func (e cgIntrinsicCycleSpaceError) Error() string {
	return fmt.Sprintf(
		"nezha cg exact Johnson intrinsic cycle space is intractable: SCC size=%d contains RMW complete-bidirected clique size=%d key=%q; guaranteed internal elementary-cycle lower bound >=%s; refusing heuristic/cycle-cap substitution",
		e.ComponentSize, e.CliqueSize, e.Key, e.LowerBound,
	)
}

func (cgIntrinsicCycleSpaceError) FatalConsensusPlanning() bool { return true }

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
	return p.PlanBlockContext(context.Background(), block, nil)
}

// PlanBlockContext runs the conventional Nezha CG once over the complete
// reserved candidate set. Cycle victims remain algorithmic abort decisions, but
// the MBE experiment lifecycle mirrors ACG: victims are removed from this block,
// released back to FIFO mempool, and retried later instead of terminal failures.
func (p cgScheduler) PlanBlockContext(ctx context.Context, block realblock.Block, report func(consensusPlanningProgress)) (ConsensusExecutionPlanningResult, error) {
	graphPlan, err := buildCGPlanWithContext(ctx, block, cgPlanningWorkerCount(p.config), report)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	accepted, deferred, err := splitCGFirstPassCandidates(block.TxList, graphPlan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	if len(block.TxList) > 0 && len(accepted) == 0 {
		return ConsensusExecutionPlanningResult{}, fmt.Errorf("nezha cg planning deferred every candidate transaction")
	}
	block.TxList = accepted
	block.TxIDs = transactionIDs(accepted)
	consensusPlan := cgConsensusPlan{Version: cgConsensusPlanVersion, GraphPlan: graphPlan, DeferredTransactions: append([]tx.SignedTransaction(nil), deferred...)}
	var raw []byte
	if len(deferred) == 0 {
		// No victim: preserve the original pure literatureGraphPlan payload.
		raw, err = literatureMarshalConsensusPlan(graphPlan)
	} else {
		raw, err = marshalCGConsensusPlan(consensusPlan)
	}
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: cgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: graphPlan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block, Deferred: append([]tx.SignedTransaction(nil), deferred...), Events: cgFirstPassScheduleEvents(graphPlan)}, nil
}

func (p cgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return fmt.Errorf("cg retry execution plan missing or has the wrong algorithm id")
	}
	plan, err := parseCGConsensusPlan(block.ExecutionPlan.Payload)
	if err != nil {
		return err
	}
	return verifyCGConsensusPlan(block, plan, true)
}

func (p cgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil || input.Block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return BlockExecutionResult{}, fmt.Errorf("cg retry execution plan missing before execution")
	}
	parseStarted := time.Now()
	consensusPlan, err := parseCGConsensusPlan(input.Block.ExecutionPlan.Payload)
	parseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	verifyStarted := time.Now()
	verifyMode := "reference_projection"
	if input.ExecutionPlanVerified {
		verifyMode = "preverified_projection"
		err = verifyCGConsensusPlan(input.Block, consensusPlan, false)
	} else {
		err = verifyCGConsensusPlan(input.Block, consensusPlan, true)
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result, err := executeCGPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, consensusPlan.GraphPlan, configuredWorkerCount(p.config, input.WorkerCount))
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
	result.ActualMetrics["nezha_cg_retry_consensus_plan_version"] = consensusPlan.Version
	result.ActualMetrics["cg_cycle_deferred_tx_ids"] = transactionIDs(consensusPlan.DeferredTransactions)
	return result, nil
}

type cgConsensusPlan struct {
	Version              string                 `json:"version"`
	GraphPlan            literatureGraphPlan    `json:"graph_plan"`
	DeferredTransactions []tx.SignedTransaction `json:"deferred_transactions,omitempty"`
}

func marshalCGConsensusPlan(plan cgConsensusPlan) ([]byte, error) {
	// Keep literatureGraphPlan fields at the top level. Existing lifecycle code
	// can parse this payload as an ordinary CG plan; retry metadata is additive
	// and protected by the envelope PayloadDigest.
	graphRaw, err := literatureMarshalConsensusPlan(plan.GraphPlan)
	if err != nil {
		return nil, err
	}
	fields := map[string]json.RawMessage{}
	if err := literatureJSONUnmarshal(graphRaw, &fields); err != nil {
		return nil, fmt.Errorf("decode cg graph plan for retry overlay: %w", err)
	}
	versionRaw, err := literatureJSONMarshal(plan.Version)
	if err != nil {
		return nil, err
	}
	deferredRaw, err := literatureJSONMarshal(plan.DeferredTransactions)
	if err != nil {
		return nil, err
	}
	fields["cg_retry_plan_version"] = versionRaw
	fields["cg_deferred_transactions"] = deferredRaw
	return literatureJSONMarshal(fields)
}

func parseCGConsensusPlan(raw []byte) (cgConsensusPlan, error) {
	graphPlan, err := literatureParsePlan(raw, cgPlanAlgorithmID)
	if err != nil {
		return cgConsensusPlan{}, err
	}
	var extension struct {
		Version              string                 `json:"cg_retry_plan_version"`
		DeferredTransactions []tx.SignedTransaction `json:"cg_deferred_transactions,omitempty"`
	}
	if err := literatureJSONUnmarshal(raw, &extension); err != nil {
		return cgConsensusPlan{}, fmt.Errorf("decode cg retry metadata: %w", err)
	}
	if extension.Version == "" {
		if len(graphPlan.AbortedTransactionIDs) != 0 {
			return cgConsensusPlan{}, fmt.Errorf("cg plan with cycle victims is missing retry metadata")
		}
		return cgConsensusPlan{Version: "none", GraphPlan: graphPlan}, nil
	}
	if extension.Version != cgConsensusPlanVersion {
		return cgConsensusPlan{}, fmt.Errorf("unsupported cg retry metadata version %q", extension.Version)
	}
	return cgConsensusPlan{
		Version:              extension.Version,
		GraphPlan:            graphPlan,
		DeferredTransactions: append([]tx.SignedTransaction(nil), extension.DeferredTransactions...),
	}, nil
}

func splitCGFirstPassCandidates(items []tx.SignedTransaction, plan literatureGraphPlan) ([]tx.SignedTransaction, []tx.SignedTransaction, error) {
	if !sameStringList(transactionIDs(items), plan.CandidateTransactionIDs) {
		return nil, nil, fmt.Errorf("nezha cg first-pass candidate identity mismatch")
	}
	aborted := make(map[string]bool, len(plan.AbortedTransactionIDs))
	for _, id := range plan.AbortedTransactionIDs {
		if id == "" || aborted[id] {
			return nil, nil, fmt.Errorf("nezha cg first-pass abort set contains duplicate/empty transaction id")
		}
		aborted[id] = true
	}
	accepted := make([]tx.SignedTransaction, 0, len(items)-len(aborted))
	deferred := make([]tx.SignedTransaction, 0, len(aborted))
	seen := map[string]bool{}
	for _, item := range items {
		if aborted[item.TxID] {
			deferred = append(deferred, item)
			seen[item.TxID] = true
		} else {
			accepted = append(accepted, item)
		}
	}
	if len(seen) != len(aborted) {
		return nil, nil, fmt.Errorf("nezha cg abort set references transaction outside candidate block")
	}
	return accepted, deferred, nil
}

func cgFirstPassScheduleEvents(plan literatureGraphPlan) []ScheduleEvent {
	aborted := map[string]bool{}
	for _, id := range plan.AbortedTransactionIDs {
		aborted[id] = true
	}
	waveByID := map[string]int{}
	for waveIndex, wave := range plan.Waves {
		for _, id := range wave {
			waveByID[id] = waveIndex
		}
	}
	events := make([]ScheduleEvent, 0, len(plan.CandidateTransactionIDs))
	for _, id := range plan.CandidateTransactionIDs {
		if aborted[id] {
			events = append(events, ScheduleEvent{TxID: id, Track: "cg", QueueName: "mempool_deferred", DecisionReason: "nezha_cg_cycle_victim_deferred_retry", Blocked: true})
		} else {
			events = append(events, ScheduleEvent{TxID: id, Track: "cg", QueueName: fmt.Sprintf("cg_wave_%d", waveByID[id]), DecisionReason: "nezha_cg_accepted", LocalExecution: true})
		}
	}
	return events
}

func verifyCGConsensusPlan(block realblock.Block, plan cgConsensusPlan, fullProjection bool) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return fmt.Errorf("cg retry execution plan is missing")
	}
	if block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) || block.ExecutionPlan.PlanDigest != plan.GraphPlan.PlanDigest {
		return fmt.Errorf("cg retry execution plan envelope mismatch")
	}
	if plan.GraphPlan.BlockHeight != block.Height {
		return fmt.Errorf("cg retry graph plan block height mismatch")
	}
	deferredIDs := transactionIDs(plan.DeferredTransactions)
	if !sameStringList(deferredIDs, plan.GraphPlan.AbortedTransactionIDs) {
		return fmt.Errorf("cg retry deferred identity does not match cycle-victim set")
	}
	deferredSet := map[string]bool{}
	for _, id := range deferredIDs {
		if id == "" || deferredSet[id] {
			return fmt.Errorf("cg retry deferred set invalid")
		}
		deferredSet[id] = true
	}
	expectedAccepted := make([]string, 0, len(plan.GraphPlan.CandidateTransactionIDs)-len(deferredSet))
	for _, id := range plan.GraphPlan.CandidateTransactionIDs {
		if !deferredSet[id] {
			expectedAccepted = append(expectedAccepted, id)
		}
	}
	if !sameStringList(transactionIDs(block.TxList), expectedAccepted) {
		return fmt.Errorf("cg retry accepted block is not exact victim projection")
	}
	acceptedByID := make(map[string]tx.SignedTransaction, len(block.TxList))
	for _, item := range block.TxList {
		if item.TxID == "" {
			return fmt.Errorf("cg retry accepted block contains empty transaction id")
		}
		if _, exists := acceptedByID[item.TxID]; exists {
			return fmt.Errorf("cg retry accepted block contains duplicate transaction %s", item.TxID)
		}
		acceptedByID[item.TxID] = item
	}
	deferredByID := make(map[string]tx.SignedTransaction, len(plan.DeferredTransactions))
	for _, item := range plan.DeferredTransactions {
		if item.TxID == "" {
			return fmt.Errorf("cg retry deferred plan contains empty transaction id")
		}
		if _, exists := deferredByID[item.TxID]; exists {
			return fmt.Errorf("cg retry deferred plan contains duplicate transaction %s", item.TxID)
		}
		deferredByID[item.TxID] = item
	}
	fullItems := make([]tx.SignedTransaction, 0, len(plan.GraphPlan.CandidateTransactionIDs))
	for _, id := range plan.GraphPlan.CandidateTransactionIDs {
		if item, ok := acceptedByID[id]; ok {
			if _, both := deferredByID[id]; both {
				return fmt.Errorf("cg retry tx %s in both projections", id)
			}
			fullItems = append(fullItems, item)
			continue
		}
		item, ok := deferredByID[id]
		if !ok {
			return fmt.Errorf("cg retry candidate %s cannot be reconstructed", id)
		}
		fullItems = append(fullItems, item)
	}
	if len(acceptedByID)+len(deferredByID) != len(fullItems) {
		return fmt.Errorf("cg retry plan contains identities outside candidate set")
	}
	reconstructed := block
	reconstructed.TxList = fullItems
	reconstructed.TxIDs = transactionIDs(fullItems)
	reconstructed.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   cgPlanAlgorithmID,
		PayloadDigest: block.ExecutionPlan.PayloadDigest,
		PlanDigest:    plan.GraphPlan.PlanDigest,
		Payload:       append([]byte(nil), block.ExecutionPlan.Payload...),
	}
	if fullProjection {
		return verifyCGPlanSmart(reconstructed, plan.GraphPlan, 1)
	}
	return verifyCGPreverifiedProjection(reconstructed, plan.GraphPlan)
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

type cgPlanningProbe struct {
	ctx              context.Context
	report           func(consensusPlanningProgress)
	workUnits        int64
	detailCount      int64
	lastCheckWork    int64
	lastReportWork   int64
	lastReportDetail int64
	phase            string
}

func newCGPlanningProbe(ctx context.Context, report func(consensusPlanningProgress)) *cgPlanningProbe {
	if ctx == nil {
		ctx = context.Background()
	}
	return &cgPlanningProbe{ctx: ctx, report: report}
}

func (p *cgPlanningProbe) checkpoint(phase string, delta, detail int64, force bool) error {
	if p == nil {
		return nil
	}
	if delta > 0 {
		p.workUnits += delta
	}
	if detail > p.detailCount {
		p.detailCount = detail
	}
	phaseChanged := phase != "" && phase != p.phase
	if phaseChanged {
		p.phase = phase
		force = true
	}
	if force || p.workUnits-p.lastCheckWork >= 1024 {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		p.lastCheckWork = p.workUnits
	}
	if p.report != nil && (force || p.workUnits-p.lastReportWork >= 16384 || p.detailCount-p.lastReportDetail >= 1024) {
		p.report(consensusPlanningProgress{AlgorithmID: cgPlanAlgorithmID, Phase: p.phase, WorkUnits: p.workUnits, DetailCount: p.detailCount})
		p.lastReportWork = p.workUnits
		p.lastReportDetail = p.detailCount
	}
	return nil
}

// buildCGPlanWithWorkers is a semantic port of the conventional CG baseline
// shipped with the official Nezha artifact (CGCL-codes/Nezha):
// core/classical_graph.go::NewBuildConflictGraph + graph/tarjanscc.go +
// graph/johnsonce.go + test.go::TestConflictGraph.
//
// Two source-level details are intentionally preserved here:
//  1. WW construction appends one i->j adjacency entry for every shared write
//     address. Parallel WW arcs are therefore meaningful input multiplicity to
//     JohnsonCE and MUST NOT be collapsed into a simple graph.
//  2. RW construction appends reader->writer only if that target is not already
//     present in the reader's adjacency row, matching isExistForInt in the
//     reference NewBuildConflictGraph implementation.
//
// The published CG planner path is sequential, so the requested PLANNER worker
// count is deliberately ignored. The official RebuildGraph +
// BasicTopologicalSort order is stored in SerializationOrder as reference
// evidence. The shared plan's Waves field is used only as an MBE execution
// backend carrier for dependency-ready frontiers; it is NOT a Nezha-CG
// algorithm concept. This lets worker-count experiments vary execution
// parallelism without changing graph construction, JohnsonCE victims, or the
// official reference serialization order.
func buildCGPlanWithWorkers(block realblock.Block, workerCount int) (literatureGraphPlan, error) {
	return buildCGPlanWithContext(context.Background(), block, workerCount, nil)
}

func buildCGPlanWithContext(ctx context.Context, block realblock.Block, _ int, report func(consensusPlanningProgress)) (literatureGraphPlan, error) {
	probe := newCGPlanningProbe(ctx, report)
	if err := probe.checkpoint("access_descriptors", 1, 0, true); err != nil {
		return literatureGraphPlan{}, err
	}
	constructionStarted := time.Now()
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	adjacency, edgeList, err := cgBuildOfficialConflictMultigraphContext(items, probe)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	constructionMS := time.Since(constructionStarted).Milliseconds()

	sortingStarted := time.Now()
	executionFrontiers, officialOrder, abortedIndexes, err := cgResolveOfficialAdjacencyContext(items, adjacency, probe)
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
		Waves: executionFrontiers,
	}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	plan = literatureFinalizePlan(plan)
	// literatureFinalizePlan derives SerializationOrder from the shared Waves
	// carrier. CG must retain the authors' BasicTopologicalSort order separately
	// as reference evidence, because dependency-ready executor frontiers may
	// flatten to a different but dependency-equivalent order.
	plan.SerializationOrder = plan.SerializationOrder[:0]
	for _, index := range officialOrder {
		plan.SerializationOrder = append(plan.SerializationOrder, items[index].TxID)
	}
	plan.PlanDigest = literaturePlanDigest(plan)
	if err := probe.checkpoint("complete", 1, int64(len(abortedIndexes)), true); err != nil {
		return literatureGraphPlan{}, err
	}
	return plan, nil
}

// cgBuildOfficialConflictMultigraph preserves the adjacency-list multiplicity
// emitted by Nezha core.NewBuildConflictGraph. The MBE access-list contract has
// already canonicalized duplicate declarations of the same key inside one
// transaction, so each shared logical write address contributes one WW arc.
func cgBuildOfficialConflictMultigraph(items []literatureTxAccess) ([][]int, []literatureGraphEdge) {
	adjacency, edges, err := cgBuildOfficialConflictMultigraphContext(items, nil)
	if err != nil {
		panic(err)
	}
	return adjacency, edges
}

func cgBuildOfficialConflictMultigraphContext(items []literatureTxAccess, probe *cgPlanningProbe) ([][]int, []literatureGraphEdge, error) {
	if probe != nil {
		if err := probe.checkpoint("conflict_multigraph", 1, 0, true); err != nil {
			return nil, nil, err
		}
	}
	adjacency := make([][]int, len(items))
	edgeList := make([]literatureGraphEdge, 0)
	writes := make([]map[string]bool, len(items))
	writeOwners := map[string][]int{}
	for index, item := range items {
		writes[index] = literatureStringSet(item.WriteKeys)
		for _, key := range item.WriteKeys {
			writeOwners[key] = append(writeOwners[key], index)
		}
	}

	// Official NewBuildConflictGraph WW loop: for each later transaction j,
	// append j once for every write node whose key is also written by i. There is
	// intentionally no duplicate-target guard on this branch.
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if probe != nil {
				if err := probe.checkpoint("conflict_multigraph_ww", 1, int64(len(edgeList)), false); err != nil {
					return nil, nil, err
				}
			}
			for _, key := range items[j].WriteKeys {
				if writes[i][key] {
					adjacency[i] = append(adjacency[i], j)
					edgeList = append(edgeList, literatureGraphEdge{From: i, To: j})
				}
			}
		}
	}

	// Official RW loop: reader precedes every other writer of the read key, but
	// isExistForInt suppresses a target already present from WW or another read.
	for reader, item := range items {
		for _, key := range item.ReadKeys {
			if probe != nil {
				if err := probe.checkpoint("conflict_multigraph_rw", 1, int64(len(edgeList)), false); err != nil {
					return nil, nil, err
				}
			}
			for _, writer := range writeOwners[key] {
				if writer == reader || cgAdjacencyContains(adjacency[reader], writer) {
					continue
				}
				adjacency[reader] = append(adjacency[reader], writer)
				edgeList = append(edgeList, literatureGraphEdge{From: reader, To: writer})
			}
		}
	}
	if probe != nil {
		if err := probe.checkpoint("conflict_multigraph_complete", 1, int64(len(edgeList)), true); err != nil {
			return nil, nil, err
		}
	}
	return adjacency, edgeList, nil
}

func cgAdjacencyContains(row []int, target int) bool {
	for _, value := range row {
		if value == target {
			return true
		}
	}
	return false
}

// cgUnitCompleteBidirectedComponent identifies the special case where an
// original Tarjan SCC is exactly a simple complete bidirected digraph: every
// ordered pair of distinct vertices occurs once and only once, and no self arc
// is present. For this graph family the official JohnsonCE/BreakCycles result
// has a closed form: all vertices have equal cycle membership, strict '>' tie
// breaking removes the lowest ordinal, and the same invariant repeats on the
// induced complete graph. Thus every vertex except the highest ordinal is an
// exact victim, without enumerating the factorial cycle family.
func cgUnitCompleteBidirectedComponent(component []int, adjacency [][]int) bool {
	if len(component) < 2 {
		return false
	}
	inComponent := make([]bool, len(adjacency))
	for _, vertex := range component {
		if vertex < 0 || vertex >= len(adjacency) {
			return false
		}
		inComponent[vertex] = true
	}
	for _, from := range component {
		counts := make(map[int]int, len(component)-1)
		for _, to := range adjacency[from] {
			if !inComponent[to] {
				continue
			}
			if to == from {
				return false
			}
			counts[to]++
		}
		if len(counts) != len(component)-1 {
			return false
		}
		for _, to := range component {
			if to == from {
				continue
			}
			if counts[to] != 1 {
				return false
			}
		}
	}
	return true
}

func cgCompleteBidirectedVictims(component []int) []int {
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	if len(vertices) <= 1 {
		return nil
	}
	return append([]int(nil), vertices[:len(vertices)-1]...)
}

// cgMaxRMWCliqueInComponent finds a provable complete-bidirected transaction
// clique induced by one logical key that every member both reads and writes.
// Under Nezha NewBuildConflictGraph, earlier writers point to later writers by
// WW and later RMW readers point back to earlier writers by RW. Therefore a
// group of m such transactions contains every simple directed cycle of K_m,
// irrespective of any additional edges in the surrounding SCC.
func cgMaxRMWCliqueMembersInComponent(items []literatureTxAccess, component []int) (string, []int) {
	membersByKey := map[string][]int{}
	for _, vertex := range component {
		if vertex < 0 || vertex >= len(items) {
			continue
		}
		reads := literatureStringSet(items[vertex].ReadKeys)
		seenRMW := map[string]bool{}
		for _, key := range items[vertex].WriteKeys {
			if reads[key] && !seenRMW[key] {
				membersByKey[key] = append(membersByKey[key], vertex)
				seenRMW[key] = true
			}
		}
	}
	bestKey := ""
	var bestMembers []int
	for key, members := range membersByKey {
		if len(members) > len(bestMembers) ||
			(len(members) == len(bestMembers) && len(members) > 0 && (bestKey == "" || key < bestKey)) {
			bestKey = key
			bestMembers = append(bestMembers[:0], members...)
		}
	}
	sort.Ints(bestMembers)
	return bestKey, append([]int(nil), bestMembers...)
}

func cgMaxRMWCliqueInComponent(items []literatureTxAccess, component []int) (string, int) {
	key, members := cgMaxRMWCliqueMembersInComponent(items, component)
	return key, len(members)
}

// cgMandatoryRMWCliqueVictims applies only after a large RMW clique has made
// exact Johnson enumeration intrinsically intractable. Every pair of clique
// members has a directed 2-cycle, so any acyclic residual graph can retain at
// most one member: at least m-1 clique vertices are mathematically mandatory
// feedback vertices regardless of the surrounding SCC.
//
// We retain the greatest original ordinal and remove the lower ordinals. That
// is exactly the official strict-'>' BreakCycles tie outcome for an isolated
// complete-bidirected SCC. In a mixed SCC, fringe cycles can change the exact
// reference victim order/survivor, so this is deliberately recorded as an MBE
// scalable engineering adaptation rather than claimed as exact Johnson-victim
// equivalence. After the mandatory reduction, every residual cyclic SCC is
// processed again by the exact Johnson + BreakCycles path.
func cgMandatoryRMWCliqueVictims(members []int) []int {
	vertices := append([]int(nil), members...)
	sort.Ints(vertices)
	if len(vertices) <= 1 {
		return nil
	}
	return append([]int(nil), vertices[:len(vertices)-1]...)
}

func cgRMWCliqueCycleLowerBound(cliqueSize int) *big.Int {
	// A simple complete bidirected K_m has exactly
	// sum_{k=2..m} C(m,k)*(k-1)! elementary directed cycles. An RMW clique
	// guarantees at least those unit-edge cycles even when the surrounding SCC
	// or the original multigraph contributes additional occurrences. big.Int is
	// used only on the intractability path so diagnostic evidence never overflows.
	total := new(big.Int)
	if cliqueSize < 2 {
		return total
	}
	for k := 2; k <= cliqueSize; k++ {
		term := big.NewInt(1)
		for i := 0; i < k; i++ {
			term.Mul(term, big.NewInt(int64(cliqueSize-i)))
		}
		term.Div(term, big.NewInt(int64(k)))
		total.Add(total, term)
	}
	return total
}

func cgSimpleEdgesFromAdjacency(adjacency [][]int) map[int]map[int]bool {
	edges := map[int]map[int]bool{}
	for from, row := range adjacency {
		for _, to := range row {
			if edges[from] == nil {
				edges[from] = map[int]bool{}
			}
			edges[from][to] = true
		}
	}
	return edges
}

// cgResolveCyclesAndWaves retains the historical simple-graph helper signature
// used by focused JohnsonCE regression tests. The returned slices are MBE
// dependency-ready executor frontiers, not a Nezha-CG algorithm primitive.
func cgResolveCyclesAndWaves(items []literatureTxAccess, edges map[int]map[int]bool) ([][]string, []int, error) {
	frontiers, _, aborted, err := cgResolveOfficialAdjacency(items, cgNezhaAdjacency(len(items), edges))
	return frontiers, aborted, err
}

// cgResolveOfficialAdjacency follows the exact Nezha reference planner path:
// Tarjan SCC -> one Johnson elementary-cycle enumeration per original SCC ->
// BreakCycles maximum-membership victim selection -> RebuildGraph ->
// BasicTopologicalSort. In addition, MBE derives dependency-ready execution
// frontiers from the already-decided residual graph. Those frontiers are an
// execution-resource adaptation only and never feed back into victim selection
// or the official reference serialization order.
func cgResolveOfficialAdjacency(items []literatureTxAccess, adjacency [][]int) ([][]string, []int, []int, error) {
	return cgResolveOfficialAdjacencyContext(items, adjacency, nil)
}

func cgResolveOfficialAdjacencyContext(items []literatureTxAccess, adjacency [][]int, probe *cgPlanningProbe) ([][]string, []int, []int, error) {
	if probe != nil {
		if err := probe.checkpoint("tarjan_scc", 1, 0, true); err != nil {
			return nil, nil, nil, err
		}
	}
	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}

	simpleEdges := cgSimpleEdgesFromAdjacency(adjacency)
	aborted := make([]int, 0)
	remainingJohnsonWork := cgJohnsonPlanTraversalWorkBudget

	// Recompute SCCs only after a resolution step has removed vertices. Exact
	// Johnson remains one-shot per tractable SCC. The loop is needed because a
	// mandatory large-clique reduction intentionally resolves only the
	// intrinsically explosive clique first; any residual fringe cycles are then
	// handed back to the exact reference path.
	for resolutionRound := 0; ; resolutionRound++ {
		components := cgCyclicSCCs(len(items), simpleEdges, active)
		if len(components) == 0 {
			break
		}
		progressed := false

		for componentIndex, component := range components {
			if probe != nil {
				detail := int64(resolutionRound)<<32 | int64(componentIndex)
				if err := probe.checkpoint("johnson_scc", 1, detail, true); err != nil {
					return nil, nil, nil, err
				}
			}

			var victims []int
			if cgUnitCompleteBidirectedComponent(component, adjacency) {
				// Exact closed form for the unit-multiplicity complete-bidirected SCC.
				victims = cgCompleteBidirectedVictims(component)
				if probe != nil {
					if err := probe.checkpoint("johnson_complete_bidirected_exact", 1, int64(len(victims)), true); err != nil {
						return nil, nil, nil, err
					}
				}
			} else {
				key, cliqueMembers := cgMaxRMWCliqueMembersInComponent(items, component)
				if len(cliqueMembers) >= cgIntrinsicRMWCliqueGuardSize {
					// A large RMW clique creates a provably astronomical Johnson
					// cycle space. All but one clique member are mandatory feedback
					// vertices because every pair forms a directed 2-cycle. Reduce
					// only those mandatory vertices, then loop back and run exact
					// Johnson on whatever cyclic fringe remains.
					victims = cgMandatoryRMWCliqueVictims(cliqueMembers)
					if len(victims) == 0 {
						return nil, nil, nil, fmt.Errorf("nezha cg large RMW clique reduction made no progress: key=%q size=%d", key, len(cliqueMembers))
					}
					if probe != nil {
						if err := probe.checkpoint("rmw_clique_mandatory_reduction", 1, int64(len(victims)), true); err != nil {
							return nil, nil, nil, err
						}
					}
				} else {
					var cycleSet *cgNezhaCycleSet
					var truncated bool
					var traversalWork int64
					var err error

					if remainingJohnsonWork > 0 {
						callBudget := cgJohnsonTraversalWorkBudget
						if remainingJohnsonWork < callBudget {
							callBudget = remainingJohnsonWork
						}
						cycleSet, truncated, traversalWork, err = cgNezhaFindCyclesBoundedContext(
							component, adjacency, probe,
							cgJohnsonCycleOccurrenceBudget, callBudget,
						)
						remainingJohnsonWork -= traversalWork
						if remainingJohnsonWork < 0 {
							remainingJohnsonWork = 0
						}
					} else {
						truncated = true
						witness := cgDeterministicCycleWitness(component, adjacency)
						if len(witness) == 0 {
							return nil, nil, nil, fmt.Errorf("nezha cg exhausted Johnson plan budget without residual cycle witness")
						}
						cycleSet = cgNewNezhaCycleSet(len(adjacency))
						cycleSet.addCycle(witness)
						if err = cycleSet.buildSparseIndexContext(probe); err != nil {
							return nil, nil, nil, err
						}
						if probe != nil {
							if err = probe.checkpoint("johnson_plan_budget_cycle_witness", 1, int64(len(witness)), true); err != nil {
								return nil, nil, nil, err
							}
						}
					}
					if err != nil {
						return nil, nil, nil, err
					}
					if truncated && probe != nil {
						detail := int64(cycleSet.cycleOccurrenceCount())<<32 | (traversalWork & 0xffffffff)
						if err := probe.checkpoint("johnson_budget_prefix_fallback", 1, detail, true); err != nil {
							return nil, nil, nil, err
						}
					}
					victims, err = cgNezhaBreakCyclesContext(cycleSet, probe)
					if err != nil {
						return nil, nil, nil, err
					}
				}
			}

			for _, victim := range victims {
				if victim < 0 || victim >= len(active) {
					return nil, nil, nil, fmt.Errorf("nezha cg cycle resolution returned out-of-range victim %d", victim)
				}
				if !active[victim] {
					continue
				}
				active[victim] = false
				aborted = append(aborted, victim)
				progressed = true
			}
		}

		if !progressed {
			return nil, nil, nil, fmt.Errorf("nezha cg cycle resolution made no progress with %d cyclic SCCs remaining", len(components))
		}
	}

	sort.Ints(aborted)
	if probe != nil {
		if err := probe.checkpoint("basic_topological_sort", 1, int64(len(aborted)), true); err != nil {
			return nil, nil, nil, err
		}
	}
	officialOrder, err := cgBasicTopologicalOrder(items, adjacency, active)
	if err != nil {
		return nil, nil, nil, err
	}
	if probe != nil {
		if err := probe.checkpoint("mbe_execution_frontiers", 1, int64(len(aborted)), true); err != nil {
			return nil, nil, nil, err
		}
	}
	frontiers, err := cgExecutionFrontiersForActive(items, adjacency, active)
	if err != nil {
		return nil, nil, nil, err
	}
	return frontiers, officialOrder, aborted, nil
}

// cgExecutionFrontiersForActive derives MBE executor-ready sets from the fixed
// residual graph. Parallel-arc multiplicity is counted exactly in indegrees.
// This function is outside the Nezha algorithm boundary: changing the requested
// execution worker count never changes these frontiers, the aborted set, or the
// BasicTopologicalSort serialization evidence.
func cgExecutionFrontiersForActive(items []literatureTxAccess, adjacency [][]int, active []bool) ([][]string, error) {
	if len(adjacency) != len(items) || len(active) != len(items) {
		return nil, fmt.Errorf("nezha cg execution frontier graph size mismatch")
	}
	indegree := make([]int, len(items))
	remaining := 0
	for from, row := range adjacency {
		if !active[from] {
			continue
		}
		remaining++
		for _, to := range row {
			if to < 0 || to >= len(items) {
				return nil, fmt.Errorf("nezha cg adjacency target out of range: %d", to)
			}
			if active[to] {
				indegree[to]++
			}
		}
	}
	done := make([]bool, len(items))
	frontiers := make([][]string, 0)
	for remaining > 0 {
		ready := make([]int, 0)
		for index := range items {
			if active[index] && !done[index] && indegree[index] == 0 {
				ready = append(ready, index)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("nezha cg residual graph is cyclic during executor frontier derivation")
		}
		sort.Ints(ready)
		frontier := make([]string, 0, len(ready))
		for _, index := range ready {
			done[index] = true
			remaining--
			frontier = append(frontier, items[index].TxID)
		}
		for _, index := range ready {
			for _, to := range adjacency[index] {
				if active[to] && !done[to] {
					indegree[to]--
				}
			}
		}
		frontiers = append(frontiers, frontier)
	}
	return frontiers, nil
}

// cgBasicTopologicalOrder mirrors Nezha AlGraph.RebuildGraph followed by
// BasicTopologicalSort for the non-aborted residual graph. Indegree and
// decrement operations count every parallel arc, and newly-ready vertices are
// appended to the FIFO queue in adjacency order.
func cgBasicTopologicalOrder(items []literatureTxAccess, adjacency [][]int, active []bool) ([]int, error) {
	if len(adjacency) != len(items) || len(active) != len(items) {
		return nil, fmt.Errorf("nezha cg residual graph size mismatch")
	}
	indegree := make([]int, len(items))
	remaining := 0
	for from, row := range adjacency {
		if !active[from] {
			continue
		}
		remaining++
		for _, to := range row {
			if to < 0 || to >= len(items) {
				return nil, fmt.Errorf("nezha cg adjacency target out of range: %d", to)
			}
			if active[to] {
				indegree[to]++
			}
		}
	}
	queue := make([]int, 0, remaining)
	for index := range items {
		if active[index] && indegree[index] == 0 {
			queue = append(queue, index)
		}
	}
	order := make([]int, 0, remaining)
	seen := make([]bool, len(items))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if seen[current] || !active[current] {
			continue
		}
		seen[current] = true
		order = append(order, current)
		for _, to := range adjacency[current] {
			if !active[to] || seen[to] {
				continue
			}
			indegree[to]--
			if indegree[to] == 0 {
				queue = append(queue, to)
			}
		}
	}
	if len(order) != remaining {
		return nil, fmt.Errorf("nezha cg cycle resolution left a cyclic residual graph")
	}
	return order, nil
}

// cgNezhaCycleSet is a multiplicity-compressed representation of the
// official JohnsonCE boolMap. Johnson traversal still emits every elementary
// cycle occurrence exactly as the reference does; only storage is compacted.
// Cycles with the same vertex-membership row share one record whose
// multiplicity counts how many reference occurrences produced that row.
// BreakCycles consumes that multiplicity exactly, so membership counts, victim
// decisions, aborted vertices, and the residual graph remain source-equivalent.
type cgNezhaCycleSet struct {
	offsets                 []int
	members                 []int
	multiplicity            []int64
	membershipIndex         map[string]int
	canonicalMembersScratch []int
	membershipKeyScratch    []byte
	membershipCount         []int64
	cyclesByVertex          [][]int
	activeCycle             []bool
	remainingMembership     int64
	cycleOccurrenceCountRaw int64
}

func cgNewNezhaCycleSet(vertexCount int) *cgNezhaCycleSet {
	return &cgNezhaCycleSet{
		offsets:         []int{0},
		membershipIndex: make(map[string]int),
		membershipCount: make([]int64, vertexCount),
	}
}

// addCycle is intentionally called once for every cycle occurrence found by
// the Johnson traversal. It does not skip or predict traversal work. The only
// optimization is that equal boolMap membership rows share storage.
func (set *cgNezhaCycleSet) addCycle(path []int) {
	if len(path) == 0 {
		return
	}
	set.cycleOccurrenceCountRaw++
	for _, vertex := range path {
		set.membershipCount[vertex]++
		set.remainingMembership++
	}

	// The reference boolMap discards path order and retains only membership.
	// Canonicalize that same membership row without allocating per occurrence:
	// reuse scratch buffers, sort the copied vertex IDs, then use a fixed-width
	// binary key for an exact (collision-free) map lookup. Persistent key/member
	// storage is allocated only for a newly observed membership row.
	if cap(set.canonicalMembersScratch) < len(path) {
		set.canonicalMembersScratch = make([]int, len(path))
	} else {
		set.canonicalMembersScratch = set.canonicalMembersScratch[:len(path)]
	}
	copy(set.canonicalMembersScratch, path)
	sort.Ints(set.canonicalMembersScratch)
	keyLen := len(path) * 8
	if cap(set.membershipKeyScratch) < keyLen {
		set.membershipKeyScratch = make([]byte, keyLen)
	} else {
		set.membershipKeyScratch = set.membershipKeyScratch[:keyLen]
	}
	for i, vertex := range set.canonicalMembersScratch {
		binary.LittleEndian.PutUint64(set.membershipKeyScratch[i*8:(i+1)*8], uint64(vertex))
	}

	// Go's direct []byte->string map lookup does not retain the scratch buffer.
	// A stable key copy is created only on first sight of a membership row.
	if cycleIndex, ok := set.membershipIndex[string(set.membershipKeyScratch)]; ok {
		set.multiplicity[cycleIndex]++
		return
	}

	cycleIndex := len(set.multiplicity)
	key := string(set.membershipKeyScratch)
	set.membershipIndex[key] = cycleIndex
	set.members = append(set.members, set.canonicalMembersScratch...)
	set.offsets = append(set.offsets, len(set.members))
	set.multiplicity = append(set.multiplicity, 1)
}

func (set *cgNezhaCycleSet) uniqueCycleCount() int {
	return len(set.multiplicity)
}

func (set *cgNezhaCycleSet) cycleOccurrenceCount() int64 {
	return set.cycleOccurrenceCountRaw
}

func (set *cgNezhaCycleSet) cycleMembers(cycleIndex int) []int {
	return set.members[set.offsets[cycleIndex]:set.offsets[cycleIndex+1]]
}

func (set *cgNezhaCycleSet) buildSparseIndex() {
	if err := set.buildSparseIndexContext(nil); err != nil {
		panic(err)
	}
}

func (set *cgNezhaCycleSet) buildSparseIndexContext(probe *cgPlanningProbe) error {
	set.cyclesByVertex = make([][]int, len(set.membershipCount))
	set.activeCycle = make([]bool, set.uniqueCycleCount())
	for cycleIndex := 0; cycleIndex < set.uniqueCycleCount(); cycleIndex++ {
		if probe != nil {
			if err := probe.checkpoint("johnson_sparse_index", 1, set.cycleOccurrenceCount(), false); err != nil {
				return err
			}
		}
		set.activeCycle[cycleIndex] = true
		for _, vertex := range set.cycleMembers(cycleIndex) {
			set.cyclesByVertex[vertex] = append(set.cyclesByVertex[vertex], cycleIndex)
		}
	}
	if probe != nil {
		if err := probe.checkpoint("johnson_sparse_index_complete", 1, set.cycleOccurrenceCount(), true); err != nil {
			return err
		}
	}
	return nil
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
	return cgNezhaFindCyclesContext(component, adjacency, nil)
}

func cgNezhaFindCyclesContext(component []int, adjacency [][]int, probe *cgPlanningProbe) (*cgNezhaCycleSet, error) {
	cycleSet, truncated, _, err := cgNezhaFindCyclesBoundedContext(component, adjacency, probe, 0, 0)
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, fmt.Errorf("unbounded Nezha Johnson traversal unexpectedly truncated")
	}
	return cycleSet, nil
}

func cgNezhaFindCyclesBoundedContext(component []int, adjacency [][]int, probe *cgPlanningProbe, maxCycleOccurrences, maxTraversalWork int64) (*cgNezhaCycleSet, bool, int64, error) {
	vertexCount := len(adjacency)
	cycleSet := cgNewNezhaCycleSet(vertexCount)
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	if len(vertices) < 2 {
		if err := cycleSet.buildSparseIndexContext(probe); err != nil {
			return nil, false, 0, err
		}
		return cycleSet, false, 0, nil
	}
	for _, vertex := range vertices {
		if vertex < 0 || vertex >= vertexCount {
			return nil, false, 0, fmt.Errorf("nezha cg SCC vertex out of range: %d", vertex)
		}
	}
	if len(vertices) == 2 {
		cycleSet.addCycle([]int{vertices[0], vertices[1]})
		if err := cycleSet.buildSparseIndexContext(probe); err != nil {
			return nil, false, 1, err
		}
		return cycleSet, false, 1, nil
	}
	explore := make([]bool, vertexCount)
	for _, v := range vertices {
		explore[v] = true
	}
	var work int64
	truncated := false
	consume := func() bool {
		work++
		if maxTraversalWork > 0 && work >= maxTraversalWork {
			truncated = true
			return false
		}
		if maxCycleOccurrences > 0 && cycleSet.cycleOccurrenceCount() >= maxCycleOccurrences {
			truncated = true
			return false
		}
		return true
	}
	for _, start := range vertices {
		if truncated {
			break
		}
		if probe != nil {
			if err := probe.checkpoint("johnson_enumeration", 1, cycleSet.cycleOccurrenceCount(), true); err != nil {
				return nil, false, work, err
			}
		}
		blocked := make([]bool, vertexCount)
		blockedMap := make([][]int, vertexCount)
		stack := make([]int, 0, len(vertices))
		var recurErr error
		var unblock func(int)
		unblock = func(v int) {
			if recurErr != nil || truncated || !consume() {
				return
			}
			if probe != nil {
				if err := probe.checkpoint("johnson_enumeration", 1, cycleSet.cycleOccurrenceCount(), false); err != nil {
					recurErr = err
					return
				}
			}
			blocked[v] = false
			for _, dep := range blockedMap[v] {
				if truncated {
					return
				}
				if blocked[dep] {
					unblock(dep)
				}
			}
			blockedMap[v] = nil
		}
		var visit func(int) bool
		visit = func(cur int) bool {
			if recurErr != nil || truncated || !consume() {
				return false
			}
			if probe != nil {
				if err := probe.checkpoint("johnson_enumeration", 1, cycleSet.cycleOccurrenceCount(), false); err != nil {
					recurErr = err
					return false
				}
			}
			found := false
			stack = append(stack, cur)
			blocked[cur] = true
			for _, next := range adjacency[cur] {
				if truncated || !consume() {
					break
				}
				if !explore[next] {
					continue
				}
				if next == start {
					found = true
					cycleSet.addCycle(stack)
					if maxCycleOccurrences > 0 && cycleSet.cycleOccurrenceCount() >= maxCycleOccurrences {
						truncated = true
						break
					}
				} else if !blocked[next] && visit(next) {
					found = true
				}
			}
			if !truncated && recurErr == nil {
				if found {
					unblock(cur)
				} else {
					for _, next := range adjacency[cur] {
						if explore[next] {
							blockedMap[next] = append(blockedMap[next], cur)
						}
					}
				}
			}
			stack = stack[:len(stack)-1]
			return found
		}
		visit(start)
		if recurErr != nil {
			return nil, false, work, recurErr
		}
		if !truncated {
			explore[start] = false
		}
	}
	if truncated && cycleSet.cycleOccurrenceCount() == 0 {
		witness := cgDeterministicCycleWitness(vertices, adjacency)
		if len(witness) == 0 {
			return nil, false, work, fmt.Errorf("bounded Johnson truncated without cycle witness")
		}
		cycleSet.addCycle(witness)
	}
	if err := cycleSet.buildSparseIndexContext(probe); err != nil {
		return nil, false, work, err
	}
	if probe != nil {
		phase := "johnson_complete"
		if truncated {
			phase = "johnson_prefix_complete"
		}
		if err := probe.checkpoint(phase, 1, cycleSet.cycleOccurrenceCount(), true); err != nil {
			return nil, false, work, err
		}
	}
	return cycleSet, truncated, work, nil
}

func cgDeterministicCycleWitness(component []int, adjacency [][]int) []int {
	allowed := make([]bool, len(adjacency))
	for _, v := range component {
		if v >= 0 && v < len(allowed) {
			allowed[v] = true
		}
	}
	color := make([]uint8, len(adjacency))
	stack := make([]int, 0, len(component))
	pos := map[int]int{}
	var witness []int
	var dfs func(int) bool
	dfs = func(v int) bool {
		color[v] = 1
		pos[v] = len(stack)
		stack = append(stack, v)
		set := map[int]bool{}
		for _, c := range adjacency[v] {
			if c >= 0 && c < len(allowed) && allowed[c] {
				set[c] = true
			}
		}
		children := make([]int, 0, len(set))
		for c := range set {
			children = append(children, c)
		}
		sort.Ints(children)
		for _, c := range children {
			if color[c] == 0 {
				if dfs(c) {
					return true
				}
			} else if color[c] == 1 {
				witness = append([]int(nil), stack[pos[c]:]...)
				return true
			}
		}
		delete(pos, v)
		stack = stack[:len(stack)-1]
		color[v] = 2
		return false
	}
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	for _, v := range vertices {
		if color[v] == 0 && dfs(v) {
			return witness
		}
	}
	return nil
}

// cgNezhaBreakCycles ports graph/johnsonce.go::BreakCycles. The reference scans
// vertex ordinals with strict '>' when choosing the maximum membership count,
// so the lowest original ordinal wins ties. Clearing a selected victim removes
// every still-active cycle that contains it and decrements all membership counts.
func cgNezhaBreakCycles(cycleSet *cgNezhaCycleSet) ([]int, error) {
	return cgNezhaBreakCyclesContext(cycleSet, nil)
}

func cgNezhaBreakCyclesContext(cycleSet *cgNezhaCycleSet, probe *cgPlanningProbe) ([]int, error) {
	if cycleSet == nil {
		return nil, fmt.Errorf("nezha cg cycle set is nil")
	}
	invalid := make([]bool, len(cycleSet.membershipCount))
	for cycleSet.remainingMembership != 0 {
		if probe != nil {
			if err := probe.checkpoint("break_cycles", 1, cycleSet.cycleOccurrenceCount(), false); err != nil {
				return nil, err
			}
		}
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
			if probe != nil {
				if err := probe.checkpoint("break_cycles", 1, cycleSet.cycleOccurrenceCount(), false); err != nil {
					return nil, err
				}
			}
			if !cycleSet.activeCycle[cycleIndex] {
				continue
			}
			cycleSet.activeCycle[cycleIndex] = false
			weight := cycleSet.multiplicity[cycleIndex]
			if weight <= 0 {
				return nil, fmt.Errorf("nezha cg cycle multiplicity is not positive for cycle %d", cycleIndex)
			}
			for _, member := range cycleSet.cycleMembers(cycleIndex) {
				cycleSet.remainingMembership -= weight
				cycleSet.membershipCount[member] -= weight
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
	if probe != nil {
		if err := probe.checkpoint("break_cycles_complete", 1, int64(len(victims)), true); err != nil {
			return nil, err
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

// verifyCGPlanSmart validates the exact Nezha conflict multigraph and the
// consensus-bound residual projection without re-running JohnsonCE. The primary
// remains solely responsible for the exact official Tarjan/JohnsonCE/BreakCycles
// victim decision. Backups independently verify candidate/access identity,
// official WW/RW adjacency multiplicity, abort-set shape, residual acyclicity,
// and BasicTopologicalSort commit order. This removes PBFT-wide duplicate cycle
// enumeration without changing the producer algorithm.
func verifyCGPlanSmart(block realblock.Block, plan literatureGraphPlan, _ int) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return fmt.Errorf("%s execution plan is missing", cgPlanAlgorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", cgPlanAlgorithmID)
	}
	if plan.ValidatorMode != cgSmartValidatorMode {
		return fmt.Errorf("cg projection-validator mode mismatch: %s", plan.ValidatorMode)
	}
	if plan.BlockHeight != block.Height {
		return fmt.Errorf("cg plan block height mismatch")
	}
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return err
	}
	candidateIDs := transactionIDs(block.TxList)
	if !sameStringList(plan.CandidateTransactionIDs, candidateIDs) {
		return fmt.Errorf("cg candidate transaction projection mismatch")
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	if plan.DeclaredAccessSetDigest != accessDigest || plan.DeclaredReadKeyCount != readKeyCount || plan.DeclaredWriteKeyCount != writeKeyCount {
		return fmt.Errorf("cg declared access summary mismatch")
	}

	expectedAdjacency, expectedEdges := cgBuildOfficialConflictMultigraph(items)
	if plan.Metrics.EdgeCount != len(expectedEdges) {
		return fmt.Errorf("cg official multigraph edge-count mismatch: expected=%d metrics=%d", len(expectedEdges), plan.Metrics.EdgeCount)
	}
	if plan.ConsensusWireVersion == "" {
		if len(plan.Edges) != len(expectedEdges) {
			return fmt.Errorf("cg legacy multigraph edge-count mismatch: plan=%d expected=%d", len(plan.Edges), len(expectedEdges))
		}
		for index := range expectedEdges {
			if plan.Edges[index] != expectedEdges[index] {
				return fmt.Errorf("cg official multigraph edge mismatch at %d: plan=%d->%d expected=%d->%d", index, plan.Edges[index].From, plan.Edges[index].To, expectedEdges[index].From, expectedEdges[index].To)
			}
		}
	} else if err := literatureVerifyCompactEdgeCommitment(plan, expectedEdges); err != nil {
		return err
	}
	if plan.Metrics.TransactionCount != len(items) || plan.Metrics.PairChecks != len(items)*(len(items)-1)/2 || plan.Metrics.PlanningWorkerCount != 1 {
		return fmt.Errorf("cg official construction metric mismatch")
	}

	active := make([]bool, len(items))
	for i := range active {
		active[i] = true
	}
	abortedSeen := make(map[string]bool, len(plan.AbortedTransactionIDs))
	candidateIndex := make(map[string]int, len(items))
	for index, item := range items {
		candidateIndex[item.TxID] = index
	}
	cyclicMembership := make([]bool, len(items))
	for _, component := range cgCyclicSCCs(len(items), cgSimpleEdgesFromAdjacency(expectedAdjacency), active) {
		for _, vertex := range component {
			cyclicMembership[vertex] = true
		}
	}
	for _, id := range plan.AbortedTransactionIDs {
		index, ok := candidateIndex[id]
		if !ok || id == "" || abortedSeen[id] {
			return fmt.Errorf("cg plan has invalid/duplicate aborted transaction %q", id)
		}
		if !cyclicMembership[index] {
			return fmt.Errorf("cg plan abort %s is outside an original cyclic SCC", id)
		}
		abortedSeen[id] = true
		active[index] = false
	}
	if plan.Metrics.AbortCount != len(plan.AbortedTransactionIDs) || plan.Metrics.CycleResolutionCount != len(plan.AbortedTransactionIDs) {
		return fmt.Errorf("cg cycle-abort metric mismatch")
	}

	order, err := cgBasicTopologicalOrder(items, expectedAdjacency, active)
	if err != nil {
		return err
	}
	expectedOrder := make([]string, 0, len(order))
	for _, index := range order {
		expectedOrder = append(expectedOrder, items[index].TxID)
	}
	if !sameStringList(plan.SerializationOrder, expectedOrder) {
		return fmt.Errorf("cg BasicTopologicalSort serialization mismatch")
	}
	expectedFrontiers, err := cgExecutionFrontiersForActive(items, expectedAdjacency, active)
	if err != nil {
		return err
	}
	if !cgSameWaves(plan.Waves, expectedFrontiers) {
		return fmt.Errorf("cg MBE execution-frontier projection mismatch")
	}
	expectedMaxWidth := 0
	for _, frontier := range expectedFrontiers {
		if len(frontier) > expectedMaxWidth {
			expectedMaxWidth = len(frontier)
		}
	}
	if plan.Metrics.WaveCount != len(expectedFrontiers) || plan.Metrics.MaximumWaveWidth != expectedMaxWidth {
		return fmt.Errorf("cg MBE execution-frontier metric mismatch")
	}
	return nil
}

// verifyCGPreverifiedProjection is used only after this exact immutable block
// and plan have already passed verifyCGPlanSmart in the consensus path. Unlike
// the generic literature helper it does not require flatten(Waves) to equal
// SerializationOrder, because CG intentionally stores two distinct truths:
// the authors' BasicTopologicalSort reference order and MBE execution frontiers.
func verifyCGPreverifiedProjection(block realblock.Block, plan literatureGraphPlan) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		return fmt.Errorf("%s execution plan is missing", cgPlanAlgorithmID)
	}
	if plan.AlgorithmID != cgPlanAlgorithmID || plan.PlanDigest == "" {
		return fmt.Errorf("%s parsed plan identity mismatch", cgPlanAlgorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", cgPlanAlgorithmID)
	}
	if plan.BlockHeight != block.Height {
		return fmt.Errorf("%s block height mismatch", cgPlanAlgorithmID)
	}
	candidateIDs := transactionIDs(block.TxList)
	if !sameStringList(plan.CandidateTransactionIDs, candidateIDs) || plan.Metrics.TransactionCount != len(candidateIDs) {
		return fmt.Errorf("%s candidate transaction projection mismatch", cgPlanAlgorithmID)
	}
	candidateSet := make(map[string]bool, len(candidateIDs))
	for _, id := range candidateIDs {
		candidateSet[id] = true
	}
	seenExecution := map[string]bool{}
	for _, frontier := range plan.Waves {
		for _, id := range frontier {
			if !candidateSet[id] || seenExecution[id] {
				return fmt.Errorf("%s invalid executor-frontier transaction %s", cgPlanAlgorithmID, id)
			}
			seenExecution[id] = true
		}
	}
	aborted := map[string]bool{}
	for _, id := range plan.AbortedTransactionIDs {
		if !candidateSet[id] || seenExecution[id] || aborted[id] {
			return fmt.Errorf("%s invalid aborted transaction %s", cgPlanAlgorithmID, id)
		}
		aborted[id] = true
	}
	if len(seenExecution)+len(aborted) != len(candidateIDs) {
		return fmt.Errorf("%s plan does not cover every candidate transaction", cgPlanAlgorithmID)
	}
	seenReference := map[string]bool{}
	for _, id := range plan.SerializationOrder {
		if !candidateSet[id] || aborted[id] || seenReference[id] {
			return fmt.Errorf("%s invalid BasicTopologicalSort reference transaction %s", cgPlanAlgorithmID, id)
		}
		seenReference[id] = true
	}
	if len(seenReference) != len(seenExecution) {
		return fmt.Errorf("%s reference/execution projection coverage mismatch", cgPlanAlgorithmID)
	}
	return nil
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
