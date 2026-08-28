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
	acgExecutionID     = "acg_execution"
	acgSchedulerID     = "acg_scheduler"
	acgBlockExecutorID = "acg_block_executor"
	// acgPlanAlgorithmID identifies the unmodified Nezha CreateGraph -> QueuesSort
	// -> DeSS graph/HS algorithm. acgConsensusPlanAlgorithmID is an MBE-only
	// lifecycle wrapper that consensus-binds the first-pass HS decision while
	// deferring HS-aborted transactions to the existing FIFO mempool retry path.
	acgPlanAlgorithmID          = "nezha_acg_hs_official_reference_v1"
	acgConsensusPlanAlgorithmID = "nezha_acg_hs_retry_consensus_v2"
	acgConsensusPlanVersion     = "mbe_nezha_acg_retry_plan_v2"
	nezhaInitialSequence        = 10
)

type acgExecution struct{ basicPlugin }
type acgScheduler struct{ basicPlugin }
type acgBlockExecutor struct{ basicPlugin }

func (p acgExecution) Classify(tx.SignedTransaction) ExecutionDecision {
	return ExecutionDecision{Track: "acg", Reason: "nezha_address_conflict_graph_hierarchical_sorting"}
}
func (p acgScheduler) Order(items []tx.SignedTransaction, _ ExecutionPlugin) []tx.SignedTransaction {
	return append([]tx.SignedTransaction(nil), items...)
}
func (p acgScheduler) Schedule(items []tx.SignedTransaction, _ ExecutionPlugin) ScheduleResult {
	return ScheduleResult{Ordered: append([]tx.SignedTransaction(nil), items...)}
}
func (p acgScheduler) PlanBlock(block realblock.Block) (ConsensusExecutionPlanningResult, error) {
	// Run the Nezha algorithm exactly once on the original candidate set. The
	// algorithmic HS abort decision is preserved in graphPlan; MBE changes only
	// the experiment lifecycle of those victims from terminal failure to FIFO
	// deferral/retry in a later block.
	graphPlan, err := buildACGPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	accepted, deferred, err := splitACGFirstPassCandidates(block.TxList, graphPlan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	if len(block.TxList) > 0 && len(accepted) == 0 {
		return ConsensusExecutionPlanningResult{}, fmt.Errorf("nezha acg planning deferred every candidate transaction")
	}

	block.TxList = accepted
	block.TxIDs = transactionIDs(accepted)
	consensusPlan := acgConsensusPlan{
		Version:              acgConsensusPlanVersion,
		GraphPlan:            graphPlan,
		DeferredTransactions: append([]tx.SignedTransaction(nil), deferred...),
	}
	raw, err := marshalACGConsensusPlan(consensusPlan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   acgConsensusPlanAlgorithmID,
		PayloadDigest: stableTextDigest(string(raw)),
		PlanDigest:    graphPlan.PlanDigest,
		Payload:       raw,
	}
	return ConsensusExecutionPlanningResult{
		Block:    block,
		Deferred: append([]tx.SignedTransaction(nil), deferred...),
		Events:   acgFirstPassScheduleEvents(graphPlan),
	}, nil
}
func (p acgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != acgConsensusPlanAlgorithmID {
		return fmt.Errorf("acg retry execution plan missing or has the wrong algorithm id")
	}
	plan, err := parseACGConsensusPlan(block.ExecutionPlan.Payload)
	if err != nil {
		return err
	}
	return verifyACGConsensusPlan(block, plan, true)
}
func (p acgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil || input.Block.ExecutionPlan.AlgorithmID != acgConsensusPlanAlgorithmID {
		return BlockExecutionResult{}, fmt.Errorf("acg retry execution plan missing before execution")
	}
	parseStarted := time.Now()
	consensusPlan, err := parseACGConsensusPlan(input.Block.ExecutionPlan.Payload)
	parseMS := time.Since(parseStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	verifyStarted := time.Now()
	verifyMode := "full_recompute"
	if input.ExecutionPlanVerified {
		verifyMode = "preverified_projection"
		err = verifyACGConsensusPlan(input.Block, consensusPlan, false)
	} else {
		err = verifyACGConsensusPlan(input.Block, consensusPlan, true)
	}
	verifyMS := time.Since(verifyStarted).Milliseconds()
	if err != nil {
		return BlockExecutionResult{}, err
	}
	result, err := executeACGPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, consensusPlan.GraphPlan, configuredWorkerCount(p.config, input.WorkerCount))
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
	result.ActualMetrics["nezha_hs_retry_consensus_plan_version"] = consensusPlan.Version
	return result, nil
}

// acgConsensusPlan is deliberately ACG-owned. It keeps the unmodified Nezha HS
// plan over the full first-pass candidate set and carries only the signed
// transactions that HS rejected. Accepted transactions remain in the block body.
// The wrapper is therefore sufficient for every validator to reconstruct and
// verify the exact same first-pass CreateGraph -> QueuesSort -> DeSS decision.
type acgConsensusPlan struct {
	Version              string                 `json:"version"`
	GraphPlan            literatureGraphPlan    `json:"graph_plan"`
	DeferredTransactions []tx.SignedTransaction `json:"deferred_transactions,omitempty"`
}

func marshalACGConsensusPlan(plan acgConsensusPlan) ([]byte, error) {
	return literatureJSONMarshal(plan)
}

func parseACGConsensusPlan(raw []byte) (acgConsensusPlan, error) {
	var plan acgConsensusPlan
	if err := literatureJSONUnmarshal(raw, &plan); err != nil {
		return plan, fmt.Errorf("decode acg retry consensus plan: %w", err)
	}
	if plan.Version != acgConsensusPlanVersion {
		return plan, fmt.Errorf("unsupported acg retry consensus plan version %q", plan.Version)
	}
	graphRaw, err := literatureMarshalPlan(plan.GraphPlan)
	if err != nil {
		return plan, err
	}
	parsed, err := literatureParsePlan(graphRaw, acgPlanAlgorithmID)
	if err != nil {
		return plan, err
	}
	plan.GraphPlan = parsed
	return plan, nil
}

func splitACGFirstPassCandidates(items []tx.SignedTransaction, plan literatureGraphPlan) ([]tx.SignedTransaction, []tx.SignedTransaction, error) {
	if !sameStringList(transactionIDs(items), plan.CandidateTransactionIDs) {
		return nil, nil, fmt.Errorf("nezha acg first-pass candidate identity mismatch")
	}
	aborted := make(map[string]bool, len(plan.AbortedTransactionIDs))
	for _, id := range plan.AbortedTransactionIDs {
		if id == "" || aborted[id] {
			return nil, nil, fmt.Errorf("nezha acg first-pass abort set contains duplicate/empty transaction id")
		}
		aborted[id] = true
	}
	accepted := make([]tx.SignedTransaction, 0, len(items)-len(aborted))
	deferred := make([]tx.SignedTransaction, 0, len(aborted))
	seenAborted := map[string]bool{}
	for _, item := range items {
		if aborted[item.TxID] {
			deferred = append(deferred, item)
			seenAborted[item.TxID] = true
		} else {
			accepted = append(accepted, item)
		}
	}
	if len(seenAborted) != len(aborted) {
		return nil, nil, fmt.Errorf("nezha acg first-pass abort set references a transaction outside the candidate block")
	}
	return accepted, deferred, nil
}

func acgFirstPassScheduleEvents(plan literatureGraphPlan) []ScheduleEvent {
	aborted := make(map[string]bool, len(plan.AbortedTransactionIDs))
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
			events = append(events, ScheduleEvent{TxID: id, Track: "acg", QueueName: "mempool_deferred", DecisionReason: "nezha_hs_abort_deferred_retry", Blocked: true})
			continue
		}
		events = append(events, ScheduleEvent{TxID: id, Track: "acg", QueueName: fmt.Sprintf("hs_wave_%d", waveByID[id]), DecisionReason: "nezha_hs_accepted", LocalExecution: true})
	}
	return events
}

func verifyACGConsensusPlan(block realblock.Block, plan acgConsensusPlan, fullRecompute bool) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != acgConsensusPlanAlgorithmID {
		return fmt.Errorf("acg retry execution plan is missing")
	}
	if block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) || block.ExecutionPlan.PlanDigest != plan.GraphPlan.PlanDigest {
		return fmt.Errorf("acg retry execution plan envelope mismatch")
	}
	if plan.GraphPlan.BlockHeight != block.Height {
		return fmt.Errorf("acg retry graph plan block height mismatch")
	}

	deferredIDs := transactionIDs(plan.DeferredTransactions)
	if !sameStringList(deferredIDs, plan.GraphPlan.AbortedTransactionIDs) {
		return fmt.Errorf("acg retry deferred identity does not match the Nezha HS abort set")
	}
	aborted := make(map[string]bool, len(deferredIDs))
	for _, id := range deferredIDs {
		if id == "" || aborted[id] {
			return fmt.Errorf("acg retry deferred set contains duplicate/empty transaction id")
		}
		aborted[id] = true
	}
	expectedAccepted := make([]string, 0, len(plan.GraphPlan.CandidateTransactionIDs)-len(aborted))
	for _, id := range plan.GraphPlan.CandidateTransactionIDs {
		if !aborted[id] {
			expectedAccepted = append(expectedAccepted, id)
		}
	}
	if !sameStringList(transactionIDs(block.TxList), expectedAccepted) {
		return fmt.Errorf("acg retry accepted block is not the exact first-pass HS projection")
	}

	acceptedByID := make(map[string]tx.SignedTransaction, len(block.TxList))
	for _, item := range block.TxList {
		if item.TxID == "" {
			return fmt.Errorf("acg retry accepted block contains empty transaction id")
		}
		if _, exists := acceptedByID[item.TxID]; exists {
			return fmt.Errorf("acg retry accepted block contains duplicate transaction %s", item.TxID)
		}
		acceptedByID[item.TxID] = item
	}
	deferredByID := make(map[string]tx.SignedTransaction, len(plan.DeferredTransactions))
	for _, item := range plan.DeferredTransactions {
		if item.TxID == "" {
			return fmt.Errorf("acg retry deferred plan contains empty transaction id")
		}
		if _, exists := deferredByID[item.TxID]; exists {
			return fmt.Errorf("acg retry deferred plan contains duplicate transaction %s", item.TxID)
		}
		deferredByID[item.TxID] = item
	}

	fullItems := make([]tx.SignedTransaction, 0, len(plan.GraphPlan.CandidateTransactionIDs))
	for _, id := range plan.GraphPlan.CandidateTransactionIDs {
		if item, ok := acceptedByID[id]; ok {
			if _, alsoDeferred := deferredByID[id]; alsoDeferred {
				return fmt.Errorf("acg retry transaction %s appears in both accepted and deferred projections", id)
			}
			fullItems = append(fullItems, item)
			continue
		}
		item, ok := deferredByID[id]
		if !ok {
			return fmt.Errorf("acg retry candidate %s cannot be reconstructed", id)
		}
		fullItems = append(fullItems, item)
	}
	if len(acceptedByID)+len(deferredByID) != len(fullItems) {
		return fmt.Errorf("acg retry plan contains transaction identities outside the original candidate set")
	}

	reconstructed := block
	reconstructed.TxList = fullItems
	reconstructed.TxIDs = transactionIDs(fullItems)
	graphRaw, err := literatureMarshalPlan(plan.GraphPlan)
	if err != nil {
		return err
	}
	reconstructed.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: acgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(graphRaw)), PlanDigest: plan.GraphPlan.PlanDigest, Payload: graphRaw}
	if err := verifyPreverifiedLiteratureGraphProjection(reconstructed, plan.GraphPlan, acgPlanAlgorithmID); err != nil {
		return err
	}
	if !fullRecompute {
		return nil
	}
	recomputed, err := buildACGPlan(reconstructed)
	if err != nil {
		return err
	}
	if recomputed.PlanDigest != plan.GraphPlan.PlanDigest {
		return fmt.Errorf("acg retry deterministic first-pass plan mismatch")
	}
	return nil
}

type nezhaRWNode struct {
	txIndex  int
	key      string
	write    bool
	sequence int32
	assigned bool
}
type nezhaEdge struct {
	nodes   []*nezhaRWNode
	aborted bool
}
type nezhaQueue struct {
	reads, writes     []*nezhaRWNode
	maxRead, maxWrite int32
}

// buildACGPlan is a direct semantic adaptation of CGCL-codes/Nezha's published
// core/conflict_queue.go. One queue is built per state address, queue dependencies
// are ranked first, then DeSS assigns transaction sequence numbers queue-by-queue.
// Transactions that the reference HS marks aborted remain explicit algorithmic
// evidence. The ACG-owned consensus wrapper defers those signed transactions to
// MBE's existing FIFO retry lifecycle without changing CreateGraph/QueuesSort/DeSS.
func buildACGPlan(block realblock.Block) (literatureGraphPlan, error) {
	constructionStarted := time.Now()
	items, err := literatureAccessDescriptors(block.TxList, block.ShardID)
	if err != nil {
		return literatureGraphPlan{}, err
	}
	accessDigest, readKeyCount, writeKeyCount := literatureDeclaredAccessSummary(items)
	edges := make([]*nezhaEdge, len(items))
	queues := map[string]*nezhaQueue{}
	for i, item := range items {
		edge := &nezhaEdge{}
		readSet := literatureStringSet(item.ReadKeys)
		writeSet := literatureStringSet(item.WriteKeys)
		keys := map[string]bool{}
		for k := range readSet {
			keys[k] = true
		}
		for k := range writeSet {
			keys[k] = true
		}
		ordered := literatureSortedBoolKeys(keys)
		for _, key := range ordered {
			q := queues[key]
			if q == nil {
				q = &nezhaQueue{}
				queues[key] = q
			}
			if readSet[key] {
				n := &nezhaRWNode{txIndex: i, key: key}
				edge.nodes = append(edge.nodes, n)
				q.reads = append(q.reads, n)
			}
			if writeSet[key] {
				n := &nezhaRWNode{txIndex: i, key: key, write: true}
				edge.nodes = append(edge.nodes, n)
				q.writes = append(q.writes, n)
			}
		}
		if len(edge.nodes) == 0 {
			n := &nezhaRWNode{txIndex: i, key: "", sequence: nezhaInitialSequence, assigned: true}
			edge.nodes = []*nezhaRWNode{n}
		}
		edges[i] = edge
	}
	constructionMS := time.Since(constructionStarted).Milliseconds()

	sortingStarted := time.Now()
	queueOrder, dependencyCount := nezhaQueueOrder(queues, edges)
	for _, key := range queueOrder {
		nezhaSortInQueue(queues[key], edges)
	}

	groups := map[int32][]int{}
	aborted := []string{}
	for i, edge := range edges {
		if edge.aborted {
			aborted = append(aborted, items[i].TxID)
			continue
		}
		seq := edge.nodes[0].sequence
		if seq == 0 {
			seq = nezhaInitialSequence
			nezhaAssignSequence(edge.nodes[0], edges)
		}
		groups[seq] = append(groups[seq], i)
	}
	seqs := make([]int, 0, len(groups))
	for seq := range groups {
		seqs = append(seqs, int(seq))
	}
	sort.Ints(seqs)
	waves := make([][]string, 0, len(seqs))
	for _, raw := range seqs {
		idxs := groups[int32(raw)]
		sort.Ints(idxs)
		wave := make([]string, 0, len(idxs))
		for _, i := range idxs {
			wave = append(wave, items[i].TxID)
		}
		waves = append(waves, wave)
	}
	sortingMS := time.Since(sortingStarted).Milliseconds()
	plan := literatureGraphPlan{AlgorithmID: acgPlanAlgorithmID, BlockHeight: block.Height, DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount, Waves: waves, AbortedTransactionIDs: aborted, Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: dependencyCount, AbortCount: len(aborted), GraphConstructionMS: constructionMS, SortingMS: sortingMS}}
	for _, item := range items {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	return literatureFinalizePlan(plan), nil
}

func nezhaQueueOrder(queues map[string]*nezhaQueue, edges []*nezhaEdge) ([]string, int) {
	keys := make([]string, 0, len(queues))
	for k := range queues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	index := map[string]int{}
	for i, k := range keys {
		index[k] = i
	}
	deps := make([]map[int]bool, len(keys))
	count := 0
	for _, key := range keys {
		qidx := index[key]
		deps[qidx] = map[int]bool{}
		for _, w := range queues[key].writes {
			for _, n := range edges[w.txIndex].nodes {
				if !n.write && n.key != "" && n.key != key {
					to := index[n.key]
					if !deps[qidx][to] {
						deps[qidx][to] = true
						count++
					}
				}
			}
		}
	}
	indeg := make([]int, len(keys))
	outdeg := make([]int, len(keys))
	adj := make([][]int, len(keys))
	for i, m := range deps {
		for j := range m {
			adj[i] = append(adj[i], j)
			indeg[j]++
		}
		sort.Ints(adj[i])
		outdeg[i] = len(adj[i])
	}
	sorted := make([]bool, len(keys))
	order := make([]int, 0, len(keys))
	for len(order) < len(keys) {
		ready := []int{}
		for i := range keys {
			if !sorted[i] && indeg[i] == 0 {
				ready = append(ready, i)
			}
		}
		if len(ready) == 0 {
			min := int(^uint(0) >> 1)
			for i := range keys {
				if !sorted[i] && indeg[i] < min {
					min = indeg[i]
				}
			}
			best := -1
			bestOut := -1
			for i := range keys {
				if !sorted[i] && indeg[i] == min && outdeg[i] > bestOut {
					best = i
					bestOut = outdeg[i]
				}
			}
			ready = []int{best}
		}
		sort.Ints(ready)
		for _, v := range ready {
			if sorted[v] {
				continue
			}
			sorted[v] = true
			order = append(order, v)
			for _, to := range adj[v] {
				if !sorted[to] {
					indeg[to]--
				}
			}
		}
	}
	out := make([]string, 0, len(order))
	for _, i := range order {
		out = append(out, keys[i])
	}
	return out, count
}

func nezhaAssignSequence(node *nezhaRWNode, edges []*nezhaEdge) {
	for _, n := range edges[node.txIndex].nodes {
		n.sequence = node.sequence
	}
}

func nezhaSortInQueue(q *nezhaQueue, edges []*nezhaEdge) {
	tmpReads := []*nezhaRWNode{}
	for _, r := range q.reads {
		if r.sequence != 0 && !edges[r.txIndex].aborted {
			tmpReads = append(tmpReads, r)
		}
	}
	if len(tmpReads) == 0 {
		for _, r := range q.reads {
			if edges[r.txIndex].aborted {
				continue
			}
			r.sequence = nezhaInitialSequence
			r.assigned = true
			nezhaAssignSequence(r, edges)
			q.maxRead = r.sequence
		}
	} else {
		min := int32(1 << 30)
		for _, r := range tmpReads {
			r.assigned = true
			if r.sequence < min {
				min = r.sequence
			}
			if r.sequence > q.maxRead {
				q.maxRead = r.sequence
			}
		}
		for _, r := range q.reads {
			if r.sequence != 0 || edges[r.txIndex].aborted {
				continue
			}
			r.sequence = min
			r.assigned = true
			nezhaAssignSequence(r, edges)
		}
	}
	tmpSame := []*nezhaRWNode{}
	tmpBySeq := map[int32][]*nezhaRWNode{}
	for _, w := range q.writes {
		if w.sequence == 0 || edges[w.txIndex].aborted {
			continue
		}
		if w.sequence <= q.maxRead {
			same, before := false, false
			for _, n := range edges[w.txIndex].nodes {
				if !n.write && n.assigned {
					if n.key == w.key {
						same = true
						break
					}
					before = true
				}
			}
			if same {
				tmpSame = append(tmpSame, w)
			} else if before {
				edges[w.txIndex].aborted = true
			} else {
				tmpBySeq[w.sequence] = append(tmpBySeq[w.sequence], w)
			}
		} else {
			tmpBySeq[w.sequence] = append(tmpBySeq[w.sequence], w)
		}
	}
	keys := make([]int, 0, len(tmpBySeq))
	for s := range tmpBySeq {
		keys = append(keys, int(s))
	}
	sort.Ints(keys)
	if q.maxRead == 0 {
		q.maxWrite = nezhaInitialSequence - 1
	} else {
		q.maxWrite = q.maxRead
	}
	for i, w := range tmpSame {
		if edges[w.txIndex].aborted {
			continue
		}
		if i == 0 {
			q.maxWrite++
			w.sequence = q.maxWrite
			q.maxRead = q.maxWrite
			w.assigned = true
			nezhaAssignSequence(w, edges)
		} else {
			edges[w.txIndex].aborted = true
		}
	}
	for _, w := range q.writes {
		if w.sequence != 0 || edges[w.txIndex].aborted {
			continue
		}
		for candidate := q.maxWrite + 1; ; candidate++ {
			if !nezhaSeqExists(keys, int(candidate)) {
				w.sequence = candidate
				q.maxWrite = candidate
				w.assigned = true
				nezhaAssignSequence(w, edges)
				break
			}
		}
	}
	if len(keys) > 0 && q.maxWrite < int32(keys[len(keys)-1]) {
		q.maxWrite = int32(keys[len(keys)-1])
	}
	for _, raw := range keys {
		for i, w := range tmpBySeq[int32(raw)] {
			if edges[w.txIndex].aborted {
				continue
			}
			if int32(raw) > q.maxRead && i == 0 {
				continue
			}
			q.maxWrite++
			w.sequence = q.maxWrite
			w.assigned = true
			nezhaAssignSequence(w, edges)
		}
	}
}
func nezhaSeqExists(values []int, value int) bool {
	i := sort.SearchInts(values, value)
	return i < len(values) && values[i] == value
}
