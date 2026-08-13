package v5

import (
	"context"
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	acgExecutionID     = "acg_execution"
	acgSchedulerID     = "acg_scheduler"
	acgBlockExecutorID = "acg_block_executor"
	// This profile follows the authors' published Nezha reference implementation:
	// CreateGraph -> QueuesSort (address-rank division) -> DeSS hierarchical sorting.
	acgPlanAlgorithmID   = "nezha_acg_hs_official_reference_v1"
	nezhaInitialSequence = 10
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
	plan, err := buildACGPlan(block)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	raw, err := literatureMarshalPlan(plan)
	if err != nil {
		return ConsensusExecutionPlanningResult{}, err
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: acgPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	return ConsensusExecutionPlanningResult{Block: block}, nil
}
func (p acgScheduler) VerifyBlockPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil {
		return fmt.Errorf("acg execution plan missing")
	}
	plan, err := literatureParsePlan(block.ExecutionPlan.Payload, acgPlanAlgorithmID)
	if err != nil {
		return err
	}
	return literatureVerifyPlan(block, plan, acgPlanAlgorithmID, buildACGPlan)
}
func (p acgBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.Block.ExecutionPlan == nil {
		return BlockExecutionResult{}, fmt.Errorf("acg execution plan missing")
	}
	plan, err := literatureParsePlan(input.Block.ExecutionPlan.Payload, acgPlanAlgorithmID)
	if err != nil {
		return BlockExecutionResult{}, err
	}
	if err := literatureVerifyPlan(input.Block, plan, acgPlanAlgorithmID, buildACGPlan); err != nil {
		return BlockExecutionResult{}, err
	}
	return executeACGPlanWithCommitment(ctx, input.Block, input.BaseStateSnapshot, input.BaseStateCommitment, plan, configuredWorkerCount(p.config, input.WorkerCount))
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
// Transactions that the reference HS marks aborted are carried explicitly in the
// consensus-bound plan and terminalized as failed no-op outcomes by the executor.
func buildACGPlan(block realblock.Block) (literatureGraphPlan, error) {
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
			// The official RW capture can contain both an r-node and w-node for one key.
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
			// Keep an access-free transaction representable and deterministic.
			n := &nezhaRWNode{txIndex: i, key: "", sequence: nezhaInitialSequence, assigned: true}
			edge.nodes = []*nezhaRWNode{n}
		}
		edges[i] = edge
	}
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
	plan := literatureGraphPlan{AlgorithmID: acgPlanAlgorithmID, BlockHeight: block.Height, DeclaredAccessSetDigest: accessDigest, DeclaredReadKeyCount: readKeyCount, DeclaredWriteKeyCount: writeKeyCount, Waves: waves, AbortedTransactionIDs: aborted, Metrics: literatureGraphMetrics{TransactionCount: len(items), EdgeCount: dependencyCount, AbortCount: len(aborted)}}
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
