package execution

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func batchSIReferenceAWRT(items []batchSITxDescriptor) (map[string][]string, int) {
	awrt := map[string][]string{}
	references := 0
	for _, item := range items {
		for _, key := range item.WriteKeys {
			awrt[key] = append(awrt[key], item.TxID)
			references++
		}
	}
	for key := range awrt {
		sort.Strings(awrt[key])
	}
	return awrt, references
}

func batchSIReferenceWRBP(items []batchSITxDescriptor) ([]batchSIPartition, int) {
	ordered := append([]batchSITxDescriptor(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].IDRank != ordered[j].IDRank {
			return ordered[i].IDRank < ordered[j].IDRank
		}
		return ordered[i].TxID < ordered[j].TxID
	})
	currentBatch := map[string]int{}
	opportunities := map[string]map[int]bool{}
	batches := map[int][]batchSITxDescriptor{}
	reuseCount := 0
	for _, item := range ordered {
		if len(item.WriteKeys) == 0 {
			batches[1] = append(batches[1], item)
			continue
		}
		maxBatch := 1
		for _, key := range item.WriteKeys {
			value := currentBatch[key]
			if value < 1 {
				value = 1
			}
			if value > maxBatch {
				maxBatch = value
			}
		}
		available := []map[int]bool{}
		for _, key := range item.WriteKeys {
			value := currentBatch[key]
			if value < 1 {
				value = 1
			}
			set := batchSICloneIntSet(opportunities[key])
			if value == maxBatch {
				set[maxBatch] = true
			} else {
				for candidate := value; candidate < maxBatch; candidate++ {
					set[candidate] = true
				}
			}
			available = append(available, set)
		}
		assigned := batchSIMinIntersection(available)
		if assigned < 1 {
			assigned = maxBatch
		}
		for _, key := range item.WriteKeys {
			value := currentBatch[key]
			if value < 1 {
				value = 1
			}
			if assigned < value {
				delete(opportunities[key], assigned)
				reuseCount++
				continue
			}
			if opportunities[key] == nil {
				opportunities[key] = map[int]bool{}
			}
			if value < assigned {
				for candidate := value; candidate < assigned; candidate++ {
					opportunities[key][candidate] = true
				}
			}
			currentBatch[key] = assigned + 1
		}
		batches[assigned] = append(batches[assigned], item)
	}
	return batchSICompactPartitions(batches), reuseCount
}

func batchSIReferenceSequential(items []batchSITxDescriptor) []batchSIPartition {
	ordered := append([]batchSITxDescriptor(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].IDRank != ordered[j].IDRank {
			return ordered[i].IDRank < ordered[j].IDRank
		}
		return ordered[i].TxID < ordered[j].TxID
	})
	nextBatch := map[string]int{}
	batches := map[int][]batchSITxDescriptor{}
	for _, item := range ordered {
		if len(item.WriteKeys) == 0 {
			batches[1] = append(batches[1], item)
			continue
		}
		assigned := 1
		for _, key := range item.WriteKeys {
			value := nextBatch[key]
			if value < 1 {
				value = 1
			}
			if value > assigned {
				assigned = value
			}
		}
		for _, key := range item.WriteKeys {
			nextBatch[key] = assigned + 1
		}
		batches[assigned] = append(batches[assigned], item)
	}
	return batchSICompactPartitions(batches)
}

func batchSIReferenceOFAS(items []batchSITxDescriptor, priorityMode string) ([]batchSITxDescriptor, []batchSITxDescriptor, int) {
	byID := make(map[string]batchSITxDescriptor, len(items))
	writerByKey := map[string]string{}
	transactionReadCount := map[string]int{}
	writerReadCount := map[string]int{}
	maximumReaderOrder := map[string]int{}
	for _, item := range items {
		byID[item.TxID] = item
		for _, key := range item.WriteKeys {
			writerByKey[key] = item.TxID
		}
	}
	dependencyEdges := map[string]map[string]bool{}
	for _, reader := range items {
		ownWrites := batchSIStringSet(reader.WriteKeys)
		for _, key := range reader.ReadKeys {
			if ownWrites[key] {
				continue
			}
			transactionReadCount[reader.TxID]++
			writerID := writerByKey[key]
			if writerID == "" || writerID == reader.TxID {
				continue
			}
			writerReadCount[writerID]++
			if reader.IDRank > maximumReaderOrder[writerID] {
				maximumReaderOrder[writerID] = reader.IDRank
			}
			if dependencyEdges[reader.TxID] == nil {
				dependencyEdges[reader.TxID] = map[string]bool{}
			}
			dependencyEdges[reader.TxID][writerID] = true
		}
	}
	edgeCount := 0
	for _, children := range dependencyEdges {
		edgeCount += len(children)
	}
	priority := append([]batchSITxDescriptor(nil), items...)
	higherPriority := func(left, right batchSITxDescriptor) bool {
		if priorityMode == BatchSIPriorityTxID {
			return left.TxID < right.TxID
		}
		if writerReadCount[left.TxID] != writerReadCount[right.TxID] {
			return writerReadCount[left.TxID] < writerReadCount[right.TxID]
		}
		if transactionReadCount[left.TxID] != transactionReadCount[right.TxID] {
			return transactionReadCount[left.TxID] > transactionReadCount[right.TxID]
		}
		if left.IDRank != right.IDRank {
			return left.IDRank < right.IDRank
		}
		return left.TxID < right.TxID
	}
	sort.SliceStable(priority, func(i, j int) bool { return higherPriority(priority[i], priority[j]) })
	priorityIndex := map[string]int{}
	for index, item := range priority {
		priorityIndex[item.TxID] = index
	}
	sortOrder := map[string]int{}
	isSorted := map[string]bool{}
	isAborted := map[string]bool{}
	for _, item := range items {
		sortOrder[item.TxID] = item.IDRank
	}
	aborted := make([]batchSITxDescriptor, 0)
	for _, item := range priority {
		if maximumReaderOrder[item.TxID] > sortOrder[item.TxID] {
			sortOrder[item.TxID] = maximumReaderOrder[item.TxID]
		}
		abortCurrent := false
		ownWrites := batchSIStringSet(item.WriteKeys)
		for _, key := range item.ReadKeys {
			if ownWrites[key] {
				continue
			}
			writerID := writerByKey[key]
			if writerID == "" || writerID == item.TxID || isAborted[writerID] || !isSorted[writerID] {
				continue
			}
			if sortOrder[item.TxID] > maximumReaderOrder[writerID] {
				isAborted[item.TxID] = true
				abortCurrent = true
				aborted = append(aborted, item)
				break
			}
		}
		if abortCurrent {
			continue
		}
		isSorted[item.TxID] = true
		for _, key := range item.ReadKeys {
			if ownWrites[key] {
				continue
			}
			writerID := writerByKey[key]
			if writerID != "" && writerID != item.TxID && sortOrder[item.TxID] > maximumReaderOrder[writerID] {
				maximumReaderOrder[writerID] = sortOrder[item.TxID]
			}
		}
	}
	active := map[string]bool{}
	for _, item := range items {
		if !isAborted[item.TxID] {
			active[item.TxID] = true
		}
	}
	var ordered []batchSITxDescriptor
	for {
		indegree := map[string]int{}
		for id := range active {
			indegree[id] = 0
		}
		for parent, children := range dependencyEdges {
			if !active[parent] {
				continue
			}
			for child := range children {
				if active[child] {
					indegree[child]++
				}
			}
		}
		ready := make([]string, 0)
		for id, degree := range indegree {
			if degree == 0 {
				ready = append(ready, id)
			}
		}
		orderedIDs := make([]string, 0, len(active))
		for len(ready) > 0 {
			sort.SliceStable(ready, func(i, j int) bool {
				left, right := ready[i], ready[j]
				if sortOrder[left] != sortOrder[right] {
					return sortOrder[left] < sortOrder[right]
				}
				if priorityIndex[left] != priorityIndex[right] {
					return priorityIndex[left] < priorityIndex[right]
				}
				return left < right
			})
			id := ready[0]
			ready = ready[1:]
			orderedIDs = append(orderedIDs, id)
			children := make([]string, 0, len(dependencyEdges[id]))
			for child := range dependencyEdges[id] {
				if active[child] {
					children = append(children, child)
				}
			}
			sort.Strings(children)
			for _, child := range children {
				indegree[child]--
				if indegree[child] == 0 {
					ready = append(ready, child)
				}
			}
		}
		if len(orderedIDs) == len(active) {
			ordered = make([]batchSITxDescriptor, 0, len(orderedIDs))
			for _, id := range orderedIDs {
				ordered = append(ordered, byID[id])
			}
			break
		}
		cycleIDs := make([]string, 0)
		seen := map[string]bool{}
		for _, id := range orderedIDs {
			seen[id] = true
		}
		for id := range active {
			if !seen[id] {
				cycleIDs = append(cycleIDs, id)
			}
		}
		sort.SliceStable(cycleIDs, func(i, j int) bool {
			left, right := byID[cycleIDs[i]], byID[cycleIDs[j]]
			return higherPriority(right, left)
		})
		victimID := cycleIDs[0]
		isAborted[victimID] = true
		aborted = append(aborted, byID[victimID])
		delete(active, victimID)
	}
	sort.SliceStable(aborted, func(i, j int) bool {
		if aborted[i].IDRank != aborted[j].IDRank {
			return aborted[i].IDRank < aborted[j].IDRank
		}
		return aborted[i].TxID < aborted[j].TxID
	})
	return ordered, aborted, edgeCount
}

func batchSIIDs(items []batchSITxDescriptor) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = items[i].TxID
	}
	return out
}

func batchSISignedIDs(items []tx.SignedTransaction) []string {
	out := make([]string, len(items))
	for i := range items {
		out[i] = items[i].TxID
	}
	return out
}

func batchSIPartitionIDs(items []batchSIPartition) [][]string {
	out := make([][]string, len(items))
	for i := range items {
		out[i] = batchSIIDs(items[i].Txs)
	}
	return out
}

func batchSIRandomTransactions(r *rand.Rand, n, keyCount int) []tx.SignedTransaction {
	items := make([]tx.SignedTransaction, 0, n)
	for i := 0; i < n; i++ {
		reads := map[string]bool{}
		writes := map[string]bool{}
		readN := r.Intn(5)
		writeN := r.Intn(4)
		for j := 0; j < readN; j++ {
			reads[fmt.Sprintf("k%d", r.Intn(keyCount))] = true
		}
		for j := 0; j < writeN; j++ {
			writes[fmt.Sprintf("k%d", r.Intn(keyCount))] = true
		}
		rkeys := make([]string, 0, len(reads))
		wkeys := make([]string, 0, len(writes))
		for key := range reads {
			rkeys = append(rkeys, key)
		}
		for key := range writes {
			wkeys = append(wkeys, key)
		}
		sort.Strings(rkeys)
		sort.Strings(wkeys)
		items = append(items, batchSITestTx(fmt.Sprintf("T%04d", i+1), rkeys, wkeys))
	}
	return items
}

func batchSIReferencePlan(b block.Block, config BatchSIConfig) (BatchSIPlanningResult, error) {
	ordinals := make(map[string]int, len(b.TxList))
	for index, item := range b.TxList {
		ordinals[item.TxID] = index + 1
	}
	descriptors, normalizedOrdinals, err := batchSIDescriptors(b.TxList, ordinals, b.ShardID)
	if err != nil {
		return BatchSIPlanningResult{}, err
	}
	awrt, awrtRefs := batchSIReferenceAWRT(descriptors)
	var partitions []batchSIPartition
	var reuse int
	if config.PartitionMode == BatchSIPartitionSequential {
		partitions = batchSIReferenceSequential(descriptors)
	} else {
		partitions, reuse = batchSIReferenceWRBP(descriptors)
	}
	deferredByID := map[string]batchSITxDescriptor{}
	edges := 0
	for i := range partitions {
		var ordered, rejected []batchSITxDescriptor
		var edgeCount int
		if config.OrderingMode == BatchSIOrderingDependencyGraph {
			ordered, rejected, edgeCount = batchSIDependencyGraphOrder(partitions[i].Txs)
		} else {
			ordered, rejected, edgeCount = batchSIReferenceOFAS(partitions[i].Txs, config.PriorityMode)
		}
		partitions[i].Txs = ordered
		edges += edgeCount
		for _, item := range rejected {
			deferredByID[item.TxID] = item
		}
	}
	finalPartitions := make([]batchSIPartition, 0, len(partitions))
	maxWidth := 0
	for _, p := range partitions {
		if len(p.Txs) == 0 {
			continue
		}
		if len(p.Txs) > maxWidth {
			maxWidth = len(p.Txs)
		}
		finalPartitions = append(finalPartitions, p)
	}
	readOnly := 0
	for _, item := range descriptors {
		if len(item.WriteKeys) == 0 {
			readOnly++
		}
	}
	plan := BatchSIPlan{
		AlgorithmID:             BatchSIPlanAlgorithmID,
		Version:                 BatchSIPlanVersion,
		BlockHeight:             b.Height,
		PartitionMode:           config.PartitionMode,
		OrderingMode:            config.OrderingMode,
		PriorityMode:            config.PriorityMode,
		CandidateTransactionIDs: make([]string, 0, len(b.TxList)),
		TransactionOrdinals:     normalizedOrdinals,
		Metrics: BatchSIPlanMetrics{
			TransactionCount:                     len(descriptors),
			CandidateTransactionCount:            len(descriptors),
			AcceptedTransactionCount:             len(descriptors) - len(deferredByID),
			FirstPassOFASAbortedTransactionCount: len(deferredByID),
			ReadOnlyTransactionCount:             readOnly,
			AWRTAddressCount:                     len(awrt),
			AWRTWriteReferenceCount:              awrtRefs,
			BatchCount:                           len(finalPartitions),
			MaximumBatchWidth:                    maxWidth,
			WriteOpportunityReuseCount:           reuse,
			DependencyEdgeCount:                  edges,
			OFASAbortedTransactionCount:          len(deferredByID),
			PlanningIterationCount:               1,
		},
	}
	for _, item := range b.TxList {
		plan.CandidateTransactionIDs = append(plan.CandidateTransactionIDs, item.TxID)
	}
	byID := make(map[string]tx.SignedTransaction, len(descriptors))
	for _, item := range descriptors {
		byID[item.TxID] = item.Item
	}
	for _, p := range finalPartitions {
		batch := BatchSIBatch{BatchNumber: p.Number}
		for _, item := range p.Txs {
			batch.OrderedTransactionIDs = append(batch.OrderedTransactionIDs, item.TxID)
		}
		plan.Batches = append(plan.Batches, batch)
	}
	plan.OrderEvidence = batchSIOrderEvidence(plan.TransactionOrdinals, plan.Batches)
	ordered := make([]tx.SignedTransaction, 0, plan.Metrics.AcceptedTransactionCount)
	for _, batch := range plan.Batches {
		for _, id := range batch.OrderedTransactionIDs {
			ordered = append(ordered, byID[id])
		}
	}
	deferred := make([]tx.SignedTransaction, 0, len(deferredByID))
	for _, descriptor := range deferredByID {
		deferred = append(deferred, descriptor.Item)
	}
	sort.Slice(deferred, func(i, j int) bool {
		left, right := normalizedOrdinals[deferred[i].TxID], normalizedOrdinals[deferred[j].TxID]
		if left != right {
			return left < right
		}
		return deferred[i].TxID < deferred[j].TxID
	})
	plan.DeferredTransactions = append([]tx.SignedTransaction(nil), deferred...)
	plan.PlanDigest = batchSIPlanDigest(plan)
	return BatchSIPlanningResult{Plan: plan, Ordered: ordered, Deferred: deferred}, nil
}

func TestBatchSIEquivalentEngineeringOptimizationDifferential(t *testing.T) {
	for seed := int64(1); seed <= 120; seed++ {
		r := rand.New(rand.NewSource(seed))
		items := batchSIRandomTransactions(r, 12+r.Intn(52), 4+r.Intn(18))
		b := batchSITestBlock(items...)
		configs := []BatchSIConfig{
			DefaultBatchSIConfig(),
			{WorkerCount: 4, PartitionMode: BatchSIPartitionSequential, OrderingMode: BatchSIOrderingOFAS, PriorityMode: BatchSIPriorityPaper, ExecutionMode: BatchSIExecutionSnapshotParallel},
			{WorkerCount: 4, PartitionMode: BatchSIPartitionWRBP, OrderingMode: BatchSIOrderingOFAS, PriorityMode: BatchSIPriorityTxID, ExecutionMode: BatchSIExecutionSnapshotParallel},
		}
		for _, config := range configs {
			reference, err := batchSIReferencePlan(b, config)
			if err != nil {
				t.Fatalf("seed=%d reference config=%+v: %v", seed, config, err)
			}
			optimized, err := BuildBatchSIPlan(b, config)
			if err != nil {
				t.Fatalf("seed=%d optimized config=%+v: %v", seed, config, err)
			}
			if reference.Plan.PlanDigest != optimized.Plan.PlanDigest {
				t.Fatalf("seed=%d config=%+v plan digest drift: ref=%s opt=%s\nref batches=%v\nopt batches=%v\nref deferred=%v\nopt deferred=%v", seed, config, reference.Plan.PlanDigest, optimized.Plan.PlanDigest, reference.Plan.Batches, optimized.Plan.Batches, batchSISignedIDs(reference.Deferred), batchSISignedIDs(optimized.Deferred))
			}
			if !reflect.DeepEqual(batchSISignedIDs(reference.Ordered), batchSISignedIDs(optimized.Ordered)) || !reflect.DeepEqual(batchSISignedIDs(reference.Deferred), batchSISignedIDs(optimized.Deferred)) {
				t.Fatalf("seed=%d config=%+v accepted/deferred order drift", seed, config)
			}
		}

		descriptors, _, err := batchSIDescriptors(items, nil, "shard-0")
		if err != nil {
			t.Fatal(err)
		}
		shuffled := append([]batchSITxDescriptor(nil), descriptors...)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		refParts, refReuse := batchSIReferenceWRBP(shuffled)
		optParts, optReuse := batchSIWRBPPartition(shuffled)
		if refReuse != optReuse || !reflect.DeepEqual(batchSIPartitionIDs(refParts), batchSIPartitionIDs(optParts)) {
			t.Fatalf("seed=%d direct unsorted WRBP drift", seed)
		}
		refSeq := batchSIReferenceSequential(shuffled)
		optSeq := batchSISequentialPartition(shuffled)
		if !reflect.DeepEqual(batchSIPartitionIDs(refSeq), batchSIPartitionIDs(optSeq)) {
			t.Fatalf("seed=%d direct unsorted sequential partition drift", seed)
		}
		for _, p := range refParts {
			for _, priority := range []string{BatchSIPriorityPaper, BatchSIPriorityTxID} {
				ro, ra, re := batchSIReferenceOFAS(p.Txs, priority)
				oo, oa, oe := batchSIOFASOrder(p.Txs, priority)
				if re != oe || !reflect.DeepEqual(batchSIIDs(ro), batchSIIDs(oo)) || !reflect.DeepEqual(batchSIIDs(ra), batchSIIDs(oa)) {
					t.Fatalf("seed=%d batch=%d priority=%s OFAS drift", seed, p.Number, priority)
				}
			}
		}
	}
}

func TestBatchSIAWRTOrdinalRepresentationIsReferenceEquivalent(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("hash-z", nil, []string{"a", "b"}),
		batchSITestTx("hash-a", nil, []string{"a"}),
		batchSITestTx("hash-m", nil, []string{"b"}),
	}
	descriptors, _, err := batchSIDescriptors(items, nil, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	ref, refCount := batchSIReferenceAWRT(descriptors)
	opt, optCount := batchSIBuildAWRT(descriptors)
	if refCount != optCount || len(ref) != len(opt) {
		t.Fatalf("AWRT count drift: ref=%d/%d opt=%d/%d", len(ref), refCount, len(opt), optCount)
	}
	byOrdinal := map[int]string{}
	for _, item := range descriptors {
		byOrdinal[item.IDRank] = item.TxID
	}
	for key, ordinals := range opt {
		ids := make([]string, len(ordinals))
		for i, ordinal := range ordinals {
			ids[i] = byOrdinal[ordinal]
		}
		sort.Strings(ids)
		if !reflect.DeepEqual(ids, ref[key]) {
			t.Fatalf("AWRT semantic drift for %s: ref=%v opt=%v", key, ref[key], ids)
		}
	}
}
