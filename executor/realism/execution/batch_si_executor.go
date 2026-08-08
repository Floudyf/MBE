package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	BatchSIBlockExecutorID      = "batch_si_block_executor"
	BatchSIBlockExecutorVersion = "1.0.0"
	BatchSIPlanAlgorithmID      = "batch_si_execution_plan_v1"
	BatchSIPlanVersion          = "1.1.0"

	BatchSIPartitionWRBP       = "wrbp"
	BatchSIPartitionSequential = "sequential"

	BatchSIOrderingOFAS            = "ofas"
	BatchSIOrderingDependencyGraph = "dependency_graph"

	BatchSIPriorityPaper = "paper"
	BatchSIPriorityTxID  = "txid"

	BatchSIExecutionSnapshotParallel = "snapshot_parallel"
	BatchSIExecutionSnapshotSerial   = "snapshot_serial"
)

// BatchSIConfig controls only Batch-SI and its own ablations. No other executor
// is called by this implementation.
type BatchSIConfig struct {
	WorkerCount   int    `json:"worker_count"`
	PartitionMode string `json:"partition_mode"`
	OrderingMode  string `json:"ordering_mode"`
	PriorityMode  string `json:"priority_mode"`
	ExecutionMode string `json:"execution_mode"`
}

func DefaultBatchSIConfig() BatchSIConfig {
	return BatchSIConfig{
		WorkerCount:   4,
		PartitionMode: BatchSIPartitionWRBP,
		OrderingMode:  BatchSIOrderingOFAS,
		PriorityMode:  BatchSIPriorityPaper,
		ExecutionMode: BatchSIExecutionSnapshotParallel,
	}
}

func (c BatchSIConfig) Normalized() BatchSIConfig {
	defaults := DefaultBatchSIConfig()
	if c.WorkerCount == 0 {
		c.WorkerCount = defaults.WorkerCount
	}
	if c.PartitionMode == "" {
		c.PartitionMode = defaults.PartitionMode
	}
	if c.OrderingMode == "" {
		c.OrderingMode = defaults.OrderingMode
	}
	if c.PriorityMode == "" {
		c.PriorityMode = defaults.PriorityMode
	}
	if c.ExecutionMode == "" {
		c.ExecutionMode = defaults.ExecutionMode
	}
	return c
}

func (c BatchSIConfig) Validate() error {
	c = c.Normalized()
	if c.WorkerCount < 1 || c.WorkerCount > 8 {
		return fmt.Errorf("batch-si worker_count must be between 1 and 8")
	}
	switch c.PartitionMode {
	case BatchSIPartitionWRBP, BatchSIPartitionSequential:
	default:
		return fmt.Errorf("unsupported batch-si partition_mode %q", c.PartitionMode)
	}
	switch c.OrderingMode {
	case BatchSIOrderingOFAS, BatchSIOrderingDependencyGraph:
	default:
		return fmt.Errorf("unsupported batch-si ordering_mode %q", c.OrderingMode)
	}
	switch c.PriorityMode {
	case BatchSIPriorityPaper, BatchSIPriorityTxID:
	default:
		return fmt.Errorf("unsupported batch-si priority_mode %q", c.PriorityMode)
	}
	switch c.ExecutionMode {
	case BatchSIExecutionSnapshotParallel, BatchSIExecutionSnapshotSerial:
	default:
		return fmt.Errorf("unsupported batch-si execution_mode %q", c.ExecutionMode)
	}
	return nil
}

type BatchSIBatch struct {
	BatchNumber           int      `json:"batch_number"`
	OrderedTransactionIDs []string `json:"ordered_transaction_ids"`
}

type BatchSIPlanMetrics struct {
	TransactionCount            int `json:"transaction_count"`
	ReadOnlyTransactionCount    int `json:"read_only_transaction_count"`
	AWRTAddressCount            int `json:"awrt_address_count"`
	AWRTWriteReferenceCount     int `json:"awrt_write_reference_count"`
	BatchCount                  int `json:"batch_count"`
	MaximumBatchWidth           int `json:"maximum_batch_width"`
	WriteOpportunityReuseCount  int `json:"write_opportunity_reuse_count"`
	DependencyEdgeCount         int `json:"dependency_edge_count"`
	OFASAbortedTransactionCount int `json:"ofas_aborted_transaction_count"`
	PlanningIterationCount      int `json:"planning_iteration_count"`
}

type BatchSIOrderEvidence struct {
	TxID                  string `json:"tx_id"`
	TransactionOrdinal    int    `json:"transaction_ordinal"`
	BatchNumber           int    `json:"batch_number"`
	SerializationPosition int    `json:"serialization_position"`
}

type BatchSIPlan struct {
	AlgorithmID         string                 `json:"algorithm_id"`
	Version             string                 `json:"version"`
	BlockHeight         uint64                 `json:"block_height"`
	PartitionMode       string                 `json:"partition_mode"`
	OrderingMode        string                 `json:"ordering_mode"`
	PriorityMode        string                 `json:"priority_mode"`
	TransactionOrdinals map[string]int         `json:"transaction_ordinals"`
	OrderEvidence       []BatchSIOrderEvidence `json:"order_evidence"`
	Batches             []BatchSIBatch         `json:"batches"`
	Metrics             BatchSIPlanMetrics     `json:"metrics"`
	PlanDigest          string                 `json:"plan_digest"`
}

type BatchSIPlanningResult struct {
	Plan     BatchSIPlan
	Ordered  []tx.SignedTransaction
	Deferred []tx.SignedTransaction
}

type BatchSIMetrics struct {
	WorkerCount                    int    `json:"worker_count"`
	PartitionMode                  string `json:"partition_mode"`
	OrderingMode                   string `json:"ordering_mode"`
	PriorityMode                   string `json:"priority_mode"`
	ExecutionMode                  string `json:"execution_mode"`
	BatchCount                     int    `json:"batch_count"`
	MaximumBatchWidth              int    `json:"maximum_batch_width"`
	AverageBatchWidthMilli         int    `json:"average_batch_width_milli"`
	AWRTAddressCount               int    `json:"awrt_address_count"`
	AWRTWriteReferenceCount        int    `json:"awrt_write_reference_count"`
	WriteOpportunityReuseCount     int    `json:"write_opportunity_reuse_count"`
	DependencyEdgeCount            int    `json:"dependency_edge_count"`
	OFASAbortedTransactionCount    int    `json:"ofas_aborted_transaction_count"`
	PlanningIterationCount         int    `json:"planning_iteration_count"`
	SnapshotCount                  int    `json:"snapshot_count"`
	ExecutionTaskCount             int    `json:"execution_task_count"`
	MaximumObservedParallelWidth   int    `json:"maximum_observed_parallel_width"`
	BatchSnapshotCreateMS          int64  `json:"batch_snapshot_create_ms"`
	TransactionExecutionMS         int64  `json:"transaction_execution_ms"`
	DeterministicMaterializationMS int64  `json:"deterministic_materialization_ms"`
}

type batchSITxDescriptor struct {
	Item      tx.SignedTransaction
	TxID      string
	ReadKeys  []string
	WriteKeys []string
	IDRank    int
}

type batchSIPartition struct {
	Number int
	Txs    []batchSITxDescriptor
}

// BuildBatchSIPlan performs AWRT, WRBP (or its own sequential ablation), and
// OFAS (or the Batch-SI-local dependency-graph ablation). Paper transaction
// identifiers T.id are the deterministic transaction ordinals supplied by the
// ordering/consensus input sequence; TxID remains transaction identity only.
func BuildBatchSIPlan(b block.Block, config BatchSIConfig) (BatchSIPlanningResult, error) {
	ordinals := make(map[string]int, len(b.TxList))
	for index, item := range b.TxList {
		if strings.TrimSpace(item.TxID) != "" {
			ordinals[item.TxID] = index + 1
		}
	}
	return BuildBatchSIPlanWithOrdinals(b, config, ordinals)
}

// BuildBatchSIPlanWithOrdinals preserves the ordering node's original T.id
// mapping even after OFAS has reordered the accepted transaction set.
func BuildBatchSIPlanWithOrdinals(b block.Block, config BatchSIConfig, ordinals map[string]int) (BatchSIPlanningResult, error) {
	config = config.Normalized()
	if err := config.Validate(); err != nil {
		return BatchSIPlanningResult{}, err
	}
	descriptors, normalizedOrdinals, err := batchSIDescriptors(b.TxList, ordinals, b.ShardID)
	if err != nil {
		return BatchSIPlanningResult{}, err
	}
	active := append([]batchSITxDescriptor(nil), descriptors...)
	deferredByID := map[string]batchSITxDescriptor{}
	iterations := 0
	var finalPartitions []batchSIPartition
	var finalMetrics BatchSIPlanMetrics
	for {
		iterations++
		if iterations > len(descriptors)+1 {
			return BatchSIPlanningResult{}, fmt.Errorf("batch-si planning did not converge")
		}
		awrt, awrtReferences := batchSIBuildAWRT(active)
		var partitions []batchSIPartition
		var opportunityReuse int
		switch config.PartitionMode {
		case BatchSIPartitionSequential:
			partitions = batchSISequentialPartition(active)
		default:
			partitions, opportunityReuse = batchSIWRBPPartition(active)
		}
		aborted := map[string]batchSITxDescriptor{}
		dependencyEdges := 0
		for index := range partitions {
			var ordered []batchSITxDescriptor
			var rejected []batchSITxDescriptor
			var edges int
			switch config.OrderingMode {
			case BatchSIOrderingDependencyGraph:
				ordered, rejected, edges = batchSIDependencyGraphOrder(partitions[index].Txs)
			default:
				ordered, rejected, edges = batchSIOFASOrder(partitions[index].Txs, config.PriorityMode)
			}
			partitions[index].Txs = ordered
			dependencyEdges += edges
			for _, item := range rejected {
				aborted[item.TxID] = item
			}
		}
		if len(aborted) == 0 {
			finalPartitions = partitions
			maximumWidth := 0
			readOnlyCount := 0
			for _, item := range active {
				if len(item.WriteKeys) == 0 {
					readOnlyCount++
				}
			}
			for _, partition := range partitions {
				if len(partition.Txs) > maximumWidth {
					maximumWidth = len(partition.Txs)
				}
			}
			finalMetrics = BatchSIPlanMetrics{
				TransactionCount:            len(active),
				ReadOnlyTransactionCount:    readOnlyCount,
				AWRTAddressCount:            len(awrt),
				AWRTWriteReferenceCount:     awrtReferences,
				BatchCount:                  len(partitions),
				MaximumBatchWidth:           maximumWidth,
				WriteOpportunityReuseCount:  opportunityReuse,
				DependencyEdgeCount:         dependencyEdges,
				OFASAbortedTransactionCount: len(deferredByID),
				PlanningIterationCount:      iterations,
			}
			break
		}
		for id, item := range aborted {
			deferredByID[id] = item
		}
		next := make([]batchSITxDescriptor, 0, len(active)-len(aborted))
		for _, item := range active {
			if _, rejected := aborted[item.TxID]; !rejected {
				next = append(next, item)
			}
		}
		active = next
	}

	plan := BatchSIPlan{
		AlgorithmID:         BatchSIPlanAlgorithmID,
		Version:             BatchSIPlanVersion,
		BlockHeight:         b.Height,
		PartitionMode:       config.PartitionMode,
		OrderingMode:        config.OrderingMode,
		PriorityMode:        config.PriorityMode,
		TransactionOrdinals: normalizedOrdinals,
		Metrics:             finalMetrics,
	}
	byID := make(map[string]tx.SignedTransaction, len(active))
	for _, item := range active {
		byID[item.TxID] = item.Item
	}
	for _, partition := range finalPartitions {
		batch := BatchSIBatch{BatchNumber: partition.Number}
		for _, item := range partition.Txs {
			batch.OrderedTransactionIDs = append(batch.OrderedTransactionIDs, item.TxID)
		}
		plan.Batches = append(plan.Batches, batch)
	}
	plan.OrderEvidence = batchSIOrderEvidence(plan.TransactionOrdinals, plan.Batches)
	plan.PlanDigest = batchSIPlanDigest(plan)
	ordered := make([]tx.SignedTransaction, 0, len(active))
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
	return BatchSIPlanningResult{Plan: plan, Ordered: ordered, Deferred: deferred}, nil
}

func MarshalBatchSIPlan(plan BatchSIPlan) ([]byte, error) {
	return json.Marshal(plan)
}

func ParseBatchSIPlan(raw []byte) (BatchSIPlan, error) {
	var plan BatchSIPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return plan, fmt.Errorf("decode batch-si plan: %w", err)
	}
	if plan.AlgorithmID != BatchSIPlanAlgorithmID || plan.Version != BatchSIPlanVersion {
		return plan, fmt.Errorf("unsupported batch-si plan %s/%s", plan.AlgorithmID, plan.Version)
	}
	if plan.PlanDigest == "" || plan.PlanDigest != batchSIPlanDigest(plan) {
		return plan, fmt.Errorf("batch-si plan digest mismatch")
	}
	return plan, nil
}

// VerifyBatchSIPlan recomputes the accepted-set plan. Deferred transactions are
// intentionally absent from the proposed block and are released to the leader's
// mempool before PBFT.
func VerifyBatchSIPlan(b block.Block, plan BatchSIPlan, config BatchSIConfig) error {
	config = config.Normalized()
	if plan.PartitionMode != config.PartitionMode || plan.OrderingMode != config.OrderingMode || plan.PriorityMode != config.PriorityMode {
		return fmt.Errorf("batch-si plan config mismatch")
	}
	if len(plan.TransactionOrdinals) != len(b.TxList) {
		return fmt.Errorf("batch-si transaction ordinal count mismatch")
	}
	seenOrdinals := map[int]bool{}
	for _, item := range b.TxList {
		ordinal := plan.TransactionOrdinals[item.TxID]
		if ordinal < 1 {
			return fmt.Errorf("batch-si missing paper transaction ordinal for %s", item.TxID)
		}
		if seenOrdinals[ordinal] {
			return fmt.Errorf("batch-si duplicate paper transaction ordinal %d", ordinal)
		}
		seenOrdinals[ordinal] = true
	}
	candidate := b
	candidate.ExecutionPlan = nil
	result, err := BuildBatchSIPlanWithOrdinals(candidate, config, plan.TransactionOrdinals)
	if err != nil {
		return err
	}
	if len(result.Deferred) != 0 {
		return fmt.Errorf("batch-si accepted block still contains %d non-serializable transactions", len(result.Deferred))
	}
	if result.Plan.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("batch-si semantic recompute mismatch")
	}
	flattened := make([]string, 0, len(b.TxList))
	for _, batch := range plan.Batches {
		flattened = append(flattened, batch.OrderedTransactionIDs...)
	}
	if len(flattened) != len(b.TxList) {
		return fmt.Errorf("batch-si plan transaction count mismatch")
	}
	for index, item := range b.TxList {
		if flattened[index] != item.TxID {
			return fmt.Errorf("batch-si plan order mismatch at index %d", index)
		}
	}
	return nil
}

func batchSIOrderEvidence(ordinals map[string]int, batches []BatchSIBatch) []BatchSIOrderEvidence {
	out := make([]BatchSIOrderEvidence, 0, len(ordinals))
	position := 0
	for _, batch := range batches {
		for _, txID := range batch.OrderedTransactionIDs {
			position++
			out = append(out, BatchSIOrderEvidence{
				TxID:                  txID,
				TransactionOrdinal:    ordinals[txID],
				BatchNumber:           batch.BatchNumber,
				SerializationPosition: position,
			})
		}
	}
	return out
}

func batchSIPlanDigest(plan BatchSIPlan) string {
	clone := plan
	clone.PlanDigest = ""
	return batchSIStableDigest(clone)
}

func batchSIDescriptors(items []tx.SignedTransaction, ordinals map[string]int, shardID string) ([]batchSITxDescriptor, map[string]int, error) {
	seen := map[string]bool{}
	seenOrdinals := map[int]bool{}
	normalized := make(map[string]int, len(items))
	out := make([]batchSITxDescriptor, 0, len(items))
	for index, item := range items {
		if strings.TrimSpace(item.TxID) == "" {
			return nil, nil, fmt.Errorf("batch-si requires non-empty transaction ids")
		}
		if seen[item.TxID] {
			return nil, nil, fmt.Errorf("batch-si duplicate transaction id %s", item.TxID)
		}
		seen[item.TxID] = true
		ordinal := 0
		if ordinals != nil {
			ordinal = ordinals[item.TxID]
		}
		if ordinal < 1 {
			// The public BuildBatchSIPlan caller derives T.id from the
			// ordering input. Explicit ordinal callers must bind every tx.
			if ordinals != nil {
				return nil, nil, fmt.Errorf("batch-si missing paper transaction ordinal for %s", item.TxID)
			}
			ordinal = index + 1
		}
		if seenOrdinals[ordinal] {
			return nil, nil, fmt.Errorf("batch-si duplicate paper transaction ordinal %d", ordinal)
		}
		seenOrdinals[ordinal] = true
		normalized[item.TxID] = ordinal
		reads, writes := batchSIDeclaredAccess(item, shardID)
		out = append(out, batchSITxDescriptor{Item: item, TxID: item.TxID, ReadKeys: reads, WriteKeys: writes, IDRank: ordinal})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].IDRank != out[j].IDRank {
			return out[i].IDRank < out[j].IDRank
		}
		return out[i].TxID < out[j].TxID
	})
	return out, normalized, nil
}

func batchSIDeclaredAccess(item tx.SignedTransaction, shardID string) ([]string, []string) {
	reads := map[string]bool{}
	writes := map[string]bool{}
	if len(item.AccessList) > 0 {
		for _, access := range item.AccessList {
			if access.Key == "" {
				continue
			}
			switch access.Mode {
			case tx.AccessRead:
				reads[access.Key] = true
			case tx.AccessWrite:
				writes[access.Key] = true
			case tx.AccessReadWrite, tx.AccessCommutativeDelta:
				reads[access.Key] = true
				writes[access.Key] = true
			}
		}
		if item.AccessListSource == "legacy_state_keys" {
			for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
				reads[key] = true
				writes[key] = true
			}
		}
	} else {
		for _, key := range item.StateKeys {
			if key != "" {
				reads[key] = true
				writes[key] = true
			}
		}
		for _, key := range []string{"balance:" + item.Sender, "nonce:" + item.Sender, "balance:" + item.Receiver, "nonce:" + item.Receiver} {
			reads[key] = true
			writes[key] = true
		}
	}
	if batchSIIsCrossShardTargetCommit(item, shardID) {
		// MBE cross-shard target-commit state is deterministic protocol state.
		// Keep Batch-SI strict declared-write validation while making the
		// protocol-owned write explicit in the effective execution envelope.
		writes["relay_commit:"+item.TxID] = true
	}
	return batchSISortedKeys(reads), batchSISortedKeys(writes)
}

func batchSIBuildAWRT(items []batchSITxDescriptor) (map[string][]string, int) {
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

func batchSIWRBPPartition(items []batchSITxDescriptor) ([]batchSIPartition, int) {
	ordered := append([]batchSITxDescriptor(nil), items...)
	// Algorithm 1 consumes the ordering node's deterministic paper T.id order.
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
			// Advancing on equality is required to preserve the WRBP invariant
			// that each address has at most one writer in a batch.
			currentBatch[key] = assigned + 1
		}
		batches[assigned] = append(batches[assigned], item)
	}
	return batchSICompactPartitions(batches), reuseCount
}

func batchSISequentialPartition(items []batchSITxDescriptor) []batchSIPartition {
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

func batchSICompactPartitions(batches map[int][]batchSITxDescriptor) []batchSIPartition {
	numbers := make([]int, 0, len(batches))
	for number, items := range batches {
		if len(items) > 0 {
			numbers = append(numbers, number)
		}
	}
	sort.Ints(numbers)
	out := make([]batchSIPartition, 0, len(numbers))
	for _, number := range numbers {
		out = append(out, batchSIPartition{Number: number, Txs: batches[number]})
	}
	return out
}

func batchSIOFASOrder(items []batchSITxDescriptor, priorityMode string) ([]batchSITxDescriptor, []batchSITxDescriptor, int) {
	// WRBP guarantees that each logical key has at most one writer in a batch.
	// The implementation below follows Algorithm 2's state directly:
	//   tRNum  = reads performed by the transaction (excluding its own writes)
	//   kRNum  = reads observed on the transaction's written keys
	//   kMaxR  = maximum current reader order for those written keys
	//   sort   = the transaction's serial order label
	// Transactions are visited by the paper priority and Rule 2 defers a
	// transaction when a previously ordered writer cannot be moved behind it.
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
			// Algorithm 2, Rule 2: the already ordered writer cannot be
			// shifted behind the current reader without reversing an earlier
			// dependency when the reader's raised order exceeds kMaxR.
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
		// Rule 2 must remove at least one transaction from a remaining cycle.
		// Keep higher-priority writers and defer the lowest-priority participant.
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

func batchSIDependencyGraphOrder(items []batchSITxDescriptor) ([]batchSITxDescriptor, []batchSITxDescriptor, int) {
	active := append([]batchSITxDescriptor(nil), items...)
	aborted := []batchSITxDescriptor{}
	totalEdges := 0
	for {
		ordered, cyclic, edges := batchSITopologicalOrder(active)
		totalEdges += edges
		if len(cyclic) == 0 {
			return ordered, aborted, totalEdges
		}
		// Deterministically remove one cycle participant and rebuild. This is
		// local to the Batch-SI ablation and does not call the CG implementation.
		sort.Slice(cyclic, func(i, j int) bool { return cyclic[i].TxID > cyclic[j].TxID })
		victim := cyclic[0]
		aborted = append(aborted, victim)
		next := active[:0]
		for _, item := range active {
			if item.TxID != victim.TxID {
				next = append(next, item)
			}
		}
		active = append([]batchSITxDescriptor(nil), next...)
	}
}

func batchSITopologicalOrder(items []batchSITxDescriptor) ([]batchSITxDescriptor, []batchSITxDescriptor, int) {
	byID := map[string]batchSITxDescriptor{}
	writer := map[string]string{}
	for _, item := range items {
		byID[item.TxID] = item
		for _, key := range item.WriteKeys {
			writer[key] = item.TxID
		}
	}
	edges := map[string]map[string]bool{}
	indegree := map[string]int{}
	for _, item := range items {
		indegree[item.TxID] = 0
	}
	for _, item := range items {
		ownWrites := batchSIStringSet(item.WriteKeys)
		for _, key := range item.ReadKeys {
			if ownWrites[key] {
				continue
			}
			writerID := writer[key]
			if writerID == "" || writerID == item.TxID {
				continue
			}
			if edges[item.TxID] == nil {
				edges[item.TxID] = map[string]bool{}
			}
			if !edges[item.TxID][writerID] {
				edges[item.TxID][writerID] = true
				indegree[writerID]++
			}
		}
	}
	ready := []string{}
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	ordered := []batchSITxDescriptor{}
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		ordered = append(ordered, byID[id])
		children := make([]string, 0, len(edges[id]))
		for child := range edges[id] {
			children = append(children, child)
		}
		sort.Strings(children)
		for _, child := range children {
			indegree[child]--
			if indegree[child] == 0 {
				ready = append(ready, child)
				sort.Strings(ready)
			}
		}
	}
	cyclic := []batchSITxDescriptor{}
	if len(ordered) != len(items) {
		seen := map[string]bool{}
		for _, item := range ordered {
			seen[item.TxID] = true
		}
		for _, item := range items {
			if !seen[item.TxID] {
				cyclic = append(cyclic, item)
			}
		}
	}
	edgeCount := 0
	for _, children := range edges {
		edgeCount += len(children)
	}
	return ordered, cyclic, edgeCount
}

func batchSICloneIntSet(input map[int]bool) map[int]bool {
	out := map[int]bool{}
	for value := range input {
		out[value] = true
	}
	return out
}

func batchSIMinIntersection(sets []map[int]bool) int {
	if len(sets) == 0 {
		return 1
	}
	values := make([]int, 0, len(sets[0]))
	for value := range sets[0] {
		values = append(values, value)
	}
	sort.Ints(values)
	for _, value := range values {
		present := true
		for _, set := range sets[1:] {
			if !set[value] {
				present = false
				break
			}
		}
		if present {
			return value
		}
	}
	return 0
}

func batchSIStringSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		out[value] = true
	}
	return out
}

func batchSISortedKeys(values map[string]bool) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// BatchSIExecutor implements transaction evaluation, snapshot execution, and
// deterministic materialization independently from the other MBE methods.
type BatchSIExecutor struct {
	Config                BatchSIConfig
	DefaultInitialBalance int64
	Metrics               BatchSIMetrics
}

func NewBatchSIExecutor(config BatchSIConfig) *BatchSIExecutor {
	return &BatchSIExecutor{Config: config.Normalized(), DefaultInitialBalance: 1_000_000}
}

func (e *BatchSIExecutor) ExecuteBlock(ctx context.Context, b block.Block, base map[string]string) (Result, error) {
	e.Config = e.Config.Normalized()
	if err := e.Config.Validate(); err != nil {
		return Result{}, err
	}
	if b.ExecutionPlan == nil || b.ExecutionPlan.AlgorithmID != BatchSIPlanAlgorithmID {
		return Result{}, fmt.Errorf("batch-si requires %s execution plan", BatchSIPlanAlgorithmID)
	}
	plan, err := ParseBatchSIPlan(b.ExecutionPlan.Payload)
	if err != nil {
		return Result{}, err
	}
	if b.ExecutionPlan.PayloadDigest != batchSITextDigest(string(b.ExecutionPlan.Payload)) || b.ExecutionPlan.PlanDigest != plan.PlanDigest {
		return Result{}, fmt.Errorf("batch-si execution plan envelope mismatch")
	}
	if err := VerifyBatchSIPlan(b, plan, e.Config); err != nil {
		return Result{}, err
	}
	working := batchSICopySnapshot(base)
	before := state.RootOfSnapshot(working)
	result := Result{
		BlockHash:       b.BlockHash,
		Height:          b.Height,
		StateRootBefore: before,
		Deterministic:   true,
		StateUpdates:    map[string]string{},
		BlockExecutorID: BatchSIBlockExecutorID,
		ExecutorVersion: BatchSIBlockExecutorVersion,
		WorkerCount:     e.Config.WorkerCount,
	}
	byID := make(map[string]tx.SignedTransaction, len(b.TxList))
	blockIndex := make(map[string]int, len(b.TxList))
	for index, item := range b.TxList {
		byID[item.TxID] = item
		blockIndex[item.TxID] = index
	}
	allDeltas := make([]TxDelta, 0, len(b.TxList))
	allReceipts := make([]Receipt, 0, len(b.TxList))
	maximumObserved := 0
	var snapshotMS, executionMS, materializationMS int64
	for _, batch := range plan.Batches {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		snapshotStarted := time.Now()
		snapshot := batchSICopySnapshot(working)
		snapshotMS += time.Since(snapshotStarted).Milliseconds()
		transactions := make([]tx.SignedTransaction, 0, len(batch.OrderedTransactionIDs))
		for _, id := range batch.OrderedTransactionIDs {
			item, ok := byID[id]
			if !ok {
				return Result{}, fmt.Errorf("batch-si plan references unknown transaction %s", id)
			}
			transactions = append(transactions, item)
		}
		executionStarted := time.Now()
		batchResults, observed, err := e.executeBatch(ctx, b, transactions, snapshot, blockIndex)
		executionMS += time.Since(executionStarted).Milliseconds()
		if err != nil {
			return Result{}, err
		}
		if observed > maximumObserved {
			maximumObserved = observed
		}
		materializeStarted := time.Now()
		for _, txResult := range batchResults {
			if err := batchSIValidateDeclaredWrites(txResult.Item, txResult.Delta.WriteSet, b.ShardID); err != nil {
				return Result{}, err
			}
			for _, key := range batchSISortedWriteKeys(txResult.Delta.WriteSet) {
				working[batchSIQualifiedKey(b.ShardID, key)] = txResult.Delta.WriteSet[key]
			}
			receipt := txResult.Receipt
			receipt.StateRootAfterTx = state.RootOfSnapshot(working)
			delta := txResult.Delta
			delta.Receipt = receipt
			allDeltas = append(allDeltas, delta)
			allReceipts = append(allReceipts, receipt)
			if receipt.Success {
				result.SuccessfulTxs++
			} else {
				result.FailedTxs++
			}
		}
		materializationMS += time.Since(materializeStarted).Milliseconds()
	}
	result.Receipts = allReceipts
	result.TxDeltas = allDeltas
	result.StateRootAfter = state.RootOfSnapshot(working)
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = batchSIStateDelta(base, working)
	declared := batchSIDeclaredAccessSet(b.TxList, b.ShardID)
	flattened := []string{}
	indexes := []int{}
	for _, batch := range plan.Batches {
		for _, id := range batch.OrderedTransactionIDs {
			flattened = append(flattened, id)
			indexes = append(indexes, blockIndex[id])
		}
	}
	result.Plan = ExecutionPlan{
		EngineID:                BatchSIBlockExecutorID,
		EngineVersion:           BatchSIBlockExecutorVersion,
		BlockHash:               b.BlockHash,
		BlockHeight:             b.Height,
		OrderedTransactionIDs:   flattened,
		OriginalTransactionIdxs: indexes,
		DeclaredAccessSetDigest: batchSIStableDigest(declared),
		DeclaredReadKeyCount:    len(declared.ReadKeys),
		DeclaredWriteKeyCount:   len(declared.WriteKeys),
		WorkerCount:             e.Config.WorkerCount,
		PlanDigest:              plan.PlanDigest,
	}
	result.PlanDigest = plan.PlanDigest
	averageMilli := 0
	if len(plan.Batches) > 0 {
		averageMilli = len(b.TxList) * 1000 / len(plan.Batches)
	}
	e.Metrics = BatchSIMetrics{
		WorkerCount:                    e.Config.WorkerCount,
		PartitionMode:                  plan.PartitionMode,
		OrderingMode:                   plan.OrderingMode,
		PriorityMode:                   plan.PriorityMode,
		ExecutionMode:                  e.Config.ExecutionMode,
		BatchCount:                     plan.Metrics.BatchCount,
		MaximumBatchWidth:              plan.Metrics.MaximumBatchWidth,
		AverageBatchWidthMilli:         averageMilli,
		AWRTAddressCount:               plan.Metrics.AWRTAddressCount,
		AWRTWriteReferenceCount:        plan.Metrics.AWRTWriteReferenceCount,
		WriteOpportunityReuseCount:     plan.Metrics.WriteOpportunityReuseCount,
		DependencyEdgeCount:            plan.Metrics.DependencyEdgeCount,
		OFASAbortedTransactionCount:    plan.Metrics.OFASAbortedTransactionCount,
		PlanningIterationCount:         plan.Metrics.PlanningIterationCount,
		SnapshotCount:                  len(plan.Batches),
		ExecutionTaskCount:             len(b.TxList),
		MaximumObservedParallelWidth:   maximumObserved,
		BatchSnapshotCreateMS:          snapshotMS,
		TransactionExecutionMS:         executionMS,
		DeterministicMaterializationMS: materializationMS,
	}
	return result, nil
}

type batchSITxExecutionResult struct {
	Item    tx.SignedTransaction
	Receipt Receipt
	Delta   TxDelta
}

func (e *BatchSIExecutor) executeBatch(ctx context.Context, b block.Block, items []tx.SignedTransaction, snapshot map[string]string, blockIndex map[string]int) ([]batchSITxExecutionResult, int, error) {
	results := make([]batchSITxExecutionResult, len(items))
	if len(items) == 0 {
		return results, 0, nil
	}
	workerCount := e.Config.WorkerCount
	if e.Config.ExecutionMode == BatchSIExecutionSnapshotSerial {
		workerCount = 1
	}
	if workerCount > len(items) {
		workerCount = len(items)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	type job struct{ index int }
	jobs := make(chan job)
	var wg sync.WaitGroup
	var active int
	var maximum int
	var activeMu sync.Mutex
	var firstErr error
	var errMu sync.Mutex
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if ctx.Err() != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					errMu.Unlock()
					continue
				}
				activeMu.Lock()
				active++
				if active > maximum {
					maximum = active
				}
				activeMu.Unlock()
				item := items[task.index]
				receipt, delta := e.executeTransaction(b, item, snapshot, blockIndex[item.TxID])
				results[task.index] = batchSITxExecutionResult{Item: item, Receipt: receipt, Delta: delta}
				activeMu.Lock()
				active--
				activeMu.Unlock()
			}
		}()
	}
	for index := range items {
		select {
		case jobs <- job{index: index}:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, maximum, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, maximum, firstErr
	}
	return results, maximum, nil
}

func (e *BatchSIExecutor) executeTransaction(b block.Block, item tx.SignedTransaction, snapshot map[string]string, originalIndex int) (Receipt, TxDelta) {
	overlay := newBatchSIOverlay(b.ShardID, snapshot)
	receipt := Receipt{TxID: item.TxID, BlockHash: b.BlockHash, Height: b.Height, Success: false, ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...)}
	batchSIApplyDeclaredSemanticReads(overlay, item.AccessList)
	if batchSIIsPureCommutativeDelta(item.AccessList) {
		overlay.applyCommutativeDeltas(item.AccessList)
		receipt.Success = true
	} else if batchSIIsCrossShardTargetCommit(item, b.ShardID) {
		overlay.set("relay_commit:"+item.TxID, "1")
		receipt.Success = true
	} else if batchSIIsDirectAccessTransaction(item) {
		batchSIApplyDirectAccessTransaction(overlay, item)
		receipt.Success = true
	} else {
		senderAccount, receiverAccount := batchSITransferAccounts(item)
		_, declaredWriteKeys := batchSIDeclaredAccess(item, b.ShardID)
		declaredWrites := batchSIStringSet(declaredWriteKeys)
		overlay.ensureDeclaredAccount(senderAccount, e.DefaultInitialBalance, declaredWrites)
		overlay.ensureDeclaredAccount(receiverAccount, 0, declaredWrites)
		expectedNonce := overlay.nonce(senderAccount)
		switch {
		case item.Nonce != expectedNonce:
			receipt.Error = fmt.Sprintf("nonce_mismatch_expected_%d_got_%d", expectedNonce, item.Nonce)
		case item.Value <= 0:
			receipt.Error = "invalid_value"
		case overlay.balance(senderAccount) < item.Value:
			receipt.Error = "insufficient_balance"
		default:
			senderBalance := overlay.balance(senderAccount)
			overlay.setBalance(senderAccount, senderBalance-item.Value)
			overlay.setBalance(receiverAccount, overlay.balance(receiverAccount)+item.Value)
			overlay.setNonce(senderAccount, item.Nonce+1)
			overlay.applyCommutativeDeltas(item.AccessList)
			receipt.Success = true
		}
	}
	delta := TxDelta{TxID: item.TxID, OriginalIndex: originalIndex, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: overlay.logicalWrites(), Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
	return receipt, delta
}

type batchSIAccountAccess struct {
	balancePresent  bool
	balanceWritable bool
	noncePresent    bool
	nonceWritable   bool
}

// batchSITransferAccounts resolves the account aliases declared by the access
// list. MBE synthetic transactions are signed by a deterministic public-key
// address while their workload-level access list intentionally keeps stable
// logical account aliases such as client_s0_6. Batch-SI must execute against
// the declared keys used by AWRT/WRBP/OFAS; otherwise it would schedule one
// key set and materialize another.
func batchSITransferAccounts(item tx.SignedTransaction) (string, string) {
	accounts := map[string]batchSIAccountAccess{}
	for _, access := range item.AccessList {
		key := strings.TrimSpace(access.Key)
		account := ""
		isBalance := false
		switch {
		case strings.HasPrefix(key, "balance:"):
			account = strings.TrimSpace(strings.TrimPrefix(key, "balance:"))
			isBalance = true
		case strings.HasPrefix(key, "nonce:"):
			account = strings.TrimSpace(strings.TrimPrefix(key, "nonce:"))
		default:
			continue
		}
		if account == "" {
			continue
		}
		entry := accounts[account]
		writable := access.Mode == tx.AccessWrite || access.Mode == tx.AccessReadWrite || access.Mode == tx.AccessCommutativeDelta
		if isBalance {
			entry.balancePresent = true
			entry.balanceWritable = entry.balanceWritable || writable
		} else {
			entry.noncePresent = true
			entry.nonceWritable = entry.nonceWritable || writable
		}
		accounts[account] = entry
	}

	sender := strings.TrimSpace(item.Sender)
	receiver := strings.TrimSpace(item.Receiver)

	receiverDeclared := false
	if entry, ok := accounts[receiver]; ok && entry.balanceWritable && entry.noncePresent {
		receiverDeclared = true
	}

	if entry, ok := accounts[sender]; !ok || !entry.balanceWritable || !entry.nonceWritable {
		candidates := make([]string, 0)
		fallbackCandidates := make([]string, 0)
		for account, access := range accounts {
			if !access.balanceWritable || !access.nonceWritable {
				continue
			}
			fallbackCandidates = append(fallbackCandidates, account)
			if !receiverDeclared || account != receiver {
				candidates = append(candidates, account)
			}
		}
		sort.Strings(candidates)
		sort.Strings(fallbackCandidates)
		switch {
		case len(candidates) > 0:
			sender = candidates[0]
		case len(fallbackCandidates) > 0:
			sender = fallbackCandidates[0]
		}
	}

	if !receiverDeclared {
		candidates := make([]string, 0)
		for account, access := range accounts {
			if account == sender {
				continue
			}
			if access.balanceWritable && access.noncePresent {
				candidates = append(candidates, account)
			}
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			receiver = candidates[0]
		}
	}

	return sender, receiver
}

type batchSIOverlay struct {
	shardID string
	values  map[string]string
	reads   []ReadObservation
	writes  map[string]string
}

func newBatchSIOverlay(shardID string, base map[string]string) *batchSIOverlay {
	return &batchSIOverlay{shardID: shardID, values: batchSICopySnapshot(base), writes: map[string]string{}}
}

func (o *batchSIOverlay) key(key string) string {
	return batchSIQualifiedKey(o.shardID, key)
}
func (o *batchSIOverlay) get(key string) string {
	value := o.values[o.key(key)]
	o.reads = append(o.reads, ReadObservation{Key: key, Value: value, ValueDigest: batchSIDigestValue(value), Source: "batch_si_batch_snapshot"})
	return value
}
func (o *batchSIOverlay) set(key, value string) { o.values[o.key(key)] = value; o.writes[key] = value }
func (o *batchSIOverlay) logicalWrites() map[string]string {
	out := map[string]string{}
	for key, value := range o.writes {
		out[key] = value
	}
	return out
}
func (o *batchSIOverlay) ensureAccount(account string, balance int64) {
	if o.get("balance:"+account) == "" {
		o.setBalance(account, balance)
	}
	if o.get("nonce:"+account) == "" {
		o.setNonce(account, 0)
	}
}

func (o *batchSIOverlay) ensureDeclaredAccount(account string, balance int64, declaredWrites map[string]bool) {
	balanceKey := "balance:" + account
	if declaredWrites[balanceKey] && o.get(balanceKey) == "" {
		o.setBalance(account, balance)
	}
	nonceKey := "nonce:" + account
	if declaredWrites[nonceKey] && o.get(nonceKey) == "" {
		o.setNonce(account, 0)
	}
}
func (o *batchSIOverlay) balance(account string) int64 {
	value, _ := strconv.ParseInt(o.get("balance:"+account), 10, 64)
	return value
}
func (o *batchSIOverlay) setBalance(account string, value int64) {
	o.set("balance:"+account, strconv.FormatInt(value, 10))
}
func (o *batchSIOverlay) nonce(account string) uint64 {
	value, _ := strconv.ParseUint(o.get("nonce:"+account), 10, 64)
	return value
}
func (o *batchSIOverlay) setNonce(account string, value uint64) {
	o.set("nonce:"+account, strconv.FormatUint(value, 10))
}
func (o *batchSIOverlay) applyCommutativeDeltas(accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessCommutativeDelta || access.Key == "" {
			continue
		}
		current, _ := strconv.ParseInt(o.get(access.Key), 10, 64)
		o.set(access.Key, strconv.FormatInt(current+access.Delta, 10))
	}
}

func batchSIIsDirectAccessTransaction(item tx.SignedTransaction) bool {
	return item.AccessListSchema != "" && item.AccessListSchema != "dcl_sale_access_template_v1" && item.AccessListSource != "legacy_state_keys"
}
func batchSIApplyDirectAccessTransaction(overlay *batchSIOverlay, item tx.SignedTransaction) {
	for _, access := range item.AccessList {
		if access.Key == "" {
			continue
		}
		switch access.Mode {
		case tx.AccessRead:
			overlay.get(access.Key)
		case tx.AccessCommutativeDelta:
			overlay.applyCommutativeDeltas([]tx.AccessItem{access})
		case tx.AccessReadWrite:
			previous := overlay.get(access.Key)
			overlay.set(access.Key, batchSIDirectAccessValue(item, access, previous))
		case tx.AccessWrite:
			overlay.set(access.Key, batchSIDirectAccessValue(item, access, ""))
		}
	}
}
func batchSIDirectAccessValue(item tx.SignedTransaction, access tx.AccessItem, previous string) string {
	return batchSIStableDigest(struct {
		LogicalTxID string `json:"logical_tx_id"`
		Key         string `json:"key"`
		Semantics   string `json:"semantics"`
		Previous    string `json:"previous,omitempty"`
	}{tx.SemanticID(item), access.Key, access.UpdateSemantics, previous})
}
func batchSIApplyDeclaredSemanticReads(overlay *batchSIOverlay, accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode == tx.AccessRead && access.Key != "" && access.UpdateSemantics == "category_metadata" {
			overlay.get(access.Key)
		}
	}
}
func batchSIIsPureCommutativeDelta(accesses []tx.AccessItem) bool {
	if len(accesses) == 0 {
		return false
	}
	foundDelta := false
	for _, access := range accesses {
		if access.Mode == tx.AccessRead {
			continue
		}
		if access.Mode != tx.AccessCommutativeDelta {
			return false
		}
		foundDelta = true
	}
	return foundDelta
}
func batchSIIsCrossShardTargetCommit(item tx.SignedTransaction, shardID string) bool {
	return batchSICrossShardTargetFromPayload(item.Payload) == shardID && shardID != ""
}
func batchSICrossShardTargetFromPayload(payload string) string {
	if !strings.HasPrefix(payload, "v5_cross:") {
		return ""
	}
	target := strings.TrimPrefix(payload, "v5_cross:")
	if colon := strings.Index(target, ":"); colon >= 0 {
		target = target[:colon]
	}
	return strings.TrimSpace(target)
}
func batchSIValidateDeclaredWrites(item tx.SignedTransaction, writes map[string]string, shardID string) error {
	_, declaredWrites := batchSIDeclaredAccess(item, shardID)
	declared := batchSIStringSet(declaredWrites)
	for key := range writes {
		if !declared[key] {
			return fmt.Errorf("batch-si transaction %s wrote undeclared key %s", item.TxID, key)
		}
	}
	return nil
}
func batchSIQualifiedKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}
func batchSICopySnapshot(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
func batchSIStateDelta(before, after map[string]string) []StateUpdate {
	keys := []string{}
	for key, value := range after {
		if before[key] != value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]StateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, StateUpdate{Key: key, Value: after[key]})
	}
	return out
}
func batchSIDeclaredAccessSet(items []tx.SignedTransaction, shardID string) AccessSet {
	reads, writes := map[string]bool{}, map[string]bool{}
	for _, item := range items {
		r, w := batchSIDeclaredAccess(item, shardID)
		for _, key := range r {
			reads[key] = true
		}
		for _, key := range w {
			writes[key] = true
		}
	}
	return AccessSet{ReadKeys: batchSISortedKeys(reads), WriteKeys: batchSISortedKeys(writes)}
}
func batchSISortedWriteKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func batchSIDigestValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func batchSIStableDigest(value any) string {
	payload, _ := json.Marshal(value)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
func batchSITextDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
