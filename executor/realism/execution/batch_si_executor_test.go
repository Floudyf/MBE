package execution

import (
	"context"
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func batchSITestTx(id string, reads, writes []string) tx.SignedTransaction {
	access := make([]tx.AccessItem, 0, len(reads)+len(writes))
	for _, key := range reads {
		access = append(access, tx.AccessItem{Key: key, Mode: tx.AccessRead, UpdateSemantics: "read"})
	}
	for _, key := range writes {
		access = append(access, tx.AccessItem{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	}
	return tx.SignedTransaction{
		TxID:             id,
		Sender:           "sender-" + id,
		Receiver:         "receiver-" + id,
		Nonce:            0,
		Value:            1,
		StateKeys:        append(append([]string(nil), reads...), writes...),
		AccessList:       access,
		AccessListSchema: "direct_access_v3",
		AccessListSource: "batch_si_test",
		Payload:          "batch_si_test",
	}
}

func batchSITestBlock(items ...tx.SignedTransaction) block.Block {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.TxID)
	}
	return block.Block{ShardID: "shard-0", Height: 7, TxIDs: ids, TxList: items}
}

func TestBatchSIWRBPMatchesPaperExampleAndHasNoIntraBatchWW(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"k2"}, []string{"k1", "k4"}),
		batchSITestTx("T2", []string{"k4"}, []string{"k1", "k3"}),
		batchSITestTx("T3", []string{"k3"}, []string{"k2"}),
		batchSITestTx("T4", []string{"k1"}, []string{"k4"}),
		batchSITestTx("T5", []string{"k7"}, []string{"k1", "k2"}),
		batchSITestTx("T6", []string{"k2"}, []string{"k3", "k4"}),
		batchSITestTx("T7", []string{"k1"}, []string{"k2"}),
		batchSITestTx("T8", []string{"k4"}, []string{"k3"}),
	}
	descriptors, _, err := batchSIDescriptors(items, nil, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	partitions, reuse := batchSIWRBPPartition(descriptors)
	if len(partitions) != 3 {
		t.Fatalf("WRBP paper example should use three batches, got %d", len(partitions))
	}
	if reuse != 2 {
		t.Fatalf("WRBP paper example should reuse two opportunity batches, got %d", reuse)
	}
	actualBatches := make([][]string, 0, len(partitions))
	for _, partition := range partitions {
		ids := make([]string, 0, len(partition.Txs))
		for _, item := range partition.Txs {
			ids = append(ids, item.TxID)
		}
		actualBatches = append(actualBatches, ids)
	}
	expectedBatches := [][]string{{"T1", "T3", "T8"}, {"T2", "T4", "T7"}, {"T5", "T6"}}
	if !reflect.DeepEqual(actualBatches, expectedBatches) {
		t.Fatalf("WRBP Figure 7 batch membership mismatch: got %v want %v", actualBatches, expectedBatches)
	}
	for _, partition := range partitions {
		writers := map[string]string{}
		for _, item := range partition.Txs {
			for _, key := range item.WriteKeys {
				if prior := writers[key]; prior != "" {
					t.Fatalf("batch %d contains WW conflict on %s: %s and %s", partition.Number, key, prior, item.TxID)
				}
				writers[key] = item.TxID
			}
		}
	}
}

func TestBatchSIReadOnlyTransactionsEnterFirstBatch(t *testing.T) {
	readOnly := batchSITestTx("T1", []string{"k1"}, nil)
	writer := batchSITestTx("T2", nil, []string{"k1"})
	result, err := BuildBatchSIPlan(batchSITestBlock(readOnly, writer), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.Batches) == 0 || result.Plan.Batches[0].BatchNumber != 1 {
		t.Fatalf("missing first batch: %+v", result.Plan.Batches)
	}
	found := false
	for _, id := range result.Plan.Batches[0].OrderedTransactionIDs {
		found = found || id == readOnly.TxID
	}
	if !found {
		t.Fatalf("read-only transaction was not assigned to the first batch")
	}
}

func TestBatchSIOFASPlacesReaderBeforeWriter(t *testing.T) {
	reader := batchSITestTx("T2", []string{"k"}, []string{"other"})
	writer := batchSITestTx("T1", nil, []string{"k"})
	descriptors, _, err := batchSIDescriptors([]tx.SignedTransaction{writer, reader}, nil, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	ordered, aborted, _ := batchSIOFASOrder(descriptors, BatchSIPriorityPaper)
	if len(aborted) != 0 {
		t.Fatalf("unexpected aborts: %+v", aborted)
	}
	if len(ordered) != 2 || ordered[0].TxID != reader.TxID || ordered[1].TxID != writer.TxID {
		t.Fatalf("OFAS did not place reader before writer: %+v", ordered)
	}
}

func TestBatchSIOFASCycleDefersTransactionInSingleFirstPass(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"k1"}, []string{"k3"}),
		batchSITestTx("T2", []string{"k2"}, []string{"k1"}),
		batchSITestTx("T3", []string{"k3"}, []string{"k2"}),
	}
	result, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deferred) < 1 {
		t.Fatalf("cyclic batch should defer at least one transaction")
	}
	if len(result.Ordered)+len(result.Deferred) != len(items) {
		t.Fatalf("planning lost transactions: accepted=%d deferred=%d", len(result.Ordered), len(result.Deferred))
	}
	if result.Plan.Metrics.PlanningIterationCount != 1 {
		t.Fatalf("Batch-SI must execute one AWRT/WRBP/OFAS pass per proposal, got %d", result.Plan.Metrics.PlanningIterationCount)
	}
	if result.Plan.Metrics.FirstPassOFASAbortedTransactionCount != len(result.Deferred) {
		t.Fatalf("first-pass abort evidence mismatch: metrics=%d deferred=%d", result.Plan.Metrics.FirstPassOFASAbortedTransactionCount, len(result.Deferred))
	}
	acceptedBlock := batchSITestBlock(result.Ordered...)
	acceptedBlock.Height = result.Plan.BlockHeight
	if err := VerifyBatchSIPlan(acceptedBlock, result.Plan, DefaultBatchSIConfig()); err != nil {
		t.Fatalf("fixed first-pass plan should verify without accepted-set replanning: %v", err)
	}
}

func TestBatchSIFixedPlanVerificationRejectsCandidateTamper(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"k1"}, []string{"k3"}),
		batchSITestTx("T2", []string{"k2"}, []string{"k1"}),
		batchSITestTx("T3", []string{"k3"}, []string{"k2"}),
	}
	result, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	acceptedBlock := batchSITestBlock(result.Ordered...)
	plan := result.Plan
	plan.CandidateTransactionIDs = append([]string(nil), plan.CandidateTransactionIDs...)
	plan.CandidateTransactionIDs[0] = "tampered"
	if err := VerifyBatchSIPlan(acceptedBlock, plan, DefaultBatchSIConfig()); err == nil {
		t.Fatal("tampered candidate evidence must not verify")
	}
}

func TestBatchSIPaperTransactionIDUsesConsensusBlockOrdinal(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("hash-z", []string{"a"}, []string{"b"}),
		batchSITestTx("hash-a", []string{"c"}, []string{"a"}),
		batchSITestTx("hash-m", nil, []string{"c"}),
	}
	left, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left.Plan.TransactionOrdinals, map[string]int{"hash-z": 1, "hash-a": 2, "hash-m": 3}) {
		t.Fatalf("paper T.id must follow ordering input, not TxID lexical order: %#v", left.Plan.TransactionOrdinals)
	}
	reversed := []tx.SignedTransaction{items[2], items[1], items[0]}
	right, err := BuildBatchSIPlan(batchSITestBlock(reversed...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(right.Plan.TransactionOrdinals, map[string]int{"hash-m": 1, "hash-a": 2, "hash-z": 3}) {
		t.Fatalf("reversed consensus input must carry reversed paper ordinals: %#v", right.Plan.TransactionOrdinals)
	}
	if left.Plan.PlanDigest == right.Plan.PlanDigest {
		t.Fatalf("consensus order changed but Batch-SI plan digest did not: %s", left.Plan.PlanDigest)
	}
}

func TestBatchSIExplicitOrdinalsSurviveAcceptedSetReordering(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T3", []string{"a"}, []string{"b"}),
		batchSITestTx("T1", []string{"c"}, []string{"a"}),
		batchSITestTx("T2", nil, []string{"c"}),
	}
	ordinals := map[string]int{"T3": 30, "T1": 10, "T2": 20}
	planned, err := BuildBatchSIPlanWithOrdinals(batchSITestBlock(items...), DefaultBatchSIConfig(), ordinals)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(planned.Plan.TransactionOrdinals, ordinals) {
		t.Fatalf("explicit ordering-node T.id mapping drifted: got=%#v want=%#v", planned.Plan.TransactionOrdinals, ordinals)
	}
	acceptedBlock := batchSITestBlock(planned.Ordered...)
	acceptedOrdinals := map[string]int{}
	for _, item := range planned.Ordered {
		acceptedOrdinals[item.TxID] = ordinals[item.TxID]
	}
	accepted, err := BuildBatchSIPlanWithOrdinals(acceptedBlock, DefaultBatchSIConfig(), acceptedOrdinals)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyBatchSIPlan(acceptedBlock, accepted.Plan, DefaultBatchSIConfig()); err != nil {
		t.Fatalf("consensus-bound ordinal plan should verify: %v", err)
	}
}

func TestBatchSISerializationOrderReplaysToSameFinalStateAsSerialOracle(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
		batchSITestTx("T3", []string{"a"}, []string{"c"}),
	}
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	ordinals := map[string]int{}
	for _, item := range planned.Ordered {
		ordinals[item.TxID] = planned.Plan.TransactionOrdinals[item.TxID]
	}
	accepted, err := BuildBatchSIPlanWithOrdinals(b, config, ordinals)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalBatchSIPlan(accepted.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{AlgorithmID: BatchSIPlanAlgorithmID, PayloadDigest: batchSITextDigest(string(raw)), PlanDigest: accepted.Plan.PlanDigest, Payload: raw}
	base := map[string]string{}
	batchResult, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, base)
	if err != nil {
		t.Fatal(err)
	}
	serialBlock := b
	serialBlock.ExecutionPlan = nil
	serialResult := NewSerialExecutor().ExecuteBlock(serialBlock, base)
	if batchResult.StateRootAfter != serialResult.StateRootAfter || batchResult.ReceiptRoot != serialResult.ReceiptRoot {
		t.Fatalf("Batch-SI serialization replay diverged from serial oracle: batch=%s/%s serial=%s/%s", batchResult.StateRootAfter, batchResult.ReceiptRoot, serialResult.StateRootAfter, serialResult.ReceiptRoot)
	}
}

func TestBatchSIWorkerCountsProduceSameStateRoot(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", nil, []string{"b"}),
		batchSITestTx("T3", []string{"a"}, []string{"c"}),
	}
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{AlgorithmID: BatchSIPlanAlgorithmID, PayloadDigest: batchSITextDigest(string(raw)), PlanDigest: planned.Plan.PlanDigest, Payload: raw}
	roots := map[string]bool{}
	for _, workers := range []int{1, 2, 4, 8} {
		cfg := config
		cfg.WorkerCount = workers
		result, err := NewBatchSIExecutor(cfg).ExecuteBlock(context.Background(), b, map[string]string{})
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		roots[result.StateRootAfter] = true
		if len(result.Receipts) != len(b.TxList) || len(result.TxDeltas) != len(b.TxList) {
			t.Fatalf("workers=%d incomplete results", workers)
		}
	}
	if len(roots) != 1 {
		t.Fatalf("worker count changed final state root: %v", roots)
	}
}

func TestBatchSIAblationsRemainDeterministic(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"a"}, []string{"b"}),
		batchSITestTx("T2", []string{"c"}, []string{"a"}),
		batchSITestTx("T3", nil, []string{"c"}),
	}
	configs := []BatchSIConfig{
		DefaultBatchSIConfig(),
		{WorkerCount: 4, PartitionMode: BatchSIPartitionSequential, OrderingMode: BatchSIOrderingOFAS, PriorityMode: BatchSIPriorityPaper, ExecutionMode: BatchSIExecutionSnapshotParallel},
		{WorkerCount: 4, PartitionMode: BatchSIPartitionWRBP, OrderingMode: BatchSIOrderingDependencyGraph, PriorityMode: BatchSIPriorityPaper, ExecutionMode: BatchSIExecutionSnapshotParallel},
		{WorkerCount: 4, PartitionMode: BatchSIPartitionWRBP, OrderingMode: BatchSIOrderingOFAS, PriorityMode: BatchSIPriorityTxID, ExecutionMode: BatchSIExecutionSnapshotParallel},
		{WorkerCount: 4, PartitionMode: BatchSIPartitionWRBP, OrderingMode: BatchSIOrderingOFAS, PriorityMode: BatchSIPriorityPaper, ExecutionMode: BatchSIExecutionSnapshotSerial},
	}
	for _, config := range configs {
		first, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
		if err != nil {
			t.Fatalf("config=%+v: %v", config, err)
		}
		second, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
		if err != nil {
			t.Fatalf("config=%+v: %v", config, err)
		}
		if first.Plan.PlanDigest != second.Plan.PlanDigest {
			t.Fatalf("config=%+v is nondeterministic", config)
		}
	}
}

func TestBatchSIRejectsInvalidPrivateConfiguration(t *testing.T) {
	invalidWorkers := DefaultBatchSIConfig()
	invalidWorkers.WorkerCount = 9
	if _, err := BuildBatchSIPlan(batchSITestBlock(batchSITestTx("T1", nil, []string{"k"})), invalidWorkers); err == nil {
		t.Fatal("worker_count greater than eight must be rejected")
	}
	invalidMode := DefaultBatchSIConfig()
	invalidMode.OrderingMode = "other_scheme"
	if _, err := BuildBatchSIPlan(batchSITestBlock(batchSITestTx("T1", nil, []string{"k"})), invalidMode); err == nil {
		t.Fatal("unknown ordering mode must be rejected instead of silently falling back")
	}
}

func TestBatchSISameBatchReadersUseCommonImmutableSnapshot(t *testing.T) {
	reader := batchSITestTx("T1", []string{"k"}, nil)
	writer := batchSITestTx("T2", nil, []string{"k"})
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(reader, writer), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Plan.Batches) != 1 {
		t.Fatalf("reader and sole writer should share the first batch: %#v", planned.Plan.Batches)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{AlgorithmID: BatchSIPlanAlgorithmID, PayloadDigest: batchSITextDigest(string(raw)), PlanDigest: planned.Plan.PlanDigest, Payload: raw}
	result, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, map[string]string{"shard-0::k": "old"})
	if err != nil {
		t.Fatal(err)
	}
	var readerDelta TxDelta
	for _, delta := range result.TxDeltas {
		if delta.TxID == reader.TxID {
			readerDelta = delta
		}
	}
	if len(readerDelta.ReadSet) != 1 || readerDelta.ReadSet[0].Value != "old" || readerDelta.ReadSet[0].Source != "batch_si_batch_snapshot" {
		t.Fatalf("reader did not observe the common immutable batch snapshot: %#v", readerDelta.ReadSet)
	}
}

func TestBatchSINextBatchReadsPreviousBatchCommittedState(t *testing.T) {
	first := batchSITestTx("T1", nil, []string{"k"})
	second := batchSITestTx("T2", []string{"k"}, []string{"k"})
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(first, second), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Plan.Batches) != 2 {
		t.Fatalf("two writers for one key must be placed in sequential batches: %#v", planned.Plan.Batches)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{AlgorithmID: BatchSIPlanAlgorithmID, PayloadDigest: batchSITextDigest(string(raw)), PlanDigest: planned.Plan.PlanDigest, Payload: raw}
	result, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, map[string]string{"shard-0::k": "initial"})
	if err != nil {
		t.Fatal(err)
	}
	var firstWrite string
	var secondReads []ReadObservation
	for _, delta := range result.TxDeltas {
		switch delta.TxID {
		case first.TxID:
			firstWrite = delta.WriteSet["k"]
		case second.TxID:
			secondReads = delta.ReadSet
		}
	}
	if firstWrite == "" {
		t.Fatal("first batch did not produce its declared write")
	}
	found := false
	for _, read := range secondReads {
		if read.Key == "k" && read.Value == firstWrite {
			found = true
		}
	}
	if !found {
		t.Fatalf("second batch did not read the previous batch committed value %q: %#v", firstWrite, secondReads)
	}
}

func TestBatchSIProducesNoUndeclaredWrites(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", nil, []string{"a"}),
		batchSITestTx("T2", []string{"a"}, []string{"b"}),
	}
	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{AlgorithmID: BatchSIPlanAlgorithmID, PayloadDigest: batchSITextDigest(string(raw)), PlanDigest: planned.Plan.PlanDigest, Payload: raw}
	result, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]tx.SignedTransaction{}
	for _, item := range b.TxList {
		byID[item.TxID] = item
	}
	for _, delta := range result.TxDeltas {
		if err := batchSIValidateDeclaredWrites(byID[delta.TxID], delta.WriteSet, "shard-0"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBatchSIOFASMatchesPaperWorkedExample(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("T1", []string{"k1", "k6"}, []string{"k2"}),
		batchSITestTx("T2", []string{"k3", "k5"}, []string{"k1"}),
		batchSITestTx("T3", []string{"k3"}, []string{"k3"}),
		batchSITestTx("T4", []string{"k4"}, []string{"k5", "k6"}),
		batchSITestTx("T5", []string{"k2"}, []string{"k4"}),
	}
	descriptors, _, err := batchSIDescriptors(items, nil, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	ordered, aborted, edges := batchSIOFASOrder(descriptors, BatchSIPriorityPaper)
	if edges != 6 {
		t.Fatalf("paper example should contain six cross-transaction reader-to-writer dependencies, got %d", edges)
	}
	if len(aborted) != 1 || aborted[0].TxID != "T4" {
		t.Fatalf("paper example should abort only T4, got %+v", aborted)
	}
	ids := make([]string, 0, len(ordered))
	for _, item := range ordered {
		ids = append(ids, item.TxID)
	}
	expected := []string{"T5", "T1", "T2", "T3"}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("paper example order mismatch: got %v want %v", ids, expected)
	}
}

func TestBatchSIUsesDeclaredLogicalAccountAliasesForSignedSyntheticTransfer(t *testing.T) {
	logicalSender := "client_s0_6"
	receiver := "receiver_s0"
	generated, _, _, err := tx.Generate(tx.GenerateOptions{
		Count:      1,
		Sender:     logicalSender,
		Receiver:   receiver,
		StartNonce: 0,
		Value:      1,
		StateKeys:  []string{"shard:s0:account", "asset:6"},
		AccessList: tx.DefaultTransferAccessList(logicalSender, receiver),
		Seed:       "batch-si-logical-account-alias",
	})
	if err != nil {
		t.Fatal(err)
	}
	item := generated[0]
	item.Payload = "v5_safe"
	if item.Sender == logicalSender {
		t.Fatalf("fixture did not reproduce the signed-address/logical-alias split")
	}

	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(item), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{
		AlgorithmID:   BatchSIPlanAlgorithmID,
		PayloadDigest: batchSITextDigest(string(raw)),
		PlanDigest:    planned.Plan.PlanDigest,
		Payload:       raw,
	}
	result, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TxDeltas) != 1 {
		t.Fatalf("expected one delta, got %d", len(result.TxDeltas))
	}
	writes := result.TxDeltas[0].WriteSet
	for _, key := range []string{
		"balance:" + logicalSender,
		"nonce:" + logicalSender,
		"balance:" + receiver,
	} {
		if _, ok := writes[key]; !ok {
			t.Fatalf("missing declared logical write %s: %#v", key, writes)
		}
	}
	if _, ok := writes["balance:"+item.Sender]; ok {
		t.Fatalf("physical signing address leaked into state writes: %#v", writes)
	}
	if _, ok := writes["nonce:"+receiver]; ok {
		t.Fatalf("read-only receiver nonce was materialized as a write: %#v", writes)
	}
	if err := batchSIValidateDeclaredWrites(item, writes, "shard-0"); err != nil {
		t.Fatalf("resolved transfer wrote outside the declared access list: %v", err)
	}
}

func TestBatchSITransferAccountResolutionPrefersExactDeclaredAddresses(t *testing.T) {
	item := tx.SignedTransaction{
		Sender:   "0xsender",
		Receiver: "0xreceiver",
		AccessList: tx.DefaultTransferAccessList(
			"0xsender",
			"0xreceiver",
		),
	}
	sender, receiver := batchSITransferAccounts(item)
	if sender != item.Sender || receiver != item.Receiver {
		t.Fatalf("exact declared accounts changed: sender=%s receiver=%s", sender, receiver)
	}
}

func TestBatchSITransferAccountResolutionDoesNotConfuseWritableReceiverNonce(t *testing.T) {
	accessList := tx.DefaultTransferAccessList("z_sender", "a_receiver")
	for index := range accessList {
		if accessList[index].Key == "nonce:a_receiver" {
			accessList[index].Mode = tx.AccessReadWrite
		}
	}
	item := tx.SignedTransaction{
		Sender:     "0xphysical",
		Receiver:   "a_receiver",
		AccessList: accessList,
	}
	sender, receiver := batchSITransferAccounts(item)
	if sender != "z_sender" || receiver != "a_receiver" {
		t.Fatalf("writable receiver nonce confused account roles: sender=%s receiver=%s", sender, receiver)
	}
}

func TestBatchSILegacyStateKeysAugmentTransferAccountAccesses(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:             "T-legacy-transfer",
		Sender:           "0xlegacy-sender",
		Receiver:         "receiver_legacy",
		Nonce:            0,
		Value:            1,
		StateKeys:        []string{"legacy:asset:1"},
		AccessList:       []tx.AccessItem{{Key: "legacy:asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "dataset_state_key"}},
		AccessListSchema: "legacy_access_inference_v1",
		AccessListSource: "legacy_state_keys",
		Payload:          "legacy_transfer",
	}
	reads, writes := batchSIDeclaredAccess(item, "shard-0")
	readSet := batchSIStringSet(reads)
	writeSet := batchSIStringSet(writes)
	for _, key := range []string{
		"balance:" + item.Sender,
		"nonce:" + item.Sender,
		"balance:" + item.Receiver,
		"nonce:" + item.Receiver,
	} {
		if !readSet[key] || !writeSet[key] {
			t.Fatalf("legacy transfer account key was not added to effective access set: %s", key)
		}
	}

	config := DefaultBatchSIConfig()
	planned, err := BuildBatchSIPlan(batchSITestBlock(item), config)
	if err != nil {
		t.Fatal(err)
	}
	b := batchSITestBlock(planned.Ordered...)
	raw, err := MarshalBatchSIPlan(planned.Plan)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlan = &block.ExecutionPlanEnvelope{
		AlgorithmID:   BatchSIPlanAlgorithmID,
		PayloadDigest: batchSITextDigest(string(raw)),
		PlanDigest:    planned.Plan.PlanDigest,
		Payload:       raw,
	}
	result, err := NewBatchSIExecutor(config).ExecuteBlock(context.Background(), b, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TxDeltas) != 1 {
		t.Fatalf("expected one legacy transfer delta, got %d", len(result.TxDeltas))
	}
	if err := batchSIValidateDeclaredWrites(item, result.TxDeltas[0].WriteSet, "shard-0"); err != nil {
		t.Fatalf("legacy transfer wrote outside its effective Batch-SI access set: %v", err)
	}
}

func TestBatchSICrossShardTargetCommitDeclaresProtocolSystemWrite(t *testing.T) {
	item := batchSITestTx("cross-target", []string{"k"}, []string{"k"})
	item.Payload = "v5_cross:shard-1"
	block := batchSITestBlock(item)
	block.ShardID = "shard-1"
	planned, err := BuildBatchSIPlan(block, DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Ordered) != 1 {
		t.Fatalf("target commit should remain admitted: %+v", planned)
	}
	_, writes := batchSIDeclaredAccess(item, "shard-1")
	found := false
	for _, key := range writes {
		if key == "relay_commit:"+item.TxID {
			found = true
		}
	}
	if !found {
		t.Fatalf("target shard did not declare relay protocol write: %v", writes)
	}
	if err := batchSIValidateDeclaredWrites(item, map[string]string{"relay_commit:" + item.TxID: "1"}, "shard-1"); err != nil {
		t.Fatal(err)
	}
	if err := batchSIValidateDeclaredWrites(item, map[string]string{"unexpected_system_key": "1"}, "shard-1"); err == nil {
		t.Fatalf("arbitrary undeclared writes must remain rejected")
	}
}

func TestBatchSICrossShardSourceDoesNotOverdeclareRelayCommit(t *testing.T) {
	item := batchSITestTx("cross-source", []string{"k"}, []string{"k"})
	item.Payload = "v5_cross:shard-1"
	_, writes := batchSIDeclaredAccess(item, "shard-0")
	for _, key := range writes {
		if key == "relay_commit:"+item.TxID {
			t.Fatalf("source shard overdeclared target-only system key")
		}
	}
}

func TestBatchSIOrderEvidenceBindsOrdinalBatchAndSerializationPosition(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSITestTx("z", nil, []string{"a"}),
		batchSITestTx("a", nil, []string{"b"}),
		batchSITestTx("m", []string{"a"}, []string{"c"}),
	}
	planned, err := BuildBatchSIPlan(batchSITestBlock(items...), DefaultBatchSIConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Plan.OrderEvidence) != len(planned.Ordered) {
		t.Fatalf("missing order evidence: %+v", planned.Plan.OrderEvidence)
	}
	seenPositions := map[int]bool{}
	for _, row := range planned.Plan.OrderEvidence {
		if row.TransactionOrdinal != planned.Plan.TransactionOrdinals[row.TxID] {
			t.Fatalf("ordinal evidence drift: %+v", row)
		}
		if row.BatchNumber < 1 || row.SerializationPosition < 1 {
			t.Fatalf("invalid order evidence: %+v", row)
		}
		if seenPositions[row.SerializationPosition] {
			t.Fatalf("duplicate serialization position %d", row.SerializationPosition)
		}
		seenPositions[row.SerializationPosition] = true
	}
	clone := planned.Plan
	clone.OrderEvidence[0].SerializationPosition++
	if batchSIPlanDigest(clone) == planned.Plan.PlanDigest {
		t.Fatalf("plan digest must bind order evidence")
	}
}
