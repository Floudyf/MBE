package execution

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestAriaExecutorIndependentTransactionsCommitInOneEpoch(t *testing.T) {
	items := append(
		mustGenerateForExecutionTest(t, "aria-independent-a", 1, "alice", "bob", 10, "v5_safe"),
		mustGenerateForExecutionTest(t, "aria-independent-b", 1, "carol", "dave", 10, "v5_safe")...,
	)
	b := blockForExecutionTest(items)
	executor := NewAriaExecutor(2)
	result, err := executor.ExecuteBlock(context.Background(), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if executor.Metrics.EpochCount != 1 || executor.Metrics.ConflictAbortCount != 0 {
		t.Fatalf("unexpected Aria metrics: %+v", executor.Metrics)
	}
	if result.SuccessfulTxs != len(items) || result.FailedTxs != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := result.Plan.OrderedTransactionIDs; !reflect.DeepEqual(got, []string{items[0].TxID, items[1].TxID}) {
		t.Fatalf("unexpected commit order: %#v", got)
	}
}

func TestAriaExecutorRetriesSameSenderNonceChainAcrossEpochs(t *testing.T) {
	items := mustGenerateForExecutionTest(t, "aria-nonce-chain", 4, "alice", "bob", 10, "v5_safe")
	b := blockForExecutionTest(items)
	executor := NewAriaExecutor(4)
	result, err := executor.ExecuteBlock(context.Background(), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	serial := NewSerialExecutor().ExecuteBlock(b, nil)
	if result.StateRootAfter != serial.StateRootAfter || result.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("nonce-chain result diverged from ordered execution: aria=%s/%s serial=%s/%s", result.StateRootAfter, result.ReceiptRoot, serial.StateRootAfter, serial.ReceiptRoot)
	}
	if executor.Metrics.EpochCount != len(items) {
		t.Fatalf("expected one nonce to advance per epoch, metrics=%+v", executor.Metrics)
	}
	if executor.Metrics.RetryableNonceCount == 0 || executor.Metrics.ReexecutionCount == 0 {
		t.Fatalf("missing nonce retry evidence: %+v", executor.Metrics)
	}
}

func TestAriaRuleOneAndRuleTwoMatchOfficialDependencyConditions(t *testing.T) {
	attempts := []ariaAttempt{
		{
			Index: 0,
			Delta: TxDelta{
				ReadSet:  []ReadObservation{{Key: "b"}},
				WriteSet: map[string]string{"a": "1"},
			},
		},
		{
			Index: 1,
			Delta: TxDelta{
				ReadSet:  []ReadObservation{{Key: "a"}},
				WriteSet: map[string]string{"b": "1"},
			},
		},
	}
	metrics := AriaMetrics{}
	reservations := buildAriaReservations(attempts, &metrics)
	conflict := analyzeAriaConflict(attempts[1], 2, reservations)
	if !conflict.RAW || !conflict.WAR || conflict.WAW {
		t.Fatalf("expected RAW+WAR without WAW: %+v", conflict)
	}
	ruleOneCommit := !conflict.WAW && !conflict.RAW
	ruleTwoCommit := !conflict.WAW && !(conflict.WAR && conflict.RAW)
	if ruleOneCommit || ruleTwoCommit {
		t.Fatalf("RAW+WAR cycle must abort under both rules: rule1=%t rule2=%t", ruleOneCommit, ruleTwoCommit)
	}

	rawOnly := []ariaAttempt{
		{Index: 0, Delta: TxDelta{WriteSet: map[string]string{"a": "1"}}},
		{Index: 1, Delta: TxDelta{ReadSet: []ReadObservation{{Key: "a"}}, WriteSet: map[string]string{"c": "1"}}},
	}
	reservations = buildAriaReservations(rawOnly, &metrics)
	conflict = analyzeAriaConflict(rawOnly[1], 2, reservations)
	if !conflict.RAW || conflict.WAR || conflict.WAW {
		t.Fatalf("expected RAW-only dependency: %+v", conflict)
	}
	if !(!conflict.WAW && !(conflict.WAR && conflict.RAW)) {
		t.Fatal("official deterministic reordering rule should accept RAW-only transaction")
	}
	if !conflict.RAW {
		t.Fatal("basic Rule 1 should reject RAW-only transaction")
	}
}

func TestAriaExecutorDeterministicAcrossWorkerCounts(t *testing.T) {
	items := append(
		mustGenerateForExecutionTest(t, "aria-workers-a", 3, "alice", "bob", 10, "v5_safe"),
		mustGenerateForExecutionTest(t, "aria-workers-b", 2, "carol", "dave", 11, "v5_safe")...,
	)
	b := blockForExecutionTest(items)
	var baselineState, baselineReceipt, baselinePlan string
	for _, workers := range []int{1, 2, 4, 8} {
		executor := NewAriaExecutor(workers)
		result, err := executor.ExecuteBlock(context.Background(), b, nil)
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if baselineState == "" {
			baselineState, baselineReceipt, baselinePlan = result.StateRootAfter, result.ReceiptRoot, result.PlanDigest
			continue
		}
		if result.StateRootAfter != baselineState || result.ReceiptRoot != baselineReceipt {
			t.Fatalf("workers=%d changed logical result", workers)
		}
		// Worker count is deliberately bound into the plan digest as execution
		// configuration evidence, so only same-worker repeats must match it.
		repeat, err := executor.ExecuteBlock(context.Background(), b, nil)
		if err != nil || repeat.PlanDigest != result.PlanDigest {
			t.Fatalf("workers=%d plan is not repeatable: err=%v first=%s repeat=%s", workers, err, result.PlanDigest, repeat.PlanDigest)
		}
	}
	if baselinePlan == "" {
		t.Fatal("missing baseline plan")
	}
}

func TestAriaExecutorRuleOneMatchesSerialOrder(t *testing.T) {
	items := append(
		mustGenerateForExecutionTest(t, "aria-rule1-a", 2, "alice", "bob", 10, "v5_safe"),
		mustGenerateForExecutionTest(t, "aria-rule1-b", 1, "carol", "dave", 10, "v5_safe")...,
	)
	b := blockForExecutionTest(items)
	executor := NewAriaExecutor(3)
	executor.Reordering = false
	result, err := executor.ExecuteBlock(context.Background(), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	serial := NewSerialExecutor().ExecuteBlock(b, nil)
	if result.StateRootAfter != serial.StateRootAfter {
		t.Fatalf("Rule 1 final state diverged from the serializable reference: aria=%s serial=%s", result.StateRootAfter, serial.StateRootAfter)
	}
	if result.SerialEquivalent {
		t.Fatal("Aria may defer an earlier transaction behind an independent later transaction and must not advertise exact input-order equivalence")
	}
}

func TestAriaExecutorRejectsUnresolvableFutureNonce(t *testing.T) {
	items := withNonce(mustGenerateForExecutionTest(t, "aria-missing-nonce", 1, "alice", "bob", 10, "v5_safe"), 2)
	b := blockForExecutionTest(items)
	_, err := NewAriaExecutor(1).ExecuteBlock(context.Background(), b, nil)
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("expected no-progress rejection, got %v", err)
	}
}

func TestAriaReservationsSelectMinimumTID(t *testing.T) {
	attempts := []ariaAttempt{
		{Index: 0, Tx: tx.SignedTransaction{TxID: "t0"}, Delta: TxDelta{ReadSet: []ReadObservation{{Key: "r"}}, WriteSet: map[string]string{"w": "0"}}},
		{Index: 1, Tx: tx.SignedTransaction{TxID: "t1"}, Delta: TxDelta{ReadSet: []ReadObservation{{Key: "r"}}, WriteSet: map[string]string{"w": "1"}}},
	}
	metrics := AriaMetrics{}
	table := buildAriaReservations(attempts, &metrics)
	if table.MinReader["r"] != 1 || table.MinWriter["w"] != 1 {
		t.Fatalf("reservation table did not retain minimum TID: %+v", table)
	}
	if metrics.ReadReservationCount != 2 || metrics.WriteReservationCount != 2 {
		t.Fatalf("reservation metrics drifted: %+v", metrics)
	}
}

func TestAriaCommitOrderMaterializesAValidSerialExecution(t *testing.T) {
	items := append(
		mustGenerateForExecutionTest(t, "aria-serializable-a", 3, "alice", "hot", 7, "v5_safe"),
		mustGenerateForExecutionTest(t, "aria-serializable-b", 2, "bob", "hot", 9, "v5_safe")...,
	)
	items = append(items, mustGenerateForExecutionTest(t, "aria-serializable-c", 2, "carol", "dave", 11, "v5_safe")...)
	b := blockForExecutionTest(items)
	for _, reordering := range []bool{false, true} {
		executor := NewAriaExecutor(4)
		executor.Reordering = reordering
		result, err := executor.ExecuteBlock(context.Background(), b, nil)
		if err != nil {
			t.Fatalf("reordering=%t: %v", reordering, err)
		}
		ordered := make([]tx.SignedTransaction, 0, len(items))
		for _, index := range result.Plan.OriginalTransactionIdxs {
			ordered = append(ordered, items[index])
		}
		serial := NewSerialExecutor().ExecuteBlock(blockForExecutionTest(ordered), nil)
		if result.StateRootAfter != serial.StateRootAfter {
			t.Fatalf("reordering=%t commit order is not a valid serial materialization: aria=%s serial=%s order=%v", reordering, result.StateRootAfter, serial.StateRootAfter, result.Plan.OriginalTransactionIdxs)
		}
	}
}

func TestAriaCandidateSelectionDefersNonceChainInOriginalOrder(t *testing.T) {
	items := mustGenerateForExecutionTest(t, "aria-candidate-nonce", 4, "alice", "bob", 10, "v5_safe")
	executor := NewAriaExecutor(4)
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, items, map[string]string{}, len(items))
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 1 || selection.Selected[0].TxID != items[0].TxID {
		t.Fatalf("one-batch Aria should select only the current nonce, got %#v", selection.Selected)
	}
	if len(selection.Deferred) != len(items)-1 {
		t.Fatalf("expected remaining nonce chain to be deferred, got %#v", selection.Deferred)
	}
	for index, item := range selection.Deferred {
		if item.TxID != items[index+1].TxID {
			t.Fatalf("deferred relative order changed at %d: got=%s want=%s", index, item.TxID, items[index+1].TxID)
		}
		if !strings.Contains(selection.DeferredReasons[item.TxID], "aria_nonce_gap") {
			t.Fatalf("missing nonce-gap reason for %s: %#v", item.TxID, selection.DeferredReasons)
		}
	}
	if selection.Metrics.BatchLifecycle != "one_consensus_block_per_aria_batch" || selection.Metrics.FallbackMode != "disabled" {
		t.Fatalf("candidate lifecycle evidence drifted: %+v", selection.Metrics)
	}
}

func TestAriaFixedBlockValidationRejectsHiddenSecondEpoch(t *testing.T) {
	items := mustGenerateForExecutionTest(t, "aria-fixed-block", 2, "alice", "bob", 10, "v5_safe")
	b := blockForExecutionTest(items)
	executor := NewAriaExecutor(2)
	executor.MaximumEpochs = 1
	_, err := executor.ExecuteBlock(context.Background(), b, map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "maximum epochs") {
		t.Fatalf("fixed Aria block must reject transactions requiring a hidden next epoch, got %v", err)
	}
}

func TestAriaCandidateSelectionMaterializesRecomputedPrivateDeltas(t *testing.T) {
	items := append(
		mustGenerateForExecutionTest(t, "aria-materialize-a", 1, "alice", "bob", 10, "v5_safe"),
		mustGenerateForExecutionTest(t, "aria-materialize-b", 1, "carol", "dave", 11, "v5_safe")...,
	)
	executor := NewAriaExecutor(2)
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, items, map[string]string{}, len(items))
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != len(items) || len(selection.SelectedDeltas) != len(items) {
		t.Fatalf("incomplete candidate selection: %#v", selection)
	}
	b := blockForExecutionTest(selection.Selected)
	result, err := executor.MaterializeCandidateSelection(b, map[string]string{}, selection)
	if err != nil {
		t.Fatal(err)
	}
	serial := NewSerialExecutor().ExecuteBlock(b, map[string]string{})
	if result.StateRootAfter != serial.StateRootAfter || result.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("materialized private deltas diverged: aria=%s/%s serial=%s/%s", result.StateRootAfter, result.ReceiptRoot, serial.StateRootAfter, serial.ReceiptRoot)
	}
	if result.ExecutorVersion != AriaBlockExecutorVersion || result.PlanDigest == "" {
		t.Fatalf("missing materialization provenance: %#v", result)
	}
}
