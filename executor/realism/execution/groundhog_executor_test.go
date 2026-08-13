package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func groundhogTransfer(id, sender, receiver string, nonce uint64, value int64) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID: id, Sender: sender, Receiver: receiver, Nonce: nonce, Value: value,
		StateKeys:  []string{"acct:" + sender, "acct:" + receiver},
		AccessList: tx.DefaultTransferAccessList(sender, receiver),
		Payload:    "groundhog-test:" + id,
	}
}

func TestGroundhogCandidateSelectionDefersAggregateOverspend(t *testing.T) {
	executor := NewGroundhogExecutor(4)
	base := map[string]string{"s0::balance:alice": "10"}
	items := []tx.SignedTransaction{
		groundhogTransfer("tx-1", "alice", "bob", 0, 7),
		groundhogTransfer("tx-2", "alice", "carol", 1, 6),
	}
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, items, base, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 1 || len(selection.Deferred) != 1 {
		t.Fatalf("aggregate overspend must select exactly one candidate and defer one: %+v", selection)
	}
	// Groundhog proposal assembly is allowed to abort a conflicting candidate
	// nondeterministically. The invariant is the aggregate reservation budget,
	// not which same-snapshot withdrawal wins the proposer race.
	selectedID := selection.Selected[0].TxID
	deferredID := selection.Deferred[0].TxID
	if selectedID == deferredID ||
		(selectedID != "tx-1" && selectedID != "tx-2") ||
		(deferredID != "tx-1" && deferredID != "tx-2") {
		t.Fatalf("unexpected proposal candidate identities: selected=%q deferred=%q selection=%+v", selectedID, deferredID, selection)
	}
	if !strings.Contains(selection.DeferredReasons[deferredID], "nonnegative_constraint") {
		t.Fatalf("deferred candidate must carry nonnegative reservation conflict evidence: %+v", selection)
	}
	if selection.Metrics.ConstraintConflictCount != 1 || selection.Metrics.ReservationRollbackCount != 1 {
		t.Fatalf("missing reservation conflict evidence: %+v", selection.Metrics)
	}
}

func TestGroundhogFixedBlockRejectsConflictWithoutPartialState(t *testing.T) {
	executor := NewGroundhogExecutor(4)
	base := map[string]string{"s0::balance:alice": "10"}
	b := block.Block{BlockHash: "b1", ShardID: "s0", Height: 1, TxList: []tx.SignedTransaction{
		groundhogTransfer("tx-1", "alice", "bob", 0, 7),
		groundhogTransfer("tx-2", "alice", "carol", 1, 6),
	}}
	_, err := executor.ExecuteBlock(context.Background(), b, base)
	var conflict GroundhogBlockConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected GroundhogBlockConflictError, got %v", err)
	}
	if conflict.TxID != "tx-2" || conflict.Key != "balance:alice" {
		t.Fatalf("unexpected conflict: %+v", conflict)
	}
	if base["s0::balance:alice"] != "10" || len(base) != 1 {
		t.Fatalf("base snapshot was mutated: %v", base)
	}
}

func TestGroundhogCommutativeDeltasMergeFromOneSnapshot(t *testing.T) {
	executor := NewGroundhogExecutor(2)
	items := []tx.SignedTransaction{
		{TxID: "tx-1", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 2}}},
		{TxID: "tx-2", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 3}}},
	}
	b := block.Block{BlockHash: "b1", ShardID: "s0", Height: 1, TxList: items}
	result, err := executor.ExecuteBlock(context.Background(), b, map[string]string{"s0::hot": "10"})
	if err != nil {
		t.Fatal(err)
	}
	if result.StateUpdates["s0::hot"] != "15" {
		t.Fatalf("unexpected merged value: %v", result.StateUpdates)
	}
	if len(result.Receipts) != 2 || !result.Receipts[0].Success || !result.Receipts[1].Success {
		t.Fatalf("unexpected receipts: %+v", result.Receipts)
	}
	for _, delta := range result.TxDeltas {
		if len(delta.ReadSet) != 1 || delta.ReadSet[0].Value != "10" || delta.ReadSet[0].Source != "groundhog_block_start_snapshot" {
			t.Fatalf("transaction did not read the common block snapshot: %+v", delta.ReadSet)
		}
	}
	if executor.Metrics.IntegerMergeCount != 2 || executor.Metrics.ModifiedKeyCount != 1 {
		t.Fatalf("unexpected metrics: %+v", executor.Metrics)
	}
}

func TestGroundhogDifferentBytesWritesConflict(t *testing.T) {
	executor := NewGroundhogExecutor(2)
	write := []tx.AccessItem{{Key: "object:hot", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	items := []tx.SignedTransaction{
		{TxID: "tx-1", Sender: "a", Receiver: "x", Value: 1, AccessList: write, Payload: "one"},
		{TxID: "tx-2", Sender: "b", Receiver: "y", Value: 1, AccessList: write, Payload: "two"},
	}
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, items, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 1 || len(selection.Deferred) != 1 {
		t.Fatalf("different bytes writes should not coexist: %+v", selection)
	}
}

func TestGroundhogTerminalFailureIsIncludedAsFailedNoOp(t *testing.T) {
	executor := NewGroundhogExecutor(2)
	invalid := groundhogTransfer("tx-bad", "alice", "bob", 0, 20)
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, []tx.SignedTransaction{invalid}, map[string]string{"s0::balance:alice": "5"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 1 || len(selection.Deferred) != 0 {
		t.Fatalf("terminal application failure must be selected for a final receipt: %+v", selection)
	}
	b := block.Block{BlockHash: "b1", ShardID: "s0", Height: 1, TxList: selection.Selected}
	result, err := executor.ExecuteBlock(context.Background(), b, map[string]string{"s0::balance:alice": "5"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Success || result.Receipts[0].Error != "insufficient_balance" {
		t.Fatalf("unexpected terminal receipt: %+v", result.Receipts)
	}
	if result.StateUpdates["s0::balance:alice"] != "5" || len(result.StateDelta) != 0 {
		t.Fatalf("failed transaction produced state: updates=%v delta=%v", result.StateUpdates, result.StateDelta)
	}
}

func TestGroundhogWorkerCountsAreDeterministic(t *testing.T) {
	items := []tx.SignedTransaction{
		groundhogTransfer("tx-1", "a", "hot", 0, 2),
		groundhogTransfer("tx-2", "b", "hot", 0, 3),
		groundhogTransfer("tx-3", "c", "hot", 0, 4),
		groundhogTransfer("tx-4", "d", "hot", 0, 5),
	}
	b := block.Block{BlockHash: "b1", ShardID: "s0", Height: 1, TxList: items}
	var root, receiptRoot, planDigest string
	for _, workers := range []int{1, 2, 4, 8} {
		executor := NewGroundhogExecutor(workers)
		result, err := executor.ExecuteBlock(context.Background(), b, nil)
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if root == "" {
			root, receiptRoot, planDigest = result.StateRootAfter, result.ReceiptRoot, result.PlanDigest
			continue
		}
		if result.StateRootAfter != root || result.ReceiptRoot != receiptRoot {
			t.Fatalf("worker-dependent result workers=%d result=%+v", workers, result)
		}
		// The plan records worker count as execution evidence, so its digest is
		// expected to differ. State and receipts must remain identical.
		if result.PlanDigest == "" || planDigest == "" {
			t.Fatal("plan digest missing")
		}
	}
}

func TestGroundhogSameBlockCreditsDoNotFundWithdrawals(t *testing.T) {
	executor := NewGroundhogExecutor(3)
	items := []tx.SignedTransaction{
		{TxID: "credit", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 10}}},
		{TxID: "debit-1", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: -7}}},
		{TxID: "debit-2", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: -7}}},
	}
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, items, map[string]string{"s0::hot": "10"}, len(items))
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 2 || len(selection.Deferred) != 1 {
		t.Fatalf("same-block credits must not increase the withdrawal budget: %+v", selection)
	}
	selected := map[string]bool{}
	for _, item := range selection.Selected {
		selected[item.TxID] = true
	}
	deferredID := selection.Deferred[0].TxID
	// A proposer may drop either conflicting debit under parallel reservation.
	// The credit must be accepted, and exactly one debit may spend the
	// block-start withdrawal budget.
	if !selected["credit"] ||
		(deferredID != "debit-1" && deferredID != "debit-2") ||
		selected["debit-1"] == selected["debit-2"] ||
		selected[deferredID] {
		t.Fatalf("same-block credits changed withdrawal-budget semantics: %+v", selection)
	}
	if !strings.Contains(selection.DeferredReasons[deferredID], "nonnegative_constraint") {
		t.Fatalf("deferred debit must carry nonnegative reservation conflict evidence: %+v", selection)
	}
	b := block.Block{BlockHash: "b-credit", ShardID: "s0", Height: 1, TxList: items}
	_, err = executor.ExecuteBlock(context.Background(), b, map[string]string{"s0::hot": "10"})
	var conflict GroundhogBlockConflictError
	if !errors.As(err, &conflict) || conflict.TxID != "debit-2" || conflict.Key != "hot" {
		t.Fatalf("fixed block must reject aggregate withdrawals beyond the block-start balance: %v", err)
	}
}

func TestGroundhogOrderedSetClearAndInsertAreOrderIndependent(t *testing.T) {
	base := map[string]string{}
	insert := groundhogModification{Key: "replay", Kind: groundhogOrderedSetInsert, Tag: 1, Hash: "hash-a"}
	clear := groundhogModification{Key: "replay", Kind: groundhogOrderedSetClear, Threshold: 1}
	materialized := []string{}
	for _, order := range [][]groundhogModification{{insert, clear}, {clear, insert}} {
		table := newGroundhogReservationTable("s0", base, 4)
		for _, modification := range order {
			if _, err := table.reserveTransaction([]groundhogModification{modification}); err != nil {
				t.Fatalf("order %v failed: %v", order, err)
			}
		}
		state, err := table.materialize()
		if err != nil {
			t.Fatal(err)
		}
		materialized = append(materialized, state["s0::replay"])
	}
	if materialized[0] != materialized[1] {
		t.Fatalf("clear/insert order changed materialization: %q vs %q", materialized[0], materialized[1])
	}
	var decoded groundhogOrderedSet
	if err := json.Unmarshal([]byte(materialized[0]), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Entries) != 0 || decoded.Cleared != 1 {
		t.Fatalf("insert at or below clear threshold must be absent after materialization: %+v", decoded)
	}
}

func TestGroundhogOrderedSetClearDoesNotEraseDuplicateEvidence(t *testing.T) {
	table := newGroundhogReservationTable("s0", nil, 4)
	insert := groundhogModification{Key: "replay", Kind: groundhogOrderedSetInsert, Tag: 1, Hash: "same"}
	if _, err := table.reserveTransaction([]groundhogModification{insert}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "replay", Kind: groundhogOrderedSetClear, Threshold: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "replay", Kind: groundhogOrderedSetInsert, Tag: 2, Hash: "same"}}); err == nil || !strings.Contains(err.Error(), "duplicate_hash") {
		t.Fatalf("clear must not allow duplicate insertion in the same block: %v", err)
	}
}

func TestGroundhogIdenticalBytesWritesMerge(t *testing.T) {
	table := newGroundhogReservationTable("s0", nil, 4)
	write := groundhogModification{Key: "raw", Kind: groundhogBytesSet, Bytes: "same-value"}
	if _, err := table.reserveTransaction([]groundhogModification{write}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.reserveTransaction([]groundhogModification{write}); err != nil {
		t.Fatalf("identical raw-memory writes should merge: %v", err)
	}
	state, err := table.materialize()
	if err != nil {
		t.Fatal(err)
	}
	if state["s0::raw"] != "same-value" {
		t.Fatalf("unexpected bytes materialization: %v", state)
	}
}

func TestGroundhogReservationFailureRollsBackEarlierObjectChanges(t *testing.T) {
	table := newGroundhogReservationTable("s0", nil, 4)
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "z-conflict", Kind: groundhogBytesSet, Bytes: "existing"}}); err != nil {
		t.Fatal(err)
	}
	_, err := table.reserveTransaction([]groundhogModification{
		{Key: "a-new", Kind: groundhogIntegerAdd, BaseInt: 10, Delta: -1},
		{Key: "z-conflict", Kind: groundhogBytesSet, Bytes: "different"},
	})
	if err == nil {
		t.Fatal("expected transaction reservation conflict")
	}
	if _, exists := table.Objects["a-new"]; exists {
		t.Fatalf("partial reservation survived rollback: %+v", table.Objects["a-new"])
	}
	state, materializeErr := table.materialize()
	if materializeErr != nil {
		t.Fatal(materializeErr)
	}
	if state["s0::z-conflict"] != "existing" {
		t.Fatalf("pre-existing reservation changed during rollback: %v", state)
	}
}

func TestGroundhogOrderedSetDefaultsMatchReferenceBounds(t *testing.T) {
	executor := NewGroundhogExecutor(1)
	if executor.OrderedSetLimit != GroundhogOrderedSetInitialLimit {
		t.Fatalf("Groundhog ordered-set initial limit drifted: got=%d", executor.OrderedSetLimit)
	}
	if GroundhogOrderedSetMaximumLimit != 65_535 {
		t.Fatalf("Groundhog ordered-set maximum drifted: got=%d", GroundhogOrderedSetMaximumLimit)
	}
}

func TestGroundhogOrderedSetIncreaseCannotExceedReferenceMaximum(t *testing.T) {
	base := map[string]string{
		groundhogQualifiedKey("s0", "set"): `{"limit":65534,"entries":[]}`,
	}
	table := newGroundhogReservationTable("s0", base, GroundhogOrderedSetInitialLimit)
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "set", Kind: groundhogOrderedSetGrow, Increase: 1}}); err != nil {
		t.Fatalf("growth to reference maximum should succeed: %v", err)
	}
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "set", Kind: groundhogOrderedSetGrow, Increase: 1}}); err == nil || !strings.Contains(err.Error(), "ordered_set_limit_overflow") {
		t.Fatalf("growth beyond reference maximum must fail, got %v", err)
	}
}

func TestGroundhogOrderedSetCapacityConflict(t *testing.T) {
	table := newGroundhogReservationTable("s0", nil, 1)
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "set", Kind: groundhogOrderedSetInsert, Tag: 1, Hash: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "set", Kind: groundhogOrderedSetInsert, Tag: 2, Hash: "b"}}); err == nil || !strings.Contains(err.Error(), "ordered_set_capacity") {
		t.Fatalf("expected ordered-set capacity conflict, got %v", err)
	}
}

func TestGroundhogIntegerOverflowIsRejectedBeforeCandidateConstruction(t *testing.T) {
	table := newGroundhogReservationTable("s0", nil, 4)
	if _, err := table.reserveTransaction([]groundhogModification{{Key: "counter", Kind: groundhogIntegerAdd, BaseInt: int64(^uint64(0) >> 1), Delta: 1}}); err == nil || !strings.Contains(err.Error(), "integer_overflow") {
		t.Fatalf("expected integer overflow reservation conflict, got %v", err)
	}
	if _, exists := table.Objects["counter"]; exists {
		t.Fatalf("overflowing reservation left state behind: %+v", table.Objects["counter"])
	}
}

func TestGroundhogConcurrentReservationMatchesSequentialMaterialization(t *testing.T) {
	base := map[string]string{}
	mods := [][]groundhogModification{}
	for index := 0; index < 32; index++ {
		mods = append(mods, []groundhogModification{{Key: "k" + strconv.Itoa(index), Kind: groundhogIntegerAdd, BaseInt: 10, Delta: int64(index + 1)}})
	}
	parallel := newGroundhogReservationTable("s0", base, GroundhogOrderedSetInitialLimit)
	var wg sync.WaitGroup
	errs := make(chan error, len(mods))
	for _, group := range mods {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := parallel.reserveTransaction(group)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	parallelState, err := parallel.materialize()
	if err != nil {
		t.Fatal(err)
	}

	sequential := newGroundhogReservationTable("s0", base, GroundhogOrderedSetInitialLimit)
	for _, group := range mods {
		if _, err := sequential.reserveTransaction(group); err != nil {
			t.Fatal(err)
		}
	}
	sequentialState, err := sequential.materialize()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parallelState, sequentialState) {
		t.Fatalf("concurrent reservation drifted from deterministic materialization\nparallel=%v\nsequential=%v", parallelState, sequentialState)
	}
}

func TestGroundhogFixedBlockReportsConcurrentReservationEngine(t *testing.T) {
	executor := NewGroundhogExecutor(4)
	items := []tx.SignedTransaction{}
	for index := 0; index < 16; index++ {
		items = append(items, tx.SignedTransaction{TxID: fmt.Sprintf("tx-%02d", index), AccessList: []tx.AccessItem{{Key: fmt.Sprintf("hot-%02d", index), Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}})
	}
	_, err := executor.ExecuteBlock(context.Background(), block.Block{BlockHash: "parallel", ShardID: "s0", Height: 1, TxList: items}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if executor.Metrics.ReservationEngine != "object_key_concurrent_reserve_revert_commit" {
		t.Fatalf("unexpected reservation engine: %+v", executor.Metrics)
	}
	if executor.Metrics.ReservationParallelWidth < 1 {
		t.Fatalf("missing concurrent reservation evidence: %+v", executor.Metrics)
	}
}
