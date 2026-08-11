package execution

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const AriaBlockExecutorID = "aria_block_executor"
const AriaBlockExecutorVersion = "0.2.0"

// AriaMetrics records mechanism-level evidence for the Aria block executor.
// Counts refer to real execution attempts inside Aria epochs; no serial replay is
// included in the timed execution path.
type AriaMetrics struct {
	WorkerCount               int    `json:"worker_count"`
	EpochCount                int    `json:"epoch_count"`
	MaximumEpochWidth         int    `json:"maximum_epoch_width"`
	MaximumParallelWidth      int    `json:"maximum_parallel_width"`
	ExecutionAttemptCount     int    `json:"execution_attempt_count"`
	CommittedTransactionCount int    `json:"committed_transaction_count"`
	FinalizedTransactionCount int    `json:"finalized_transaction_count"`
	ConflictAbortCount        int    `json:"conflict_abort_count"`
	ReexecutionCount          int    `json:"reexecution_count"`
	RetryableNonceCount       int    `json:"retryable_nonce_count"`
	WAWDependencyCount        int    `json:"waw_dependency_count"`
	RAWDependencyCount        int    `json:"raw_dependency_count"`
	WARDependencyCount        int    `json:"war_dependency_count"`
	ReadReservationCount      int    `json:"read_reservation_count"`
	WriteReservationCount     int    `json:"write_reservation_count"`
	ReadOnlyFastCommitCount   int    `json:"read_only_fast_commit_count"`
	ApplicationFailureCount   int    `json:"application_failure_count"`
	CandidateTransactionCount int    `json:"candidate_transaction_count"`
	SelectedTransactionCount  int    `json:"selected_transaction_count"`
	DeferredTransactionCount  int    `json:"deferred_transaction_count"`
	ReorderingEnabled         bool   `json:"reordering_enabled"`
	ReadOnlyOptimization      bool   `json:"read_only_optimization"`
	BatchLifecycle            string `json:"batch_lifecycle"`
	FallbackMode              string `json:"fallback_mode"`
	EpochWidths               []int  `json:"epoch_widths"`
}

// AriaEpochTrace is deterministic evidence of one execution/commit barrier pair.
type AriaEpochTrace struct {
	Epoch                 int            `json:"epoch"`
	CandidateIndexes      []int          `json:"candidate_indexes"`
	CommittedIndexes      []int          `json:"committed_indexes"`
	DeferredIndexes       []int          `json:"deferred_indexes"`
	DeferredReasonByIndex map[int]string `json:"deferred_reason_by_index"`
}

// AriaExecutor reimplements the core Aria execution and commit phases over the
// MBE transaction semantics. A consensus block is the initial Aria batch;
// transactions aborted by Rule 1/Rule 2 are retried, in relative order, in an
// internal next epoch so the surrounding blockchain block lifecycle is kept
// unchanged.
type AriaExecutor struct {
	DefaultInitialBalance int64
	WorkerCount           int
	MaximumEpochs         int
	Reordering            bool
	ReadOnlyOptimization  bool
	RetryNonceGaps        bool
	Metrics               AriaMetrics
	Trace                 []AriaEpochTrace
	serialSemantics       *SerialExecutor
}

func NewAriaExecutor(workerCount int) *AriaExecutor {
	if workerCount < 1 {
		workerCount = 1
	}
	return &AriaExecutor{
		DefaultInitialBalance: 1_000_000,
		WorkerCount:           workerCount,
		MaximumEpochs:         0,
		Reordering:            true,
		ReadOnlyOptimization:  true,
		RetryNonceGaps:        true,
		serialSemantics:       NewSerialExecutor(),
	}
}

type ariaAttempt struct {
	Index       int
	Tx          tx.SignedTransaction
	Delta       TxDelta
	Retryable   bool
	RetryReason string
}

type ariaConflict struct {
	WAW bool
	RAW bool
	WAR bool
}

type ariaReservationTable struct {
	MinReader map[string]int
	MinWriter map[string]int
}

type ariaPlanEpoch struct {
	Epoch                   int      `json:"epoch"`
	CandidateTransactionIDs []string `json:"candidate_transaction_ids"`
	CommittedTransactionIDs []string `json:"committed_transaction_ids"`
	DeferredTransactionIDs  []string `json:"deferred_transaction_ids"`
	DeferredReasons         []string `json:"deferred_reasons"`
}

type AriaCandidateSelection struct {
	Selected        []tx.SignedTransaction `json:"selected"`
	SelectedDeltas  []TxDelta              `json:"selected_deltas"`
	Deferred        []tx.SignedTransaction `json:"deferred"`
	DeferredReasons map[string]string      `json:"deferred_reasons"`
	Metrics         AriaMetrics            `json:"metrics"`
	Trace           AriaEpochTrace         `json:"trace"`
}

// SelectCandidateTransactions performs exactly one Aria batch over the block-start
// snapshot. Conflict-aborted transactions are returned in their original relative
// order so the block producer can release them back to the mempool for the next
// consensus block.
func (e *AriaExecutor) SelectCandidateTransactions(ctx context.Context, shardID string, height uint64, candidates []tx.SignedTransaction, base map[string]string, limit int) (AriaCandidateSelection, error) {
	selection := AriaCandidateSelection{DeferredReasons: map[string]string{}}
	if err := ctx.Err(); err != nil {
		return selection, err
	}
	if len(candidates) == 0 {
		return selection, nil
	}
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	workerCount := e.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	if e.serialSemantics == nil {
		e.serialSemantics = NewSerialExecutor()
	}
	e.serialSemantics.DefaultInitialBalance = e.DefaultInitialBalance
	blockValue := block.Block{ShardID: shardID, Height: height, TxList: append([]tx.SignedTransaction(nil), candidates...)}
	pending := make([]int, len(candidates))
	for index := range pending {
		pending[index] = index
	}
	// The producer/validator supplies an immutable block-start snapshot; Aria
	// attempts keep private writes, so no physical state copy is needed here.
	attempts, maximumParallel, err := e.executeEpoch(ctx, blockValue, base, pending, workerCount)
	if err != nil {
		return selection, err
	}
	metrics := AriaMetrics{
		WorkerCount:               workerCount,
		EpochCount:                1,
		MaximumEpochWidth:         len(candidates),
		MaximumParallelWidth:      maximumParallel,
		ExecutionAttemptCount:     len(attempts),
		CandidateTransactionCount: len(candidates),
		ReorderingEnabled:         e.Reordering,
		ReadOnlyOptimization:      e.ReadOnlyOptimization,
		BatchLifecycle:            "one_consensus_block_per_aria_batch",
		FallbackMode:              "disabled",
		EpochWidths:               []int{len(candidates)},
	}
	reservations := buildAriaReservations(attempts, &metrics)
	committedIndexes := make([]int, 0, limit)
	deferredIndexes := make([]int, 0, len(candidates))
	deferredReasons := map[int]string{}
	for position, attempt := range attempts {
		if len(selection.Selected) >= limit {
			selection.Deferred = append(selection.Deferred, attempt.Tx)
			selection.DeferredReasons[attempt.Tx.TxID] = "aria_candidate_limit"
			deferredIndexes = append(deferredIndexes, attempt.Index)
			deferredReasons[attempt.Index] = "aria_candidate_limit"
			continue
		}
		if attempt.Retryable {
			metrics.RetryableNonceCount++
			selection.Deferred = append(selection.Deferred, attempt.Tx)
			selection.DeferredReasons[attempt.Tx.TxID] = attempt.RetryReason
			deferredIndexes = append(deferredIndexes, attempt.Index)
			deferredReasons[attempt.Index] = attempt.RetryReason
			continue
		}
		if len(attempt.Delta.WriteSet) == 0 && !attempt.Delta.Receipt.Success {
			selection.Selected = append(selection.Selected, attempt.Tx)
			selection.SelectedDeltas = append(selection.SelectedDeltas, cloneTxDelta(attempt.Delta))
			metrics.ApplicationFailureCount++
			committedIndexes = append(committedIndexes, attempt.Index)
			continue
		}
		if e.ReadOnlyOptimization && len(attempt.Delta.WriteSet) == 0 {
			metrics.ReadOnlyFastCommitCount++
			selection.Selected = append(selection.Selected, attempt.Tx)
			selection.SelectedDeltas = append(selection.SelectedDeltas, cloneTxDelta(attempt.Delta))
			committedIndexes = append(committedIndexes, attempt.Index)
			continue
		}
		conflict := analyzeAriaConflict(attempt, position+1, reservations)
		if conflict.WAW {
			metrics.WAWDependencyCount++
		}
		if conflict.RAW {
			metrics.RAWDependencyCount++
		}
		if conflict.WAR {
			metrics.WARDependencyCount++
		}
		canCommit := !conflict.WAW && !conflict.RAW
		if e.Reordering {
			canCommit = !conflict.WAW && !(conflict.WAR && conflict.RAW)
		}
		if canCommit {
			selection.Selected = append(selection.Selected, attempt.Tx)
			selection.SelectedDeltas = append(selection.SelectedDeltas, cloneTxDelta(attempt.Delta))
			committedIndexes = append(committedIndexes, attempt.Index)
			continue
		}
		metrics.ConflictAbortCount++
		reason := ariaConflictReason(conflict, e.Reordering)
		selection.Deferred = append(selection.Deferred, attempt.Tx)
		selection.DeferredReasons[attempt.Tx.TxID] = reason
		deferredIndexes = append(deferredIndexes, attempt.Index)
		deferredReasons[attempt.Index] = reason
	}
	metrics.SelectedTransactionCount = len(selection.Selected)
	metrics.DeferredTransactionCount = len(selection.Deferred)
	metrics.FinalizedTransactionCount = len(selection.Selected)
	metrics.CommittedTransactionCount = len(selection.Selected) - metrics.ApplicationFailureCount
	selection.Metrics = metrics
	selection.Trace = AriaEpochTrace{
		Epoch:                 1,
		CandidateIndexes:      append([]int(nil), pending...),
		CommittedIndexes:      committedIndexes,
		DeferredIndexes:       deferredIndexes,
		DeferredReasonByIndex: deferredReasons,
	}
	return selection, nil
}

// MaterializeCandidateSelection applies the private write sets produced while
// executing the complete consensus-bound Aria candidate batch. Selected
// transactions are not executed a second time: validators commit the exact
// deltas they independently recomputed from the common block-start snapshot.
func (e *AriaExecutor) MaterializeCandidateSelection(b block.Block, base map[string]string, selection AriaCandidateSelection) (Result, error) {
	workerCount := e.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	if len(b.TxList) != len(selection.Selected) || len(selection.Selected) != len(selection.SelectedDeltas) {
		return Result{}, fmt.Errorf("aria selected materialization size mismatch: block=%d selected=%d deltas=%d", len(b.TxList), len(selection.Selected), len(selection.SelectedDeltas))
	}
	working := copySnapshot(base)
	before := state.RootOfSnapshot(base)
	result := Result{
		BlockHash:        b.BlockHash,
		Height:           b.Height,
		StateRootBefore:  before,
		Deterministic:    true,
		EVMExecution:     false,
		FabricExecution:  false,
		StateUpdates:     map[string]string{},
		BlockExecutorID:  AriaBlockExecutorID,
		ExecutorVersion:  AriaBlockExecutorVersion,
		WorkerCount:      workerCount,
		SerialEquivalent: false,
	}
	commitOrder := make([]int, 0, len(b.TxList))
	trace := AriaEpochTrace{Epoch: 1, DeferredReasonByIndex: map[int]string{}}
	for index, item := range b.TxList {
		selected := selection.Selected[index]
		if item.TxID == "" || selected.TxID != item.TxID {
			return Result{}, fmt.Errorf("aria selected transaction mismatch at index %d: block=%s selection=%s", index, item.TxID, selected.TxID)
		}
		delta := cloneTxDelta(selection.SelectedDeltas[index])
		if delta.TxID != item.TxID {
			return Result{}, fmt.Errorf("aria selected delta mismatch at index %d: block=%s delta=%s", index, item.TxID, delta.TxID)
		}
		delta.OriginalIndex = index
		delta.Receipt.BlockHash = b.BlockHash
		delta.Receipt.Height = b.Height
		for _, key := range sortedAriaWriteKeys(delta.WriteSet) {
			working[ariaQualifiedKey(b.ShardID, key)] = delta.WriteSet[key]
		}
		delta.Receipt.StateRootAfterTx = state.RootOfSnapshot(working)
		delta.Success = delta.Receipt.Success
		delta.Error = delta.Receipt.Error
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, delta.Receipt)
		if delta.Receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
		commitOrder = append(commitOrder, index)
		trace.CandidateIndexes = append(trace.CandidateIndexes, index)
		trace.CommittedIndexes = append(trace.CommittedIndexes, index)
	}
	result.StateRootAfter = state.RootOfSnapshot(working)
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	result.StateUpdates = copySnapshot(working)
	result.StateDelta = stateDelta(base, working)
	planEpoch := ariaPlanEpochFromTrace(b, trace)
	result.Plan = buildAriaPlan(b, declaredAccessSet(b.TxList), workerCount, e, commitOrder, []ariaPlanEpoch{planEpoch})
	result.PlanDigest = result.Plan.PlanDigest
	e.Metrics = selection.Metrics
	e.Trace = []AriaEpochTrace{selection.Trace}
	return result, nil
}

func cloneTxDelta(input TxDelta) TxDelta {
	out := input
	out.ReadSet = append([]ReadObservation(nil), input.ReadSet...)
	out.WriteSet = make(map[string]string, len(input.WriteSet))
	for key, value := range input.WriteSet {
		out.WriteSet[key] = value
	}
	out.Receipt.StateKeys = append([]string(nil), input.Receipt.StateKeys...)
	return out
}

func (e *AriaExecutor) ExecuteBlock(ctx context.Context, b block.Block, base map[string]string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	workerCount := e.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	if e.serialSemantics == nil {
		e.serialSemantics = NewSerialExecutor()
	}
	e.serialSemantics.DefaultInitialBalance = e.DefaultInitialBalance

	working := copySnapshot(base)
	pending := make([]int, len(b.TxList))
	for index := range b.TxList {
		pending[index] = index
	}
	finalDeltas := make([]TxDelta, len(b.TxList))
	finalized := make([]bool, len(b.TxList))
	commitOrder := make([]int, 0, len(b.TxList))
	planEpochs := make([]ariaPlanEpoch, 0)
	metrics := AriaMetrics{
		WorkerCount:               workerCount,
		CandidateTransactionCount: len(b.TxList),
		ReorderingEnabled:         e.Reordering,
		ReadOnlyOptimization:      e.ReadOnlyOptimization,
		BatchLifecycle:            "internal_multi_epoch_drain",
		FallbackMode:              "disabled",
	}
	e.Trace = nil

	maximumEpochs := e.MaximumEpochs
	if maximumEpochs <= 0 {
		// Rule 1 guarantees forward progress for valid transactions while
		// preserving the relative order of aborted transactions. A block-sized
		// bound is therefore sufficient for the MBE internal-epoch mapping.
		maximumEpochs = len(b.TxList) + 1
		if maximumEpochs < 1 {
			maximumEpochs = 1
		}
	}

	for epoch := 1; len(pending) > 0; epoch++ {
		if epoch > maximumEpochs {
			return Result{}, fmt.Errorf("aria maximum epochs exceeded: completed=%d pending=%d limit=%d", len(b.TxList)-len(pending), len(pending), maximumEpochs)
		}
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		// working is immutable while this epoch executes; tentative writes stay
		// transaction-local and are applied only after the epoch barrier.
		epochSnapshot := working
		attempts, maximumParallel, err := e.executeEpoch(ctx, b, epochSnapshot, pending, workerCount)
		if err != nil {
			return Result{}, err
		}
		metrics.EpochCount++
		metrics.EpochWidths = append(metrics.EpochWidths, len(pending))
		if len(pending) > metrics.MaximumEpochWidth {
			metrics.MaximumEpochWidth = len(pending)
		}
		if maximumParallel > metrics.MaximumParallelWidth {
			metrics.MaximumParallelWidth = maximumParallel
		}
		metrics.ExecutionAttemptCount += len(attempts)

		reservations := buildAriaReservations(attempts, &metrics)
		committed := make([]int, 0, len(attempts))
		deferred := make([]int, 0, len(attempts))
		deferredReasons := map[int]string{}
		attemptByIndex := make(map[int]ariaAttempt, len(attempts))

		for position, attempt := range attempts {
			attemptByIndex[attempt.Index] = attempt
			tid := position + 1 // Aria TIDs are ordered within the current batch.
			if attempt.Retryable {
				metrics.RetryableNonceCount++
				deferred = append(deferred, attempt.Index)
				deferredReasons[attempt.Index] = attempt.RetryReason
				continue
			}
			if len(attempt.Delta.WriteSet) == 0 && !attempt.Delta.Receipt.Success {
				// MBE application failures are terminal block outcomes. A failed
				// transaction with no legacy side-effect writes needs no Aria
				// reservation or retry.
				committed = append(committed, attempt.Index)
				continue
			}
			if e.ReadOnlyOptimization && len(attempt.Delta.WriteSet) == 0 {
				metrics.ReadOnlyFastCommitCount++
				committed = append(committed, attempt.Index)
				continue
			}

			conflict := analyzeAriaConflict(attempt, tid, reservations)
			if conflict.WAW {
				metrics.WAWDependencyCount++
			}
			if conflict.RAW {
				metrics.RAWDependencyCount++
			}
			if conflict.WAR {
				metrics.WARDependencyCount++
			}
			canCommit := !conflict.WAW && !conflict.RAW
			if e.Reordering {
				// Official Aria Rule 2: no WAW and not both WAR and RAW.
				canCommit = !conflict.WAW && !(conflict.WAR && conflict.RAW)
			}
			if canCommit {
				committed = append(committed, attempt.Index)
				continue
			}
			metrics.ConflictAbortCount++
			deferred = append(deferred, attempt.Index)
			deferredReasons[attempt.Index] = ariaConflictReason(conflict, e.Reordering)
		}

		if len(committed) == 0 {
			ids := make([]string, 0, len(deferred))
			reasons := make([]string, 0, len(deferred))
			for _, index := range deferred {
				ids = append(ids, b.TxList[index].TxID)
				reasons = append(reasons, deferredReasons[index])
			}
			return Result{}, fmt.Errorf("aria epoch %d made no progress: pending=%s reasons=%s", epoch, strings.Join(ids, ","), strings.Join(reasons, ","))
		}

		// Rule 1/Rule 2 prevent WAW among committed transactions. Applying their
		// private write sets in candidate order is deterministic and yields the
		// same epoch state regardless of worker scheduling.
		for _, index := range committed {
			attempt := attemptByIndex[index]
			writeKeys := make([]string, 0, len(attempt.Delta.WriteSet))
			for key := range attempt.Delta.WriteSet {
				writeKeys = append(writeKeys, key)
			}
			sort.Strings(writeKeys)
			for _, key := range writeKeys {
				working[ariaQualifiedKey(b.ShardID, key)] = attempt.Delta.WriteSet[key]
			}
			attempt.Delta.Receipt.StateRootAfterTx = state.RootOfSnapshot(working)
			attempt.Delta.Success = attempt.Delta.Receipt.Success
			attempt.Delta.Error = attempt.Delta.Receipt.Error
			finalDeltas[index] = attempt.Delta
			finalized[index] = true
			commitOrder = append(commitOrder, index)
			if !attempt.Delta.Receipt.Success {
				metrics.ApplicationFailureCount++
			}
		}

		trace := AriaEpochTrace{
			Epoch:                 epoch,
			CandidateIndexes:      append([]int(nil), pending...),
			CommittedIndexes:      append([]int(nil), committed...),
			DeferredIndexes:       append([]int(nil), deferred...),
			DeferredReasonByIndex: copyAriaReasonMap(deferredReasons),
		}
		e.Trace = append(e.Trace, trace)
		planEpochs = append(planEpochs, ariaPlanEpochFromTrace(b, trace))
		pending = deferred
	}

	for index, ok := range finalized {
		if !ok {
			return Result{}, fmt.Errorf("aria missing final result for transaction %s at index %d", b.TxList[index].TxID, index)
		}
	}
	metrics.FinalizedTransactionCount = len(b.TxList)
	metrics.SelectedTransactionCount = len(b.TxList)
	metrics.DeferredTransactionCount = 0
	metrics.CommittedTransactionCount = len(b.TxList) - metrics.ApplicationFailureCount
	metrics.ReexecutionCount = metrics.ExecutionAttemptCount - len(b.TxList)
	if metrics.ReexecutionCount < 0 {
		metrics.ReexecutionCount = 0
	}
	e.Metrics = metrics

	result := Result{
		BlockHash:        b.BlockHash,
		Height:           b.Height,
		StateRootBefore:  state.RootOfSnapshot(base),
		StateRootAfter:   state.RootOfSnapshot(working),
		Deterministic:    true,
		EVMExecution:     false,
		FabricExecution:  false,
		StateUpdates:     copySnapshot(working),
		BlockExecutorID:  AriaBlockExecutorID,
		ExecutorVersion:  AriaBlockExecutorVersion,
		WorkerCount:      workerCount,
		SerialEquivalent: false,
	}
	result.TxDeltas = make([]TxDelta, 0, len(finalDeltas))
	result.Receipts = make([]Receipt, 0, len(finalDeltas))
	for index := range finalDeltas {
		delta := finalDeltas[index]
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, delta.Receipt)
		if delta.Receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	result.StateDelta = stateDelta(base, working)
	result.Plan = buildAriaPlan(b, declaredAccessSet(b.TxList), workerCount, e, commitOrder, planEpochs)
	result.PlanDigest = result.Plan.PlanDigest
	return result, nil
}

func (e *AriaExecutor) executeEpoch(ctx context.Context, b block.Block, snapshot map[string]string, pending []int, workerCount int) ([]ariaAttempt, int, error) {
	if len(pending) == 0 {
		return nil, 0, nil
	}
	if workerCount > len(pending) {
		workerCount = len(pending)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	attempts := make([]ariaAttempt, len(pending))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var active int64
	var maximum int64

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for position := range jobs {
				if ctx.Err() != nil {
					continue
				}
				current := atomic.AddInt64(&active, 1)
				for {
					previous := atomic.LoadInt64(&maximum)
					if current <= previous || atomic.CompareAndSwapInt64(&maximum, previous, current) {
						break
					}
				}
				index := pending[position]
				item := b.TxList[index]
				overlay := newTxOverlay(b.ShardID, snapshot)
				// Tentative Aria attempts need read/write evidence, not a durable
				// per-attempt state root. Final committed receipt roots are rebuilt
				// during deterministic materialization below.
				receipt := e.serialSemantics.executeTxWithoutStateRoot(b, overlay, item)
				delta := TxDelta{
					TxID:          item.TxID,
					OriginalIndex: index,
					ReadSet:       append([]ReadObservation(nil), overlay.reads...),
					WriteSet:      overlay.logicalWrites(),
					Receipt:       receipt,
					Success:       receipt.Success,
					Error:         receipt.Error,
				}
				retryable, reason := ariaRetryableFailure(receipt, e.RetryNonceGaps)
				attempts[position] = ariaAttempt{Index: index, Tx: item, Delta: delta, Retryable: retryable, RetryReason: reason}
				atomic.AddInt64(&active, -1)
			}
		}()
	}
	for position := range pending {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, int(atomic.LoadInt64(&maximum)), ctx.Err()
		case jobs <- position:
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, int(atomic.LoadInt64(&maximum)), err
	}
	return attempts, int(atomic.LoadInt64(&maximum)), nil
}

func buildAriaReservations(attempts []ariaAttempt, metrics *AriaMetrics) ariaReservationTable {
	table := ariaReservationTable{MinReader: map[string]int{}, MinWriter: map[string]int{}}
	for position, attempt := range attempts {
		if attempt.Retryable || (!attempt.Delta.Receipt.Success && len(attempt.Delta.WriteSet) == 0) {
			continue
		}
		tid := position + 1
		readKeys := uniqueAriaReadKeys(attempt.Delta.ReadSet)
		writeKeys := sortedAriaWriteKeys(attempt.Delta.WriteSet)
		metrics.ReadReservationCount += len(readKeys)
		metrics.WriteReservationCount += len(writeKeys)
		for _, key := range readKeys {
			if previous := table.MinReader[key]; previous == 0 || tid < previous {
				table.MinReader[key] = tid
			}
		}
		for _, key := range writeKeys {
			if previous := table.MinWriter[key]; previous == 0 || tid < previous {
				table.MinWriter[key] = tid
			}
		}
	}
	return table
}

func analyzeAriaConflict(attempt ariaAttempt, tid int, table ariaReservationTable) ariaConflict {
	conflict := ariaConflict{}
	for _, key := range uniqueAriaReadKeys(attempt.Delta.ReadSet) {
		if writer := table.MinWriter[key]; writer != 0 && writer < tid {
			conflict.RAW = true
		}
	}
	for _, key := range sortedAriaWriteKeys(attempt.Delta.WriteSet) {
		if writer := table.MinWriter[key]; writer != 0 && writer < tid {
			conflict.WAW = true
		}
		if reader := table.MinReader[key]; reader != 0 && reader < tid {
			conflict.WAR = true
		}
	}
	return conflict
}

func ariaConflictReason(conflict ariaConflict, reordering bool) string {
	parts := []string{}
	if conflict.WAW {
		parts = append(parts, "waw")
	}
	if conflict.RAW {
		parts = append(parts, "raw")
	}
	if conflict.WAR {
		parts = append(parts, "war")
	}
	mode := "rule1"
	if reordering {
		mode = "rule2"
	}
	if len(parts) == 0 {
		return "aria_" + mode + "_deferred"
	}
	return "aria_" + mode + "_" + strings.Join(parts, "_")
}

func ariaRetryableFailure(receipt Receipt, enabled bool) (bool, string) {
	if !enabled || receipt.Success {
		return false, ""
	}
	var expected, got uint64
	if _, err := fmt.Sscanf(receipt.Error, "nonce_mismatch_expected_%d_got_%d", &expected, &got); err == nil && got > expected {
		return true, fmt.Sprintf("aria_nonce_gap_expected_%d_got_%d", expected, got)
	}
	return false, ""
}

func uniqueAriaReadKeys(reads []ReadObservation) []string {
	seen := map[string]bool{}
	for _, read := range reads {
		if read.Key != "" {
			seen[read.Key] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedAriaWriteKeys(writes map[string]string) []string {
	keys := make([]string, 0, len(writes))
	for key := range writes {
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func ariaQualifiedKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func copyAriaReasonMap(input map[int]string) map[int]string {
	out := make(map[int]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func ariaPlanEpochFromTrace(b block.Block, trace AriaEpochTrace) ariaPlanEpoch {
	out := ariaPlanEpoch{Epoch: trace.Epoch}
	for _, index := range trace.CandidateIndexes {
		out.CandidateTransactionIDs = append(out.CandidateTransactionIDs, b.TxList[index].TxID)
	}
	for _, index := range trace.CommittedIndexes {
		out.CommittedTransactionIDs = append(out.CommittedTransactionIDs, b.TxList[index].TxID)
	}
	for _, index := range trace.DeferredIndexes {
		out.DeferredTransactionIDs = append(out.DeferredTransactionIDs, b.TxList[index].TxID)
		out.DeferredReasons = append(out.DeferredReasons, trace.DeferredReasonByIndex[index])
	}
	return out
}

func buildAriaPlan(b block.Block, declared AccessSet, workerCount int, executor *AriaExecutor, commitOrder []int, epochs []ariaPlanEpoch) ExecutionPlan {
	ids := make([]string, 0, len(commitOrder))
	indexes := make([]int, 0, len(commitOrder))
	for _, index := range commitOrder {
		ids = append(ids, b.TxList[index].TxID)
		indexes = append(indexes, index)
	}
	plan := ExecutionPlan{
		EngineID:                AriaBlockExecutorID,
		EngineVersion:           AriaBlockExecutorVersion,
		BlockHash:               b.BlockHash,
		BlockHeight:             b.Height,
		OrderedTransactionIDs:   ids,
		OriginalTransactionIdxs: indexes,
		DeclaredAccessSetDigest: stableDigest(declared),
		DeclaredReadKeyCount:    len(declared.ReadKeys),
		DeclaredWriteKeyCount:   len(declared.WriteKeys),
		WorkerCount:             workerCount,
	}
	plan.PlanDigest = stableDigest(struct {
		EngineID                string          `json:"engine_id"`
		EngineVersion           string          `json:"engine_version"`
		BlockHash               string          `json:"block_hash"`
		BlockHeight             uint64          `json:"block_height"`
		OrderedTransactionIDs   []string        `json:"ordered_transaction_ids"`
		OriginalTransactionIdxs []int           `json:"original_transaction_indexes"`
		DeclaredAccessSetDigest string          `json:"declared_access_set_digest"`
		DeclaredReadKeyCount    int             `json:"declared_read_key_count"`
		DeclaredWriteKeyCount   int             `json:"declared_write_key_count"`
		WorkerCount             int             `json:"worker_count"`
		Reordering              bool            `json:"reordering"`
		ReadOnlyOptimization    bool            `json:"read_only_optimization"`
		RetryNonceGaps          bool            `json:"retry_nonce_gaps"`
		Epochs                  []ariaPlanEpoch `json:"epochs"`
	}{
		EngineID:                plan.EngineID,
		EngineVersion:           plan.EngineVersion,
		BlockHash:               plan.BlockHash,
		BlockHeight:             plan.BlockHeight,
		OrderedTransactionIDs:   plan.OrderedTransactionIDs,
		OriginalTransactionIdxs: plan.OriginalTransactionIdxs,
		DeclaredAccessSetDigest: plan.DeclaredAccessSetDigest,
		DeclaredReadKeyCount:    plan.DeclaredReadKeyCount,
		DeclaredWriteKeyCount:   plan.DeclaredWriteKeyCount,
		WorkerCount:             plan.WorkerCount,
		Reordering:              executor.Reordering,
		ReadOnlyOptimization:    executor.ReadOnlyOptimization,
		RetryNonceGaps:          executor.RetryNonceGaps,
		Epochs:                  epochs,
	})
	return plan
}
