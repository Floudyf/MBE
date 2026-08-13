package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const GroundhogBlockExecutorID = "groundhog_block_executor"
const GroundhogBlockExecutorVersion = "0.2.0"
const GroundhogOrderedSetInitialLimit = 64
const GroundhogOrderedSetMaximumLimit = 65_535

type GroundhogMetrics struct {
	WorkerCount                    int    `json:"worker_count"`
	MaximumParallelWidth           int    `json:"maximum_parallel_width"`
	ReservationParallelWidth       int    `json:"reservation_parallel_width"`
	ReservationEngine              string `json:"reservation_engine"`
	ExecutionAttemptCount          int    `json:"execution_attempt_count"`
	ReservationCount               int    `json:"reservation_count"`
	ConstraintConflictCount        int    `json:"constraint_conflict_count"`
	ReservationRollbackCount       int    `json:"reservation_rollback_count"`
	IntegerMergeCount              int    `json:"integer_merge_count"`
	BytesMergeCount                int    `json:"bytes_merge_count"`
	OrderedSetMergeCount           int    `json:"ordered_set_merge_count"`
	ModifiedKeyCount               int    `json:"modified_key_count"`
	ApplicationFailureCount        int    `json:"application_failure_count"`
	CommittedTransactionCount      int    `json:"committed_transaction_count"`
	DeferredTransactionCount       int    `json:"deferred_transaction_count"`
	CandidateTransactionCount      int    `json:"candidate_transaction_count"`
	SelectedTransactionCount       int    `json:"selected_transaction_count"`
	FallbackMode                   string `json:"fallback_mode"`
	SnapshotSemantics              string `json:"snapshot_semantics"`
	TypedModificationSemantics     string `json:"typed_modification_semantics"`
	TransactionExecutionMS         int64  `json:"transaction_execution_ms"`
	DeterministicMaterializationMS int64  `json:"deterministic_materialization_ms"`
	StateCommitmentMS              int64  `json:"state_commitment_ms"`
}

type GroundhogTransactionTrace struct {
	Index            int      `json:"index"`
	TxID             string   `json:"tx_id"`
	Status           string   `json:"status"`
	Reason           string   `json:"reason,omitempty"`
	ReadKeys         []string `json:"read_keys,omitempty"`
	ModificationKeys []string `json:"modification_keys,omitempty"`
}

type GroundhogCandidateSelection struct {
	Selected        []tx.SignedTransaction      `json:"selected"`
	Deferred        []tx.SignedTransaction      `json:"deferred"`
	DeferredReasons map[string]string           `json:"deferred_reasons"`
	Metrics         GroundhogMetrics            `json:"metrics"`
	Trace           []GroundhogTransactionTrace `json:"trace"`
}

type GroundhogBlockConflictError struct {
	TxID   string
	Key    string
	Reason string
}

func (e GroundhogBlockConflictError) Error() string {
	return fmt.Sprintf("groundhog fixed block conflict tx=%s key=%s reason=%s", e.TxID, e.Key, e.Reason)
}

type GroundhogExecutor struct {
	DefaultInitialBalance int64
	WorkerCount           int
	OrderedSetLimit       int
	Metrics               GroundhogMetrics
	Trace                 []GroundhogTransactionTrace
}

func NewGroundhogExecutor(workerCount int) *GroundhogExecutor {
	if workerCount < 1 {
		workerCount = 1
	}
	return &GroundhogExecutor{
		DefaultInitialBalance: 1_000_000,
		WorkerCount:           workerCount,
		OrderedSetLimit:       GroundhogOrderedSetInitialLimit,
	}
}

type groundhogModificationKind string

const (
	groundhogIntegerAdd       groundhogModificationKind = "nonnegative_int64_set_add"
	groundhogBytesSet         groundhogModificationKind = "bytes_set"
	groundhogOrderedSetInsert groundhogModificationKind = "ordered_set_insert"
	groundhogOrderedSetClear  groundhogModificationKind = "ordered_set_clear"
	groundhogOrderedSetGrow   groundhogModificationKind = "ordered_set_increase_limit"
)

type groundhogModification struct {
	Key       string
	Kind      groundhogModificationKind
	BaseInt   int64
	Delta     int64
	Bytes     string
	Tag       uint64
	Hash      string
	Threshold uint64
	Increase  int
}

type groundhogAttempt struct {
	Index         int
	Tx            tx.SignedTransaction
	Reads         []ReadObservation
	Modifications []groundhogModification
	TerminalError string
}

type groundhogOrderedEntry struct {
	Tag  uint64 `json:"tag"`
	Hash string `json:"hash"`
}

type groundhogOrderedSet struct {
	Limit   int                     `json:"limit"`
	Cleared uint64                  `json:"cleared_through,omitempty"`
	Entries []groundhogOrderedEntry `json:"entries"`
}

type groundhogObject struct {
	Kind          groundhogModificationKind
	BaseInt       int64
	PositiveDelta int64
	NegativeDelta int64
	Bytes         string
	Set           groundhogOrderedSet
	SetByHash     map[string]uint64
}

type groundhogReservationTable struct {
	objectsMu       sync.RWMutex
	keyLocksMu      sync.Mutex
	keyLocks        map[string]*sync.Mutex
	ShardID         string
	Base            map[string]string
	OrderedSetLimit int
	Objects         map[string]*groundhogObject
}

type groundhogReservationStats struct {
	Reservations int
	Integers     int
	Bytes        int
	OrderedSets  int
}

func newGroundhogReservationTable(shardID string, base map[string]string, orderedSetLimit int) *groundhogReservationTable {
	if orderedSetLimit < 1 {
		orderedSetLimit = GroundhogOrderedSetInitialLimit
	}
	return &groundhogReservationTable{
		ShardID: shardID,
		// The block-start snapshot is caller-owned and immutable during
		// reservation. Objects contain all private reservation state.
		Base:            base,
		OrderedSetLimit: orderedSetLimit,
		Objects:         map[string]*groundhogObject{},
		keyLocks:        map[string]*sync.Mutex{},
	}
}

func (e *GroundhogExecutor) SelectCandidateTransactions(
	ctx context.Context,
	shardID string,
	height uint64,
	candidates []tx.SignedTransaction,
	base map[string]string,
	limit int,
) (GroundhogCandidateSelection, error) {
	if err := ctx.Err(); err != nil {
		return GroundhogCandidateSelection{}, err
	}
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	workerCount := e.workerCountFor(len(candidates))
	table := newGroundhogReservationTable(shardID, base, e.OrderedSetLimit)
	selection := GroundhogCandidateSelection{DeferredReasons: map[string]string{}}
	metrics := newGroundhogMetrics(workerCount)
	metrics.CandidateTransactionCount = len(candidates)
	metrics.ReservationEngine = "object_key_parallel_streaming_proposal_reserve_revert_commit"

	// Groundhog Algorithm 2 consumes a transaction stream until the proposed
	// block is large enough. Its main loop is parallelizable. Execute and reserve
	// only a bounded set of in-flight candidates at a time; if conflicts leave
	// free block slots, continue drawing from the stream. This avoids executing
	// the entire ready pool after the block is already full while preserving the
	// paper's ability to search beyond an arbitrary fixed prefix.
	cursor := 0
	for cursor < len(candidates) && len(selection.Selected) < limit {
		if err := ctx.Err(); err != nil {
			return GroundhogCandidateSelection{}, err
		}
		remaining := limit - len(selection.Selected)
		chunkSize := workerCount
		if chunkSize > remaining {
			chunkSize = remaining
		}
		if chunkSize > len(candidates)-cursor {
			chunkSize = len(candidates) - cursor
		}
		chunk := candidates[cursor : cursor+chunkSize]
		attempts, executionParallel, err := e.executeAttempts(ctx, block.Block{ShardID: shardID, Height: height}, base, chunk, e.workerCountFor(len(chunk)))
		if err != nil {
			return GroundhogCandidateSelection{}, err
		}
		for index := range attempts {
			attempts[index].Index += cursor
		}
		if executionParallel > metrics.MaximumParallelWidth {
			metrics.MaximumParallelWidth = executionParallel
		}
		metrics.ExecutionAttemptCount += len(attempts)
		stats, reservationErrors, reservationParallel, err := reserveGroundhogProposalAttemptsConcurrent(ctx, table, attempts, e.workerCountFor(len(attempts)))
		if err != nil {
			return GroundhogCandidateSelection{}, err
		}
		if reservationParallel > metrics.ReservationParallelWidth {
			metrics.ReservationParallelWidth = reservationParallel
		}
		for index, attempt := range attempts {
			trace := traceFromGroundhogAttempt(attempt)
			if attempt.TerminalError != "" {
				selection.Selected = append(selection.Selected, attempt.Tx)
				metrics.ApplicationFailureCount++
				trace.Status = "selected_terminal_failure"
				trace.Reason = attempt.TerminalError
				selection.Trace = append(selection.Trace, trace)
				continue
			}
			if reservationErrors[index] != nil {
				selection.Deferred = append(selection.Deferred, attempt.Tx)
				selection.DeferredReasons[attempt.Tx.TxID] = reservationErrors[index].Error()
				metrics.ConstraintConflictCount++
				metrics.ReservationRollbackCount++
				trace.Status = "deferred"
				trace.Reason = reservationErrors[index].Error()
				selection.Trace = append(selection.Trace, trace)
				continue
			}
			selection.Selected = append(selection.Selected, attempt.Tx)
			addGroundhogReservationStats(&metrics, stats[index])
			trace.Status = "selected"
			selection.Trace = append(selection.Trace, trace)
		}
		cursor += chunkSize
	}
	// Candidates not drawn after B reached its limit remain in the mempool. They
	// were reserved by MBE's pool API only to provide a stable finite stream view.
	for ; cursor < len(candidates); cursor++ {
		item := candidates[cursor]
		selection.Deferred = append(selection.Deferred, item)
		selection.DeferredReasons[item.TxID] = "groundhog_candidate_limit"
		selection.Trace = append(selection.Trace, GroundhogTransactionTrace{Index: cursor, TxID: item.TxID, Status: "deferred", Reason: "groundhog_candidate_limit"})
	}
	metrics.SelectedTransactionCount = len(selection.Selected)
	metrics.DeferredTransactionCount = len(selection.Deferred)
	metrics.CommittedTransactionCount = len(selection.Selected) - metrics.ApplicationFailureCount
	metrics.ModifiedKeyCount = len(table.Objects)
	selection.Metrics = metrics
	return selection, nil
}

func (e *GroundhogExecutor) ExecuteBlock(ctx context.Context, b block.Block, base map[string]string) (Result, error) {
	return e.ExecuteBlockWithCommitment(ctx, b, base, nil)
}

func (e *GroundhogExecutor) ExecuteBlockWithCommitment(ctx context.Context, b block.Block, base map[string]string, baseCommitment *state.Commitment) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	executionStarted := time.Now()
	workerCount := e.workerCountFor(len(b.TxList))
	attempts, maximumParallel, err := e.executeAttempts(ctx, b, base, b.TxList, workerCount)
	if err != nil {
		return Result{}, err
	}
	metrics := newGroundhogMetrics(workerCount)
	metrics.MaximumParallelWidth = maximumParallel
	metrics.ExecutionAttemptCount = len(attempts)
	metrics.CandidateTransactionCount = len(attempts)
	metrics.SelectedTransactionCount = len(attempts)
	table := newGroundhogReservationTable(b.ShardID, base, e.OrderedSetLimit)
	e.Trace = nil

	reservationStats, reservationParallel, reserveErr := reserveGroundhogAttemptsConcurrent(ctx, table, attempts, workerCount)
	if reserveErr != nil {
		metrics.ConstraintConflictCount++
		metrics.ReservationRollbackCount++
		e.Metrics = metrics
		return Result{}, reserveErr
	}
	metrics.ReservationParallelWidth = reservationParallel
	metrics.ReservationEngine = "object_key_concurrent_reserve_revert_commit"
	for index, attempt := range attempts {
		trace := traceFromGroundhogAttempt(attempt)
		if attempt.TerminalError != "" {
			metrics.ApplicationFailureCount++
			trace.Status = "terminal_failure"
			trace.Reason = attempt.TerminalError
			e.Trace = append(e.Trace, trace)
			continue
		}
		addGroundhogReservationStats(&metrics, reservationStats[index])
		trace.Status = "committed"
		e.Trace = append(e.Trace, trace)
	}

	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	materializeStarted := time.Now()
	working, err := table.materialize()
	materializationDuration := time.Since(materializeStarted)
	if err != nil {
		return Result{}, err
	}
	metrics.ModifiedKeyCount = len(table.Objects)
	metrics.CommittedTransactionCount = len(b.TxList) - metrics.ApplicationFailureCount
	e.Metrics = metrics

	transactionExecutionDuration := time.Since(executionStarted) - materializationDuration
	commitmentStarted := time.Now()
	commitment := state.CloneOrBuild(baseCommitment, base)
	beforeRoot := commitment.Root()
	for _, update := range stateDelta(base, working) {
		commitment.Set(update.Key, update.Value)
	}
	afterRoot := commitment.Root()
	stateCommitmentDuration := time.Since(commitmentStarted)
	metrics.TransactionExecutionMS = transactionExecutionDuration.Milliseconds()
	metrics.DeterministicMaterializationMS = materializationDuration.Milliseconds()
	metrics.StateCommitmentMS = stateCommitmentDuration.Milliseconds()
	e.Metrics = metrics
	result := Result{
		BlockHash:                      b.BlockHash,
		Height:                         b.Height,
		StateRootBefore:                beforeRoot,
		StateRootAfter:                 afterRoot,
		Deterministic:                  true,
		EVMExecution:                   false,
		FabricExecution:                false,
		StateUpdates:                   copySnapshot(working),
		BlockExecutorID:                GroundhogBlockExecutorID,
		ExecutorVersion:                GroundhogBlockExecutorVersion,
		WorkerCount:                    workerCount,
		SerialEquivalent:               false,
		TransactionExecutionMS:         metrics.TransactionExecutionMS,
		DeterministicMaterializationMS: metrics.DeterministicMaterializationMS,
		StateCommitmentMS:              metrics.StateCommitmentMS,
		StateRootVersion:               state.CommitmentVersion,
	}
	for _, attempt := range attempts {
		receipt := Receipt{
			TxID:             attempt.Tx.TxID,
			BlockHash:        b.BlockHash,
			Height:           b.Height,
			Success:          attempt.TerminalError == "",
			Error:            attempt.TerminalError,
			ExecutionCost:    1,
			StateKeys:        append([]string(nil), attempt.Tx.StateKeys...),
			StateRootAfterTx: afterRoot,
		}
		writes := map[string]string{}
		if receipt.Success {
			for _, modification := range attempt.Modifications {
				writes[modification.Key] = working[groundhogQualifiedKey(b.ShardID, modification.Key)]
			}
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
		delta := TxDelta{
			TxID:          attempt.Tx.TxID,
			OriginalIndex: attempt.Index,
			ReadSet:       append([]ReadObservation(nil), attempt.Reads...),
			WriteSet:      writes,
			Receipt:       receipt,
			Success:       receipt.Success,
			Error:         receipt.Error,
		}
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, receipt)
	}
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	result.StateDelta = stateDelta(base, working)
	result.Plan = buildGroundhogPlan(b, declaredAccessSet(b.TxList), workerCount, metrics, table)
	result.PlanDigest = result.Plan.PlanDigest
	return result, nil
}

func newGroundhogMetrics(workerCount int) GroundhogMetrics {
	return GroundhogMetrics{
		WorkerCount:                workerCount,
		FallbackMode:               "disabled",
		SnapshotSemantics:          "block_start_snapshot",
		TypedModificationSemantics: "groundhog_typed_commutative_v1",
		ReservationEngine:          "object_key_concurrent_reserve_revert_commit",
	}
}

func (e *GroundhogExecutor) workerCountFor(size int) int {
	workers := e.WorkerCount
	if workers < 1 {
		workers = 1
	}
	if size > 0 && workers > size {
		workers = size
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

func (e *GroundhogExecutor) executeAttempts(ctx context.Context, b block.Block, base map[string]string, items []tx.SignedTransaction, workerCount int) ([]groundhogAttempt, int, error) {
	attempts := make([]groundhogAttempt, len(items))
	if len(items) == 0 {
		return attempts, 0, nil
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	var active int64
	var maximum int64
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
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
				attempts[index] = e.interpretTransaction(b, base, index, items[index])
				atomic.AddInt64(&active, -1)
			}
		}()
	}
	for index := range items {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, int(atomic.LoadInt64(&maximum)), ctx.Err()
		case jobs <- index:
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, int(atomic.LoadInt64(&maximum)), err
	}
	return attempts, int(atomic.LoadInt64(&maximum)), nil
}

func (e *GroundhogExecutor) interpretTransaction(b block.Block, base map[string]string, index int, item tx.SignedTransaction) groundhogAttempt {
	attempt := groundhogAttempt{Index: index, Tx: item}
	readSeen := map[string]bool{}
	read := func(key string) string {
		if key == "" {
			return ""
		}
		value := base[groundhogQualifiedKey(b.ShardID, key)]
		if !readSeen[key] {
			attempt.Reads = append(attempt.Reads, ReadObservation{Key: key, Value: value, ValueDigest: digestValue(value), Source: "groundhog_block_start_snapshot"})
			readSeen[key] = true
		}
		return value
	}

	if isCrossShardTargetCommit(item, b.ShardID) || strings.HasPrefix(item.Payload, "v5_cross:") {
		attempt.TerminalError = "groundhog_cross_shard_unsupported"
		return attempt
	}

	pureCommutative := isPureCommutativeDelta(item.AccessList)
	if !pureCommutative && !isDirectAccessTransaction(item) {
		if item.Value <= 0 {
			attempt.TerminalError = "invalid_value"
			return attempt
		}
		senderKey := "balance:" + item.Sender
		receiverKey := "balance:" + item.Receiver
		senderBalance := groundhogParseInt(read(senderKey), e.DefaultInitialBalance)
		receiverBalance := groundhogParseInt(read(receiverKey), 0)
		if senderBalance < item.Value {
			attempt.TerminalError = "insufficient_balance"
			return attempt
		}
		attempt.Modifications = append(attempt.Modifications,
			groundhogModification{Key: senderKey, Kind: groundhogIntegerAdd, BaseInt: senderBalance, Delta: -item.Value},
			groundhogModification{Key: receiverKey, Kind: groundhogIntegerAdd, BaseInt: receiverBalance, Delta: item.Value},
		)
		if item.Nonce == ^uint64(0) {
			attempt.TerminalError = "groundhog_nonce_overflow"
			attempt.Modifications = nil
			return attempt
		}
		replayKey := "groundhog:replay:" + item.Sender
		_ = read(replayKey)
		attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: replayKey, Kind: groundhogOrderedSetInsert, Tag: item.Nonce + 1, Hash: item.TxID})
	}

	for _, access := range item.AccessList {
		if access.Key == "" {
			continue
		}
		value := read(access.Key)
		if access.Mode == tx.AccessRead {
			continue
		}
		if strings.HasPrefix(access.Key, "balance:") || strings.HasPrefix(access.Key, "nonce:") {
			// Transfer balances and Groundhog replay protection are mapped above.
			// Legacy nonce writes are intentionally not materialized because an
			// unordered Groundhog block has no predecessor transaction nonce.
			continue
		}
		switch access.Mode {
		case tx.AccessCommutativeDelta:
			baseValue := groundhogParseInt(value, 0)
			if baseValue < 0 || (access.Delta < 0 && groundhogWouldUnderflowNonnegative(baseValue, access.Delta)) {
				attempt.TerminalError = "groundhog_individual_nonnegative_constraint"
				attempt.Modifications = nil
				return attempt
			}
			attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: access.Key, Kind: groundhogIntegerAdd, BaseInt: baseValue, Delta: access.Delta})
		case tx.AccessWrite, tx.AccessReadWrite:
			switch access.UpdateSemantics {
			case "ordered_set_insert":
				if item.Nonce == ^uint64(0) {
					attempt.TerminalError = "groundhog_nonce_overflow"
					attempt.Modifications = nil
					return attempt
				}
				attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: access.Key, Kind: groundhogOrderedSetInsert, Tag: item.Nonce + 1, Hash: tx.SemanticID(item)})
			case "ordered_set_clear":
				threshold := uint64(0)
				if access.Delta > 0 {
					threshold = uint64(access.Delta)
				}
				attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: access.Key, Kind: groundhogOrderedSetClear, Threshold: threshold})
			case "ordered_set_increase_limit":
				increase := int(access.Delta)
				if increase < 0 {
					attempt.TerminalError = "groundhog_invalid_ordered_set_limit_increase"
					attempt.Modifications = nil
					return attempt
				}
				attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: access.Key, Kind: groundhogOrderedSetGrow, Increase: increase})
			default:
				target := stableDigest(struct {
					LogicalTxID string `json:"logical_tx_id"`
					Key         string `json:"key"`
					Payload     string `json:"payload"`
					Semantics   string `json:"semantics"`
				}{tx.SemanticID(item), access.Key, item.Payload, access.UpdateSemantics})
				attempt.Modifications = append(attempt.Modifications, groundhogModification{Key: access.Key, Kind: groundhogBytesSet, Bytes: target})
			}
		}
	}

	normalized, normalizeErr := normalizeGroundhogModifications(attempt.Modifications)
	if normalizeErr != nil {
		attempt.TerminalError = normalizeErr.Error()
		attempt.Modifications = nil
		return attempt
	}
	attempt.Modifications = normalized
	sort.Slice(attempt.Reads, func(i, j int) bool { return attempt.Reads[i].Key < attempt.Reads[j].Key })
	return attempt
}

func normalizeGroundhogModifications(input []groundhogModification) ([]groundhogModification, error) {
	type integerAccumulation struct {
		BaseInt  int64
		Positive int64
		Negative int64
		Seen     bool
	}
	ints := map[string]integerAccumulation{}
	bytesValues := map[string]groundhogModification{}
	others := []groundhogModification{}
	for _, modification := range input {
		switch modification.Kind {
		case groundhogIntegerAdd:
			accumulation := ints[modification.Key]
			if accumulation.Seen && accumulation.BaseInt != modification.BaseInt {
				return nil, fmt.Errorf("groundhog_transaction_base_value_mismatch:%s", modification.Key)
			}
			if !accumulation.Seen {
				accumulation.BaseInt = modification.BaseInt
				accumulation.Seen = true
			}
			var next int64
			var overflow bool
			if modification.Delta < 0 {
				next, overflow = groundhogAddInt64(accumulation.Negative, modification.Delta)
				if overflow {
					return nil, fmt.Errorf("groundhog_transaction_integer_overflow:%s", modification.Key)
				}
				accumulation.Negative = next
			} else {
				next, overflow = groundhogAddInt64(accumulation.Positive, modification.Delta)
				if overflow {
					return nil, fmt.Errorf("groundhog_transaction_integer_overflow:%s", modification.Key)
				}
				accumulation.Positive = next
			}
			ints[modification.Key] = accumulation
		case groundhogBytesSet:
			if existing, ok := bytesValues[modification.Key]; ok && existing.Bytes != modification.Bytes {
				return nil, fmt.Errorf("groundhog_transaction_bytes_conflict:%s", modification.Key)
			}
			bytesValues[modification.Key] = modification
		default:
			others = append(others, modification)
		}
	}
	out := make([]groundhogModification, 0, len(ints)*2+len(bytesValues)+len(others))
	for key, accumulation := range ints {
		if accumulation.BaseInt < 0 || groundhogWouldUnderflowNonnegative(accumulation.BaseInt, accumulation.Negative) {
			return nil, fmt.Errorf("groundhog_transaction_nonnegative_constraint:%s", key)
		}
		if _, overflow := groundhogFinalIntValue(accumulation.BaseInt, accumulation.Negative, accumulation.Positive); overflow {
			return nil, fmt.Errorf("groundhog_transaction_integer_overflow:%s", key)
		}
		if accumulation.Negative != 0 {
			out = append(out, groundhogModification{Key: key, Kind: groundhogIntegerAdd, BaseInt: accumulation.BaseInt, Delta: accumulation.Negative})
		}
		if accumulation.Positive != 0 {
			out = append(out, groundhogModification{Key: key, Kind: groundhogIntegerAdd, BaseInt: accumulation.BaseInt, Delta: accumulation.Positive})
		}
		if accumulation.Negative == 0 && accumulation.Positive == 0 {
			out = append(out, groundhogModification{Key: key, Kind: groundhogIntegerAdd, BaseInt: accumulation.BaseInt})
		}
	}
	for _, modification := range bytesValues {
		out = append(out, modification)
	}
	out = append(out, others...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		// Negative reservations are checked before additions.  Groundhog keeps
		// the two aggregates separately, so same-block credits cannot fund
		// same-block withdrawals.
		if out[i].Kind == groundhogIntegerAdd && (out[i].Delta < 0) != (out[j].Delta < 0) {
			return out[i].Delta < 0
		}
		if out[i].Tag != out[j].Tag {
			return out[i].Tag < out[j].Tag
		}
		return out[i].Hash < out[j].Hash
	})
	return out, nil
}

func (t *groundhogReservationTable) lockModificationKeys(modifications []groundhogModification) func() {
	keys := make([]string, 0, len(modifications))
	seen := map[string]bool{}
	for _, modification := range modifications {
		if modification.Key != "" && !seen[modification.Key] {
			seen[modification.Key] = true
			keys = append(keys, modification.Key)
		}
	}
	sort.Strings(keys)
	locks := make([]*sync.Mutex, 0, len(keys))
	t.keyLocksMu.Lock()
	for _, key := range keys {
		lock := t.keyLocks[key]
		if lock == nil {
			lock = &sync.Mutex{}
			t.keyLocks[key] = lock
		}
		locks = append(locks, lock)
	}
	t.keyLocksMu.Unlock()
	for _, lock := range locks {
		lock.Lock()
	}
	return func() {
		for index := len(locks) - 1; index >= 0; index-- {
			locks[index].Unlock()
		}
	}
}

func (t *groundhogReservationTable) object(key string) (*groundhogObject, bool) {
	t.objectsMu.RLock()
	defer t.objectsMu.RUnlock()
	object, ok := t.Objects[key]
	return object, ok
}
func (t *groundhogReservationTable) setObject(key string, object *groundhogObject) {
	t.objectsMu.Lock()
	t.Objects[key] = object
	t.objectsMu.Unlock()
}
func (t *groundhogReservationTable) deleteObject(key string) {
	t.objectsMu.Lock()
	delete(t.Objects, key)
	t.objectsMu.Unlock()
}

func (t *groundhogReservationTable) reserveTransaction(modifications []groundhogModification) (groundhogReservationStats, error) {
	unlock := t.lockModificationKeys(modifications)
	defer unlock()
	backups := map[string]*groundhogObject{}
	existed := map[string]bool{}
	stats := groundhogReservationStats{}
	for _, modification := range modifications {
		if _, seen := backups[modification.Key]; !seen {
			object, ok := t.object(modification.Key)
			existed[modification.Key] = ok
			if ok {
				backups[modification.Key] = cloneGroundhogObject(object)
			} else {
				backups[modification.Key] = nil
			}
		}
		if err := t.applyModification(modification); err != nil {
			for key, backup := range backups {
				if existed[key] {
					t.setObject(key, backup)
				} else {
					t.deleteObject(key)
				}
			}
			return groundhogReservationStats{}, err
		}
		stats.Reservations++
		switch modification.Kind {
		case groundhogIntegerAdd:
			stats.Integers++
		case groundhogBytesSet:
			stats.Bytes++
		default:
			stats.OrderedSets++
		}
	}
	return stats, nil
}

type groundhogReservationResult struct {
	Index int
	Stats groundhogReservationStats
	Err   error
}

// reserveGroundhogProposalAttemptsConcurrent is the leader-side Algorithm 2
// reserve/rollback phase. Individual conflicts reject only that candidate;
// successful reservations remain committed to the in-progress proposal.
func reserveGroundhogProposalAttemptsConcurrent(ctx context.Context, table *groundhogReservationTable, attempts []groundhogAttempt, workerCount int) ([]groundhogReservationStats, []error, int, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	jobs := make(chan int, len(attempts))
	results := make(chan groundhogReservationResult, len(attempts))
	var wg sync.WaitGroup
	var inflight int64
	var maximum int64
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				attempt := attempts[index]
				if attempt.TerminalError != "" {
					results <- groundhogReservationResult{Index: index}
					continue
				}
				current := atomic.AddInt64(&inflight, 1)
				for {
					observed := atomic.LoadInt64(&maximum)
					if current <= observed || atomic.CompareAndSwapInt64(&maximum, observed, current) {
						break
					}
				}
				stats, err := table.reserveTransaction(attempt.Modifications)
				atomic.AddInt64(&inflight, -1)
				results <- groundhogReservationResult{Index: index, Stats: stats, Err: err}
			}
		}()
	}
	for index := range attempts {
		jobs <- index
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()
	stats := make([]groundhogReservationStats, len(attempts))
	errs := make([]error, len(attempts))
	for result := range results {
		stats[result.Index] = result.Stats
		errs[result.Index] = result.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, int(maximum), err
	}
	return stats, errs, int(maximum), nil
}

func reserveGroundhogAttemptsConcurrent(ctx context.Context, table *groundhogReservationTable, attempts []groundhogAttempt, workerCount int) ([]groundhogReservationStats, int, error) {
	if workerCount < 1 {
		workerCount = 1
	}
	jobs := make(chan int, len(attempts))
	results := make(chan groundhogReservationResult, len(attempts))
	var wg sync.WaitGroup
	var inflight int64
	var maximum int64
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				attempt := attempts[index]
				if attempt.TerminalError != "" {
					results <- groundhogReservationResult{Index: index}
					continue
				}
				current := atomic.AddInt64(&inflight, 1)
				for {
					observed := atomic.LoadInt64(&maximum)
					if current <= observed || atomic.CompareAndSwapInt64(&maximum, observed, current) {
						break
					}
				}
				stats, err := table.reserveTransaction(attempt.Modifications)
				atomic.AddInt64(&inflight, -1)
				results <- groundhogReservationResult{Index: index, Stats: stats, Err: err}
			}
		}()
	}
	for index := range attempts {
		jobs <- index
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()
	stats := make([]groundhogReservationStats, len(attempts))
	errors := make([]error, len(attempts))
	for result := range results {
		stats[result.Index] = result.Stats
		errors[result.Index] = result.Err
	}
	if err := ctx.Err(); err != nil {
		return nil, int(maximum), err
	}
	for _, err := range errors {
		if err != nil {
			// The producer should never propose a conflicting fixed Groundhog block.
			// Recompute only the invalid/error path in consensus order so every
			// replica reports the same canonical conflicting transaction.  The
			// valid performance path stays concurrent.
			canonical := newGroundhogReservationTable(table.ShardID, table.Base, table.OrderedSetLimit)
			for index, attempt := range attempts {
				if attempt.TerminalError != "" {
					continue
				}
				if _, canonicalErr := canonical.reserveTransaction(attempt.Modifications); canonicalErr != nil {
					key, reason := groundhogConflictParts(canonicalErr.Error())
					return nil, int(maximum), GroundhogBlockConflictError{TxID: attempt.Tx.TxID, Key: key, Reason: reason}
				}
				_ = index
			}
			return nil, int(maximum), fmt.Errorf("groundhog concurrent reservation rejected a serially valid fixed block")
		}
	}
	return stats, int(maximum), nil
}

func (t *groundhogReservationTable) applyModification(modification groundhogModification) error {
	object, _ := t.object(modification.Key)
	if object == nil {
		object = t.newObject(modification)
		t.setObject(modification.Key, object)
	}
	if !groundhogKindsCompatible(object.Kind, modification.Kind) {
		return fmt.Errorf("groundhog_constraint_conflict:%s:mixed_object_types", modification.Key)
	}
	switch modification.Kind {
	case groundhogIntegerAdd:
		if object.BaseInt != modification.BaseInt {
			return fmt.Errorf("groundhog_constraint_conflict:%s:base_value_mismatch", modification.Key)
		}
		if object.BaseInt < 0 {
			return fmt.Errorf("groundhog_constraint_conflict:%s:negative_base_value", modification.Key)
		}
		if modification.Delta < 0 {
			next, overflow := groundhogAddInt64(object.NegativeDelta, modification.Delta)
			if overflow {
				return fmt.Errorf("groundhog_constraint_conflict:%s:integer_overflow", modification.Key)
			}
			if groundhogWouldUnderflowNonnegative(object.BaseInt, next) {
				return fmt.Errorf("groundhog_constraint_conflict:%s:nonnegative_constraint", modification.Key)
			}
			if _, overflow := groundhogFinalIntValue(object.BaseInt, next, object.PositiveDelta); overflow {
				return fmt.Errorf("groundhog_constraint_conflict:%s:integer_overflow", modification.Key)
			}
			object.NegativeDelta = next
		} else {
			next, overflow := groundhogAddInt64(object.PositiveDelta, modification.Delta)
			if overflow {
				return fmt.Errorf("groundhog_constraint_conflict:%s:integer_overflow", modification.Key)
			}
			if _, overflow := groundhogFinalIntValue(object.BaseInt, object.NegativeDelta, next); overflow {
				return fmt.Errorf("groundhog_constraint_conflict:%s:integer_overflow", modification.Key)
			}
			object.PositiveDelta = next
		}
	case groundhogBytesSet:
		if object.Bytes != "" && object.Bytes != modification.Bytes {
			return fmt.Errorf("groundhog_constraint_conflict:%s:different_bytes_values", modification.Key)
		}
		object.Bytes = modification.Bytes
	case groundhogOrderedSetGrow:
		if modification.Increase < 0 {
			return fmt.Errorf("groundhog_constraint_conflict:%s:negative_limit_increase", modification.Key)
		}
		if modification.Increase > GroundhogOrderedSetMaximumLimit-object.Set.Limit {
			return fmt.Errorf("groundhog_constraint_conflict:%s:ordered_set_limit_overflow", modification.Key)
		}
		object.Set.Limit += modification.Increase
	case groundhogOrderedSetClear:
		if modification.Threshold > object.Set.Cleared {
			object.Set.Cleared = modification.Threshold
		}
		// Keep committed and newly inserted hashes in the reservation index.
		// Clear only affects materialization; it does not erase duplicate or
		// capacity evidence during the current unordered block.
	case groundhogOrderedSetInsert:
		if _, exists := object.SetByHash[modification.Hash]; exists {
			return fmt.Errorf("groundhog_constraint_conflict:%s:duplicate_hash", modification.Key)
		}
		if len(object.SetByHash) >= object.Set.Limit {
			return fmt.Errorf("groundhog_constraint_conflict:%s:ordered_set_capacity", modification.Key)
		}
		object.SetByHash[modification.Hash] = modification.Tag
	default:
		return fmt.Errorf("groundhog_constraint_conflict:%s:unknown_modification", modification.Key)
	}
	return nil
}

func (t *groundhogReservationTable) newObject(modification groundhogModification) *groundhogObject {
	object := &groundhogObject{Kind: modification.Kind}
	if modification.Kind == groundhogOrderedSetInsert || modification.Kind == groundhogOrderedSetClear || modification.Kind == groundhogOrderedSetGrow {
		object.Kind = groundhogOrderedSetInsert
		object.Set = decodeGroundhogOrderedSet(t.Base[groundhogQualifiedKey(t.ShardID, modification.Key)], t.OrderedSetLimit)
		object.SetByHash = map[string]uint64{}
		for _, entry := range object.Set.Entries {
			if entry.Tag > object.Set.Cleared {
				object.SetByHash[entry.Hash] = entry.Tag
			}
		}
		return object
	}
	if modification.Kind == groundhogIntegerAdd {
		object.BaseInt = modification.BaseInt
	}
	return object
}

func groundhogKindsCompatible(existing, incoming groundhogModificationKind) bool {
	if existing == incoming {
		return true
	}
	return existing == groundhogOrderedSetInsert && (incoming == groundhogOrderedSetClear || incoming == groundhogOrderedSetGrow)
}

func (t *groundhogReservationTable) materialize() (map[string]string, error) {
	working := copySnapshot(t.Base)
	keys := make([]string, 0, len(t.Objects))
	for key := range t.Objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		object := t.Objects[key]
		qualified := groundhogQualifiedKey(t.ShardID, key)
		switch object.Kind {
		case groundhogIntegerAdd:
			value, overflow := groundhogFinalIntValue(object.BaseInt, object.NegativeDelta, object.PositiveDelta)
			if overflow {
				return nil, fmt.Errorf("groundhog materialization integer constraint key=%s", key)
			}
			working[qualified] = strconv.FormatInt(value, 10)
		case groundhogBytesSet:
			working[qualified] = object.Bytes
		case groundhogOrderedSetInsert:
			entries := make([]groundhogOrderedEntry, 0, len(object.SetByHash))
			for hash, tag := range object.SetByHash {
				if tag > object.Set.Cleared {
					entries = append(entries, groundhogOrderedEntry{Tag: tag, Hash: hash})
				}
			}
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].Tag != entries[j].Tag {
					return entries[i].Tag < entries[j].Tag
				}
				return entries[i].Hash < entries[j].Hash
			})
			object.Set.Entries = entries
			payload, err := json.Marshal(object.Set)
			if err != nil {
				return nil, err
			}
			working[qualified] = string(payload)
		default:
			return nil, fmt.Errorf("groundhog materialization unknown object kind %s", object.Kind)
		}
	}
	return working, nil
}

func decodeGroundhogOrderedSet(value string, defaultLimit int) groundhogOrderedSet {
	if defaultLimit < 1 {
		defaultLimit = GroundhogOrderedSetInitialLimit
	}
	if defaultLimit > GroundhogOrderedSetMaximumLimit {
		defaultLimit = GroundhogOrderedSetMaximumLimit
	}
	out := groundhogOrderedSet{Limit: defaultLimit}
	if strings.TrimSpace(value) != "" {
		_ = json.Unmarshal([]byte(value), &out)
	}
	if out.Limit < 1 {
		out.Limit = defaultLimit
	}
	if out.Limit > GroundhogOrderedSetMaximumLimit {
		out.Limit = GroundhogOrderedSetMaximumLimit
	}
	return out
}

func cloneGroundhogObject(input *groundhogObject) *groundhogObject {
	if input == nil {
		return nil
	}
	out := *input
	out.Set.Entries = append([]groundhogOrderedEntry(nil), input.Set.Entries...)
	out.SetByHash = map[string]uint64{}
	for key, value := range input.SetByHash {
		out.SetByHash[key] = value
	}
	return &out
}

func addGroundhogReservationStats(metrics *GroundhogMetrics, stats groundhogReservationStats) {
	metrics.ReservationCount += stats.Reservations
	metrics.IntegerMergeCount += stats.Integers
	metrics.BytesMergeCount += stats.Bytes
	metrics.OrderedSetMergeCount += stats.OrderedSets
}

func groundhogAddInt64(left, right int64) (int64, bool) {
	result := left + right
	if (right > 0 && result < left) || (right < 0 && result > left) {
		return 0, true
	}
	return result, false
}

func groundhogWouldUnderflowNonnegative(base, negativeDelta int64) bool {
	if base < 0 || negativeDelta > 0 {
		return true
	}
	value, overflow := groundhogAddInt64(base, negativeDelta)
	return overflow || value < 0
}

func groundhogFinalIntValue(base, negativeDelta, positiveDelta int64) (int64, bool) {
	if groundhogWouldUnderflowNonnegative(base, negativeDelta) || positiveDelta < 0 {
		return 0, true
	}
	value, overflow := groundhogAddInt64(base, negativeDelta)
	if overflow {
		return 0, true
	}
	return groundhogAddInt64(value, positiveDelta)
}

func groundhogParseInt(value string, fallback int64) int64 {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func groundhogQualifiedKey(shardID, key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return shardID + "::" + key
}

func traceFromGroundhogAttempt(attempt groundhogAttempt) GroundhogTransactionTrace {
	trace := GroundhogTransactionTrace{Index: attempt.Index, TxID: attempt.Tx.TxID}
	for _, read := range attempt.Reads {
		trace.ReadKeys = append(trace.ReadKeys, read.Key)
	}
	seen := map[string]bool{}
	for _, modification := range attempt.Modifications {
		if !seen[modification.Key] {
			trace.ModificationKeys = append(trace.ModificationKeys, modification.Key)
			seen[modification.Key] = true
		}
	}
	sort.Strings(trace.ReadKeys)
	sort.Strings(trace.ModificationKeys)
	return trace
}

func groundhogConflictParts(message string) (string, string) {
	const prefix = "groundhog_constraint_conflict:"
	remainder := strings.TrimPrefix(message, prefix)
	if remainder == message {
		return "", message
	}
	separator := strings.LastIndex(remainder, ":")
	if separator <= 0 || separator+1 >= len(remainder) {
		return "", remainder
	}
	return remainder[:separator], remainder[separator+1:]
}

func buildGroundhogPlan(b block.Block, declared AccessSet, workerCount int, metrics GroundhogMetrics, table *groundhogReservationTable) ExecutionPlan {
	ids := make([]string, 0, len(b.TxList))
	indexes := make([]int, 0, len(b.TxList))
	for index, item := range b.TxList {
		ids = append(ids, item.TxID)
		indexes = append(indexes, index)
	}
	modifiedKeys := make([]string, 0, len(table.Objects))
	for key := range table.Objects {
		modifiedKeys = append(modifiedKeys, key)
	}
	sort.Strings(modifiedKeys)
	plan := ExecutionPlan{
		EngineID:                GroundhogBlockExecutorID,
		EngineVersion:           GroundhogBlockExecutorVersion,
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
		ExecutionPlan
		SnapshotSemantics          string   `json:"snapshot_semantics"`
		TypedModificationSemantics string   `json:"typed_modification_semantics"`
		ModifiedKeys               []string `json:"modified_keys"`
		FallbackMode               string   `json:"fallback_mode"`
	}{plan, metrics.SnapshotSemantics, metrics.TypedModificationSemantics, modifiedKeys, metrics.FallbackMode})
	return plan
}
