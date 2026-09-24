package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type calvinBlockExecutor struct {
	basicPlugin
	mode string
}

type calvinLockMode uint8

const (
	calvinSharedLock calvinLockMode = iota + 1
	calvinExclusiveLock
)

type calvinLockRequest struct {
	key      string
	txIndex  int
	mode     calvinLockMode
	granted  bool
	released bool
}

type calvinLockManager struct {
	mu                  sync.Mutex
	queues              map[string][]*calvinLockRequest
	byTx                map[int][]*calvinLockRequest
	waitCount           []int
	participant         []bool
	enqueued            []bool
	waitStarted         []time.Time
	waitMS              []int64
	lockCount           int
	sharedCount         int
	exclCount           int
	waitEvents          int
	blockedLockRequests int
	wakeups             int
}

func newCalvinLockManager(txs []tx.SignedTransaction, localShard string, home func(string) string) (*calvinLockManager, error) {
	m := &calvinLockManager{
		queues:      map[string][]*calvinLockRequest{},
		byTx:        map[int][]*calvinLockRequest{},
		waitCount:   make([]int, len(txs)),
		participant: make([]bool, len(txs)),
		enqueued:    make([]bool, len(txs)),
		waitStarted: make([]time.Time, len(txs)),
		waitMS:      make([]int64, len(txs)),
	}
	for index, item := range txs {
		accesses, err := calvinCanonicalAccesses(item.AccessList)
		if err != nil {
			return nil, err
		}
		for _, access := range accesses {
			// localShard=="" is the Stateless Calvin global logical lock table:
			// include every declared key regardless of its persistent state home.
			if localShard != "" && home(access.Key) != localShard {
				continue
			}
			mode := calvinSharedLock
			if calvinIsWriteMode(access.Mode) {
				mode = calvinExclusiveLock
			}
			req := &calvinLockRequest{key: access.Key, txIndex: index, mode: mode}
			m.queues[access.Key] = append(m.queues[access.Key], req)
			m.byTx[index] = append(m.byTx[index], req)
			m.waitCount[index]++
			m.participant[index] = true
			m.lockCount++
			if mode == calvinSharedLock {
				m.sharedCount++
			} else {
				m.exclCount++
			}
		}
	}
	keys := make([]string, 0, len(m.queues))
	for key := range m.queues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		m.advanceKeyLocked(key, nil)
	}
	for index := range txs {
		if m.participant[index] && m.waitCount[index] > 0 {
			m.waitEvents++
			m.blockedLockRequests += m.waitCount[index]
			m.waitStarted[index] = time.Now()
		}
	}
	return m, nil
}

func (m *calvinLockManager) initialReady() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	ready := []int{}
	for index := range m.participant {
		if m.participant[index] && m.waitCount[index] == 0 && !m.enqueued[index] {
			m.enqueued[index] = true
			ready = append(ready, index)
		}
	}
	return ready
}

func (m *calvinLockManager) release(txIndex int) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.byTx[txIndex]))
	for _, req := range m.byTx[txIndex] {
		req.released = true
		keys = append(keys, req.key)
	}
	sort.Strings(keys)
	ready := []int{}
	for _, key := range keys {
		m.advanceKeyLocked(key, &ready)
	}
	return ready
}

func (m *calvinLockManager) advanceKeyLocked(key string, ready *[]int) {
	queue := m.queues[key]
	first := -1
	for index, req := range queue {
		if !req.released {
			first = index
			break
		}
	}
	if first < 0 {
		return
	}
	grant := func(req *calvinLockRequest) {
		if req.granted || req.released {
			return
		}
		req.granted = true
		m.waitCount[req.txIndex]--
		if m.waitCount[req.txIndex] == 0 && m.participant[req.txIndex] && !m.enqueued[req.txIndex] {
			// Initial grant discovery calls advanceKeyLocked with ready=nil. Do not
			// mark the transaction enqueued until initialReady actually publishes it.
			if ready != nil {
				m.enqueued[req.txIndex] = true
			}
			if !m.waitStarted[req.txIndex].IsZero() {
				m.waitMS[req.txIndex] += time.Since(m.waitStarted[req.txIndex]).Milliseconds()
				m.wakeups++
			}
			if ready != nil {
				*ready = append(*ready, req.txIndex)
			}
		}
	}
	if queue[first].mode == calvinExclusiveLock {
		grant(queue[first])
		return
	}
	for index := first; index < len(queue); index++ {
		req := queue[index]
		if req.released {
			continue
		}
		if req.mode == calvinExclusiveLock {
			break
		}
		grant(req)
	}
}

func (m *calvinLockManager) participantCount() int {
	count := 0
	for _, value := range m.participant {
		if value {
			count++
		}
	}
	return count
}

func calvinCanonicalAccesses(accesses []tx.AccessItem) ([]tx.AccessItem, error) {
	if len(accesses) == 0 {
		return nil, fmt.Errorf("calvin_access_violation: signed AccessList is required")
	}
	seen := map[string]bool{}
	out := make([]tx.AccessItem, 0, len(accesses))
	for _, access := range accesses {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			return nil, fmt.Errorf("calvin_access_violation: empty access key")
		}
		if seen[key] {
			return nil, fmt.Errorf("calvin_access_violation: duplicate access key %s", key)
		}
		seen[key] = true
		switch access.Mode {
		case tx.AccessRead, tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		default:
			return nil, fmt.Errorf("calvin_access_violation: unsupported access mode %s for %s", access.Mode, key)
		}
		copy := access
		copy.Key = key
		out = append(out, copy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func calvinIsWriteMode(mode tx.AccessMode) bool {
	switch mode {
	case tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		return true
	default:
		return false
	}
}

func calvinNeedsReadValue(mode tx.AccessMode) bool {
	switch mode {
	case tx.AccessRead, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		return true
	default:
		return false
	}
}

type calvinTxTopology struct {
	ReadHomes      []string
	ActiveHomes    []string
	ExecutionHomes []string
	AllHomes       []string
	OutcomeHome    string
	ReadKeysByHome map[string][]string
}

func calvinTopology(item tx.SignedTransaction, home func(string) string) (calvinTxTopology, error) {
	accesses, err := calvinCanonicalAccesses(item.AccessList)
	if err != nil {
		return calvinTxTopology{}, err
	}
	reads := map[string]bool{}
	active := map[string]bool{}
	all := map[string]bool{}
	readKeys := map[string][]string{}
	for _, access := range accesses {
		shardID := home(access.Key)
		if shardID == "" {
			return calvinTxTopology{}, fmt.Errorf("calvin has no state home for key %s", access.Key)
		}
		all[shardID] = true
		if calvinNeedsReadValue(access.Mode) {
			reads[shardID] = true
			readKeys[shardID] = append(readKeys[shardID], access.Key)
		}
		if calvinIsWriteMode(access.Mode) {
			active[shardID] = true
		}
	}
	for shardID := range readKeys {
		sort.Strings(readKeys[shardID])
	}
	readHomes := sortedBoolKeys(reads)
	activeHomes := sortedBoolKeys(active)
	executionHomes := append([]string(nil), activeHomes...)
	// Calvin's public source defines active participants as writers. For a pure
	// read-only transaction there is no writer to own the MBE terminal receipt,
	// so choose the first deterministic reader as the execution/outcome home.
	// This is an MBE lifecycle adaptation; it does not add writes or change locks.
	if len(executionHomes) == 0 && len(readHomes) > 0 {
		executionHomes = []string{readHomes[0]}
	}
	if len(executionHomes) == 0 {
		return calvinTxTopology{}, fmt.Errorf("calvin transaction %s has no execution participant", item.TxID)
	}
	return calvinTxTopology{
		ReadHomes: readHomes, ActiveHomes: activeHomes, ExecutionHomes: executionHomes,
		AllHomes: sortedBoolKeys(all), OutcomeHome: executionHomes[0], ReadKeysByHome: readKeys,
	}, nil
}

type calvinWorkerResult struct {
	index                    int
	delta                    execution.TxDelta
	receipt                  execution.Receipt
	localWrites              map[string]string
	localReadCount           int
	remoteReads              int
	remoteMessages           int
	remotePhysicalMessages   int
	remoteWaitMS             int64
	statelessBlockStartReads int
	statelessExactReads      int
	statelessFetchCount      int
	statelessFetchPhysical   int
	statelessWriteCount      int
	statelessWritePhysical   int
	statelessWriteWaitMS     int64
	outcomeMessages          int
	outcomePhysical          int
	outcomeWaitMS            int64
	executed                 bool
	err                      error
}

type calvinExecutionLimiter struct {
	slots   chan struct{}
	tracker *fixedBlockWorkerBatchTracker
}

func newCalvinExecutionLimiter(workerCount int, tracker *fixedBlockWorkerBatchTracker) *calvinExecutionLimiter {
	if workerCount < 1 {
		workerCount = 1
	}
	return &calvinExecutionLimiter{slots: make(chan struct{}, workerCount), tracker: tracker}
}

func (l *calvinExecutionLimiter) enter(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.slots <- struct{}{}:
		l.tracker.enter()
		return nil
	}
}

func (l *calvinExecutionLimiter) leave() {
	l.tracker.leave()
	<-l.slots
}

type calvinReadyQueueTracker struct {
	mu      sync.Mutex
	current int
	maximum int
}

func (t *calvinReadyQueueTracker) enqueue() {
	t.mu.Lock()
	t.current++
	if t.current > t.maximum {
		t.maximum = t.current
	}
	t.mu.Unlock()
}

func (t *calvinReadyQueueTracker) dequeue() {
	t.mu.Lock()
	if t.current > 0 {
		t.current--
	}
	t.mu.Unlock()
}

func (t *calvinReadyQueueTracker) max() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maximum
}

func calvinTimeoutFromConfig(config map[string]any, key string) time.Duration {
	raw, ok := config[key]
	if !ok {
		return 0
	}
	var ms int64
	switch value := raw.(type) {
	case int:
		ms = int64(value)
	case int64:
		ms = value
	case float64:
		ms = int64(value)
	}
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func calvinExecutionItem(item tx.SignedTransaction) tx.SignedTransaction {
	// v5_cross is the legacy MBE Relay/Finalize control payload. Calvin explicitly
	// disables that protocol; allowing SerialExecutor's target-commit shortcut to
	// see it would silently reintroduce legacy cross-shard semantics.
	copy := item
	if strings.HasPrefix(copy.Payload, "v5_cross:") {
		copy.Payload = ""
	}
	return copy
}

func (p calvinBlockExecutor) ExecuteBlock(ctx context.Context, input BlockExecutionInput) (BlockExecutionResult, error) {
	if input.CalvinStateHome == nil {
		return BlockExecutionResult{}, fmt.Errorf("calvin state-home resolver is not configured")
	}
	if p.mode != calvinStatelessMode && input.CalvinReadExchange == nil && len(input.CalvinExecutionShards) > 1 {
		return BlockExecutionResult{}, fmt.Errorf("calvin READ_RESULT exchange is not configured")
	}
	if input.CalvinOutcomeExchange == nil && (len(input.CalvinExecutionShards) > 1 || p.mode == calvinStatelessMode) {
		return BlockExecutionResult{}, fmt.Errorf("calvin outcome exchange is not configured")
	}
	if p.mode == calvinStatelessMode {
		if input.CalvinStatelessFetch == nil || input.CalvinStatelessWriteback == nil || input.CalvinStatelessCollectWritebacks == nil {
			return BlockExecutionResult{}, fmt.Errorf("calvin stateless remote-state callbacks are not configured")
		}
	}
	workerCount := configuredWorkerCount(p.config, input.WorkerCount)
	if workerCount < 1 {
		workerCount = 1
	}
	localShard := strings.TrimSpace(input.ExecutionShardID)
	if localShard == "" {
		return BlockExecutionResult{}, fmt.Errorf("calvin execution shard identity is missing")
	}
	block := input.Block
	var statelessDependencies [][]tx.StateVersionDependency
	var consensusPlan calvinConsensusPlan
	if p.mode == calvinStatelessMode {
		var planErr error
		statelessDependencies, consensusPlan, planErr = calvinConsensusDependenciesByIndex(block)
		if planErr != nil {
			return BlockExecutionResult{}, planErr
		}
	}
	lockShard := localShard
	if p.mode == calvinStatelessMode {
		// Every replica builds the same logical Calvin lock queues over the full
		// declared key set. Persistent state ownership remains partitioned.
		lockShard = ""
	}
	manager, err := newCalvinLockManager(block.TxList, lockShard, input.CalvinStateHome)
	if err != nil {
		return BlockExecutionResult{}, err
	}

	executionBase := copyStringMap(input.BaseStateSnapshot)
	if p.mode == calvinStatelessMode {
		// Do not expose resident partition state to Stateless Calvin execution.
		// Each transaction receives only values fetched from deterministic homes
		// after its Calvin locks become ready.
		executionBase = map[string]string{}
	}
	working := copyStringMap(executionBase)
	var workingMu sync.Mutex
	resultByIndex := make([]execution.TxDelta, len(block.TxList))
	receiptByIndex := make([]execution.Receipt, len(block.TxList))
	setByIndex := make([]bool, len(block.TxList))
	executedByIndex := make([]bool, len(block.TxList))

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan calvinWorkerResult, len(block.TxList))
	var coordinationWG sync.WaitGroup
	tracker := &fixedBlockWorkerBatchTracker{}
	limiter := newCalvinExecutionLimiter(workerCount, tracker)
	readyTracker := &calvinReadyQueueTracker{}
	serial := execution.NewSerialExecutor()
	launch := func(index int) {
		readyTracker.enqueue()
		coordinationWG.Add(1)
		go func() {
			defer coordinationWG.Done()
			readyTracker.dequeue()
			if p.mode == calvinStatelessMode {
				done <- p.executeStatelessCalvinParticipant(execCtx, input, serial, index, statelessDependencies[index], &workingMu, working, limiter)
			} else {
				done <- p.executeCalvinParticipant(execCtx, input, serial, index, &workingMu, working, limiter)
			}
		}()
	}

	started := time.Now()
	initial := manager.initialReady()
	for _, index := range initial {
		launch(index)
	}
	participantCount := manager.participantCount()
	completed := 0
	localReadCount := 0
	remoteReadCount := 0
	remoteMessageCount := 0
	remotePhysicalMessageCount := 0
	remoteWaitMS := int64(0)
	statelessBlockStartReads := 0
	statelessExactReads := 0
	statelessFetchCount := 0
	statelessFetchPhysical := 0
	statelessWriteCount := 0
	statelessWritePhysical := 0
	statelessWriteWaitMS := int64(0)
	outcomeMessageCount := 0
	outcomePhysicalMessageCount := 0
	outcomeWaitMS := int64(0)
	activeExecutionCount := 0
	passiveParticipantCount := 0
	multiPartitionCount := 0

	for completed < participantCount {
		select {
		case <-ctx.Done():
			cancel()
			coordinationWG.Wait()
			return BlockExecutionResult{}, ctx.Err()
		case out := <-done:
			if out.err != nil {
				cancel()
				coordinationWG.Wait()
				return BlockExecutionResult{}, out.err
			}
			item := block.TxList[out.index]
			topology, topologyErr := calvinTopology(item, input.CalvinStateHome)
			if topologyErr != nil {
				cancel()
				coordinationWG.Wait()
				return BlockExecutionResult{}, topologyErr
			}
			if len(topology.AllHomes) > 1 && topology.OutcomeHome == localShard {
				multiPartitionCount++
			}
			if out.executed {
				activeExecutionCount++
			} else {
				passiveParticipantCount++
			}
			workingMu.Lock()
			for key, value := range out.localWrites {
				working[qualifyStateKey(localShard, key)] = value
			}
			workingMu.Unlock()
			resultByIndex[out.index] = out.delta
			receiptByIndex[out.index] = out.receipt
			setByIndex[out.index] = true
			executedByIndex[out.index] = out.executed
			localReadCount += out.localReadCount
			remoteReadCount += out.remoteReads
			remoteMessageCount += out.remoteMessages
			remotePhysicalMessageCount += out.remotePhysicalMessages
			remoteWaitMS += out.remoteWaitMS
			statelessBlockStartReads += out.statelessBlockStartReads
			statelessExactReads += out.statelessExactReads
			statelessFetchCount += out.statelessFetchCount
			statelessFetchPhysical += out.statelessFetchPhysical
			statelessWriteCount += out.statelessWriteCount
			statelessWritePhysical += out.statelessWritePhysical
			statelessWriteWaitMS += out.statelessWriteWaitMS
			outcomeMessageCount += out.outcomeMessages
			outcomePhysicalMessageCount += out.outcomePhysical
			outcomeWaitMS += out.outcomeWaitMS
			completed++
			ready := manager.release(out.index)
			for _, next := range ready {
				launch(next)
			}
		}
	}
	coordinationWG.Wait()

	if p.mode != calvinStatelessMode {
		for index, item := range block.TxList {
			topology, topologyErr := calvinTopology(item, input.CalvinStateHome)
			if topologyErr != nil {
				return BlockExecutionResult{}, topologyErr
			}
			if len(input.CalvinExecutionShards) == 1 {
				if !setByIndex[index] || !executedByIndex[index] {
					return BlockExecutionResult{}, fmt.Errorf("calvin single-partition transaction %s was not executed", item.TxID)
				}
				continue
			}
			seed := CalvinTxOutcome{BlockHash: block.BlockHash, Height: block.Height, TxID: item.TxID}
			publish := topology.OutcomeHome == localShard && setByIndex[index] && executedByIndex[index]
			if publish {
				seed = calvinOutcomeFromReceipt(block.BlockHash, block.Height, topology.OutcomeHome, input.NodeID, receiptByIndex[index])
			}
			exchanged, exchangeErr := input.CalvinOutcomeExchange(ctx, CalvinOutcomeExchangeInput{
				Outcome: seed, OutcomeShard: topology.OutcomeHome, Publish: publish,
				Timeout: calvinTimeoutFromConfig(p.config, "outcome_timeout_ms"),
			})
			if exchangeErr != nil {
				return BlockExecutionResult{}, exchangeErr
			}
			outcomeMessageCount += exchanged.MessageCount
			outcomePhysicalMessageCount += exchanged.PhysicalMessageCount
			outcomeWaitMS += exchanged.WaitMS
			canonicalReceipt := calvinReceiptFromOutcome(exchanged.Outcome)
			if setByIndex[index] && executedByIndex[index] {
				localOutcome := calvinOutcomeFromReceipt(block.BlockHash, block.Height, topology.OutcomeHome, exchanged.Outcome.SenderNodeID, receiptByIndex[index])
				if !calvinOutcomesEqual(localOutcome, exchanged.Outcome) {
					return BlockExecutionResult{}, fmt.Errorf("calvin active-participant outcome mismatch tx=%s local_shard=%s outcome_shard=%s", item.TxID, localShard, topology.OutcomeHome)
				}
			}
			if !setByIndex[index] {
				resultByIndex[index] = execution.TxDelta{TxID: item.TxID, OriginalIndex: index, WriteSet: map[string]string{}}
			}
			resultByIndex[index].Receipt = canonicalReceipt
			resultByIndex[index].Success = canonicalReceipt.Success
			resultByIndex[index].Error = canonicalReceipt.Error
			receiptByIndex[index] = canonicalReceipt
			setByIndex[index] = true
		}
	}
	if p.mode == calvinStatelessMode {
		inbound, collectErr := input.CalvinStatelessCollectWritebacks(ctx, block, localShard)
		if collectErr != nil {
			return BlockExecutionResult{}, collectErr
		}
		workingMu.Lock()
		for _, row := range inbound {
			if row.OrderingNoop {
				continue
			}
			working[qualifyStateKey(localShard, row.Key)] = row.Value
		}
		workingMu.Unlock()
	}
	stateDelta := executionStateDelta(executionBase, working)
	// Stateless Calvin may execute against an AccessList projection, but durable
	// evidence must still commit to the complete local partition state. Apply the
	// resulting local delta to the full pre-block snapshot for state-root truth.
	durableWorking := copyStringMap(input.BaseStateSnapshot)
	for _, update := range stateDelta {
		durableWorking[update.Key] = update.Value
	}
	executorVersion := calvinExecutionEngineVersion
	if p.mode == calvinStatelessMode {
		executorVersion = calvinStatelessEngineVersion
	}
	result := execution.Result{
		BlockHash:        block.BlockHash,
		Height:           block.Height,
		StateRootBefore:  state.RootOfSnapshot(input.BaseStateSnapshot),
		StateRootAfter:   state.RootOfSnapshot(durableWorking),
		Receipts:         receiptByIndex,
		ReceiptRoot:      execution.ReceiptRoot(receiptByIndex),
		StateUpdates:     durableWorking,
		StateDelta:       stateDelta,
		Deterministic:    true,
		EVMExecution:     false,
		FabricExecution:  false,
		BlockExecutorID:  p.ID(),
		ExecutorVersion:  executorVersion,
		WorkerCount:      workerCount,
		TxDeltas:         resultByIndex,
		StateRootVersion: state.CommitmentVersion,
	}
	for _, receipt := range receiptByIndex {
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	plan, stateHomeMappingDigest := calvinExecutionPlan(block, workerCount, p.ID(), input.CalvinStateHome)
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	result.TransactionExecutionMS = time.Since(started).Milliseconds()

	events := calvinScheduleEvents(block.TxList, manager, localShard)
	actual := map[string]any{
		"calvin_mode":                                    p.mode,
		"calvin_global_ordering":                         true,
		"calvin_legacy_2pc_enabled":                      false,
		"calvin_lock_request_count":                      manager.lockCount,
		"calvin_shared_lock_count":                       manager.sharedCount,
		"calvin_exclusive_lock_count":                    manager.exclCount,
		"calvin_lock_wait_count":                         manager.waitEvents,
		"calvin_waiting_transaction_count":               manager.waitEvents,
		"calvin_blocked_lock_request_count":              manager.blockedLockRequests,
		"calvin_lock_wakeup_count":                       manager.wakeups,
		"calvin_lock_wait_ms":                            sumInt64(manager.waitMS),
		"calvin_ready_queue_max_depth":                   readyTracker.max(),
		"calvin_local_read_count":                        localReadCount,
		"calvin_remote_read_count":                       remoteReadCount,
		"calvin_read_result_message_count":               remoteMessageCount,
		"calvin_read_result_physical_message_count":      remotePhysicalMessageCount,
		"calvin_remote_read_wait_ms":                     remoteWaitMS,
		"calvin_outcome_message_count":                   outcomeMessageCount,
		"calvin_outcome_physical_message_count":          outcomePhysicalMessageCount,
		"calvin_outcome_wait_ms":                         outcomeWaitMS,
		"calvin_active_execution_count":                  activeExecutionCount,
		"calvin_passive_participant_count":               passiveParticipantCount,
		"calvin_multi_partition_tx_count":                multiPartitionCount,
		"calvin_execution_shard_id":                      localShard,
		"calvin_execution_shard_count":                   len(input.CalvinExecutionShards),
		"calvin_state_home_mapping_digest":               stateHomeMappingDigest,
		"calvin_cross_block_lock_ahead":                  false,
		"calvin_mbe_batch_boundary_serialization":        true,
		"calvin_coordination_waits_consume_worker_slots": false,
		"maximum_parallel_width":                         tracker.max(),
		"abort_count":                                    0,
		"reexecution_count":                              0,
		"serializable":                                   true,
		"serial_equivalent":                              true,
		"transaction_execution_ms":                       result.TransactionExecutionMS,
		"state_root_version":                             state.CommitmentVersion,
		"calvin_declared_access_source":                  "signed_access_list_only",
		"calvin_commutative_delta_lock_policy":           "exclusive_write_no_aggregation",
	}
	if p.mode == calvinStatelessMode {
		actual["calvin_stateless_adaptation"] = true
		actual["calvin_stateless_projection_key_count"] = 0
		actual["calvin_state_access_semantics"] = "signed_access_remote_home_fetch_plus_same_block_home_writeback"
		actual["calvin_stateless_full_partition_visible_to_transaction"] = false
		actual["calvin_consensus_version_plan_digest"] = consensusPlan.PlanDigest
		actual["calvin_consensus_version_binding_count"] = consensusPlan.VersionBindingCount
		actual["calvin_stateless_version_source"] = "consensus_bound_block_order"
		actual["calvin_stateless_block_start_read_count"] = statelessBlockStartReads
		actual["calvin_stateless_exact_predecessor_read_count"] = statelessExactReads
		clientVersionMetadataCount := 0
		for _, item := range block.TxList {
			if item.ExecutionRouting != nil {
				clientVersionMetadataCount += len(item.ExecutionRouting.StateVersions)
			}
		}
		actual["calvin_client_state_version_metadata_count"] = clientVersionMetadataCount
		actual["calvin_stateless_remote_fetch_count"] = statelessFetchCount
		actual["calvin_stateless_remote_fetch_physical_count"] = statelessFetchPhysical
		actual["calvin_stateless_remote_writeback_count"] = statelessWriteCount
		actual["calvin_stateless_remote_writeback_physical_count"] = statelessWritePhysical
		actual["calvin_stateless_remote_writeback_wait_ms"] = statelessWriteWaitMS
		actual["calvin_stateless_uses_metatrack_state_ready"] = false
		actual["calvin_stateless_uses_toposafe_semanticsafe"] = false
	} else {
		actual["calvin_stateless_adaptation"] = false
		actual["calvin_state_access_semantics"] = "partition_resident_state_plus_read_result_forwarding"
	}

	return BlockExecutionResult{
		ExecutionResult:        result,
		StateDelta:             stateKVsFromExecutionDelta(stateDelta),
		PlanDigest:             plan.PlanDigest,
		WorkerCount:            workerCount,
		TransactionExecutionMS: result.TransactionExecutionMS,
		StateRootVersion:       state.CommitmentVersion,
		ScheduleEvents:         events,
		ActualMetrics:          actual,
	}, nil
}

func (p calvinBlockExecutor) executeCalvinParticipant(ctx context.Context, input BlockExecutionInput, serial *execution.SerialExecutor, index int, workingMu *sync.Mutex, working map[string]string, limiter *calvinExecutionLimiter) calvinWorkerResult {
	item := input.Block.TxList[index]
	localShard := input.ExecutionShardID
	topology, err := calvinTopology(item, input.CalvinStateHome)
	if err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	accesses, err := calvinCanonicalAccesses(item.AccessList)
	if err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	localValues := map[string]string{}
	localReadCount := 0
	workingMu.Lock()
	for _, access := range accesses {
		if input.CalvinStateHome(access.Key) != localShard || !calvinNeedsReadValue(access.Mode) {
			continue
		}
		localValues[access.Key] = working[qualifyStateKey(localShard, access.Key)]
		localReadCount++
	}
	workingMu.Unlock()

	executes := containsString(topology.ExecutionHomes, localShard)
	merged := map[string]string{}
	for key, value := range localValues {
		merged[key] = value
	}
	exchangeStats := CalvinReadExchangeResult{}
	if input.CalvinReadExchange != nil {
		exchangeStats, err = input.CalvinReadExchange(ctx, CalvinReadExchangeInput{
			Result: CalvinReadResult{
				BlockHash:        input.Block.BlockHash,
				Height:           input.Block.Height,
				TxID:             item.TxID,
				ExecutionShardID: localShard,
				Values:           localValues,
			},
			RequiredReadShards:      topology.ReadHomes,
			ExpectedReadKeysByShard: topology.ReadKeysByHome,
			ActiveShards:            topology.ExecutionHomes,
			WaitForRemote:           executes,
			Timeout:                 calvinTimeoutFromConfig(p.config, "read_result_timeout_ms"),
		})
		if err != nil {
			return calvinWorkerResult{index: index, err: err}
		}
		for key, value := range exchangeStats.Values {
			merged[key] = value
		}
	}

	if !executes {
		receipt := execution.Receipt{TxID: item.TxID, BlockHash: input.Block.BlockHash, Height: input.Block.Height, Success: true, ExecutionCost: 0, StateKeys: append([]string(nil), item.StateKeys...)}
		return calvinWorkerResult{
			index:                  index,
			delta:                  execution.TxDelta{TxID: item.TxID, OriginalIndex: index, Receipt: receipt, Success: true, WriteSet: map[string]string{}},
			receipt:                receipt,
			localWrites:            map[string]string{},
			localReadCount:         localReadCount,
			remoteReads:            exchangeStats.RemoteReadCount,
			remoteMessages:         exchangeStats.MessageCount,
			remotePhysicalMessages: exchangeStats.PhysicalMessageCount,
			remoteWaitMS:           exchangeStats.WaitMS,
		}
	}

	// Every active participant executes the deterministic transaction logic over
	// the same complete read image, then materializes only writes owned by its
	// local partition, exactly matching Calvin's active-participant rule.
	txSnapshot := map[string]string{}
	for key, value := range merged {
		txSnapshot[qualifyStateKey(localShard, key)] = value
	}
	workingMu.Lock()
	for _, access := range accesses {
		if input.CalvinStateHome(access.Key) == localShard {
			qualified := qualifyStateKey(localShard, access.Key)
			if _, exists := txSnapshot[qualified]; !exists {
				txSnapshot[qualified] = working[qualified]
			}
		}
	}
	workingMu.Unlock()
	localBlock := input.Block
	localBlock.ShardID = localShard
	executionItem := calvinExecutionItem(item)
	// Remote READ_RESULT coordination is intentionally outside the bounded
	// business-execution worker slots. Calvin's original scheduler can suspend a
	// transaction waiting for remote reads while the worker continues processing
	// other work; blocking a scarce MBE worker here could create thread-pool
	// starvation even though deterministic locking itself is deadlock-free.
	if err := limiter.enter(ctx); err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	receipt, delta := serial.ExecuteTransaction(localBlock, executionItem, txSnapshot, index)
	limiter.leave()
	if err := calvinValidateActualAccess(item, delta); err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	localWrites := map[string]string{}
	for key, value := range delta.WriteSet {
		if input.CalvinStateHome(key) == localShard {
			localWrites[key] = value
		}
	}
	filtered := delta
	filtered.WriteSet = localWrites
	return calvinWorkerResult{
		index:                  index,
		delta:                  filtered,
		receipt:                receipt,
		localWrites:            localWrites,
		localReadCount:         localReadCount,
		remoteReads:            exchangeStats.RemoteReadCount,
		remoteMessages:         exchangeStats.MessageCount,
		remotePhysicalMessages: exchangeStats.PhysicalMessageCount,
		remoteWaitMS:           exchangeStats.WaitMS,
		executed:               true,
	}
}

func calvinValidateActualAccess(item tx.SignedTransaction, delta execution.TxDelta) error {
	declared, err := calvinCanonicalAccesses(item.AccessList)
	if err != nil {
		return err
	}
	byKey := map[string]tx.AccessMode{}
	for _, access := range declared {
		byKey[access.Key] = access.Mode
	}
	for _, read := range delta.ReadSet {
		mode, ok := byKey[read.Key]
		if !ok || !calvinNeedsReadValue(mode) {
			return fmt.Errorf("calvin_access_violation: tx=%s undeclared read %s", item.TxID, read.Key)
		}
	}
	for key := range delta.WriteSet {
		mode, ok := byKey[key]
		if !ok || !calvinIsWriteMode(mode) {
			return fmt.Errorf("calvin_access_violation: tx=%s undeclared write %s", item.TxID, key)
		}
	}
	return nil
}

func calvinExecutionPlan(block realblock.Block, workerCount int, engineID string, home func(string) string) (execution.ExecutionPlan, string) {
	ids := transactionIDs(block.TxList)
	engineVersion := calvinExecutionEngineVersion
	if engineID == calvinStatelessExecutorID {
		engineVersion = calvinStatelessEngineVersion
	}
	indexes := make([]int, len(ids))
	readKeys := map[string]bool{}
	writeKeys := map[string]bool{}
	stateHomes := map[string]string{}
	for index, item := range block.TxList {
		indexes[index] = index
		accesses, _ := calvinCanonicalAccesses(item.AccessList)
		for _, access := range accesses {
			stateHomes[access.Key] = home(access.Key)
			if calvinNeedsReadValue(access.Mode) {
				readKeys[access.Key] = true
			}
			if calvinIsWriteMode(access.Mode) {
				writeKeys[access.Key] = true
			}
		}
	}
	homeRows := make([]string, 0, len(stateHomes))
	for key, shardID := range stateHomes {
		homeRows = append(homeRows, key+"="+shardID)
	}
	sort.Strings(homeRows)
	stateHomeDigest := stableJSONDigest(homeRows)
	plan := execution.ExecutionPlan{
		EngineID: engineID, EngineVersion: engineVersion, BlockHash: block.BlockHash, BlockHeight: block.Height,
		OrderedTransactionIDs: ids, OriginalTransactionIdxs: indexes,
		DeclaredAccessSetDigest: stableJSONDigest(map[string]any{"read_keys": sortedBoolKeys(readKeys), "write_keys": sortedBoolKeys(writeKeys)}),
		DeclaredReadKeyCount:    len(readKeys), DeclaredWriteKeyCount: len(writeKeys), WorkerCount: workerCount,
	}
	consensusPlanDigest := ""
	if block.ExecutionPlan != nil && block.ExecutionPlan.AlgorithmID == calvinConsensusPlanAlgorithmID {
		consensusPlanDigest = block.ExecutionPlan.PlanDigest
	}
	plan.PlanDigest = stableJSONDigest(map[string]any{
		"engine": plan.EngineID, "version": plan.EngineVersion, "block_hash": plan.BlockHash,
		"height": plan.BlockHeight, "ordered_tx_ids": plan.OrderedTransactionIDs,
		"declared_access_digest": plan.DeclaredAccessSetDigest, "state_home_mapping_digest": stateHomeDigest, "worker_count": workerCount,
		"consensus_calvin_plan_digest": consensusPlanDigest,
	})
	return plan, stateHomeDigest
}

func calvinScheduleEvents(items []tx.SignedTransaction, manager *calvinLockManager, localShard string) []ScheduleEvent {
	events := make([]ScheduleEvent, 0, len(items)*2)
	for index, item := range items {
		if !manager.participant[index] {
			continue
		}
		if manager.waitStarted[index].IsZero() {
			events = append(events, ScheduleEvent{TxID: item.TxID, Track: "calvin", QueueName: "calvin_ready", DecisionReason: "all_local_locks_granted", LocalExecution: true})
			continue
		}
		events = append(events, ScheduleEvent{TxID: item.TxID, Track: "calvin", QueueName: "calvin_lock_wait", DecisionReason: "deterministic_fifo_lock_queue", Blocked: true})
		events = append(events, ScheduleEvent{TxID: item.TxID, Track: "calvin", QueueName: "calvin_ready", DecisionReason: "all_local_locks_granted", LocalExecution: true, Wakeup: true, DependencyWaitMS: manager.waitMS[index]})
	}
	return events
}

func calvinProjectedLocalSnapshot(base map[string]string, items []tx.SignedTransaction, localShard string, home func(string) string) map[string]string {
	out := map[string]string{}
	for _, item := range items {
		for _, access := range item.AccessList {
			if strings.TrimSpace(access.Key) == "" || home(access.Key) != localShard {
				continue
			}
			qualified := qualifyStateKey(localShard, access.Key)
			if value, ok := base[qualified]; ok {
				out[qualified] = value
			}
		}
	}
	return out
}

func calvinDeclaredLocalKeyCount(items []tx.SignedTransaction, shard string, home func(string) string) int {
	keys := map[string]bool{}
	for _, item := range items {
		for _, access := range item.AccessList {
			if home(access.Key) == shard {
				keys[access.Key] = true
			}
		}
	}
	return len(keys)
}

func sumInt64(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return total
}

var _ BlockExecutorPlugin = calvinBlockExecutor{}
