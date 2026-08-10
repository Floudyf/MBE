package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const SerialBlockExecutorID = "serial_block_executor"
const SerialBlockExecutorVersion = "1.0.0"

type AccessSet struct {
	ReadKeys  []string `json:"read_keys"`
	WriteKeys []string `json:"write_keys"`
}

type ReadObservation struct {
	Key         string `json:"key"`
	Value       string `json:"value,omitempty"`
	ValueDigest string `json:"value_digest"`
	Source      string `json:"source"`
}

type StateUpdate struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TxDelta struct {
	TxID          string            `json:"tx_id"`
	OriginalIndex int               `json:"original_index"`
	ReadSet       []ReadObservation `json:"read_set"`
	WriteSet      map[string]string `json:"write_set"`
	Receipt       Receipt           `json:"receipt"`
	Success       bool              `json:"success"`
	Error         string            `json:"error"`
}

type ExecutionPlan struct {
	EngineID                string   `json:"engine_id"`
	EngineVersion           string   `json:"engine_version"`
	BlockHash               string   `json:"block_hash"`
	BlockHeight             uint64   `json:"block_height"`
	OrderedTransactionIDs   []string `json:"ordered_transaction_ids"`
	OriginalTransactionIdxs []int    `json:"original_transaction_indexes"`
	DeclaredAccessSetDigest string   `json:"declared_access_set_digest"`
	DeclaredReadKeyCount    int      `json:"declared_read_key_count"`
	DeclaredWriteKeyCount   int      `json:"declared_write_key_count"`
	WorkerCount             int      `json:"worker_count"`
	PlanDigest              string   `json:"plan_digest"`
}

type SerialExecutor struct {
	DefaultInitialBalance int64
}

type txExecutionOverlay interface {
	get(string) string
	set(string, string)
	snapshot() map[string]string
	logicalWrites() map[string]string
	ensureAccount(string, int64)
	balance(string) int64
	setBalance(string, int64)
	nonce(string) uint64
	setNonce(string, uint64)
	applyCommutativeDeltas([]tx.AccessItem)
}

func NewSerialExecutor() *SerialExecutor {
	return &SerialExecutor{DefaultInitialBalance: 1_000_000}
}

func (e *SerialExecutor) ExecuteBlock(b block.Block, base map[string]string) Result {
	working := copySnapshot(base)
	before := state.RootOfSnapshot(working)
	result := Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: before, Deterministic: true, EVMExecution: false, FabricExecution: false, StateUpdates: map[string]string{}, BlockExecutorID: SerialBlockExecutorID, ExecutorVersion: SerialBlockExecutorVersion, WorkerCount: 1}
	declared := declaredAccessSet(b.TxList)
	for index, item := range b.TxList {
		// Sequential execution needs one mutable block working state plus a
		// transaction-local write overlay.  This Serial-only overlay avoids a
		// full-state copy per transaction without changing the shared speculative
		// transaction primitive used by Block-STM, Aria, or other executors.
		overlay := newSerialBlockTxOverlay(b.ShardID, working)
		receipt := e.executeTxWithoutStateRoot(b, overlay, item)
		keys := make([]string, 0, len(overlay.writes))
		for key := range overlay.writes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			working[overlay.key(key)] = overlay.writes[key]
		}
		receipt.StateRootAfterTx = state.RootOfSnapshot(working)
		delta := TxDelta{TxID: item.TxID, OriginalIndex: index, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: overlay.logicalWrites(), Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
		result.TxDeltas = append(result.TxDeltas, delta)
		result.Receipts = append(result.Receipts, receipt)
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	result.StateRootAfter = state.RootOfSnapshot(working)
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	for key, value := range working {
		result.StateUpdates[key] = value
	}
	result.StateDelta = stateDelta(base, working)
	plan := buildSerialPlan(b, declared)
	result.Plan = plan
	result.PlanDigest = plan.PlanDigest
	return result
}

// ExecuteTransaction executes one transaction against an immutable caller-provided
// snapshot and returns only the receipt and logical delta required by
// speculative schedulers. It avoids constructing a one-transaction block
// result and avoids durable state-root hashing for tentative work.
func (e *SerialExecutor) ExecuteTransaction(b block.Block, item tx.SignedTransaction, base map[string]string, originalIndex int) (Receipt, TxDelta) {
	overlay := newTxOverlay(b.ShardID, base)
	receipt := e.executeTxWithoutStateRoot(b, overlay, item)
	delta := TxDelta{TxID: item.TxID, OriginalIndex: originalIndex, ReadSet: append([]ReadObservation(nil), overlay.reads...), WriteSet: overlay.logicalWrites(), Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
	return receipt, delta
}

func (e *SerialExecutor) executeTx(b block.Block, overlay txExecutionOverlay, item tx.SignedTransaction) Receipt {
	return e.executeTxWithStateRoot(b, overlay, item, true)
}

// executeTxWithoutStateRoot is used only by speculative executors.  A
// speculative receipt root is not durable evidence and is overwritten during
// deterministic ordered materialization, so hashing a complete snapshot for
// every incarnation is redundant work.
func (e *SerialExecutor) executeTxWithoutStateRoot(b block.Block, overlay txExecutionOverlay, item tx.SignedTransaction) Receipt {
	return e.executeTxWithStateRoot(b, overlay, item, false)
}

func (e *SerialExecutor) executeTxWithStateRoot(b block.Block, overlay txExecutionOverlay, item tx.SignedTransaction, includeStateRoot bool) Receipt {
	receipt := Receipt{TxID: item.TxID, BlockHash: b.BlockHash, Height: b.Height, Success: false, ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...)}
	finalize := func() Receipt {
		if includeStateRoot {
			receipt.StateRootAfterTx = state.RootOfSnapshot(overlay.snapshot())
		}
		return receipt
	}
	applyDeclaredSemanticReads(overlay, item.AccessList)
	if isPureCommutativeDelta(item.AccessList) {
		overlay.applyCommutativeDeltas(item.AccessList)
		receipt.Success = true
		return finalize()
	}
	if isCrossShardTargetCommit(item, b.ShardID) {
		overlay.set("relay_commit:"+item.TxID, "1")
		receipt.Success = true
		return finalize()
	}
	if isDirectAccessTransaction(item) {
		applyDirectAccessTransaction(overlay, item)
		receipt.Success = true
		return finalize()
	}
	overlay.ensureAccount(item.Sender, e.DefaultInitialBalance)
	overlay.ensureAccount(item.Receiver, 0)
	expectedNonce := overlay.nonce(item.Sender)
	if item.Nonce != expectedNonce {
		receipt.Error = fmt.Sprintf("nonce_mismatch_expected_%d_got_%d", expectedNonce, item.Nonce)
		return finalize()
	}
	if item.Value <= 0 {
		receipt.Error = "invalid_value"
		return finalize()
	}
	senderBalance := overlay.balance(item.Sender)
	if senderBalance < item.Value {
		receipt.Error = "insufficient_balance"
		return finalize()
	}
	overlay.setBalance(item.Sender, senderBalance-item.Value)
	overlay.setBalance(item.Receiver, overlay.balance(item.Receiver)+item.Value)
	overlay.setNonce(item.Sender, item.Nonce+1)
	overlay.applyCommutativeDeltas(item.AccessList)
	receipt.Success = true
	return finalize()
}

func isDirectAccessTransaction(item tx.SignedTransaction) bool {
	return item.AccessListSchema != "" && item.AccessListSchema != "dcl_sale_access_template_v1" && item.AccessListSource != "legacy_state_keys"
}

func applyDirectAccessTransaction(overlay txExecutionOverlay, item tx.SignedTransaction) {
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
			current := overlay.get(access.Key)
			overlay.set(access.Key, directAccessValue(item, access, current))
		case tx.AccessWrite:
			overlay.set(access.Key, directAccessValue(item, access, ""))
		}
	}
}

func directAccessValue(item tx.SignedTransaction, access tx.AccessItem, previous string) string {
	return stableDigest(struct {
		LogicalTxID string `json:"logical_tx_id"`
		Key         string `json:"key"`
		Semantics   string `json:"semantics"`
		Previous    string `json:"previous,omitempty"`
	}{tx.SemanticID(item), access.Key, access.UpdateSemantics, previous})
}

func applyDeclaredSemanticReads(overlay txExecutionOverlay, accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessRead || access.Key == "" {
			continue
		}
		switch access.UpdateSemantics {
		case "category_metadata":
			overlay.get(access.Key)
		}
	}
}

// serialBlockTxOverlay is intentionally private to SerialExecutor.ExecuteBlock.
// The existing txOverlay/newTxOverlay path is left byte-for-byte unchanged so
// speculative executors retain their reviewed snapshot-isolation behavior.
type serialBlockTxOverlay struct {
	shardID string
	base    map[string]string
	reads   []ReadObservation
	writes  map[string]string
}

func newSerialBlockTxOverlay(shardID string, base map[string]string) *serialBlockTxOverlay {
	return &serialBlockTxOverlay{shardID: shardID, base: base, writes: map[string]string{}}
}

func (o *serialBlockTxOverlay) key(key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return o.shardID + "::" + key
}

func (o *serialBlockTxOverlay) get(key string) string {
	if value, ok := o.writes[key]; ok {
		o.reads = append(o.reads, ReadObservation{Key: key, Value: value, ValueDigest: digestValue(value), Source: "state_snapshot_overlay"})
		return value
	}
	value := o.base[o.key(key)]
	o.reads = append(o.reads, ReadObservation{Key: key, Value: value, ValueDigest: digestValue(value), Source: "state_snapshot_overlay"})
	return value
}

func (o *serialBlockTxOverlay) set(key, value string) { o.writes[key] = value }

func (o *serialBlockTxOverlay) snapshot() map[string]string {
	out := copySnapshot(o.base)
	for key, value := range o.writes {
		out[o.key(key)] = value
	}
	return out
}

func (o *serialBlockTxOverlay) logicalWrites() map[string]string {
	out := make(map[string]string, len(o.writes))
	for key, value := range o.writes {
		out[key] = value
	}
	return out
}

func (o *serialBlockTxOverlay) ensureAccount(account string, balance int64) {
	if o.get("balance:"+account) == "" {
		o.setBalance(account, balance)
	}
	if o.get("nonce:"+account) == "" {
		o.setNonce(account, 0)
	}
}

func (o *serialBlockTxOverlay) balance(account string) int64 {
	value, _ := strconv.ParseInt(o.get("balance:"+account), 10, 64)
	return value
}

func (o *serialBlockTxOverlay) setBalance(account string, balance int64) {
	o.set("balance:"+account, strconv.FormatInt(balance, 10))
}

func (o *serialBlockTxOverlay) nonce(account string) uint64 {
	value, _ := strconv.ParseUint(o.get("nonce:"+account), 10, 64)
	return value
}

func (o *serialBlockTxOverlay) setNonce(account string, nonce uint64) {
	o.set("nonce:"+account, strconv.FormatUint(nonce, 10))
}

func (o *serialBlockTxOverlay) applyCommutativeDeltas(accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessCommutativeDelta || access.Key == "" {
			continue
		}
		current, _ := strconv.ParseInt(o.get(access.Key), 10, 64)
		o.set(access.Key, strconv.FormatInt(current+access.Delta, 10))
	}
}

type txOverlay struct {
	shardID string
	values  map[string]string
	reads   []ReadObservation
	writes  map[string]string
}

func newTxOverlay(shardID string, base map[string]string) *txOverlay {
	return &txOverlay{shardID: shardID, values: copySnapshot(base), writes: map[string]string{}}
}

func (o *txOverlay) key(key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return o.shardID + "::" + key
}

func (o *txOverlay) get(key string) string {
	value := o.values[o.key(key)]
	o.reads = append(o.reads, ReadObservation{Key: key, Value: value, ValueDigest: digestValue(value), Source: "state_snapshot_overlay"})
	return value
}

func (o *txOverlay) set(key, value string) {
	o.values[o.key(key)] = value
	o.writes[key] = value
}

func (o *txOverlay) snapshot() map[string]string {
	return copySnapshot(o.values)
}

func (o *txOverlay) logicalWrites() map[string]string {
	out := map[string]string{}
	for key, value := range o.writes {
		out[key] = value
	}
	return out
}

func (o *txOverlay) ensureAccount(account string, balance int64) {
	if o.get("balance:"+account) == "" {
		o.setBalance(account, balance)
	}
	if o.get("nonce:"+account) == "" {
		o.setNonce(account, 0)
	}
}

func (o *txOverlay) balance(account string) int64 {
	value, _ := strconv.ParseInt(o.get("balance:"+account), 10, 64)
	return value
}

func (o *txOverlay) setBalance(account string, balance int64) {
	o.set("balance:"+account, strconv.FormatInt(balance, 10))
}

func (o *txOverlay) nonce(account string) uint64 {
	value, _ := strconv.ParseUint(o.get("nonce:"+account), 10, 64)
	return value
}

func (o *txOverlay) setNonce(account string, nonce uint64) {
	o.set("nonce:"+account, strconv.FormatUint(nonce, 10))
}

func (o *txOverlay) applyCommutativeDeltas(accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessCommutativeDelta || access.Key == "" {
			continue
		}
		current, _ := strconv.ParseInt(o.get(access.Key), 10, 64)
		o.set(access.Key, strconv.FormatInt(current+access.Delta, 10))
	}
}

func isPureCommutativeDelta(accesses []tx.AccessItem) bool {
	if len(accesses) == 0 {
		return false
	}
	for _, access := range accesses {
		if access.Mode == tx.AccessRead {
			continue
		}
		if access.Mode != tx.AccessCommutativeDelta {
			return false
		}
	}
	return true
}

func isCrossShardTargetCommit(item tx.SignedTransaction, shardID string) bool {
	target := crossShardTargetFromPayload(item.Payload)
	return target != "" && target == shardID
}

func crossShardTargetFromPayload(payload string) string {
	if !strings.HasPrefix(payload, "v5_cross:") {
		return ""
	}
	target := strings.TrimPrefix(payload, "v5_cross:")
	if colon := strings.Index(target, ":"); colon >= 0 {
		target = target[:colon]
	}
	return strings.TrimSpace(target)
}

func copySnapshot(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func stateDelta(before, after map[string]string) []StateUpdate {
	keys := make([]string, 0, len(after))
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

func declaredAccessSet(txs []tx.SignedTransaction) AccessSet {
	readKeys := map[string]bool{}
	writeKeys := map[string]bool{}
	for _, item := range txs {
		if len(item.AccessList) > 0 {
			for _, access := range item.AccessList {
				switch access.Mode {
				case tx.AccessRead:
					readKeys[access.Key] = true
				case tx.AccessWrite:
					writeKeys[access.Key] = true
				case tx.AccessReadWrite:
					readKeys[access.Key] = true
					writeKeys[access.Key] = true
				case tx.AccessCommutativeDelta:
					readKeys[access.Key] = true
					writeKeys[access.Key] = true
				}
			}
			continue
		}
		for _, key := range item.StateKeys {
			readKeys[key] = true
			writeKeys[key] = true
		}
		readKeys["balance:"+item.Sender] = true
		readKeys["nonce:"+item.Sender] = true
		readKeys["balance:"+item.Receiver] = true
		readKeys["nonce:"+item.Receiver] = true
		writeKeys["balance:"+item.Sender] = true
		writeKeys["nonce:"+item.Sender] = true
		writeKeys["balance:"+item.Receiver] = true
		writeKeys["nonce:"+item.Receiver] = true
	}
	return AccessSet{ReadKeys: sortedSet(readKeys), WriteKeys: sortedSet(writeKeys)}
}

func buildSerialPlan(b block.Block, declared AccessSet) ExecutionPlan {
	ids := make([]string, 0, len(b.TxList))
	indexes := make([]int, 0, len(b.TxList))
	for index, item := range b.TxList {
		ids = append(ids, item.TxID)
		indexes = append(indexes, index)
	}
	plan := ExecutionPlan{EngineID: SerialBlockExecutorID, EngineVersion: SerialBlockExecutorVersion, BlockHash: b.BlockHash, BlockHeight: b.Height, OrderedTransactionIDs: ids, OriginalTransactionIdxs: indexes, DeclaredAccessSetDigest: stableDigest(declared), DeclaredReadKeyCount: len(declared.ReadKeys), DeclaredWriteKeyCount: len(declared.WriteKeys), WorkerCount: 1}
	plan.PlanDigest = stableDigest(struct {
		EngineID                string   `json:"engine_id"`
		EngineVersion           string   `json:"engine_version"`
		BlockHash               string   `json:"block_hash"`
		BlockHeight             uint64   `json:"block_height"`
		OrderedTransactionIDs   []string `json:"ordered_transaction_ids"`
		OriginalTransactionIdxs []int    `json:"original_transaction_indexes"`
		DeclaredAccessSetDigest string   `json:"declared_access_set_digest"`
		DeclaredReadKeyCount    int      `json:"declared_read_key_count"`
		DeclaredWriteKeyCount   int      `json:"declared_write_key_count"`
		WorkerCount             int      `json:"worker_count"`
	}{plan.EngineID, plan.EngineVersion, plan.BlockHash, plan.BlockHeight, plan.OrderedTransactionIDs, plan.OriginalTransactionIdxs, plan.DeclaredAccessSetDigest, plan.DeclaredReadKeyCount, plan.DeclaredWriteKeyCount, plan.WorkerCount})
	return plan
}

func sortedSet(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func digestValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func stableDigest(value any) string {
	payload, _ := json.Marshal(value)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
