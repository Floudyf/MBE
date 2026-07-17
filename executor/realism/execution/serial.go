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

func NewSerialExecutor() *SerialExecutor {
	return &SerialExecutor{DefaultInitialBalance: 1_000_000}
}

func (e *SerialExecutor) ExecuteBlock(b block.Block, base map[string]string) Result {
	working := copySnapshot(base)
	before := state.RootOfSnapshot(working)
	result := Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: before, Deterministic: true, EVMExecution: false, FabricExecution: false, StateUpdates: map[string]string{}, BlockExecutorID: SerialBlockExecutorID, ExecutorVersion: SerialBlockExecutorVersion, WorkerCount: 1}
	declared := declaredAccessSet(b.TxList)
	for index, item := range b.TxList {
		overlay := newTxOverlay(b.ShardID, working)
		receipt := e.executeTx(b, overlay, item)
		working = overlay.snapshot()
		delta := TxDelta{TxID: item.TxID, OriginalIndex: index, ReadSet: overlay.reads, WriteSet: overlay.logicalWrites(), Receipt: receipt, Success: receipt.Success, Error: receipt.Error}
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

func (e *SerialExecutor) executeTx(b block.Block, overlay *txOverlay, item tx.SignedTransaction) Receipt {
	overlay.ensureAccount(item.Sender, e.DefaultInitialBalance)
	overlay.ensureAccount(item.Receiver, 0)
	receipt := Receipt{TxID: item.TxID, BlockHash: b.BlockHash, Height: b.Height, Success: false, ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...)}
	expectedNonce := overlay.nonce(item.Sender)
	if item.Nonce != expectedNonce {
		receipt.Error = fmt.Sprintf("nonce_mismatch_expected_%d_got_%d", expectedNonce, item.Nonce)
		receipt.StateRootAfterTx = state.RootOfSnapshot(overlay.snapshot())
		return receipt
	}
	if item.Value <= 0 {
		receipt.Error = "invalid_value"
		receipt.StateRootAfterTx = state.RootOfSnapshot(overlay.snapshot())
		return receipt
	}
	senderBalance := overlay.balance(item.Sender)
	if senderBalance < item.Value {
		receipt.Error = "insufficient_balance"
		receipt.StateRootAfterTx = state.RootOfSnapshot(overlay.snapshot())
		return receipt
	}
	overlay.setBalance(item.Sender, senderBalance-item.Value)
	overlay.setBalance(item.Receiver, overlay.balance(item.Receiver)+item.Value)
	overlay.setNonce(item.Sender, item.Nonce+1)
	receipt.Success = true
	receipt.StateRootAfterTx = state.RootOfSnapshot(overlay.snapshot())
	return receipt
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
	o.reads = append(o.reads, ReadObservation{Key: key, ValueDigest: digestValue(value), Source: "state_snapshot_overlay"})
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
