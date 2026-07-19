package execution

import (
	"fmt"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type Engine struct {
	DefaultInitialBalance int64
}

type Result struct {
	BlockHash        string            `json:"block_hash"`
	Height           uint64            `json:"height"`
	StateRootBefore  string            `json:"state_root_before"`
	StateRootAfter   string            `json:"state_root_after"`
	ReceiptRoot      string            `json:"receipt_root"`
	Receipts         []Receipt         `json:"receipts"`
	StateUpdates     map[string]string `json:"state_updates"`
	StateDelta       []StateUpdate     `json:"state_delta,omitempty"`
	Deterministic    bool              `json:"deterministic_execution"`
	EVMExecution     bool              `json:"evm_execution"`
	FabricExecution  bool              `json:"fabric_execution"`
	SuccessfulTxs    int               `json:"successful_txs"`
	FailedTxs        int               `json:"failed_txs"`
	BlockExecutorID  string            `json:"block_executor_id,omitempty"`
	ExecutorVersion  string            `json:"block_executor_version,omitempty"`
	WorkerCount      int               `json:"worker_count,omitempty"`
	Plan             ExecutionPlan     `json:"execution_plan,omitempty"`
	PlanDigest       string            `json:"execution_plan_digest,omitempty"`
	TxDeltas         []TxDelta         `json:"tx_deltas,omitempty"`
	BlockSTMMetrics  BlockSTMMetrics   `json:"block_stm_metrics,omitempty"`
	SerialEquivalent bool              `json:"serial_equivalent,omitempty"`
}

func NewEngine() *Engine {
	return &Engine{DefaultInitialBalance: 1_000_000}
}

func (e *Engine) ExecuteBlock(b block.Block, db *state.DB) Result {
	before := db.Root()
	result := Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: before, Deterministic: true, EVMExecution: false, FabricExecution: false, StateUpdates: map[string]string{}}
	for _, item := range b.TxList {
		receipt := e.executeTx(b, db, item)
		result.Receipts = append(result.Receipts, receipt)
		if receipt.Success {
			result.SuccessfulTxs++
		} else {
			result.FailedTxs++
		}
	}
	result.StateRootAfter = db.Root()
	result.ReceiptRoot = ReceiptRoot(result.Receipts)
	for key, value := range db.Snapshot() {
		result.StateUpdates[key] = value
	}
	return result
}

func (e *Engine) executeTx(b block.Block, db *state.DB, item tx.SignedTransaction) Receipt {
	receipt := Receipt{TxID: item.TxID, BlockHash: b.BlockHash, Height: b.Height, Success: false, ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...)}
	if isPureCommutativeDelta(item.AccessList) {
		applyCommutativeDeltasToDB(db, item.AccessList)
		receipt.Success = true
		receipt.StateRootAfterTx = db.Root()
		return receipt
	}
	if isCrossShardTargetCommit(item, b.ShardID) {
		db.Set("relay_commit:"+item.TxID, "1")
		receipt.Success = true
		receipt.StateRootAfterTx = db.Root()
		return receipt
	}
	ensureAccount(db, item.Sender, e.DefaultInitialBalance)
	ensureAccount(db, item.Receiver, 0)
	expectedNonce := db.Nonce(item.Sender)
	if item.Nonce != expectedNonce {
		receipt.Error = fmt.Sprintf("nonce_mismatch_expected_%d_got_%d", expectedNonce, item.Nonce)
		receipt.StateRootAfterTx = db.Root()
		return receipt
	}
	if item.Value <= 0 {
		receipt.Error = "invalid_value"
		receipt.StateRootAfterTx = db.Root()
		return receipt
	}
	senderBalance := db.Balance(item.Sender)
	if senderBalance < item.Value {
		receipt.Error = "insufficient_balance"
		receipt.StateRootAfterTx = db.Root()
		return receipt
	}
	db.SetBalance(item.Sender, senderBalance-item.Value)
	db.SetBalance(item.Receiver, db.Balance(item.Receiver)+item.Value)
	db.SetNonce(item.Sender, item.Nonce+1)
	receipt.Success = true
	receipt.StateRootAfterTx = db.Root()
	return receipt
}

func ensureAccount(db *state.DB, account string, balance int64) {
	if db.Get("balance:"+account) == "" {
		db.SetBalance(account, balance)
	}
	if db.Get("nonce:"+account) == "" {
		db.SetNonce(account, 0)
	}
}

func applyCommutativeDeltasToDB(db *state.DB, accesses []tx.AccessItem) {
	for _, access := range accesses {
		if access.Mode != tx.AccessCommutativeDelta || access.Key == "" {
			continue
		}
		current, _ := parseStateInt(db.Get(access.Key))
		db.Set(access.Key, fmt.Sprintf("%d", current+access.Delta))
	}
}

func parseStateInt(value string) (int64, error) {
	var parsed int64
	_, err := fmt.Sscanf(value, "%d", &parsed)
	return parsed, err
}
