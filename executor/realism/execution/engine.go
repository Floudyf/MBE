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
	BlockHash       string            `json:"block_hash"`
	Height          uint64            `json:"height"`
	StateRootBefore string            `json:"state_root_before"`
	StateRootAfter  string            `json:"state_root_after"`
	ReceiptRoot     string            `json:"receipt_root"`
	Receipts        []Receipt         `json:"receipts"`
	StateUpdates    map[string]string `json:"state_updates"`
	Deterministic   bool              `json:"deterministic_execution"`
	EVMExecution    bool              `json:"evm_execution"`
	FabricExecution bool              `json:"fabric_execution"`
	SuccessfulTxs   int               `json:"successful_txs"`
	FailedTxs       int               `json:"failed_txs"`
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
	ensureAccount(db, item.Sender, e.DefaultInitialBalance)
	ensureAccount(db, item.Receiver, 0)
	receipt := Receipt{TxID: item.TxID, BlockHash: b.BlockHash, Height: b.Height, Success: false, ExecutionCost: 1, StateKeys: append([]string(nil), item.StateKeys...)}
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
