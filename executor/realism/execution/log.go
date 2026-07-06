package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/metrics"
)

func ReceiptRoot(receipts []Receipt) string {
	payload, _ := json.Marshal(receipts)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func WriteExecutionLogs(dir string, result Result) error {
	executionRows := [][]string{{result.BlockHash, strconv.FormatUint(result.Height, 10), result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, strconv.Itoa(result.SuccessfulTxs), strconv.Itoa(result.FailedTxs), "true", "false", "false"}}
	if err := metrics.WriteCSV(filepath.Join(dir, "execution_log.csv"), []string{"block_hash", "height", "state_root_before", "state_root_after", "receipt_root", "successful_txs", "failed_txs", "deterministic_execution", "evm_execution", "fabric_execution"}, executionRows); err != nil {
		return err
	}
	receiptRows := [][]string{}
	accessRows := [][]string{}
	for _, receipt := range result.Receipts {
		receiptRows = append(receiptRows, []string{receipt.TxID, receipt.BlockHash, strconv.FormatUint(receipt.Height, 10), strconv.FormatBool(receipt.Success), receipt.Error, strconv.FormatInt(receipt.ExecutionCost, 10), strings.Join(receipt.StateKeys, "|"), receipt.StateRootAfterTx})
		for _, key := range receipt.StateKeys {
			accessRows = append(accessRows, []string{receipt.TxID, receipt.BlockHash, key, strconv.FormatBool(receipt.Success)})
		}
	}
	if err := metrics.WriteCSV(filepath.Join(dir, "receipt_log.csv"), []string{"tx_id", "block_hash", "height", "success", "error", "execution_cost", "state_keys", "state_root_after_tx"}, receiptRows); err != nil {
		return err
	}
	return metrics.WriteCSV(filepath.Join(dir, "state_access_log.csv"), []string{"tx_id", "block_hash", "state_key", "tx_success"}, accessRows)
}
