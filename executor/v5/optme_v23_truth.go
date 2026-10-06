package v5

import (
	"fmt"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

// validateOptMEV23ObservedAccessCoverage keeps the v22 stateless projection safe
// without changing OptME scheduling: actual simulated RW sets must stay inside
// each transaction's signed AccessList. It is also valid for Stateful OptME.
func validateOptMEV23ObservedAccessCoverage(block realblock.Block, deltas []execution.TxDelta) error {
	readByTx := make([]map[string]bool, len(block.TxList))
	writeByTx := make([]map[string]bool, len(block.TxList))
	indexByTxID := make(map[string]int, len(block.TxList))
	for i, item := range block.TxList {
		if strings.TrimSpace(item.TxID) == "" {
			return fmt.Errorf("optme v23 empty transaction id at index %d", i)
		}
		if _, exists := indexByTxID[item.TxID]; exists {
			return fmt.Errorf("optme v23 duplicate transaction id: %s", item.TxID)
		}
		indexByTxID[item.TxID] = i
		reads := map[string]bool{}
		writes := map[string]bool{}
		for _, access := range item.AccessList {
			key := strings.TrimSpace(access.Key)
			if key == "" || strings.Contains(key, "::") {
				return fmt.Errorf("optme v23 invalid signed AccessList key: tx=%s key=%s", item.TxID, key)
			}
			switch access.Mode {
			case tx.AccessRead:
				reads[key] = true
			case tx.AccessWrite:
				writes[key] = true
			case tx.AccessReadWrite, tx.AccessCommutativeDelta:
				reads[key] = true
				writes[key] = true
			default:
				return fmt.Errorf("optme v23 unsupported signed AccessList mode: tx=%s key=%s mode=%s", item.TxID, key, access.Mode)
			}
		}
		readByTx[i] = reads
		writeByTx[i] = writes
	}

	for _, delta := range deltas {
		idx, ok := indexByTxID[delta.TxID]
		if !ok {
			return fmt.Errorf("optme v23 observed delta transaction not in block: %s", delta.TxID)
		}
		if delta.OriginalIndex >= 0 && delta.OriginalIndex < len(block.TxList) && block.TxList[delta.OriginalIndex].TxID != delta.TxID {
			return fmt.Errorf("optme v23 observed delta identity/index mismatch: tx=%s index=%d", delta.TxID, delta.OriginalIndex)
		}
		for _, read := range delta.ReadSet {
			key := strings.TrimSpace(read.Key)
			if key != "" && !readByTx[idx][key] {
				return fmt.Errorf("optme v23 observed read escaped signed AccessList: tx=%s key=%s", delta.TxID, key)
			}
		}
		for key := range delta.WriteSet {
			key = strings.TrimSpace(key)
			if key != "" && !writeByTx[idx][key] {
				return fmt.Errorf("optme v23 observed write escaped signed AccessList: tx=%s key=%s", delta.TxID, key)
			}
		}
	}
	return nil
}
