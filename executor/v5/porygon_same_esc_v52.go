package v5

import (
	"fmt"

	"metaverse-chainlab/executor/realism/tx"
)

// porygonSameESCDeferredWriteHazard detects the one same-ESC ordering shape
// that cannot be preserved by Porygon's delayed CTx materialization: an earlier
// CTx will later write a key through Proposal.U while a retained later ITx in
// the same sequential ESC lane also writes that key in the current round. A
// read-only later ITx is not a hazard because there is no newer value for the
// delayed U to overwrite. The direction matters: ITx -> CTx remains valid.
func porygonSameESCDeferredWriteHazard(candidate, later tx.SignedTransaction) bool {
	for _, left := range porygonCanonicalAccesses(candidate) {
		if !isWriteMode(left.Mode) {
			continue
		}
		for _, right := range porygonCanonicalAccesses(later) {
			if left.Key == right.Key && isWriteMode(right.Mode) {
				return true
			}
		}
	}
	return false
}

func porygonSameESCDeferredWriteHazardPreview(assignments []porygonTxAssignment, items []tx.SignedTransaction) (map[string]bool, map[string][]string, int, error) {
	byTx := make(map[string]tx.SignedTransaction, len(items))
	for _, item := range items {
		byTx[item.TxID] = item
	}
	preview := map[string]bool{}
	conflicts := map[string][]string{}
	for i := range assignments {
		candidate := assignments[i]
		if !candidate.CrossShard || candidate.Abandoned {
			continue
		}
		candidateItem, ok := byTx[candidate.TxID]
		if !ok {
			return nil, nil, 0, fmt.Errorf("porygon same-ESC deferred-write preview missing CTx %s", candidate.TxID)
		}
		for j := range assignments {
			itx := assignments[j]
			if itx.CrossShard || itx.Abandoned || candidate.ExecutionShard != itx.ExecutionShard || candidate.OriginalIndex >= itx.OriginalIndex {
				continue
			}
			itxItem, ok := byTx[itx.TxID]
			if !ok {
				return nil, nil, 0, fmt.Errorf("porygon same-ESC deferred-write preview missing ITx %s", itx.TxID)
			}
			if !porygonSameESCDeferredWriteHazard(candidateItem, itxItem) {
				continue
			}
			preview[candidate.TxID] = true
			conflicts[candidate.TxID] = append(conflicts[candidate.TxID], itx.TxID)
		}
	}
	for txID := range conflicts {
		conflicts[txID] = uniqueStrings(conflicts[txID])
	}
	return preview, conflicts, len(preview), nil
}

func porygonSameESCDeferredWriteHazardPairCount(assignments []porygonTxAssignment, items []tx.SignedTransaction) int {
	byTx := make(map[string]tx.SignedTransaction, len(items))
	for _, item := range items {
		byTx[item.TxID] = item
	}
	pairs := 0
	for i := range assignments {
		candidate := assignments[i]
		if !candidate.CrossShard || candidate.Abandoned {
			continue
		}
		candidateItem, ok := byTx[candidate.TxID]
		if !ok {
			continue
		}
		for j := range assignments {
			itx := assignments[j]
			if itx.CrossShard || itx.Abandoned || candidate.ExecutionShard != itx.ExecutionShard || candidate.OriginalIndex >= itx.OriginalIndex {
				continue
			}
			itxItem, ok := byTx[itx.TxID]
			if ok && porygonSameESCDeferredWriteHazard(candidateItem, itxItem) {
				pairs++
			}
		}
	}
	return pairs
}

func porygonApplySameESCDeferredWriteHazards(assignments []porygonTxAssignment, items []tx.SignedTransaction, certified map[string]porygonWaveResult) ([]porygonTxAssignment, map[string]bool, int, int, error) {
	final := append([]porygonTxAssignment(nil), assignments...)
	preview, conflicts, _, err := porygonSameESCDeferredWriteHazardPreview(final, items)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	abandoned := map[string]bool{}
	for i := range final {
		if !preview[final[i].TxID] || final[i].Abandoned {
			continue
		}
		if _, ok := certified[final[i].TxID]; !ok {
			return nil, nil, 0, 0, fmt.Errorf("porygon same-ESC deferred-write closure missing certified CTx %s", final[i].TxID)
		}
		final[i].Abandoned = true
		final[i].ConflictWith = uniqueStrings(append(final[i].ConflictWith, conflicts[final[i].TxID]...))
		final[i].ConflictReason = "esc_same_round_deferred_ctx_itx_write_conflict_abandoned"
		abandoned[final[i].TxID] = true
	}
	remaining := porygonSameESCDeferredWriteHazardPairCount(final, items)
	if remaining != 0 {
		return nil, nil, 0, remaining, fmt.Errorf("porygon same-ESC deferred-write closure incomplete: retained hazard pairs=%d", remaining)
	}
	return final, abandoned, len(abandoned), remaining, nil
}

func porygonPreviewAbandonmentClosed(preview map[string]bool, assignments []porygonTxAssignment) bool {
	if len(preview) == 0 {
		return true
	}
	final := make(map[string]porygonTxAssignment, len(assignments))
	for _, assignment := range assignments {
		final[assignment.TxID] = assignment
	}
	for txID := range preview {
		assignment, ok := final[txID]
		if !ok || !assignment.Abandoned {
			return false
		}
	}
	return true
}

func porygonMergeTxBoolSets(sets ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, set := range sets {
		for txID, value := range set {
			if value {
				out[txID] = true
			}
		}
	}
	return out
}
