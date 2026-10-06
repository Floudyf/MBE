package v5

import (
	"fmt"
	"sort"
	"strings"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
)

// porygonPaperCollapseOrderedStateUpdates consumes writes in protocol order and
// keeps the final write for each logical key. Sorting happens only after the
// semantic last-writer decision, so deterministic encoding cannot reorder U
// ahead of/behind current-round ITx writes.
func porygonPaperCollapseOrderedStateUpdates(ordered []PorygonStateUpdate) []PorygonStateUpdate {
	last := map[string]PorygonStateUpdate{}
	for _, update := range ordered {
		key := strings.TrimSpace(update.Key)
		if key == "" {
			continue
		}
		update.Key = key
		last[key] = update
	}
	keys := make([]string, 0, len(last))
	for key := range last {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]PorygonStateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, last[key])
	}
	return out
}

// porygonPaperExecutionStateDelta projects the durable StateKV representation
// into execution.Result's StateUpdate representation without recomputing a
// snapshot diff.  Both slices describe the exact same ordered materialization
// writes; only their API-layer types differ.
func porygonPaperExecutionStateDelta(materialized []state.StateKV) []execution.StateUpdate {
	out := make([]execution.StateUpdate, 0, len(materialized))
	for _, item := range materialized {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			continue
		}
		out = append(out, execution.StateUpdate{Key: key, Value: item.Value})
	}
	return out
}

func porygonPaperValidateCollapsedStateUpdates(updates []PorygonStateUpdate) error {
	seen := map[string]bool{}
	for _, update := range updates {
		key := strings.TrimSpace(update.Key)
		if key == "" {
			return fmt.Errorf("Porygon Paper2 materialization contains empty state key")
		}
		if seen[key] {
			return fmt.Errorf("Porygon Paper2 materialization contains duplicate state key %s after ordered collapse", key)
		}
		seen[key] = true
	}
	return nil
}

// porygonPaperProposalUpdateOrderIndex recovers the transaction's original OC
// order from its state updates. Every update emitted by one TxDelta carries the
// same OriginalIndex. Empty maintenance/rollback rows sort after concrete writes.
func porygonPaperProposalUpdateOrderIndex(update PorygonProposalUpdate) int {
	if len(update.Updates) == 0 {
		return int(^uint(0) >> 1)
	}
	index := update.Updates[0].OriginalIndex
	for _, row := range update.Updates[1:] {
		if row.OriginalIndex < index {
			index = row.OriginalIndex
		}
	}
	return index
}

// porygonPaperDurableStateDelta reconstructs the exact materialization writes
// for Paper2. It intentionally does NOT diff against Proposal.T(h-2): a write
// that restores the T(h-2) value is still a real write when canonical state
// h-1 contains a newer value. Proposal.U is applied first, followed by current
// successful ITx in OC order; current CTx remain S candidates and are excluded.
func porygonPaperDurableStateDelta(
	proposal PorygonProposalBody,
	deltas []execution.TxDelta,
	assignments []porygonTxAssignment,
	orderingDomain string,
	storagePartitionID string,
	shardCount int,
) ([]state.StateKV, error) {
	assignmentByID := make(map[string]porygonTxAssignment, len(assignments))
	for _, assignment := range assignments {
		if assignment.TxID == "" {
			continue
		}
		assignmentByID[assignment.TxID] = assignment
	}

	ordered := make([]PorygonStateUpdate, 0)
	for _, proposalUpdate := range proposal.U {
		for _, row := range proposalUpdate.Updates {
			if strings.TrimSpace(row.Key) == "" {
				continue
			}
			home := fmt.Sprintf("s%d", porygonStateShard(row.Key, shardCount))
			if strings.TrimSpace(storagePartitionID) != "" && home != storagePartitionID {
				continue
			}
			ordered = append(ordered, row)
		}
	}

	current := append([]execution.TxDelta(nil), deltas...)
	sort.SliceStable(current, func(i, j int) bool {
		if current[i].OriginalIndex != current[j].OriginalIndex {
			return current[i].OriginalIndex < current[j].OriginalIndex
		}
		return current[i].TxID < current[j].TxID
	})
	for _, delta := range current {
		if !delta.Success || len(delta.WriteSet) == 0 {
			continue
		}
		assignment, ok := assignmentByID[delta.TxID]
		if !ok {
			return nil, fmt.Errorf("Porygon Paper2 materialization missing assignment for %s", delta.TxID)
		}
		if assignment.Abandoned || assignment.CrossShard {
			continue
		}
		keys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			home := fmt.Sprintf("s%d", porygonStateShard(key, shardCount))
			if strings.TrimSpace(storagePartitionID) != "" && home != storagePartitionID {
				continue
			}
			ordered = append(ordered, PorygonStateUpdate{
				TxID: delta.TxID, OriginalIndex: delta.OriginalIndex,
				Key: key, Value: delta.WriteSet[key],
			})
		}
	}

	collapsed := porygonPaperCollapseOrderedStateUpdates(ordered)
	if err := porygonPaperValidateCollapsedStateUpdates(collapsed); err != nil {
		return nil, err
	}
	out := make([]state.StateKV, 0, len(collapsed))
	for _, update := range collapsed {
		storageKey := qualifyStateKey(orderingDomain, update.Key)
		if strings.TrimSpace(storagePartitionID) != "" {
			storageKey = qualifyStateKey(storagePartitionID, update.Key)
		}
		out = append(out, state.StateKV{Key: storageKey, Value: update.Value})
	}
	return out, nil
}
