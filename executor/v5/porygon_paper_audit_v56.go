package v5

import (
	"fmt"
	"sort"

	"metaverse-chainlab/executor/realism/metrics"
)

// porygonV56WritePaperIdentity snapshots the *actual* per-transaction paper
// lifecycle on each replica at artifact finalization. It is not consulted by
// OC, EC, the PBFT state machine, or the finality authority.
func (r *NodeRuntime) porygonV56WritePaperIdentity(path string) error {
	value, ok := porygonPaperRuntimeStates.Load(r)
	if !ok {
		return nil
	} // never manufacture a paper state during shutdown
	state, ok := value.(*porygonPaperRuntimeState)
	if !ok {
		return fmt.Errorf("Porygon v5.6 paper state type mismatch")
	}
	state.mu.Lock()
	rows := make([][]string, 0, len(state.txs))
	for _, item := range state.txs {
		if item == nil {
			continue
		}
		rows = append(rows, porygonV57LifecycleRow(item))
	}
	state.mu.Unlock()
	archived, err := r.porygonV57ArchivedPaperRows()
	if err != nil {
		return err
	}
	// A pruned row is still an actual paper terminal, not absent evidence.
	// Reject conflicting duplicate identities instead of fabricating success.
	byID := map[string][]string{}
	for _, row := range append(archived, rows...) {
		if previous, exists := byID[row[0]]; exists {
			if !porygonV57IdenticalPaperRows(previous, row) {
				return fmt.Errorf("Porygon v5.7 conflicting live/archive identity for %s", row[0])
			}
			continue
		}
		byID[row[0]] = row
	}
	rows = rows[:0]
	for _, row := range byID {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	return metrics.WriteCSV(path, porygonV57PaperHeader, rows)
}
