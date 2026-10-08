package v5

import (
	"fmt"
	"sort"
	"strconv"

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
	for id, item := range state.txs {
		if item == nil {
			continue
		}
		rows = append(rows, []string{
			id, string(item.Status), strconv.FormatUint(item.OriginHeight, 10),
			strconv.FormatBool(item.CrossShard),
			strconv.FormatUint(item.WitnessRound, 10),
			strconv.FormatUint(item.OrderingRound, 10),
			strconv.FormatUint(item.PreExecutionRound, 10),
			strconv.FormatUint(item.UpdateProposalHeight, 10),
			strconv.FormatUint(item.UpdateExecutionHeight, 10),
			strconv.FormatUint(item.CommitProposalHeight, 10),
			strconv.FormatUint(item.CommitRound, 10), item.RecoveryMode,
		})
	}
	state.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	return metrics.WriteCSV(path, []string{
		"tx_id", "paper_status", "origin_height", "cross_shard", "witness_round",
		"ordering_round", "pre_execution_round", "update_proposal_height",
		"update_execution_height", "commit_proposal_height", "commit_round", "recovery_mode",
	}, rows)
}
