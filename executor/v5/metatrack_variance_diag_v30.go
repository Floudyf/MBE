package v5

import (
	"fmt"
	"sort"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_VARIANCE_DIAG_V30
// Observation-only helpers for diagnosing run-to-run MetaTrack variance.
// These rows are not consensus inputs and never change routing, scheduling,
// StateReady, PBFT, state access, or exact-version semantics.
const metaTrackVarianceDiagRetentionV30 = 8192

type metaTrackProposalObservationV30 struct {
	TimestampMS                 int64
	Height                      uint64
	PoolDepthBeforeReserve      int
	ReservedTransactionCount    int
	CompleteRouteBatchCount     int
	CompleteRouteBatchSequences string
	FirstIncompleteRouteBatch   uint64
	FirstIncompleteHave         int
	FirstIncompleteExpected     int
	SelectedTransactionCount    int
	SelectedRouteBatchCount     int
	SelectedRouteBatchSequences string
	SelectionError              string
}

func metaTrackRouteBatchSequencesV30(items []tx.SignedTransaction) []uint64 {
	seen := map[uint64]bool{}
	seqs := make([]uint64, 0)
	for _, item := range items {
		if item.ExecutionRouting == nil || item.ExecutionRouting.RouteBatchSequence == 0 {
			continue
		}
		seq := item.ExecutionRouting.RouteBatchSequence
		if !seen[seq] {
			seen[seq] = true
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

func metaTrackJoinSequencesV30(seqs []uint64) string {
	parts := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		parts = append(parts, fmt.Sprint(seq))
	}
	return strings.Join(parts, "|")
}

func inspectMetaTrackProposalV30(at time.Time, height uint64, poolDepth int, reserved []tx.SignedTransaction) metaTrackProposalObservationV30 {
	obs := metaTrackProposalObservationV30{
		TimestampMS:              at.UnixMilli(),
		Height:                   height,
		PoolDepthBeforeReserve:   poolDepth,
		ReservedTransactionCount: len(reserved),
	}
	groups := map[uint64][]tx.SignedTransaction{}
	expected := map[uint64]int{}
	for _, item := range reserved {
		if item.ExecutionRouting == nil || item.ExecutionRouting.RouteBatchSequence == 0 {
			continue
		}
		seq := item.ExecutionRouting.RouteBatchSequence
		groups[seq] = append(groups[seq], item)
		if item.ExecutionRouting.RouteBatchShardTransactionCount > 0 {
			expected[seq] = item.ExecutionRouting.RouteBatchShardTransactionCount
		}
	}
	seqs := make([]uint64, 0, len(groups))
	for seq := range groups {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	completePrefix := make([]uint64, 0, len(seqs))
	for _, seq := range seqs {
		have := len(groups[seq])
		want := expected[seq]
		if want <= 0 || have != want {
			obs.FirstIncompleteRouteBatch = seq
			obs.FirstIncompleteHave = have
			obs.FirstIncompleteExpected = want
			break
		}
		completePrefix = append(completePrefix, seq)
	}
	obs.CompleteRouteBatchCount = len(completePrefix)
	obs.CompleteRouteBatchSequences = metaTrackJoinSequencesV30(completePrefix)
	return obs
}

func finishMetaTrackProposalV30(obs *metaTrackProposalObservationV30, selected []tx.SignedTransaction, err error) {
	if obs == nil {
		return
	}
	seqs := metaTrackRouteBatchSequencesV30(selected)
	obs.SelectedTransactionCount = len(selected)
	obs.SelectedRouteBatchCount = len(seqs)
	obs.SelectedRouteBatchSequences = metaTrackJoinSequencesV30(seqs)
	if err != nil {
		obs.SelectionError = err.Error()
	}
}

func (r *NodeRuntime) recordMetaTrackProposalV30(obs metaTrackProposalObservationV30) {
	if r == nil {
		return
	}
	row := []string{
		fmt.Sprint(obs.TimestampMS), r.node.NodeID, r.node.ShardID, fmt.Sprint(obs.Height),
		fmt.Sprint(obs.PoolDepthBeforeReserve), fmt.Sprint(obs.ReservedTransactionCount),
		fmt.Sprint(obs.CompleteRouteBatchCount), obs.CompleteRouteBatchSequences,
		fmt.Sprint(obs.FirstIncompleteRouteBatch), fmt.Sprint(obs.FirstIncompleteHave), fmt.Sprint(obs.FirstIncompleteExpected),
		fmt.Sprint(obs.SelectedTransactionCount), fmt.Sprint(obs.SelectedRouteBatchCount), obs.SelectedRouteBatchSequences,
		obs.SelectionError,
	}
	r.mu.Lock()
	if len(r.metaTrackProposalDiagRowsV30) < metaTrackVarianceDiagRetentionV30 {
		r.metaTrackProposalDiagRowsV30 = append(r.metaTrackProposalDiagRowsV30, row)
	}
	r.mu.Unlock()
}

func (r *NodeRuntime) recordMetaTrackAsyncDrainV30(block realblock.Block, homeShard string, itemCount int, oldestEnqueuedAt, startedAt, finishedAt time.Time, err error) {
	if r == nil {
		return
	}
	queueDelayMS := int64(0)
	if !oldestEnqueuedAt.IsZero() && !startedAt.IsZero() {
		queueDelayMS = startedAt.Sub(oldestEnqueuedAt).Milliseconds()
	}
	workMS := int64(0)
	if !startedAt.IsZero() && !finishedAt.IsZero() {
		workMS = finishedAt.Sub(startedAt).Milliseconds()
	}
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	row := []string{
		fmt.Sprint(startedAt.UnixMilli()), r.node.NodeID, r.node.ShardID,
		fmt.Sprint(block.Height), block.BlockHash, homeShard, fmt.Sprint(itemCount),
		fmt.Sprint(oldestEnqueuedAt.UnixMilli()), fmt.Sprint(startedAt.UnixMilli()), fmt.Sprint(finishedAt.UnixMilli()),
		fmt.Sprint(queueDelayMS), fmt.Sprint(workMS), fmt.Sprint(err == nil), errorText,
	}
	r.mu.Lock()
	if len(r.metaTrackAsyncDrainDiagRowsV30) < metaTrackVarianceDiagRetentionV30 {
		r.metaTrackAsyncDrainDiagRowsV30 = append(r.metaTrackAsyncDrainDiagRowsV30, row)
	}
	r.mu.Unlock()
}
