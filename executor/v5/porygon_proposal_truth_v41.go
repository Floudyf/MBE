package v5

import (
	"fmt"
	"sort"
)

// porygonPaperCertifiedUpdatesForProposal reconstructs Proposal.U directly
// from the already certified execution result of B_(h-2).  Consensus proposal
// truth must not depend on replica-local lifecycle timing in proposalUpdates.
func (r *NodeRuntime) porygonPaperCertifiedUpdatesForProposal(height uint64) ([]PorygonProposalUpdate, error) {
	if r == nil || height == 0 {
		return nil, fmt.Errorf("Porygon Proposal.U invalid proposal height %d", height)
	}
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	baseline := pipeline.baselineHeight
	if height <= baseline+2 {
		pipeline.mu.Unlock()
		return nil, nil
	}
	originHeight := height - 2
	item := pipeline.blocks[originHeight]
	if item == nil || item.Block.Height != originHeight || item.Block.BlockHash == "" {
		pipeline.mu.Unlock()
		return nil, fmt.Errorf("Porygon Proposal.U certified source execution not ready at height %d for proposal %d", originHeight, height)
	}
	phase := item.Phase
	block := item.Block
	executed := item.Execution
	pipeline.mu.Unlock()

	switch phase {
	case porygonPipelineExecuted, porygonPipelineProtocolCommitted, porygonPipelineDurable:
	default:
		return nil, fmt.Errorf("Porygon Proposal.U certified source execution not ready at height %d for proposal %d: phase=%s", originHeight, height, phase)
	}
	if executed.ExecutionResult.Height != originHeight || executed.ExecutionResult.BlockHash != block.BlockHash {
		return nil, fmt.Errorf("Porygon Proposal.U certified source execution identity mismatch at height %d", originHeight)
	}
	if executed.PorygonCertifiedExecution != nil {
		truth := porygonCloneCertifiedExecutionTruth(*executed.PorygonCertifiedExecution)
		if err := porygonValidateCertifiedExecutionTruth(truth, block.BlockHash, originHeight, height); err != nil {
			return nil, err
		}
		return porygonCloneProposalUpdates(truth.ProposalU), nil
	}
	// Focused legacy unit harnesses created before v4.5 install synthetic
	// execution records without a real Porygon block executor. Keep that isolated
	// compatibility path, but production Porygon must fail closed if the frozen
	// ESC/OC certified truth is absent.
	if r.plugins.BlockExecutor != nil && r.plugins.BlockExecutor.ID() == porygonBlockExecutorID {
		return nil, fmt.Errorf("Porygon Proposal.U frozen certified execution truth missing at height %d for proposal %d", originHeight, height)
	}
	plan, err := porygonPipelinePlan(block)
	if err != nil {
		return nil, err
	}
	deltaByID := make(map[string]struct {
		index int
		row   int
	}, len(executed.ExecutionResult.TxDeltas))
	for i, delta := range executed.ExecutionResult.TxDeltas {
		if delta.TxID == "" {
			return nil, fmt.Errorf("Porygon Proposal.U certified source contains empty TxDelta id at height %d", originHeight)
		}
		if _, exists := deltaByID[delta.TxID]; exists {
			return nil, fmt.Errorf("Porygon Proposal.U certified source contains duplicate TxDelta %s at height %d", delta.TxID, originHeight)
		}
		deltaByID[delta.TxID] = struct {
			index int
			row   int
		}{index: delta.OriginalIndex, row: i}
	}

	rows := make([]PorygonProposalUpdate, 0)
	for _, assignment := range plan.Assignments {
		if assignment.Abandoned || !assignment.CrossShard {
			continue
		}
		position, ok := deltaByID[assignment.TxID]
		if !ok {
			return nil, fmt.Errorf("Porygon Proposal.U certified source missing TxDelta %s at height %d", assignment.TxID, originHeight)
		}
		delta := executed.ExecutionResult.TxDeltas[position.row]
		if !delta.Success {
			// Post-execution OC abandonment is represented as a failed certified
			// delta and therefore must not create a future U entry.
			continue
		}
		updates := porygonPaperStateUpdates(delta, plan.ExecutionShardCount)
		update := PorygonProposalUpdate{
			TxID:           assignment.TxID,
			OriginHeight:   originHeight,
			Kind:           "commit",
			InvolvedShards: append([]int(nil), assignment.InvolvedShards...),
			Updates:        updates,
		}
		update.UpdateDigest = porygonProposalUpdateDigest(update)
		rows = append(rows, update)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].OriginHeight != rows[j].OriginHeight {
			return rows[i].OriginHeight < rows[j].OriginHeight
		}
		left := porygonPaperProposalUpdateOrderIndex(rows[i])
		right := porygonPaperProposalUpdateOrderIndex(rows[j])
		if left != right {
			return left < right
		}
		return rows[i].TxID < rows[j].TxID
	})
	return rows, nil
}

// porygonPaperVerifyCertifiedProposalU makes leader construction and backup
// verification consume exactly the same execution-derived Proposal.U truth.
func (r *NodeRuntime) porygonPaperVerifyCertifiedProposalU(height uint64, actual []PorygonProposalUpdate) error {
	actualNormal, actualRecovery := porygonV50SplitProposalUpdates(actual)
	expectedNormal, err := r.porygonPaperCertifiedUpdatesForProposal(height)
	if err != nil {
		return err
	}
	expectedNormal = porygonV50MergeProposalUpdates(expectedNormal, nil)
	expectedDigest := porygonProposalUSemanticDigest(expectedNormal)
	actualDigest := porygonProposalUSemanticDigest(actualNormal)
	if actualDigest != expectedDigest {
		r.addPorygonRuntimeMetric("porygon_proposal_u_mismatch_count", 1)
		r.addPorygonRuntimeMetric("porygon_proposal_u_mismatch_actual_count", int64(len(actualNormal)))
		r.addPorygonRuntimeMetric("porygon_proposal_u_mismatch_expected_count", int64(len(expectedNormal)))
		return fmt.Errorf("porygon L/U/T normal proposal U set mismatch at height %d: origin_height=%d actual_count=%d expected_count=%d actual_digest=%s expected_digest=%s first_diff={%s}", height, height-2, len(actualNormal), len(expectedNormal), actualDigest, expectedDigest, porygonProposalUDiffSummary(expectedNormal, actualNormal))
	}
	expectedRecovery := r.porygonV50RecoveryProposalUpdates(height)
	if porygonProposalUSemanticDigest(actualRecovery) != porygonProposalUSemanticDigest(expectedRecovery) {
		return fmt.Errorf("porygon L/U/T recovery U set mismatch at height %d: actual=%s expected=%s", height, porygonProposalUSemanticDigest(actualRecovery), porygonProposalUSemanticDigest(expectedRecovery))
	}
	if err := r.porygonV50ValidateRecoveryProposalUpdates(height, actualRecovery); err != nil {
		return err
	}
	return nil
}
