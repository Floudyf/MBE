package v5

import (
	"fmt"
	"sort"
)

const porygonCertifiedExecutionTruthVersion = "porygon_certified_execution_truth_v2"

type PorygonFutureProtocolObligation struct {
	TxID         string `json:"tx_id"`
	Kind         string `json:"kind"` // proposal_u or final_commit
	TargetHeight uint64 `json:"target_height"`
}

type PorygonCertifiedExecutionTruth struct {
	Version                     string                            `json:"version"`
	BlockHash                   string                            `json:"block_hash"`
	Height                      uint64                            `json:"height"`
	ESCResultDigests            map[string]string                 `json:"esc_result_digests"`
	RequiredExecutionShards     []string                          `json:"required_execution_shards,omitempty"`
	CommitteeEpochDigest        string                            `json:"committee_epoch_digest"`
	ESCSemanticDigest           string                            `json:"esc_semantic_digest"`
	FinalAssignmentsDigest      string                            `json:"final_assignments_digest"`
	RetainedCrossShardTxIDs     []string                          `json:"retained_cross_shard_tx_ids,omitempty"`
	PostExecutionAbandonedTxIDs []string                          `json:"post_execution_abandoned_tx_ids,omitempty"`
	FutureObligations           []PorygonFutureProtocolObligation `json:"future_obligations,omitempty"`
	FutureProposalHeight        uint64                            `json:"future_proposal_height"`
	ProposalU                   []PorygonProposalUpdate           `json:"proposal_u,omitempty"`
	ProposalUDigest             string                            `json:"proposal_u_digest"`
	SemanticDigest              string                            `json:"semantic_digest"`
}

type porygonCertifiedAssignmentTruth struct {
	TxID           string   `json:"tx_id"`
	OriginalIndex  int      `json:"original_index"`
	ExecutionShard int      `json:"execution_shard"`
	CrossShard     bool     `json:"cross_shard"`
	Abandoned      bool     `json:"abandoned"`
	ConflictReason string   `json:"conflict_reason,omitempty"`
	ConflictWith   []string `json:"conflict_with,omitempty"`
	InvolvedShards []int    `json:"involved_shards,omitempty"`
}

type porygonCertifiedESCResultIdentity struct {
	ExecutionShardID string `json:"execution_shard_id"`
	ResultDigest     string `json:"result_digest"`
}

func porygonCloneProposalUpdates(items []PorygonProposalUpdate) []PorygonProposalUpdate {
	if len(items) == 0 {
		return nil
	}
	out := make([]PorygonProposalUpdate, len(items))
	for i, item := range items {
		copyItem := item
		copyItem.InvolvedShards = append([]int(nil), item.InvolvedShards...)
		copyItem.Updates = append([]PorygonStateUpdate(nil), item.Updates...)
		out[i] = copyItem
	}
	return out
}

// porygonCanonicalProposalUpdates defines one semantic representation for
// Proposal.U.  In particular nil and [] are the same zero-entry U; JSON
// omitempty may round-trip [] to nil and must never change consensus truth.
// Outer row order is preserved because Proposal.U order is protocol-significant.
func porygonCanonicalProposalUpdates(items []PorygonProposalUpdate) []PorygonProposalUpdate {
	if len(items) == 0 {
		return nil
	}
	out := porygonCloneProposalUpdates(items)
	for i := range out {
		out[i].InvolvedShards = append([]int(nil), out[i].InvolvedShards...)
		sort.Ints(out[i].InvolvedShards)
		out[i].Updates = porygonCanonicalStateUpdates(out[i].Updates)
	}
	return out
}

func porygonProposalUSemanticDigest(items []PorygonProposalUpdate) string {
	return stableJSONDigest(porygonCanonicalProposalUpdates(items))
}

func porygonCloneCertifiedExecutionTruth(in PorygonCertifiedExecutionTruth) PorygonCertifiedExecutionTruth {
	out := in
	out.ESCResultDigests = copyRegistryStringMap(in.ESCResultDigests)
	out.RequiredExecutionShards = append([]string(nil), in.RequiredExecutionShards...)
	out.RetainedCrossShardTxIDs = append([]string(nil), in.RetainedCrossShardTxIDs...)
	out.PostExecutionAbandonedTxIDs = append([]string(nil), in.PostExecutionAbandonedTxIDs...)
	out.FutureObligations = append([]PorygonFutureProtocolObligation(nil), in.FutureObligations...)
	out.ProposalU = porygonCanonicalProposalUpdates(in.ProposalU)
	return out
}

func porygonCertifiedAssignmentDigest(assignments []porygonTxAssignment) string {
	rows := make([]porygonCertifiedAssignmentTruth, 0, len(assignments))
	for _, assignment := range assignments {
		conflictWith := append([]string(nil), assignment.ConflictWith...)
		sort.Strings(conflictWith)
		involved := append([]int(nil), assignment.InvolvedShards...)
		sort.Ints(involved)
		rows = append(rows, porygonCertifiedAssignmentTruth{
			TxID: assignment.TxID, OriginalIndex: assignment.OriginalIndex, ExecutionShard: assignment.ExecutionShard,
			CrossShard: assignment.CrossShard, Abandoned: assignment.Abandoned, ConflictReason: assignment.ConflictReason,
			ConflictWith: conflictWith, InvolvedShards: involved,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].OriginalIndex != rows[j].OriginalIndex {
			return rows[i].OriginalIndex < rows[j].OriginalIndex
		}
		return rows[i].TxID < rows[j].TxID
	})
	return stableJSONDigest(rows)
}

func porygonBatchCertificateSemanticDigest(cert PorygonESCBatchCertificate) string {
	rows := make([]porygonCertifiedESCResultIdentity, 0, len(cert.Entries))
	for _, entry := range cert.Entries {
		rows = append(rows, porygonCertifiedESCResultIdentity{ExecutionShardID: entry.ExecutionShardID, ResultDigest: entry.ResultDigest})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ExecutionShardID < rows[j].ExecutionShardID })
	projection := struct {
		BlockHash            string                              `json:"block_hash"`
		Height               uint64                              `json:"height"`
		CommitteeEpochDigest string                              `json:"committee_epoch_digest"`
		Entries              []porygonCertifiedESCResultIdentity `json:"entries"`
	}{cert.BlockHash, cert.Height, cert.CommitteeEpochDigest, rows}
	return stableJSONDigest(projection)
}

func porygonCertifiedESCMapSemanticDigest(blockHash string, height uint64, committeeEpochDigest string, rows map[string]string) string {
	items := make([]porygonCertifiedESCResultIdentity, 0, len(rows))
	for shard, digest := range rows {
		items = append(items, porygonCertifiedESCResultIdentity{ExecutionShardID: shard, ResultDigest: digest})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ExecutionShardID < items[j].ExecutionShardID })
	projection := struct {
		BlockHash            string                              `json:"block_hash"`
		Height               uint64                              `json:"height"`
		CommitteeEpochDigest string                              `json:"committee_epoch_digest"`
		Entries              []porygonCertifiedESCResultIdentity `json:"entries"`
	}{blockHash, height, committeeEpochDigest, items}
	return stableJSONDigest(projection)
}

func porygonCanonicalFutureObligations(items []PorygonFutureProtocolObligation) []PorygonFutureProtocolObligation {
	if len(items) == 0 {
		return nil
	}
	out := append([]PorygonFutureProtocolObligation(nil), items...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].TargetHeight != out[j].TargetHeight {
			return out[i].TargetHeight < out[j].TargetHeight
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].TxID < out[j].TxID
	})
	return out
}

func porygonCertifiedExecutionSemanticDigest(truth PorygonCertifiedExecutionTruth) string {
	copyTruth := porygonCloneCertifiedExecutionTruth(truth)
	copyTruth.SemanticDigest = ""
	return stableJSONDigest(copyTruth)
}

func porygonBuildCertifiedExecutionTruth(
	blockHash string,
	height uint64,
	plan porygonExecutionPlan,
	finalAssignments []porygonTxAssignment,
	certified map[string]porygonWaveResult,
	escResultDigests map[string]string,
) (PorygonCertifiedExecutionTruth, error) {
	if blockHash == "" || height == 0 || plan.BlockHeight != height {
		return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution identity mismatch at height %d", height)
	}
	originalByID := map[string]porygonTxAssignment{}
	requiredByCurrentL := map[string]bool{}
	for _, assignment := range plan.Assignments {
		originalByID[assignment.TxID] = assignment
		if assignment.Abandoned {
			continue
		}
		requiredByCurrentL[porygonExecutionShardID(assignment.ExecutionShard)] = true
		result, ok := certified[assignment.TxID]
		if !ok {
			return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution missing tx result %s at height %d", assignment.TxID, height)
		}
		if result.Delta.TxID != assignment.TxID || result.Delta.OriginalIndex != assignment.OriginalIndex {
			return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution tx identity/index mismatch for %s at height %d", assignment.TxID, height)
		}
	}
	// The authenticated ESC certificate is the authoritative shard coverage. A
	// maintenance proposal may have no L assignments while Proposal.U still
	// requires older CTx roots from one or more ESCs. Therefore do not derive
	// required coverage from current L alone; only require every retained L owner
	// to be covered by the certified ESC result set.
	requiredESCSet := map[string]bool{}
	for shard, digest := range escResultDigests {
		if shard == "" || digest == "" {
			return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution has incomplete ESC result identity at height %d", height)
		}
		requiredESCSet[shard] = true
	}
	for shard := range requiredByCurrentL {
		if !requiredESCSet[shard] {
			return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution missing owning ESC result %s at height %d", shard, height)
		}
	}
	requiredShards := sortedBoolKeys(requiredESCSet)

	rows := make([]PorygonProposalUpdate, 0)
	retainedCTx := make([]string, 0)
	postAbandoned := make([]string, 0)
	obligations := make([]PorygonFutureProtocolObligation, 0)
	for _, assignment := range finalAssignments {
		original := originalByID[assignment.TxID]
		if assignment.Abandoned {
			if original.CrossShard && !original.Abandoned {
				postAbandoned = append(postAbandoned, assignment.TxID)
			}
			continue
		}
		result, ok := certified[assignment.TxID]
		if !ok {
			return PorygonCertifiedExecutionTruth{}, fmt.Errorf("Porygon certified execution retained tx missing result %s at height %d", assignment.TxID, height)
		}
		if !result.Delta.Success {
			continue
		}
		if assignment.CrossShard {
			retainedCTx = append(retainedCTx, assignment.TxID)
			updates := porygonPaperStateUpdates(result.Delta, plan.ExecutionShardCount)
			update := PorygonProposalUpdate{
				TxID: assignment.TxID, OriginHeight: height, Kind: "commit",
				InvolvedShards: append([]int(nil), assignment.InvolvedShards...), Updates: updates,
			}
			update.UpdateDigest = porygonProposalUpdateDigest(update)
			rows = append(rows, update)
			obligations = append(obligations,
				PorygonFutureProtocolObligation{TxID: assignment.TxID, Kind: "proposal_u", TargetHeight: height + 2},
				PorygonFutureProtocolObligation{TxID: assignment.TxID, Kind: "final_commit", TargetHeight: height + 4},
			)
		} else {
			obligations = append(obligations, PorygonFutureProtocolObligation{TxID: assignment.TxID, Kind: "final_commit", TargetHeight: height + 2})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left := porygonPaperProposalUpdateOrderIndex(rows[i])
		right := porygonPaperProposalUpdateOrderIndex(rows[j])
		if left != right {
			return left < right
		}
		return rows[i].TxID < rows[j].TxID
	})
	sort.Strings(retainedCTx)
	sort.Strings(postAbandoned)
	rows = porygonCanonicalProposalUpdates(rows)

	committeeEpochDigest := porygonCommitteeEpochSeed(height, plan.OrderingDomain)
	truth := PorygonCertifiedExecutionTruth{
		Version: porygonCertifiedExecutionTruthVersion, BlockHash: blockHash, Height: height,
		ESCResultDigests: copyRegistryStringMap(escResultDigests), RequiredExecutionShards: append([]string(nil), requiredShards...),
		CommitteeEpochDigest:    committeeEpochDigest,
		ESCSemanticDigest:       porygonCertifiedESCMapSemanticDigest(blockHash, height, committeeEpochDigest, escResultDigests),
		FinalAssignmentsDigest:  porygonCertifiedAssignmentDigest(finalAssignments),
		RetainedCrossShardTxIDs: retainedCTx, PostExecutionAbandonedTxIDs: postAbandoned,
		FutureObligations:    porygonCanonicalFutureObligations(obligations),
		FutureProposalHeight: height + 2, ProposalU: rows, ProposalUDigest: porygonProposalUSemanticDigest(rows),
	}
	truth.SemanticDigest = porygonCertifiedExecutionSemanticDigest(truth)
	return truth, nil
}

func porygonValidateCertifiedExecutionTruth(truth PorygonCertifiedExecutionTruth, blockHash string, originHeight, proposalHeight uint64) error {
	if truth.Version != porygonCertifiedExecutionTruthVersion || truth.BlockHash != blockHash || truth.Height != originHeight || truth.FutureProposalHeight != proposalHeight {
		return fmt.Errorf("Porygon certified execution truth identity mismatch: origin=%d proposal=%d", originHeight, proposalHeight)
	}
	if truth.ProposalUDigest == "" || truth.ProposalUDigest != porygonProposalUSemanticDigest(truth.ProposalU) {
		return fmt.Errorf("Porygon certified execution Proposal.U digest mismatch at origin height %d", originHeight)
	}
	if truth.FinalAssignmentsDigest == "" || truth.CommitteeEpochDigest == "" || truth.ESCSemanticDigest == "" || truth.ESCSemanticDigest != porygonCertifiedESCMapSemanticDigest(truth.BlockHash, truth.Height, truth.CommitteeEpochDigest, truth.ESCResultDigests) {
		return fmt.Errorf("Porygon certified execution truth incomplete at origin height %d", originHeight)
	}
	if len(truth.RequiredExecutionShards) != len(truth.ESCResultDigests) {
		return fmt.Errorf("Porygon certified execution truth ESC coverage mismatch at origin height %d", originHeight)
	}
	for _, shard := range truth.RequiredExecutionShards {
		if truth.ESCResultDigests[shard] == "" {
			return fmt.Errorf("Porygon certified execution truth missing ESC result %s at origin height %d", shard, originHeight)
		}
	}
	if stableJSONDigest(porygonCanonicalFutureObligations(truth.FutureObligations)) != stableJSONDigest(truth.FutureObligations) {
		return fmt.Errorf("Porygon certified execution future obligations are not canonical at origin height %d", originHeight)
	}
	if truth.SemanticDigest == "" || truth.SemanticDigest != porygonCertifiedExecutionSemanticDigest(truth) {
		return fmt.Errorf("Porygon certified execution semantic digest mismatch at origin height %d", originHeight)
	}
	return nil
}

func porygonProposalUDiffSummary(expected, actual []PorygonProposalUpdate) string {
	expected = porygonCanonicalProposalUpdates(expected)
	actual = porygonCanonicalProposalUpdates(actual)
	limit := len(expected)
	if len(actual) < limit {
		limit = len(actual)
	}
	for i := 0; i < limit; i++ {
		if stableJSONDigest(expected[i]) == stableJSONDigest(actual[i]) {
			continue
		}
		return fmt.Sprintf("index=%d expected_tx=%s expected_origin=%d expected_update=%s actual_tx=%s actual_origin=%d actual_update=%s", i,
			expected[i].TxID, expected[i].OriginHeight, expected[i].UpdateDigest,
			actual[i].TxID, actual[i].OriginHeight, actual[i].UpdateDigest)
	}
	if len(expected) != len(actual) {
		return fmt.Sprintf("length expected=%d actual=%d first_extra_index=%d", len(expected), len(actual), limit)
	}
	return "digest_mismatch_without_index_diff"
}

func (r *NodeRuntime) porygonCertifiedFutureProposalPending(nextHeight uint64) bool {
	if r == nil || nextHeight == 0 {
		return false
	}
	value, explicit := porygonPipelineRuntimes.Load(r)
	if !explicit {
		if !r.porygonPipelineEnabledRuntime() {
			return false
		}
		value = r.porygonPipelineRuntime()
	}
	pipeline := value.(*porygonPipelineRuntime)
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	for _, item := range pipeline.blocks {
		if item == nil || item.Execution.PorygonCertifiedExecution == nil {
			continue
		}
		truth := item.Execution.PorygonCertifiedExecution
		for _, obligation := range truth.FutureObligations {
			if obligation.TargetHeight >= nextHeight {
				return true
			}
		}
	}
	return false
}

// porygonCertifiedCTxDispositionSnapshot returns only dispositions that have
// already crossed the authenticated ESC/OC execution boundary. true means the
// CTx remains retained/pending; false means the OC already abandoned it.
func (r *NodeRuntime) porygonCertifiedCTxDispositionSnapshot() map[string]bool {
	out := map[string]bool{}
	if r == nil {
		return out
	}
	value, ok := porygonPipelineRuntimes.Load(r)
	if !ok {
		return out
	}
	pipeline := value.(*porygonPipelineRuntime)
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	for _, item := range pipeline.blocks {
		if item == nil || item.Execution.PorygonCertifiedExecution == nil {
			continue
		}
		truth := item.Execution.PorygonCertifiedExecution
		for _, txID := range truth.RetainedCrossShardTxIDs {
			out[fmt.Sprintf("%d|%s", truth.Height, txID)] = true
		}
		for _, txID := range truth.PostExecutionAbandonedTxIDs {
			out[fmt.Sprintf("%d|%s", truth.Height, txID)] = false
		}
	}
	return out
}
