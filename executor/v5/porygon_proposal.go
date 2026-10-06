package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const porygonCompactProposalVersion = "porygon_compact_proposal_lut_v1"

type PorygonProposalUpdate struct {
	TxID           string               `json:"tx_id"`
	OriginHeight   uint64               `json:"origin_height"`
	Kind           string               `json:"kind"` // commit or rollback
	InvolvedShards []int                `json:"involved_shards"`
	Updates        []PorygonStateUpdate `json:"updates"`
	UpdateDigest   string               `json:"update_digest"`
}

type PorygonECDescriptor struct {
	ECID            string              `json:"ec_id"`
	BirthRound      uint64              `json:"birth_round"`
	WitnessRound    uint64              `json:"witness_round"`
	OrderingRound   uint64              `json:"ordering_round"`
	ExecutionRound  uint64              `json:"execution_round"`
	ExpireRound     uint64              `json:"expire_round"`
	Members         []string            `json:"members"`
	ExecutionShards map[string][]string `json:"execution_shards"`
	CommitteeDigest string              `json:"committee_digest"`
}

// PorygonProposalBody is the compact PBFT object from the paper.  L is a list
// of content-addressed Transaction Blocks, U carries older CTx updates, and T
// binds the last agreed business-state root used by this execution round.
type PorygonProposalBody struct {
	Version              string                       `json:"version"`
	Height               uint64                       `json:"height"`
	OrderingDomain       string                       `json:"ordering_domain"`
	L                    []PorygonTransactionBlockRef `json:"l"`
	U                    []PorygonProposalUpdate      `json:"u,omitempty"`
	TStateHeight         uint64                       `json:"t_state_height"`
	TStateRoot           string                       `json:"t_state_root"`
	TPartitionRoots      map[string]string            `json:"t_partition_roots,omitempty"`
	ExecutionCommittee   PorygonECDescriptor          `json:"execution_committee"`
	PreviousProposalHash string                       `json:"previous_proposal_hash"`
	PendingTxDigest      string                       `json:"pending_tx_digest,omitempty"`
	Maintenance          bool                         `json:"maintenance,omitempty"`
	ProposalDigest       string                       `json:"proposal_digest"`
}

func porygonProposalBodyDigest(body PorygonProposalBody) string {
	copyBody := body
	copyBody.ProposalDigest = ""
	sort.Slice(copyBody.L, func(i, j int) bool { return copyBody.L[i].TransactionBlockID < copyBody.L[j].TransactionBlockID })
	sort.Slice(copyBody.U, func(i, j int) bool {
		if copyBody.U[i].OriginHeight != copyBody.U[j].OriginHeight {
			return copyBody.U[i].OriginHeight < copyBody.U[j].OriginHeight
		}
		return copyBody.U[i].TxID < copyBody.U[j].TxID
	})
	return stableJSONDigest(copyBody)
}

func porygonPaperProposalUpdateShardSet(body PorygonProposalBody, shardCount int) map[string]bool {
	out := map[string]bool{}
	if shardCount < 1 {
		shardCount = 1
	}
	for _, update := range body.U {
		for _, row := range update.Updates {
			if row.Key == "" {
				continue
			}
			out[fmt.Sprintf("s%d", porygonStateShard(row.Key, shardCount))] = true
		}
	}
	return out
}

func porygonProposalUpdateDigest(update PorygonProposalUpdate) string {
	copyUpdate := update
	copyUpdate.UpdateDigest = ""
	copyUpdate.InvolvedShards = append([]int(nil), update.InvolvedShards...)
	sort.Ints(copyUpdate.InvolvedShards)
	copyUpdate.Updates = porygonCanonicalStateUpdates(copyUpdate.Updates)
	return stableJSONDigest(copyUpdate)
}

func porygonValidateProposalBody(body PorygonProposalBody, block realblock.Block) error {
	if body.Version != porygonCompactProposalVersion || body.Height != block.Height || body.OrderingDomain != block.ShardID || body.PreviousProposalHash != block.PreviousHash {
		return fmt.Errorf("Porygon compact proposal identity mismatch")
	}
	if body.TStateRoot == "" || body.ExecutionCommittee.ECID == "" {
		return fmt.Errorf("Porygon compact proposal missing T/EC")
	}
	if body.Maintenance {
		if len(body.L) != 0 {
			return fmt.Errorf("Porygon maintenance proposal must have empty L")
		}
	} else if len(body.L) == 0 {
		return fmt.Errorf("Porygon transaction proposal missing L")
	}
	seenTB := map[string]bool{}
	for _, ref := range body.L {
		if ref.TransactionBlockID == "" || ref.TransactionCount < 1 || ref.FullBodyDigest == "" || ref.WitnessCertificateDigest == "" {
			return fmt.Errorf("Porygon compact proposal contains incomplete TransactionBlockRef")
		}
		if seenTB[ref.TransactionBlockID] {
			return fmt.Errorf("Porygon compact proposal duplicate TransactionBlockRef")
		}
		seenTB[ref.TransactionBlockID] = true
	}
	for _, update := range body.U {
		if update.TxID == "" || update.OriginHeight == 0 || update.UpdateDigest == "" || update.UpdateDigest != porygonProposalUpdateDigest(update) {
			return fmt.Errorf("Porygon compact proposal invalid U update")
		}
	}
	if body.ProposalDigest == "" || body.ProposalDigest != porygonProposalBodyDigest(body) {
		return fmt.Errorf("Porygon compact proposal digest mismatch")
	}
	return nil
}

func porygonCompactProposal(block realblock.Block) (realblock.Block, error) {
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return block, err
	}
	if evidence.CompactProposal == nil {
		return block, fmt.Errorf("Porygon compact proposal context missing")
	}
	if err := porygonValidateProposalBody(*evidence.CompactProposal, block); err != nil {
		return block, err
	}
	block.TxList = nil
	block.TxIDs = nil
	block.TxRoot = realblock.TxRoot(nil)
	realblock.AssignHash(&block)
	return block, nil
}

func porygonCompactProposalTransactions(block realblock.Block) bool {
	if block.ProposalEvidence == nil || block.ProposalEvidence.AlgorithmID != porygonProposalEvidenceID || len(block.ProposalEvidence.Payload) == 0 {
		return false
	}
	var evidence porygonTransactionBlockEvidence
	if json.Unmarshal(block.ProposalEvidence.Payload, &evidence) != nil {
		return false
	}
	return evidence.CompactProposal != nil && evidence.CompactProposal.Version == porygonCompactProposalVersion && len(block.TxList) == 0
}

func (r *NodeRuntime) porygonBindCompactProposalContext(block realblock.Block) (realblock.Block, error) {
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return block, err
	}
	maintenance := len(block.TxList) == 0
	refs := []PorygonTransactionBlockRef{}
	if !maintenance {
		if evidence.WitnessCertificate == nil || evidence.WitnessCertificate.CertificateDigest == "" {
			return block, fmt.Errorf("Porygon compact transaction proposal requires witness certificate")
		}
		body := porygonBuildTransactionBlock(block.Height, block.ShardID, block.TxList, evidence.WitnessCertificate.CertificateDigest)
		if err := r.porygonStoreTransactionBlockForRole(body); err != nil {
			return block, err
		}
		ref := porygonTransactionBlockRef(body)
		ref.StorageNodeIDs = r.porygonTransactionBlockStorageNodeIDs(body)
		if len(ref.StorageNodeIDs) == 0 {
			return block, fmt.Errorf("Porygon transaction block %s has no durable Storage Role source", body.TransactionBlockID)
		}
		refs = append(refs, ref)
	} else {
		evidence.WitnessPolicy = "paper_maintenance_no_witness_v1"
		evidence.WitnessThreshold = 1
		evidence.DataAvailabilityRef = "paper_maintenance_no_transaction_block"
		evidence.WitnessCertificate = nil
	}
	stateHeight, stateRoot, partitionRoots := r.porygonPaperStateAnchorForProposal(block.Height)
	if stateRoot == "" {
		return block, fmt.Errorf("Porygon compact proposal agreed T state unavailable")
	}
	updates, updatesErr := r.porygonPaperCertifiedUpdatesForProposal(block.Height)
	if updatesErr != nil {
		return block, updatesErr
	}
	ec := porygonECDescriptorForHeight(r.plan.NodeConfigs, block.Height, block.ShardID, r.porygonExecutionShardCount(), r.porygonExecutionCommitteeCount())
	proposal := PorygonProposalBody{
		Version: porygonCompactProposalVersion, Height: block.Height, OrderingDomain: block.ShardID,
		L: refs, U: updates,
		TStateHeight: stateHeight, TStateRoot: stateRoot, TPartitionRoots: partitionRoots,
		ExecutionCommittee: ec, PreviousProposalHash: block.PreviousHash, PendingTxDigest: evidence.PreviousPendingDigest,
		Maintenance: maintenance,
	}
	proposal.ProposalDigest = porygonProposalBodyDigest(proposal)
	evidence.CompactProposal = &proposal
	if !maintenance && evidence.WitnessCertificate != nil {
		evidence.DataAvailabilityRef = evidence.WitnessCertificate.CertificateDigest
	}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, evidence); err != nil {
		return block, err
	}
	realblock.AssignHash(&block)
	return block, nil
}

func (r *NodeRuntime) porygonHydrateCompactProposal(ctx context.Context, compact realblock.Block) (realblock.Block, error) {
	if !porygonCompactProposalTransactions(compact) {
		return compact, nil
	}
	evidence, err := decodePorygonTransactionBlockEvidence(compact)
	if err != nil {
		return compact, err
	}
	proposal := evidence.CompactProposal
	if proposal == nil {
		return compact, fmt.Errorf("Porygon compact proposal body missing")
	}
	candidates := []string{}
	items := []tx.SignedTransaction{}
	txIDs := []string{}
	for _, ref := range proposal.L {
		body, err := r.porygonFetchTransactionBlock(ctx, ref, candidates)
		if err != nil {
			return compact, err
		}
		items = append(items, body.Transactions...)
		txIDs = append(txIDs, body.TransactionIDs...)
	}
	hydrated := compact
	hydrated.TxList = items
	hydrated.TxIDs = txIDs
	// Keep the compact PBFT TxRoot/BlockHash unchanged.  The TransactionBlockRef
	// and witness certificate are the consensus commitment to hydrated bytes.
	if err := r.porygonVerifyHydratedPlan(hydrated); err != nil {
		return compact, err
	}
	r.addPorygonRuntimeMetric("porygon_compact_proposal_hydration_count", 1)
	return hydrated, nil
}

func porygonSchedulerRuntimeConfig(plugin SchedulerPlugin) map[string]any {
	if plugin == nil {
		return nil
	}
	if scheduler, ok := plugin.(porygonScheduler); ok {
		return scheduler.config
	}
	return nil
}

func porygonPlanningReservedItems(original, scheduled realblock.Block) []tx.SignedTransaction {
	if porygonCompactProposalTransactions(scheduled) {
		return original.TxList
	}
	return scheduled.TxList
}

func porygonConsensusStorageBlock(block realblock.Block) realblock.Block {
	if block.ProposalEvidence == nil || block.ProposalEvidence.AlgorithmID != porygonProposalEvidenceID {
		return block
	}
	var evidence porygonTransactionBlockEvidence
	if json.Unmarshal(block.ProposalEvidence.Payload, &evidence) != nil || evidence.CompactProposal == nil {
		return block
	}
	compact := block
	compact.TxList = nil
	compact.TxIDs = nil
	compact.TxRoot = realblock.TxRoot(nil)
	// Keep the PBFT-certified BlockHash. It already commits to this compact body.
	return compact
}

func (r *NodeRuntime) porygonVerifyHydratedPlan(block realblock.Block) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != porygonPlanAlgorithmID {
		return fmt.Errorf("Porygon hydrated proposal execution plan missing")
	}
	var supplied porygonExecutionPlan
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &supplied); err != nil {
		return err
	}
	expected, err := buildPorygonPlan(block, porygonSchedulerRuntimeConfig(r.plugins.Scheduler))
	if err != nil {
		return err
	}
	if supplied.PlanDigest != expected.PlanDigest || stableJSONDigest(porygonPlanDigestProjection(supplied)) != stableJSONDigest(porygonPlanDigestProjection(expected)) {
		return fmt.Errorf("Porygon hydrated transaction body does not match consensus execution plan")
	}
	return nil
}
