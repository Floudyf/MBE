package block

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func TxRoot(txIDs []string) string {
	payload, _ := json.Marshal(txIDs)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func Hash(b Block) string {
	core := struct {
		ShardID           string             `json:"shard_id"`
		Height            uint64             `json:"height"`
		PreviousHash      string             `json:"previous_hash"`
		ProposerID        string             `json:"proposer_id"`
		Timestamp         int64              `json:"timestamp"`
		TxIDs             []string           `json:"tx_ids"`
		TxRoot            string             `json:"tx_root"`
		StateRootBefore   string             `json:"state_root_before"`
		StateRootAfter    string             `json:"state_root_after"`
		ReceiptRoot       string             `json:"receipt_root"`
		SystemStateDeltas []SystemStateDelta `json:"system_state_deltas,omitempty"`
		ExecutionPlan     *struct {
			AlgorithmID   string `json:"algorithm_id"`
			PayloadDigest string `json:"payload_digest"`
			PlanDigest    string `json:"plan_digest"`
		} `json:"execution_plan,omitempty"`
		ProposalEvidence *struct {
			AlgorithmID   string `json:"algorithm_id"`
			PayloadDigest string `json:"payload_digest"`
		} `json:"proposal_evidence,omitempty"`
	}{
		ShardID: b.ShardID, Height: b.Height, PreviousHash: b.PreviousHash, ProposerID: b.ProposerID,
		Timestamp: b.Timestamp, TxIDs: b.TxIDs, TxRoot: b.TxRoot, StateRootBefore: b.StateRootBefore,
		StateRootAfter: b.StateRootAfter, ReceiptRoot: b.ReceiptRoot, SystemStateDeltas: b.SystemStateDeltas,
	}
	if b.ExecutionPlan != nil {
		core.ExecutionPlan = &struct {
			AlgorithmID   string `json:"algorithm_id"`
			PayloadDigest string `json:"payload_digest"`
			PlanDigest    string `json:"plan_digest"`
		}{AlgorithmID: b.ExecutionPlan.AlgorithmID, PayloadDigest: b.ExecutionPlan.PayloadDigest, PlanDigest: b.ExecutionPlan.PlanDigest}
	}
	if b.ProposalEvidence != nil {
		core.ProposalEvidence = &struct {
			AlgorithmID   string `json:"algorithm_id"`
			PayloadDigest string `json:"payload_digest"`
		}{AlgorithmID: b.ProposalEvidence.AlgorithmID, PayloadDigest: b.ProposalEvidence.PayloadDigest}
	}
	payload, _ := json.Marshal(core)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func AssignHash(b *Block) {
	b.TxRoot = TxRoot(b.TxIDs)
	b.ProposerDigest = b.TxRoot
	b.BlockHash = Hash(*b)
}
