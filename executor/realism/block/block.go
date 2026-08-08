package block

import (
	"encoding/json"

	"metaverse-chainlab/executor/realism/tx"
)

type SystemStateDelta struct {
	DeltaID         string   `json:"delta_id"`
	Key             string   `json:"key"`
	Value           string   `json:"value"`
	TxID            string   `json:"tx_id,omitempty"`
	TxIDs           []string `json:"tx_ids,omitempty"`
	UpdateSemantics string   `json:"update_semantics,omitempty"`
	Delta           int64    `json:"delta,omitempty"`
	BaseValue       string   `json:"base_value,omitempty"`
	BaseValueDigest string   `json:"base_value_digest,omitempty"`
	ApplyOrigin     string   `json:"apply_origin,omitempty"`
	DeltaKind       string   `json:"delta_kind,omitempty"`
	HasInitialValue bool     `json:"has_initial_value,omitempty"`
	InitialValue    int64    `json:"initial_value"`
	HomeShard       string   `json:"home_shard"`
	ExecutionShard  string   `json:"execution_shard"`
	SourceKey       string   `json:"source_key"`
	SourceHeight    uint64   `json:"source_height"`
	RoutingOrdinal  uint64   `json:"routing_ordinal,omitempty"`
	PreviousVersion uint64   `json:"previous_version,omitempty"`
	ProducedVersion uint64   `json:"produced_version,omitempty"`
	OrderingNoop    bool     `json:"ordering_noop,omitempty"`
	SourceBlockHash string   `json:"source_block_hash"`
}

type Block struct {
	BlockHash          string                    `json:"block_hash"`
	ShardID            string                    `json:"shard_id"`
	Height             uint64                    `json:"height"`
	PreviousHash       string                    `json:"previous_hash"`
	ProposerID         string                    `json:"proposer_id"`
	Timestamp          int64                     `json:"timestamp"`
	TxIDs              []string                  `json:"tx_ids"`
	TxList             []tx.SignedTransaction    `json:"tx_list"`
	TxRoot             string                    `json:"tx_root"`
	StateRootBefore    string                    `json:"state_root_before"`
	StateRootAfter     string                    `json:"state_root_after"`
	ReceiptRoot        string                    `json:"receipt_root"`
	ProposerDigest     string                    `json:"proposer_digest"`
	StateCommit        bool                      `json:"state_commit"`
	CrossShardProtocol bool                      `json:"cross_shard_protocol"`
	SystemStateDeltas  []SystemStateDelta        `json:"system_state_deltas,omitempty"`
	ExecutionPlan      *ExecutionPlanEnvelope    `json:"execution_plan,omitempty"`
	ProposalEvidence   *ProposalEvidenceEnvelope `json:"proposal_evidence,omitempty"`
}

type ProposalEvidenceEnvelope struct {
	AlgorithmID   string          `json:"algorithm_id"`
	PayloadDigest string          `json:"payload_digest"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type ExecutionPlanEnvelope struct {
	AlgorithmID   string          `json:"algorithm_id"`
	PayloadDigest string          `json:"payload_digest"`
	PlanDigest    string          `json:"plan_digest"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}
