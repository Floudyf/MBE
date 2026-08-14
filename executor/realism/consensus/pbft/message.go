package pbft

import "metaverse-chainlab/executor/realism/block"

type PrePrepare struct {
	View      uint64      `json:"view"`
	Sequence  uint64      `json:"sequence"`
	Height    uint64      `json:"height"`
	LeaderID  string      `json:"leader_id"`
	BlockHash string      `json:"block_hash"`
	Block     block.Block `json:"block"`
	Signature string      `json:"signature,omitempty"`
}

type Prepare struct {
	View      uint64 `json:"view"`
	Sequence  uint64 `json:"sequence"`
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
	BlockHash string `json:"block_hash"`
	Signature string `json:"signature,omitempty"`
}

type Commit struct {
	View      uint64 `json:"view"`
	Sequence  uint64 `json:"sequence"`
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
	BlockHash string `json:"block_hash"`
	Signature string `json:"signature,omitempty"`
}

// CommitCertificate proves that a block reached the PBFT commit quorum in a
// concrete view/sequence. It is retained beyond durable commit so a correct
// replica that missed the original COMMIT multicast can safely catch up.
type CommitCertificate struct {
	View      uint64   `json:"view"`
	Sequence  uint64   `json:"sequence"`
	Height    uint64   `json:"height"`
	BlockHash string   `json:"block_hash"`
	Commits   []Commit `json:"commits"`
}

// PreparedCertificate is the validator-authenticated proof that a proposal
// reached the prepare quorum in one view/sequence. Each embedded PREPARE keeps
// its Ed25519 validator signature so the proof remains independently verifiable
// when carried by VIEW-CHANGE / NEW-VIEW or persisted beyond the live TCP hop.
type PreparedCertificate struct {
	View      uint64    `json:"view"`
	Sequence  uint64    `json:"sequence"`
	Height    uint64    `json:"height"`
	BlockHash string    `json:"block_hash"`
	Prepares  []Prepare `json:"prepares"`
}

// Checkpoint records a replica's durable state digest at a committed height.
type Checkpoint struct {
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
	BlockHash string `json:"block_hash"`
	StateRoot string `json:"state_root"`
	Signature string `json:"signature,omitempty"`
}

// CheckpointCertificate is a stable checkpoint proof.
type CheckpointCertificate struct {
	Height      uint64       `json:"height"`
	BlockHash   string       `json:"block_hash"`
	StateRoot   string       `json:"state_root"`
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// ViewChange carries the highest prepared certificate known by the sender plus
// its latest stable checkpoint. MBE V5 serializes one consensus height at a
// time, so one highest prepared certificate is sufficient for the active slot.
type ViewChange struct {
	View             uint64                 `json:"view"`
	NewView          uint64                 `json:"new_view"`
	NodeID           string                 `json:"node_id"`
	Height           uint64                 `json:"height"`
	LeaderID         string                 `json:"leader_id"`
	StableCheckpoint *CheckpointCertificate `json:"stable_checkpoint,omitempty"`
	Prepared         *PreparedCertificate   `json:"prepared,omitempty"`
	PreparedBlock    *block.Block           `json:"prepared_block,omitempty"`
	Signature        string                 `json:"signature,omitempty"`
}

// NewView contains the view-change quorum and the safe prepared proposal, when
// one exists. The new primary must carry that exact block hash into the new
// view; it may create a fresh proposal only when SelectedPrepared is nil.
type NewView struct {
	View             uint64               `json:"view"`
	LeaderID         string               `json:"leader_id"`
	Height           uint64               `json:"height"`
	ViewChanges      []ViewChange         `json:"view_changes"`
	SelectedPrepared *PreparedCertificate `json:"selected_prepared,omitempty"`
	SelectedBlock    *block.Block         `json:"selected_block,omitempty"`
	Signature        string               `json:"signature,omitempty"`
}
