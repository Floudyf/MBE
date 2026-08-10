package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/block"
)

func TestDurableProposalStorageCompressionRoundTripPreservesBlockTruth(t *testing.T) {
	semantic := []byte(`{"candidate_count":1000,"candidate_transactions":["` + strings.Repeat("same-state-key-and-transaction-shape,", 6000) + `"]}`)
	sum := sha256.Sum256(semantic)
	digest := hex.EncodeToString(sum[:])
	original := block.Block{
		ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0",
		TxIDs:            []string{"tx-1"},
		ProposalEvidence: &block.ProposalEvidenceEnvelope{AlgorithmID: "aria_candidate_selection_v2", PayloadDigest: digest, Payload: append(json.RawMessage(nil), semantic...)},
	}
	block.AssignHash(&original)
	stored, err := blockForDurableStorage(original)
	if err != nil {
		t.Fatal(err)
	}
	if got := durableProposalStorageEncoding(stored); got != "gzip+base64+json" {
		t.Fatalf("storage codec=%s", got)
	}
	if len(stored.ProposalEvidence.Payload) >= len(original.ProposalEvidence.Payload) {
		t.Fatalf("stored evidence did not shrink: stored=%d original=%d", len(stored.ProposalEvidence.Payload), len(original.ProposalEvidence.Payload))
	}
	if stored.BlockHash != original.BlockHash || block.Hash(stored) != original.BlockHash {
		t.Fatal("storage representation changed block commitment")
	}
	if string(original.ProposalEvidence.Payload) != string(semantic) {
		t.Fatal("storage encoding mutated live block")
	}

	encoded, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	var decoded block.Block
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := restoreBlockFromDurableStorage(&decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.ProposalEvidence.Payload) != string(semantic) {
		t.Fatal("recovery did not restore exact proposal evidence bytes")
	}
	if decoded.ProposalEvidence.PayloadDigest != digest || block.Hash(decoded) != original.BlockHash {
		t.Fatal("recovered block commitment changed")
	}
}

func TestReadCommittedRestoresCompressedProposalEvidenceForCatchup(t *testing.T) {
	dir := t.TempDir()
	semantic := []byte(`{"candidate_count":1000,"candidate_transactions":["` + strings.Repeat("recoverable-evidence,", 8000) + `"]}`)
	sum := sha256.Sum256(semantic)
	original := block.Block{
		ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", TxIDs: []string{"tx-1"},
		ProposalEvidence: &block.ProposalEvidenceEnvelope{AlgorithmID: "groundhog_candidate_selection_v1", PayloadDigest: hex.EncodeToString(sum[:]), Payload: semantic},
	}
	block.AssignHash(&original)
	stored, err := blockForDurableStorage(original)
	if err != nil {
		t.Fatal(err)
	}
	if durableProposalStorageEncoding(stored) != "gzip+base64+json" {
		t.Fatal("fixture did not compress")
	}
	if _, err := appendJSON(filepath.Join(dir, "blocks.jsonl"), stored); err != nil {
		t.Fatal(err)
	}
	marker := CommitMarker{Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0", Height: 1, BlockHash: original.BlockHash, Committed: true}
	if _, err := appendJSON(filepath.Join(dir, "commit_markers.jsonl"), marker); err != nil {
		t.Fatal(err)
	}
	store := NewBlockStore(dir, "n0", "s0")
	blocks, err := store.ReadCommitted()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("read committed blocks=%d want=1", len(blocks))
	}
	if string(blocks[0].ProposalEvidence.Payload) != string(semantic) {
		t.Fatal("ReadCommitted did not restore exact proposal evidence for catch-up")
	}
	if blocks[0].BlockHash != original.BlockHash {
		t.Fatal("ReadCommitted changed committed block hash")
	}
}

func TestDurableProposalStorageSmallPayloadStaysPlain(t *testing.T) {
	semantic := []byte(`{"candidate_count":1}`)
	sum := sha256.Sum256(semantic)
	value := block.Block{ShardID: "s0", Height: 1, ProposalEvidence: &block.ProposalEvidenceEnvelope{AlgorithmID: "small", PayloadDigest: hex.EncodeToString(sum[:]), Payload: semantic}}
	stored, err := blockForDurableStorage(value)
	if err != nil {
		t.Fatal(err)
	}
	if got := durableProposalStorageEncoding(stored); got != "plain_json" {
		t.Fatalf("small payload codec=%s", got)
	}
}

func TestDurableProposalStorageRejectsCorruptWrapper(t *testing.T) {
	semantic := []byte(`{"candidate_transactions":["` + strings.Repeat("repeat,", 10000) + `"]}`)
	sum := sha256.Sum256(semantic)
	value := block.Block{ShardID: "s0", Height: 1, ProposalEvidence: &block.ProposalEvidenceEnvelope{AlgorithmID: "large", PayloadDigest: hex.EncodeToString(sum[:]), Payload: semantic}}
	stored, err := blockForDurableStorage(value)
	if err != nil {
		t.Fatal(err)
	}
	if durableProposalStorageEncoding(stored) != "gzip+base64+json" {
		t.Fatal("fixture did not compress")
	}
	var wrapper map[string]any
	if err := json.Unmarshal(stored.ProposalEvidence.Payload, &wrapper); err != nil {
		t.Fatal(err)
	}
	wrapper["uncompressed_sha256"] = strings.Repeat("0", 64)
	stored.ProposalEvidence.Payload, err = json.Marshal(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	if err := restoreBlockFromDurableStorage(&stored); err == nil {
		t.Fatal("corrupt stored proposal wrapper was accepted")
	}
}
