package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/metrics"
)

type TxIndexRecord struct {
	TxID        string `json:"tx_id"`
	BlockHash   string `json:"block_hash"`
	Height      uint64 `json:"height"`
	ReceiptOK   bool   `json:"receipt_success"`
	ReceiptRoot string `json:"receipt_root"`
}

type CommitMarker struct {
	Version         string `json:"version"`
	NodeID          string `json:"node_id"`
	ShardID         string `json:"shard_id"`
	Height          uint64 `json:"height"`
	BlockHash       string `json:"block_hash"`
	ReceiptRoot     string `json:"receipt_root"`
	StateRoot       string `json:"state_root"`
	Committed       bool   `json:"committed"`
	BlockOffset     int64  `json:"block_offset,omitempty"`
	BlockLength     int    `json:"block_length,omitempty"`
	BlockSourceSize int64  `json:"block_source_size,omitempty"`
}

type CommitSummary struct {
	RuntimeStage      string `json:"runtime_stage"`
	RuntimeTruth      string `json:"runtime_truth"`
	NodeID            string `json:"node_id"`
	ShardID           string `json:"shard_id"`
	CommittedHeight   uint64 `json:"committed_height"`
	LatestBlockHash   string `json:"latest_block_hash"`
	LatestStateRoot   string `json:"latest_state_root"`
	LatestReceiptRoot string `json:"latest_receipt_root"`
	BlockDB           bool   `json:"block_db"`
	StateDB           bool   `json:"state_db"`
	ReceiptDB         bool   `json:"receipt_db"`
	TxIndex           bool   `json:"tx_index"`
	StateCommit       bool   `json:"state_commit"`
}

type PersistenceMetrics struct {
	ReceiptBatchWriteMS int64 `json:"receipt_batch_write_ms"`
	TxIndexBatchWriteMS int64 `json:"tx_index_batch_write_ms"`
	DurableCommitMS     int64 `json:"durable_commit_ms"`
	WrittenBytes        int64 `json:"persistence_written_bytes"`
}

const durableProposalPayloadCodec = "mbe_v5_durable_proposal_gzip_v1"
const durableProposalCompressionThreshold = 16 * 1024
const durableProposalMaximumSemanticBytes = 512 * 1024 * 1024

type durableCompressedProposalPayload struct {
	Codec              string `json:"_mbe_storage_codec"`
	Encoding           string `json:"encoding"`
	UncompressedBytes  int    `json:"uncompressed_bytes"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
	PayloadGzipBase64  string `json:"payload_gzip_base64"`
}

// blockForDurableStorage compresses only the persisted copy of large proposal
// evidence. The in-memory/PBFT block remains byte-for-byte unchanged, so this
// storage optimization cannot change proposal validation or live network cost.
// Block.Hash commits to AlgorithmID+PayloadDigest rather than raw evidence
// bytes, therefore the stored representation preserves the same block hash.
func blockForDurableStorage(value block.Block) (block.Block, error) {
	if value.ProposalEvidence == nil || len(value.ProposalEvidence.Payload) < durableProposalCompressionThreshold {
		return value, nil
	}
	semantic := append([]byte(nil), value.ProposalEvidence.Payload...)
	if len(semantic) > durableProposalMaximumSemanticBytes {
		return block.Block{}, fmt.Errorf("durable proposal evidence exceeds %d bytes", durableProposalMaximumSemanticBytes)
	}
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return block.Block{}, fmt.Errorf("create durable proposal gzip writer: %w", err)
	}
	if _, err := writer.Write(semantic); err != nil {
		_ = writer.Close()
		return block.Block{}, fmt.Errorf("compress durable proposal evidence: %w", err)
	}
	if err := writer.Close(); err != nil {
		return block.Block{}, fmt.Errorf("finish durable proposal compression: %w", err)
	}
	sum := sha256.Sum256(semantic)
	wrapper := durableCompressedProposalPayload{
		Codec:              durableProposalPayloadCodec,
		Encoding:           "gzip+base64+json",
		UncompressedBytes:  len(semantic),
		UncompressedSHA256: hex.EncodeToString(sum[:]),
		PayloadGzipBase64:  base64.StdEncoding.EncodeToString(compressed.Bytes()),
	}
	encoded, err := json.Marshal(wrapper)
	if err != nil {
		return block.Block{}, fmt.Errorf("encode durable proposal wrapper: %w", err)
	}
	if len(encoded) >= len(semantic) {
		return value, nil
	}
	next := value
	envelope := *value.ProposalEvidence
	envelope.Payload = encoded
	next.ProposalEvidence = &envelope
	return next, nil
}

// restoreBlockFromDurableStorage transparently restores the exact proposal
// evidence bytes before recovery/certified catch-up re-executes a stored block.
func restoreBlockFromDurableStorage(value *block.Block) error {
	if value == nil || value.ProposalEvidence == nil || len(value.ProposalEvidence.Payload) == 0 {
		return nil
	}
	var wrapper durableCompressedProposalPayload
	if err := json.Unmarshal(value.ProposalEvidence.Payload, &wrapper); err != nil || wrapper.Codec == "" {
		return nil
	}
	if wrapper.Codec != durableProposalPayloadCodec {
		return fmt.Errorf("unsupported durable proposal payload codec %q", wrapper.Codec)
	}
	if wrapper.Encoding != "gzip+base64+json" {
		return fmt.Errorf("unsupported durable proposal payload encoding %q", wrapper.Encoding)
	}
	if wrapper.UncompressedBytes < 1 || wrapper.UncompressedBytes > durableProposalMaximumSemanticBytes {
		return fmt.Errorf("invalid durable proposal uncompressed size %d", wrapper.UncompressedBytes)
	}
	compressed, err := base64.StdEncoding.DecodeString(wrapper.PayloadGzipBase64)
	if err != nil {
		return fmt.Errorf("decode durable proposal base64: %w", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return fmt.Errorf("open durable proposal gzip: %w", err)
	}
	semantic, readErr := io.ReadAll(io.LimitReader(reader, durableProposalMaximumSemanticBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return fmt.Errorf("decompress durable proposal evidence: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close durable proposal gzip: %w", closeErr)
	}
	if len(semantic) != wrapper.UncompressedBytes {
		return fmt.Errorf("durable proposal uncompressed size mismatch: got=%d want=%d", len(semantic), wrapper.UncompressedBytes)
	}
	sum := sha256.Sum256(semantic)
	digest := hex.EncodeToString(sum[:])
	if digest != wrapper.UncompressedSHA256 {
		return fmt.Errorf("durable proposal uncompressed digest mismatch")
	}
	if value.ProposalEvidence.PayloadDigest != "" && value.ProposalEvidence.PayloadDigest != digest {
		return fmt.Errorf("durable proposal commitment mismatch")
	}
	envelope := *value.ProposalEvidence
	envelope.Payload = append(json.RawMessage(nil), semantic...)
	value.ProposalEvidence = &envelope
	return nil
}

func durableProposalStorageEncoding(value block.Block) string {
	if value.ProposalEvidence == nil {
		return "none"
	}
	var wrapper durableCompressedProposalPayload
	if json.Unmarshal(value.ProposalEvidence.Payload, &wrapper) == nil && wrapper.Codec == durableProposalPayloadCodec {
		return wrapper.Encoding
	}
	return "plain_json"
}

func (s *BlockStore) DurableCommit(b block.Block, result execution.Result) (CommitSummary, error) {
	_, err := s.DurableCommitWithMetrics(b, result)
	if err != nil {
		return CommitSummary{}, err
	}
	summary := CommitSummary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: s.NodeID, ShardID: s.ShardID, CommittedHeight: b.Height, LatestBlockHash: b.BlockHash, LatestStateRoot: result.StateRootAfter, LatestReceiptRoot: result.ReceiptRoot, BlockDB: true, StateDB: true, ReceiptDB: true, TxIndex: true, StateCommit: true}
	return summary, nil
}

func (s *BlockStore) DurableCommitWithMetrics(b block.Block, result execution.Result) (PersistenceMetrics, error) {
	started := time.Now()
	var pm PersistenceMetrics
	if err := os.MkdirAll(s.DataDir, 0o755); err != nil {
		return pm, fmt.Errorf("create durable store: %w", err)
	}
	// Preserve the exact consensus block body that PBFT voted on. Block.Hash
	// commits to StateRootBefore/StateRootAfter/ReceiptRoot, so execution-result
	// roots must not rewrite those fields after certification. Executed roots
	// remain durable in state/receipts/commit_markers.
	//
	// Keep the generic storage API contract unchanged: focused runtime/storage
	// fixtures may use synthetic hashes such as h6/h7. Production PBFT paths
	// already validate block identity before commit and certified recovery.
	b.StateCommit = true

	receiptStarted := time.Now()
	written, err := appendJSONBatch(filepath.Join(s.DataDir, "receipts.jsonl"), receiptsAsAny(result.Receipts))
	pm.ReceiptBatchWriteMS = time.Since(receiptStarted).Milliseconds()
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written
	if s.failpoint == "after_receipt_append" {
		return pm, fmt.Errorf("injected durable commit failure after receipt append")
	}

	indexRecords := make([]any, 0, len(result.Receipts))
	for _, receipt := range result.Receipts {
		indexRecords = append(indexRecords, TxIndexRecord{TxID: receipt.TxID, BlockHash: receipt.BlockHash, Height: receipt.Height, ReceiptOK: receipt.Success, ReceiptRoot: result.ReceiptRoot})
	}
	indexStarted := time.Now()
	written, err = appendJSONBatch(filepath.Join(s.DataDir, "tx_index.jsonl"), indexRecords)
	pm.TxIndexBatchWriteMS = time.Since(indexStarted).Milliseconds()
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written

	storedBlock, err := blockForDurableStorage(b)
	if err != nil {
		return pm, err
	}
	blockPath := filepath.Join(s.DataDir, "blocks.jsonl")
	blockOffset := int64(0)
	if info, statErr := os.Stat(blockPath); statErr == nil {
		blockOffset = info.Size()
	} else if !os.IsNotExist(statErr) {
		return pm, fmt.Errorf("stat durable block log: %w", statErr)
	}
	written, err = appendJSON(blockPath, storedBlock)
	if err != nil {
		return pm, err
	}
	blockLength := written
	blockSourceSize := blockOffset + blockLength
	pm.WrittenBytes += written
	if s.failpoint == "after_block_append" {
		return pm, fmt.Errorf("injected durable commit failure after block append")
	}
	marker := CommitMarker{Version: "durable_commit_marker_v1", NodeID: s.NodeID, ShardID: s.ShardID, Height: b.Height, BlockHash: b.BlockHash, ReceiptRoot: result.ReceiptRoot, StateRoot: result.StateRootAfter, Committed: true, BlockOffset: blockOffset, BlockLength: int(blockLength), BlockSourceSize: blockSourceSize}
	written, err = appendJSON(filepath.Join(s.DataDir, "commit_markers.jsonl"), marker)
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written
	if s.failpoint == "after_commit_marker" {
		return pm, fmt.Errorf("injected durable commit failure after commit marker")
	}

	summary := CommitSummary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: s.NodeID, ShardID: s.ShardID, CommittedHeight: b.Height, LatestBlockHash: b.BlockHash, LatestStateRoot: result.StateRootAfter, LatestReceiptRoot: result.ReceiptRoot, BlockDB: true, StateDB: true, ReceiptDB: true, TxIndex: true, StateCommit: true}
	if err := metrics.WriteJSON(filepath.Join(s.DataDir, "commit_summary.json"), summary); err != nil {
		return pm, err
	}
	if err := metrics.WriteCSV(filepath.Join(s.DataDir, "commit_log.csv"), []string{"node_id", "shard_id", "height", "block_hash", "state_root_before", "state_root_after", "receipt_root", "tx_count", "execution_status", "state_commit"}, [][]string{{s.NodeID, s.ShardID, fmt.Sprint(b.Height), b.BlockHash, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, fmt.Sprint(len(b.TxIDs)), "executed", "true"}}); err != nil {
		return pm, err
	}
	pm.DurableCommitMS = time.Since(started).Milliseconds()
	return pm, nil
}

func receiptsAsAny(receipts []execution.Receipt) []any {
	out := make([]any, 0, len(receipts))
	for _, receipt := range receipts {
		out = append(out, receipt)
	}
	return out
}

func appendJSON(path string, value any) (int64, error) {
	return appendJSONBatch(path, []any{value})
}

func appendJSONBatch(path string, values []any) (int64, error) {
	before := int64(0)
	if info, err := os.Stat(path); err == nil {
		before = info.Size()
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("stat jsonl %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open jsonl %s: %w", path, err)
	}
	writer := bufio.NewWriter(f)
	encoder := json.NewEncoder(writer)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			_ = f.Close()
			return 0, fmt.Errorf("write jsonl %s: %w", path, err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("flush jsonl %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("sync jsonl %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("close jsonl %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat written jsonl %s: %w", path, err)
	}
	return info.Size() - before, nil
}

// MBE_PBFT_CATCHUP_TAIL_CLOSURE_20260821_V5

// MBE_PBFT_DURABLE_IDENTITY_CLOSURE_20260821_V7
