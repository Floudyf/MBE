package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

func TestBlockStoreWritesAndReadsCommittedBlock(t *testing.T) {
	dir := t.TempDir()
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxIDs: []string{"tx1"}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	block.AssignHash(&b)
	store := NewBlockStore(dir, "n0", "s0")
	record := CommitRecord{NodeID: "n0", ShardID: "s0", Height: 1, BlockHash: b.BlockHash, ProposerID: "n0", TxCount: 1, PrepareQuorum: true, CommitQuorum: true, Committed: true, StateCommit: false}
	if err := store.AppendCommitted(b, record); err != nil {
		t.Fatal(err)
	}
	blocks, err := store.ReadCommitted()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].BlockHash != b.BlockHash {
		t.Fatalf("unexpected committed blocks: %+v", blocks)
	}
	if err := WriteCommitCSV(filepath.Join(dir, "block_commit_log.csv"), []CommitRecord{record}); err != nil {
		t.Fatal(err)
	}
}

func TestReadCommittedSupportsRawBlocksJSONL(t *testing.T) {
	dir := t.TempDir()
	b := block.Block{ShardID: "s0", Height: 2, PreviousHash: "h1", BlockHash: "h2"}
	if err := os.WriteFile(filepath.Join(dir, "blocks.jsonl"), mustJSONLine(t, b), 0o644); err != nil {
		t.Fatal(err)
	}
	writeStorageCommitMarker(t, dir, b.BlockHash, b.Height, "root", "receipt")
	got, ok, err := NewBlockStore(dir, "n0", "s0").ReadCommittedAtHeight(2)
	if err != nil || !ok || got.BlockHash != "h2" {
		t.Fatalf("raw block was not decoded: block=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestReadCommittedIgnoresRawBlockWithoutMarker(t *testing.T) {
	dir := t.TempDir()
	b := block.Block{ShardID: "s0", Height: 2, PreviousHash: "h1", BlockHash: "h2"}
	if err := os.WriteFile(filepath.Join(dir, "blocks.jsonl"), mustJSONLine(t, b), 0o644); err != nil {
		t.Fatal(err)
	}
	blocks, err := NewBlockStore(dir, "n0", "s0").ReadCommitted()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 0 {
		t.Fatalf("unmarked raw block must not be committed: %+v", blocks)
	}
}

func TestReadCommittedRejectsEmptyRawBlock(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "blocks.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBlockStore(dir, "n0", "s0").ReadCommitted(); err == nil {
		t.Fatal("empty raw block must be rejected")
	}
}

func TestDurableCommitBatchWritesReceiptsAndTxIndex(t *testing.T) {
	dir := t.TempDir()
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", TxIDs: []string{"tx1", "tx2"}}
	block.AssignHash(&b)
	result := execution.Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts", Receipts: []execution.Receipt{
		{TxID: "tx1", BlockHash: b.BlockHash, Height: b.Height, Success: true},
		{TxID: "tx2", BlockHash: b.BlockHash, Height: b.Height, Success: false, Error: "nope"},
	}}
	metrics, err := NewBlockStore(dir, "n0", "s0").DurableCommitWithMetrics(b, result)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ReceiptBatchWriteMS < 0 || metrics.TxIndexBatchWriteMS < 0 || metrics.WrittenBytes == 0 {
		t.Fatalf("missing batch write metrics: %+v", metrics)
	}
	assertLineCount(t, filepath.Join(dir, "receipts.jsonl"), 2)
	assertLineCount(t, filepath.Join(dir, "tx_index.jsonl"), 2)
	ok, err := NewBlockStore(dir, "n0", "s0").HasTransaction("tx2")
	if err != nil || !ok {
		t.Fatalf("tx index query failed: ok=%v err=%v", ok, err)
	}
}

func TestDurableCommitRollbackAfterReceiptBatchFailure(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	checkpoint, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", TxIDs: []string{"tx1"}}
	block.AssignHash(&b)
	store.SetFailpointForTest("after_receipt_append")
	result := execution.Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts", Receipts: []execution.Receipt{{TxID: "tx1", BlockHash: b.BlockHash, Height: b.Height, Success: true}}}
	if _, err := store.DurableCommitWithMetrics(b, result); err == nil {
		t.Fatal("expected injected durable commit failure")
	}
	if err := store.Rollback(checkpoint); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blocks.jsonl", "receipts.jsonl", "tx_index.jsonl"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should have rolled back, err=%v", name, err)
		}
	}
}

func TestDurableCommitRollbackIncludesCommitMarker(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	checkpoint, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", TxIDs: []string{"tx1"}}
	block.AssignHash(&b)
	store.SetFailpointForTest("after_commit_marker")
	result := execution.Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts", Receipts: []execution.Receipt{{TxID: "tx1", BlockHash: b.BlockHash, Height: b.Height, Success: true}}}
	if _, err := store.DurableCommitWithMetrics(b, result); err == nil {
		t.Fatal("expected injected durable commit failure")
	}
	if err := store.Rollback(checkpoint); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"blocks.jsonl", "receipts.jsonl", "tx_index.jsonl", "commit_markers.jsonl"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s should have rolled back, err=%v", name, err)
		}
	}
}

func TestDurableCommitAllowsEmptyReceiptBatch(t *testing.T) {
	dir := t.TempDir()
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0"}
	block.AssignHash(&b)
	if _, err := NewBlockStore(dir, "n0", "s0").DurableCommitWithMetrics(b, execution.Result{BlockHash: b.BlockHash, Height: b.Height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "empty"}); err != nil {
		t.Fatal(err)
	}
	assertLineCount(t, filepath.Join(dir, "receipts.jsonl"), 0)
	assertLineCount(t, filepath.Join(dir, "tx_index.jsonl"), 0)
}

func assertLineCount(t *testing.T, path string, want int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line != "" {
			got++
		}
	}
	if got != want {
		t.Fatalf("%s line count=%d want=%d content=%q", path, got, want, string(data))
	}
}

func mustJSONLine(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func writeStorageCommitMarker(t *testing.T, dir, blockHash string, height uint64, stateRoot, receiptRoot string) {
	t.Helper()
	row := CommitMarker{Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0", Height: height, BlockHash: blockHash, StateRoot: stateRoot, ReceiptRoot: receiptRoot, Committed: true}
	f, err := os.OpenFile(filepath.Join(dir, "commit_markers.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(f).Encode(row); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
