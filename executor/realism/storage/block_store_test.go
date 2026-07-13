package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/block"
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
	got, ok, err := NewBlockStore(dir, "n0", "s0").ReadCommittedAtHeight(2)
	if err != nil || !ok || got.BlockHash != "h2" {
		t.Fatalf("raw block was not decoded: block=%+v ok=%v err=%v", got, ok, err)
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

func mustJSONLine(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}
