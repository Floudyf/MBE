package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
)

func appendRangeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(file).Encode(value); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func appendRangeTestCommittedBlock(t *testing.T, dir string, height uint64) realblock.Block {
	t.Helper()
	item := realblock.Block{Height: height, BlockHash: fmt.Sprintf("block-%d", height), PreviousHash: "parent"}
	appendRangeTestJSON(t, filepath.Join(dir, "blocks.jsonl"), item)
	appendRangeTestJSON(t, filepath.Join(dir, "commit_markers.jsonl"), CommitMarker{
		Version:   "durable_commit_marker_v1",
		NodeID:    "n0",
		ShardID:   "s0",
		Height:    height,
		BlockHash: item.BlockHash,
		Committed: true,
	})
	return item
}

func TestReadCommittedRangeBuildsOnceAndExtendsIncrementally(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	appendRangeTestCommittedBlock(t, dir, 1)
	appendRangeTestCommittedBlock(t, dir, 2)
	appendRangeTestCommittedBlock(t, dir, 3)

	got, err := store.ReadCommittedRange(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("range len=%d, want 2: %+v", len(got), got)
	}
	if got[0].Height != 2 || got[1].Height != 3 {
		t.Fatalf("range heights=%v", []uint64{got[0].Height, got[1].Height})
	}
	path := filepath.Join(dir, "blocks.jsonl")
	idx := blockRangeIndexForPath(path, false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	incrementalScans := idx.incrementalScanCount
	idx.mu.Unlock()
	if fullScans != 1 || incrementalScans != 0 {
		t.Fatalf("initial index scans full=%d incremental=%d", fullScans, incrementalScans)
	}

	if _, err := store.ReadCommittedRange(1, 1); err != nil {
		t.Fatal(err)
	}
	idx.mu.Lock()
	fullScans = idx.fullScanCount
	incrementalScans = idx.incrementalScanCount
	idx.mu.Unlock()
	if fullScans != 1 || incrementalScans != 0 {
		t.Fatalf("unchanged file was rescanned full=%d incremental=%d", fullScans, incrementalScans)
	}

	appendRangeTestCommittedBlock(t, dir, 4)
	got, err = store.ReadCommittedRange(4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Height != 4 {
		t.Fatalf("incremental range=%v", got)
	}
	idx.mu.Lock()
	fullScans = idx.fullScanCount
	incrementalScans = idx.incrementalScanCount
	idx.mu.Unlock()
	if fullScans != 1 || incrementalScans != 1 {
		t.Fatalf("append did not extend index incrementally full=%d incremental=%d", fullScans, incrementalScans)
	}
}

func TestReadCommittedRangeRequiresDurableCommitMarker(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	appendRangeTestCommittedBlock(t, dir, 1)
	uncommitted := realblock.Block{Height: 2, BlockHash: "uncommitted-2", PreviousHash: "block-b"}
	appendRangeTestJSON(t, filepath.Join(dir, "blocks.jsonl"), uncommitted)

	got, err := store.ReadCommittedRange(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Height != 1 {
		t.Fatalf("uncommitted durable tail leaked into range: %+v", got)
	}

	// The block offset may already be indexed before its commit marker becomes
	// durable. Adding only the marker must make the row visible without forcing
	// a block-log rescan.
	path := filepath.Join(dir, "blocks.jsonl")
	idx := blockRangeIndexForPath(path, false)
	idx.mu.Lock()
	beforeFull := idx.fullScanCount
	beforeIncremental := idx.incrementalScanCount
	idx.mu.Unlock()
	appendRangeTestJSON(t, filepath.Join(dir, "commit_markers.jsonl"), CommitMarker{
		Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0", Height: 2, BlockHash: uncommitted.BlockHash, Committed: true,
	})
	got, err = store.ReadCommittedRange(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Height != 2 {
		t.Fatalf("new commit marker did not expose indexed block: %+v", got)
	}
	idx.mu.Lock()
	afterFull := idx.fullScanCount
	afterIncremental := idx.incrementalScanCount
	idx.mu.Unlock()
	if afterFull != beforeFull || afterIncremental != beforeIncremental {
		t.Fatalf("marker-only update rescanned block log: full %d->%d incremental %d->%d", beforeFull, afterFull, beforeIncremental, afterIncremental)
	}
}

func TestReadCommittedRangePreservesLegacySourcePrecedence(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	legacy := realblock.Block{Height: 1, BlockHash: "legacy-1", PreviousHash: "genesis"}
	appendRangeTestJSON(t, filepath.Join(dir, "committed_blocks.jsonl"), struct {
		Block realblock.Block `json:"block"`
	}{Block: legacy})

	// A blocks.jsonl row with the same height must not silently replace the
	// legacy source because ReadCommitted() has always preferred
	// committed_blocks.jsonl when both are present.
	appendRangeTestJSON(t, filepath.Join(dir, "blocks.jsonl"), realblock.Block{Height: 1, BlockHash: "v5-1", PreviousHash: "genesis"})

	got, err := store.ReadCommittedRange(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].BlockHash != "legacy-1" {
		t.Fatalf("source precedence changed: %+v", got)
	}
}

func TestReadCommittedRangeDoesNotIndexPartialTail(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	appendRangeTestCommittedBlock(t, dir, 1)
	if _, err := store.ReadCommittedRange(1, 1); err != nil {
		t.Fatal(err)
	}

	item := realblock.Block{Height: 2, BlockHash: "block-2", PreviousHash: "block-1"}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "blocks.jsonl")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	cut := len(encoded) / 2
	if _, err := file.Write(encoded[:cut]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadCommittedRange(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("partial durable tail was indexed: %+v", got)
	}

	file, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(encoded[cut:], '\n')); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	appendRangeTestJSON(t, filepath.Join(dir, "commit_markers.jsonl"), CommitMarker{
		Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0", Height: 2, BlockHash: item.BlockHash, Committed: true,
	})

	got, err = store.ReadCommittedRange(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Height != 2 {
		t.Fatalf("completed durable tail was not indexed: %+v", got)
	}
}
