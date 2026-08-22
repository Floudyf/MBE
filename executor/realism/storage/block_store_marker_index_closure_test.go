package storage

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

func TestDurableCommitMarkerSeedsRangeIndexWithoutFullBlockScan(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	previous := "genesis"

	for height := uint64(1); height <= 3; height++ {
		item := realblock.Block{ShardID: "s0", Height: height, PreviousHash: previous, ProposerID: "n0"}
		realblock.AssignHash(&item)
		result := execution.Result{BlockHash: item.BlockHash, Height: height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts"}
		if _, err := store.DurableCommitWithMetrics(item, result); err != nil {
			t.Fatal(err)
		}
		previous = item.BlockHash
	}

	markerFile, err := os.Open(filepath.Join(dir, "commit_markers.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(markerFile)
	count := 0
	for {
		var marker CommitMarker
		err := decoder.Decode(&marker)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = markerFile.Close()
			t.Fatal(err)
		}
		if marker.BlockLength <= 0 || marker.BlockSourceSize <= 0 {
			_ = markerFile.Close()
			t.Fatalf("marker missing block-offset metadata: %+v", marker)
		}
		if marker.BlockSourceSize != marker.BlockOffset+int64(marker.BlockLength) {
			_ = markerFile.Close()
			t.Fatalf("marker source span mismatch: %+v", marker)
		}
		count++
	}
	if err := markerFile.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("marker count=%d want=3", count)
	}

	got, err := store.ReadCommittedRange(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Height != 2 || got[1].Height != 3 {
		t.Fatalf("range=%+v", got)
	}
	idx := blockRangeIndexForPath(filepath.Join(dir, "blocks.jsonl"), false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	markerSeeds := idx.markerSeedCount
	idx.mu.Unlock()
	if fullScans != 0 {
		t.Fatalf("new markers forced a full blocks.jsonl scan: %d", fullScans)
	}
	if markerSeeds != 1 {
		t.Fatalf("marker seed count=%d want=1", markerSeeds)
	}
}

func TestHistoricalMarkerWithoutOffsetsFallsBackSafely(t *testing.T) {
	dir := t.TempDir()
	item := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", BlockHash: "legacy-block-1"}
	appendRangeTestJSON(t, filepath.Join(dir, "blocks.jsonl"), item)
	appendRangeTestJSON(t, filepath.Join(dir, "commit_markers.jsonl"), CommitMarker{
		Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0",
		Height: 1, BlockHash: item.BlockHash, Committed: true,
	})
	store := NewBlockStore(dir, "n0", "s0")
	got, err := store.ReadCommittedRange(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].BlockHash != item.BlockHash {
		t.Fatalf("historical fallback failed: %+v", got)
	}
	idx := blockRangeIndexForPath(filepath.Join(dir, "blocks.jsonl"), false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	idx.mu.Unlock()
	if fullScans != 1 {
		t.Fatalf("historical marker should use fallback scan, full=%d", fullScans)
	}
}

func TestMarkerSeedIgnoresConcurrentPartialMarkerTail(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	previous := "genesis"
	for height := uint64(1); height <= 2; height++ {
		item := realblock.Block{ShardID: "s0", Height: height, PreviousHash: previous, ProposerID: "n0"}
		realblock.AssignHash(&item)
		result := execution.Result{BlockHash: item.BlockHash, Height: height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts"}
		if _, err := store.DurableCommitWithMetrics(item, result); err != nil {
			t.Fatal(err)
		}
		previous = item.BlockHash
	}

	markerPath := filepath.Join(dir, "commit_markers.jsonl")
	file, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(`{"version":"durable_commit_marker_v1","height":3`)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadCommittedRange(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("range len=%d want=2", len(got))
	}
	idx := blockRangeIndexForPath(filepath.Join(dir, "blocks.jsonl"), false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	markerSeeds := idx.markerSeedCount
	idx.mu.Unlock()
	if fullScans != 0 || markerSeeds != 1 {
		t.Fatalf("partial marker tail caused fallback: full=%d marker_seeds=%d", fullScans, markerSeeds)
	}
}

func TestMarkerSeedRejectsOversizedRecordLength(t *testing.T) {
	dir := t.TempDir()
	item := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", BlockHash: "block-1"}
	appendRangeTestJSON(t, filepath.Join(dir, "blocks.jsonl"), item)
	info, err := os.Stat(filepath.Join(dir, "blocks.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	appendRangeTestJSON(t, filepath.Join(dir, "commit_markers.jsonl"), CommitMarker{
		Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0",
		Height: 1, BlockHash: item.BlockHash, Committed: true,
		BlockOffset: 0, BlockLength: maxCommittedBlockRecordBytes + 1, BlockSourceSize: info.Size(),
	})
	store := NewBlockStore(dir, "n0", "s0")
	got, err := store.ReadCommittedRange(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("safe fallback failed: %+v", got)
	}
	idx := blockRangeIndexForPath(filepath.Join(dir, "blocks.jsonl"), false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	idx.mu.Unlock()
	if fullScans != 1 {
		t.Fatalf("oversized marker should force bounded reviewed fallback, full=%d", fullScans)
	}
}

func TestCommittedMarkerHashesIgnoresOnlyUnterminatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "commit_markers.jsonl")
	appendRangeTestJSON(t, path, CommitMarker{
		Version: "durable_commit_marker_v1", NodeID: "n0", ShardID: "s0",
		Height: 1, BlockHash: "block-1", Committed: true,
	})
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(`{"version":"durable_commit_marker_v1","height":2`)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	store := NewBlockStore(dir, "n0", "s0")
	hashes, err := store.committedMarkerHashes()
	if err != nil {
		t.Fatalf("unterminated concurrent marker tail must be ignored: %v", err)
	}
	if len(hashes) != 1 || !hashes["block-1"] {
		t.Fatalf("unexpected committed marker set: %#v", hashes)
	}

	malformedDir := t.TempDir()
	malformedPath := filepath.Join(malformedDir, "commit_markers.jsonl")
	if err := os.WriteFile(malformedPath, []byte("{not-json}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBlockStore(malformedDir, "n0", "s0").committedMarkerHashes(); err == nil {
		t.Fatal("newline-terminated malformed marker must remain a hard error")
	}
}

func TestReadCommittedAtHeightUsesMarkerSeededRangeIndex(t *testing.T) {
	dir := t.TempDir()
	store := NewBlockStore(dir, "n0", "s0")
	previous := "genesis"
	var want realblock.Block
	for height := uint64(1); height <= 3; height++ {
		item := realblock.Block{ShardID: "s0", Height: height, PreviousHash: previous, ProposerID: "n0"}
		realblock.AssignHash(&item)
		result := execution.Result{BlockHash: item.BlockHash, Height: height, StateRootBefore: "before", StateRootAfter: "after", ReceiptRoot: "receipts"}
		if _, err := store.DurableCommitWithMetrics(item, result); err != nil {
			t.Fatal(err)
		}
		previous = item.BlockHash
		if height == 2 {
			want = item
		}
	}
	got, ok, err := store.ReadCommittedAtHeight(2)
	if err != nil || !ok || got.BlockHash != want.BlockHash {
		t.Fatalf("height lookup mismatch: got=%+v ok=%t err=%v", got, ok, err)
	}
	idx := blockRangeIndexForPath(filepath.Join(dir, "blocks.jsonl"), false)
	idx.mu.Lock()
	fullScans := idx.fullScanCount
	markerSeeds := idx.markerSeedCount
	idx.mu.Unlock()
	if fullScans != 0 || markerSeeds != 1 {
		t.Fatalf("height lookup did not use marker-seeded range index: full=%d seeds=%d", fullScans, markerSeeds)
	}
}
