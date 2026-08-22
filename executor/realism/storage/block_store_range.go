package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"metaverse-chainlab/executor/realism/block"
)

type durableBlockOffset struct {
	offset int64
	length int
}

type durableBlockRangeIndex struct {
	mu                   sync.Mutex
	path                 string
	legacyWrapped        bool
	indexedSize          int64
	offsets              map[uint64]durableBlockOffset
	fullScanCount        int64
	incrementalScanCount int64
	markerSeedCount      int64
}

var durableBlockRangeIndexes sync.Map

func blockRangeIndexForPath(path string, legacyWrapped bool) *durableBlockRangeIndex {
	key := filepath.Clean(path)
	candidate := &durableBlockRangeIndex{
		path:          key,
		legacyWrapped: legacyWrapped,
		offsets:       map[uint64]durableBlockOffset{},
	}
	actual, _ := durableBlockRangeIndexes.LoadOrStore(key, candidate)
	return actual.(*durableBlockRangeIndex)
}

func committedBlockRangePath(dataDir string) (string, bool, error) {
	// Preserve BlockStore.ReadCommitted source precedence exactly: the legacy
	// committed_blocks.jsonl source wins when it exists; V5 durable commits use
	// blocks.jsonl only when the legacy source is absent.
	legacy := filepath.Join(dataDir, "committed_blocks.jsonl")
	if _, err := os.Stat(legacy); err == nil {
		return legacy, true, nil
	} else if !os.IsNotExist(err) {
		return "", false, err
	}
	primary := filepath.Join(dataDir, "blocks.jsonl")
	if _, err := os.Stat(primary); err == nil {
		return primary, false, nil
	} else if !os.IsNotExist(err) {
		return "", false, err
	}
	return "", false, os.ErrNotExist
}

func durableBlockHeightFromLine(line []byte, legacyWrapped bool) (uint64, error) {
	if legacyWrapped {
		var wrapper struct {
			Block json.RawMessage `json:"block"`
		}
		if err := json.Unmarshal(line, &wrapper); err != nil {
			return 0, err
		}
		if len(wrapper.Block) == 0 {
			return 0, fmt.Errorf("legacy committed block row has no block payload")
		}
		var header struct {
			Height uint64 `json:"height"`
		}
		if err := json.Unmarshal(wrapper.Block, &header); err != nil {
			return 0, err
		}
		return header.Height, nil
	}
	var header struct {
		Height uint64 `json:"height"`
	}
	if err := json.Unmarshal(line, &header); err != nil {
		return 0, err
	}
	return header.Height, nil
}

func (idx *durableBlockRangeIndex) seedFromCommitMarkers(sourceSize int64) bool {
	if idx.legacyWrapped || sourceSize <= 0 {
		return false
	}
	path := filepath.Join(filepath.Dir(idx.path), "commit_markers.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	// Consume only newline-terminated marker records. A concurrent partial tail
	// is not a completed durable marker and must not force a multi-GB fallback
	// scan of blocks.jsonl.
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines) < 2 {
		return false
	}
	seeded := map[uint64]durableBlockOffset{}
	expectedOffset := int64(0)
	lastHeight := uint64(0)
	count := 0
	for _, line := range lines[:len(lines)-1] {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var marker CommitMarker
		if json.Unmarshal(line, &marker) != nil {
			return false
		}
		if marker.Version != "durable_commit_marker_v1" || !marker.Committed || marker.Height == 0 || marker.BlockHash == "" {
			return false
		}
		if marker.BlockLength <= 0 || marker.BlockLength > maxCommittedBlockRecordBytes || marker.BlockSourceSize <= 0 {
			// Historical markers have no offsets; retain the existing safe scan.
			return false
		}
		if marker.BlockOffset != expectedOffset ||
			marker.BlockSourceSize != marker.BlockOffset+int64(marker.BlockLength) ||
			marker.BlockSourceSize > sourceSize ||
			(lastHeight > 0 && marker.Height <= lastHeight) {
			return false
		}
		seeded[marker.Height] = durableBlockOffset{offset: marker.BlockOffset, length: marker.BlockLength}
		expectedOffset = marker.BlockSourceSize
		lastHeight = marker.Height
		count++
	}
	if count == 0 {
		return false
	}
	idx.offsets = seeded
	idx.indexedSize = expectedOffset
	idx.markerSeedCount++
	return true
}

func (idx *durableBlockRangeIndex) refresh() error {
	info, err := os.Stat(idx.path)
	if err != nil {
		return err
	}
	if info.Size() == idx.indexedSize {
		return nil
	}
	if info.Size() < idx.indexedSize {
		idx.indexedSize = 0
		idx.offsets = map[uint64]durableBlockOffset{}
	}
	if idx.indexedSize == 0 && len(idx.offsets) == 0 && !idx.legacyWrapped {
		if idx.seedFromCommitMarkers(info.Size()) && info.Size() == idx.indexedSize {
			return nil
		}
	}
	start := idx.indexedSize
	if start == 0 {
		idx.fullScanCount++
	} else {
		idx.incrementalScanCount++
	}
	file, err := os.Open(idx.path)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(file, 256*1024)
	offset := start
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			// Durable JSONL appends are newline terminated. Never index a partial
			// tail that may still be in the middle of an fsync/append operation.
			if !bytes.HasSuffix(line, []byte{'\n'}) {
				break
			}
			height, err := durableBlockHeightFromLine(line, idx.legacyWrapped)
			if err != nil {
				return fmt.Errorf("decode durable block height at offset %d: %w", offset, err)
			}
			if height == 0 {
				return fmt.Errorf("durable block row at offset %d has zero height", offset)
			}
			idx.offsets[height] = durableBlockOffset{offset: offset, length: len(line)}
			offset += int64(len(line))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}
	idx.indexedSize = offset
	return nil
}

func decodeDurableBlockLine(line []byte, legacyWrapped bool) (block.Block, error) {
	raw := line
	if legacyWrapped {
		var wrapper struct {
			Block json.RawMessage `json:"block"`
		}
		if err := json.Unmarshal(line, &wrapper); err != nil {
			return block.Block{}, err
		}
		if len(wrapper.Block) == 0 {
			return block.Block{}, fmt.Errorf("legacy committed block row has no block payload")
		}
		raw = wrapper.Block
	}
	var item block.Block
	if err := json.Unmarshal(raw, &item); err != nil {
		return block.Block{}, err
	}
	if err := restoreBlockFromDurableStorage(&item); err != nil {
		return block.Block{}, err
	}
	if item.Height == 0 || item.BlockHash == "" {
		return block.Block{}, fmt.Errorf("invalid durable block row: height=%d block_hash=%q", item.Height, item.BlockHash)
	}
	return item, nil
}

// ReadCommittedRange returns only durably committed blocks in [fromHeight,
// toHeight]. The first call builds a compact height->offset index without
// restoring proposal evidence; later calls reuse that index and only scan the
// newly appended JSONL tail. Requested rows are then decoded/restored directly
// with ReadAt. This makes long certified catch-up pay one O(file-size) index
// scan plus requested-row decoding, instead of decoding the entire durable log
// again for every catch-up batch.
func (s *BlockStore) ReadCommittedRange(fromHeight, toHeight uint64) ([]block.Block, error) {
	if fromHeight == 0 || toHeight < fromHeight {
		return nil, fmt.Errorf("invalid committed block range %d..%d", fromHeight, toHeight)
	}
	path, legacyWrapped, err := committedBlockRangePath(s.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open committed block range source: %w", err)
	}
	idx := blockRangeIndexForPath(path, legacyWrapped)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.legacyWrapped != legacyWrapped {
		idx.legacyWrapped = legacyWrapped
		idx.indexedSize = 0
		idx.offsets = map[uint64]durableBlockOffset{}
	}
	if err := idx.refresh(); err != nil {
		return nil, fmt.Errorf("refresh committed block range index: %w", err)
	}

	markers, err := s.committedMarkerHashes()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	out := make([]block.Block, 0)
	for height := fromHeight; ; height++ {
		record, ok := idx.offsets[height]
		if ok {
			line := make([]byte, record.length)
			if _, err := file.ReadAt(line, record.offset); err != nil {
				return nil, fmt.Errorf("read committed block height %d: %w", height, err)
			}
			item, err := decodeDurableBlockLine(line, legacyWrapped)
			if err != nil {
				return nil, fmt.Errorf("decode committed block height %d: %w", height, err)
			}
			if item.Height != height {
				return nil, fmt.Errorf("committed block range index mismatch: requested=%d decoded=%d", height, item.Height)
			}
			if (len(markers) == 0 && legacyWrapped) || markers[item.BlockHash] {
				out = append(out, item)
			}
		}
		if height == toHeight {
			break
		}
	}
	return out, nil
}

// MBE_PBFT_CATCHUP_TAIL_CLOSURE_20260821_V5
