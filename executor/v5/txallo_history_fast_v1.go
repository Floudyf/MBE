package v5

// TxAllo MBE control-plane journal acceleration ONLY. The durable on-disk
// schema, publication order, mapping epochs and paper allocator stay frozen.
// First use/restart scans and validates the complete existing history. After a
// successful append, an in-process cursor checks size, mtime and final record
// before appending another row. Unknown external edits force a full rescan.

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type txalloHistoryCursorV1 struct {
	size         int64
	modTime      time.Time
	lastOffset   int64
	lastTailHash [32]byte
	epoch        uint64
	digest       string
	have         bool
}

// Protected by the caller's existing txalloMappingEpochHistoryMu; no new lock
// order, additional watcher, transport or durability mechanism is introduced.
var txalloHistoryCursorsV1 = map[string]txalloHistoryCursorV1{}

func txalloReadHistoryCursorV1(path string) (txalloHistoryCursorV1, error) {
	var cursor txalloHistoryCursorV1
	raw, err := txalloReadFileStable(path)
	if os.IsNotExist(err) {
		return cursor, nil
	}
	if err != nil {
		return cursor, err
	}
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		return cursor, fmt.Errorf("TxAllo mapping epoch history contains unfinished final record")
	}
	offset := 0
	for _, line := range bytes.SplitAfter(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			var row txalloMappingEpochRowV229
			if err := json.Unmarshal(bytes.TrimSpace(line), &row); err != nil {
				return cursor, fmt.Errorf("TxAllo mapping epoch history decode: %w", err)
			}
			if row.SchemaVersion != txalloDynamicMappingSchema || row.StateDigest == "" {
				return cursor, fmt.Errorf("TxAllo mapping epoch history contains invalid row")
			}
			if !cursor.have || row.Epoch > cursor.epoch {
				cursor.epoch, cursor.digest, cursor.have = row.Epoch, row.StateDigest, true
			} else if row.Epoch == cursor.epoch && row.StateDigest != cursor.digest {
				return cursor, fmt.Errorf("TxAllo mapping epoch history conflicts at epoch %d", row.Epoch)
			}
			cursor.lastOffset = int64(offset)
		}
		offset += len(line)
	}
	if cursor.have {
		cursor.lastTailHash = sha256.Sum256(raw[cursor.lastOffset:])
	}
	stat, err := os.Stat(path)
	if err != nil {
		return cursor, err
	}
	cursor.size, cursor.modTime = stat.Size(), stat.ModTime()
	return cursor, nil
}

func txalloHistoryCursorValidV1(path string, cursor txalloHistoryCursorV1) bool {
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != cursor.size || !stat.ModTime().Equal(cursor.modTime) {
		return false
	}
	if !cursor.have {
		return cursor.size == 0
	}
	if cursor.lastOffset < 0 || cursor.lastOffset > cursor.size {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if _, err = f.Seek(cursor.lastOffset, 0); err != nil {
		return false
	}
	// A last-record digest is a cheap integrity guard for the appended tail.
	// Full history integrity is checked on first access and on every restart.
	buf := make([]byte, cursor.size-cursor.lastOffset)
	if _, err = f.ReadAt(buf, cursor.lastOffset); err != nil {
		return false
	}
	return sha256.Sum256(buf) == cursor.lastTailHash
}

func txalloAppendMappingEpochHistoryFastV1(path string, snapshot txalloMappingSnapshotV22, sourceEpoch int64, algorithm string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	clean := filepath.Clean(path)
	cursor, found := txalloHistoryCursorsV1[clean]
	if !found || !txalloHistoryCursorValidV1(path, cursor) {
		var err error
		cursor, err = txalloReadHistoryCursorV1(path)
		if err != nil {
			return err
		}
	}
	if cursor.have {
		if snapshot.Epoch < cursor.epoch {
			return fmt.Errorf("TxAllo mapping epoch history regressed: got=%d latest=%d", snapshot.Epoch, cursor.epoch)
		}
		if snapshot.Epoch == cursor.epoch {
			if cursor.digest != snapshot.StateDigest {
				return fmt.Errorf("TxAllo mapping epoch history conflicts at epoch %d", snapshot.Epoch)
			}
			txalloHistoryCursorsV1[clean] = cursor
			return nil
		}
	}
	row := txalloMappingEpochRowV229{
		SchemaVersion: snapshot.SchemaVersion,
		Epoch:         snapshot.Epoch, Mapping: copyMapping(snapshot.Mapping),
		Aliases: copyMapping(snapshot.Aliases), MappingDigest: snapshot.MappingDigest,
		AliasesDigest: snapshot.AliasesDigest, StateDigest: snapshot.StateDigest,
		SourceEpoch: sourceEpoch, UpdateAlgorithm: algorithm, TimestampMS: time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	file, err := txalloOpenAppendFileStable(path, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	cursor.lastOffset = cursor.size
	cursor.size, cursor.modTime = stat.Size(), stat.ModTime()
	cursor.epoch, cursor.digest, cursor.have = snapshot.Epoch, snapshot.StateDigest, true
	cursor.lastTailHash = sha256.Sum256(payload)
	if cursor.size != cursor.lastOffset+int64(len(payload)) {
		// An external append raced the current serialized producer; never
		// silently trust the fast cursor on the next epoch.
		delete(txalloHistoryCursorsV1, clean)
		return fmt.Errorf("TxAllo mapping history size changed during append")
	}
	txalloHistoryCursorsV1[clean] = cursor
	return nil
}
