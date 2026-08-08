package state

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DB struct {
	mu            sync.Mutex
	namespace     string
	dataDir       string
	values        map[string]string
	walSinceSync  int
	walGeneration int
	lastHeight    uint64
	lastBlockHash string
}

type StateKV struct {
	Key             string   `json:"key"`
	Value           string   `json:"value"`
	TxIDs           []string `json:"tx_ids,omitempty"`
	UpdateSemantics string   `json:"update_semantics,omitempty"`
	Delta           int64    `json:"delta,omitempty"`
	BaseValue       string   `json:"base_value,omitempty"`
	BaseValueDigest string   `json:"base_value_digest,omitempty"`
	ApplyOrigin     string   `json:"apply_origin,omitempty"`
	DeltaKind       string   `json:"delta_kind,omitempty"`
	HasInitialValue bool     `json:"has_initial_value,omitempty"`
	InitialValue    int64    `json:"initial_value"`
	DeltaID         string   `json:"delta_id,omitempty"`
	BlockHeight     uint64   `json:"block_height,omitempty"`
	RoutingOrdinal  uint64   `json:"routing_ordinal,omitempty"`
	PreviousVersion uint64   `json:"previous_version,omitempty"`
	ProducedVersion uint64   `json:"produced_version,omitempty"`
	OrderingNoop    bool     `json:"ordering_noop,omitempty"`
}

type CASMismatchError struct {
	Key            string
	ExpectedDigest string
	ActualDigest   string
	DeltaID        string
	TxIDs          []string
	BlockHeight    uint64
}

func (e CASMismatchError) Error() string {
	id := e.DeltaID
	if id == "" && len(e.TxIDs) > 0 {
		id = strings.Join(e.TxIDs, "|")
	}
	return fmt.Sprintf("state CAS mismatch key=%s expected_digest=%s actual_digest=%s delta_or_tx=%s block_height=%d", e.Key, e.ExpectedDigest, e.ActualDigest, id, e.BlockHeight)
}

type FileCheckpoint struct {
	snapshotData    []byte
	snapshotMissing bool
	walSizes        map[string]int64
	replaceData     map[string][]byte
	replaceMissing  map[string]bool
}

type DeltaMetadata struct {
	BlockHeight     uint64
	BlockHash       string
	ParentHash      string
	DeltaID         string
	StateRootBefore string
	StateRootAfter  string
}

type SnapshotMetadata struct {
	Version           string `json:"version"`
	Namespace         string `json:"namespace"`
	IncludedHeight    uint64 `json:"included_height"`
	IncludedBlockHash string `json:"included_block_hash"`
	StateRoot         string `json:"state_root"`
	WALGeneration     int    `json:"wal_generation"`
}

type WALManifest struct {
	Version          string `json:"version"`
	Namespace        string `json:"namespace"`
	ActiveGeneration int    `json:"active_generation"`
}

type PersistenceOptions struct {
	DurabilityMode  string
	FSyncCadence    int
	SnapshotCadence int
}

type PersistenceMetrics struct {
	CheckpointReadMS int64  `json:"checkpoint_read_ms"`
	WALAppendMS      int64  `json:"wal_append_ms"`
	WALSyncMS        int64  `json:"wal_sync_ms"`
	SnapshotWriteMS  int64  `json:"snapshot_write_ms"`
	WrittenBytes     int64  `json:"persistence_written_bytes"`
	WALRecordCount   int    `json:"wal_record_count"`
	SnapshotCount    int    `json:"snapshot_count"`
	DurabilityMode   string `json:"durability_mode"`
	FSyncCadence     int    `json:"fsync_cadence"`
}

type walRecord struct {
	Version         string    `json:"version"`
	Namespace       string    `json:"namespace"`
	BlockHeight     uint64    `json:"block_height"`
	BlockHash       string    `json:"block_hash"`
	ParentHash      string    `json:"parent_hash"`
	DeltaID         string    `json:"delta_id"`
	StateUpdates    []StateKV `json:"state_updates"`
	StateRootBefore string    `json:"state_root_before"`
	StateRootAfter  string    `json:"state_root_after"`
	Checksum        string    `json:"checksum"`
}

func NewDB(dataDir, namespace string) *DB {
	if namespace == "" {
		namespace = "s0"
	}
	return &DB{dataDir: dataDir, namespace: namespace, values: map[string]string{}, walGeneration: 1}
}

func Open(dataDir, namespace string) (*DB, error) {
	db := NewDB(dataDir, namespace)
	if err := db.Load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return db, nil
}

func DefaultPersistenceOptions() PersistenceOptions {
	return PersistenceOptions{DurabilityMode: "strict", FSyncCadence: 1, SnapshotCadence: 128}
}

func (db *DB) Get(key string) string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.values[db.key(key)]
}

func (db *DB) Set(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.values[db.key(key)] = value
}

func (db *DB) ApplyBatch(updates map[string]string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for key, value := range updates {
		db.values[db.key(key)] = value
	}
}

func (db *DB) ApplyDeterministicBatch(updates []StateKV) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, item := range updates {
		key := db.key(item.Key)
		if err := applyStateKV(db.values, key, item); err != nil {
			return err
		}
	}
	return nil
}

func stateValueDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (db *DB) Snapshot() map[string]string {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make(map[string]string, len(db.values))
	for key, value := range db.values {
		out[key] = value
	}
	return out
}

func (db *DB) Restore(snapshot map[string]string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.values = make(map[string]string, len(snapshot))
	for key, value := range snapshot {
		db.values[key] = value
	}
}

func (db *DB) Root() string {
	return Root(db.Snapshot())
}

func RootOfSnapshot(snapshot map[string]string) string {
	return Root(snapshot)
}

func (db *DB) Save() error {
	_, err := db.writeSnapshot(true)
	return err
}

func (db *DB) PersistDelta(meta DeltaMetadata, updates []StateKV, opts PersistenceOptions) (PersistenceMetrics, error) {
	if opts.DurabilityMode == "" {
		opts.DurabilityMode = "strict"
	}
	if opts.FSyncCadence <= 0 {
		opts.FSyncCadence = 1
	}
	if meta.DeltaID == "" {
		meta.DeltaID = fmt.Sprintf("%s:%d:%s", db.namespace, meta.BlockHeight, meta.BlockHash)
	}
	record := walRecord{
		Version:         "state_delta_wal_v1",
		Namespace:       db.namespace,
		BlockHeight:     meta.BlockHeight,
		BlockHash:       meta.BlockHash,
		ParentHash:      meta.ParentHash,
		DeltaID:         meta.DeltaID,
		StateUpdates:    canonicalStateUpdates(updates),
		StateRootBefore: meta.StateRootBefore,
		StateRootAfter:  meta.StateRootAfter,
	}
	record.Checksum = walChecksum(record)
	started := timeNowMS()
	written, synced, err := db.appendWAL(record, opts)
	pm := PersistenceMetrics{WALAppendMS: timeNowMS() - started, WrittenBytes: written, WALRecordCount: 1, DurabilityMode: opts.DurabilityMode, FSyncCadence: opts.FSyncCadence}
	if err != nil {
		return pm, err
	}
	db.mu.Lock()
	db.lastHeight = meta.BlockHeight
	db.lastBlockHash = meta.BlockHash
	db.mu.Unlock()
	pm.WALSyncMS = synced
	return pm, nil
}

func (db *DB) SnapshotIfDue(blockHeight uint64, opts PersistenceOptions) (PersistenceMetrics, error) {
	if opts.SnapshotCadence <= 0 || blockHeight == 0 || blockHeight%uint64(opts.SnapshotCadence) != 0 {
		return PersistenceMetrics{DurabilityMode: opts.DurabilityMode, FSyncCadence: opts.FSyncCadence}, nil
	}
	snapshotStarted := timeNowMS()
	snapshotBytes, err := db.writeSnapshot(true)
	pm := PersistenceMetrics{SnapshotWriteMS: timeNowMS() - snapshotStarted, WrittenBytes: snapshotBytes, SnapshotCount: 1, DurabilityMode: opts.DurabilityMode, FSyncCadence: opts.FSyncCadence}
	if err != nil {
		return pm, err
	}
	db.walGeneration++
	db.mu.Lock()
	includedHash := db.lastBlockHash
	db.mu.Unlock()
	meta := SnapshotMetadata{Version: "state_snapshot_metadata_v1", Namespace: db.namespace, IncludedHeight: blockHeight, IncludedBlockHash: includedHash, StateRoot: db.Root(), WALGeneration: db.walGeneration}
	if err := db.writeSnapshotMetadata(meta); err != nil {
		return pm, err
	}
	if err := db.writeWALManifest(db.walGeneration); err != nil {
		return pm, fmt.Errorf("publish state wal manifest: %w", err)
	}
	if err := truncateAndSync(db.walPath(db.walGeneration)); err != nil {
		return pm, fmt.Errorf("create rotated state wal: %w", err)
	}
	if err := db.cleanupOldWALSegments(db.walGeneration); err != nil {
		return pm, fmt.Errorf("cleanup old state wal segments: %w", err)
	}
	return pm, nil
}

func (db *DB) walPath(generation int) string {
	if generation <= 0 {
		generation = 1
	}
	return filepath.Join(db.dataDir, fmt.Sprintf("state_delta.%d.wal", generation))
}

func (db *DB) writeWALManifest(generation int) error {
	manifest := WALManifest{Version: "state_wal_manifest_v1", Namespace: db.namespace, ActiveGeneration: generation}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(db.dataDir, "state_wal_manifest-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(name) }
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	return os.Rename(name, filepath.Join(db.dataDir, "state_wal_manifest.json"))
}

func (db *DB) loadWALManifest() (WALManifest, error) {
	payload, err := os.ReadFile(filepath.Join(db.dataDir, "state_wal_manifest.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return WALManifest{}, nil
		}
		return WALManifest{}, err
	}
	var manifest WALManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return WALManifest{}, fmt.Errorf("decode state wal manifest: %w", err)
	}
	if manifest.Version != "state_wal_manifest_v1" || manifest.Namespace != db.namespace {
		return WALManifest{}, fmt.Errorf("invalid state wal manifest version=%q namespace=%q", manifest.Version, manifest.Namespace)
	}
	return manifest, nil
}

func (db *DB) cleanupOldWALSegments(active int) error {
	matches, err := filepath.Glob(filepath.Join(db.dataDir, "state_delta.*.wal"))
	if err != nil {
		return err
	}
	for _, path := range matches {
		if path == db.walPath(active) {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (db *DB) writeSnapshotMetadata(meta SnapshotMetadata) error {
	payload, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot metadata: %w", err)
	}
	tmp, err := os.CreateTemp(db.dataDir, "state_snapshot_metadata-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot metadata temp: %w", err)
	}
	name := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(name) }
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
		cleanup()
		return fmt.Errorf("write snapshot metadata: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync snapshot metadata: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close snapshot metadata: %w", err)
	}
	if err := os.Rename(name, filepath.Join(db.dataDir, "state_snapshot_metadata.json")); err != nil {
		cleanup()
		return fmt.Errorf("publish snapshot metadata: %w", err)
	}
	return nil
}

func truncateAndSync(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (db *DB) writeSnapshot(syncFile bool) (int64, error) {
	if err := os.MkdirAll(db.dataDir, 0o755); err != nil {
		return 0, fmt.Errorf("create state dir: %w", err)
	}
	payload, err := json.MarshalIndent(db.Snapshot(), "", "  ")
	if err != nil {
		return 0, fmt.Errorf("marshal state snapshot: %w", err)
	}
	target := filepath.Join(db.dataDir, "state_snapshot.json")
	tmp, err := os.CreateTemp(db.dataDir, "state_snapshot-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("create state snapshot temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
		cleanup()
		return 0, fmt.Errorf("write state snapshot: %w", err)
	}
	if syncFile {
		err := tmp.Sync()
		if err != nil {
			cleanup()
			return 0, fmt.Errorf("sync state snapshot: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return 0, fmt.Errorf("close state snapshot: %w", err)
	}
	backup := target + ".bak"
	_ = os.Remove(backup)
	if _, err := os.Stat(target); err == nil {
		if err := os.Rename(target, backup); err != nil {
			_ = os.Remove(tmpName)
			return 0, fmt.Errorf("backup state snapshot: %w", err)
		}
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(target)
		if _, backupErr := os.Stat(backup); backupErr == nil {
			_ = os.Rename(backup, target)
		}
		_ = os.Remove(tmpName)
		return 0, fmt.Errorf("replace state snapshot: %w", err)
	}
	_ = os.Remove(backup)
	return int64(len(payload) + 1), nil
}

func (db *DB) Checkpoint() (FileCheckpoint, error) {
	path := filepath.Join(db.dataDir, "state_snapshot.json")
	cp := FileCheckpoint{walSizes: map[string]int64{}, replaceData: map[string][]byte{}, replaceMissing: map[string]bool{}}
	data, err := os.ReadFile(path)
	if err == nil {
		cp.snapshotData = data
	} else if os.IsNotExist(err) {
		cp.snapshotMissing = true
	} else {
		return FileCheckpoint{}, err
	}
	for _, path := range []string{filepath.Join(db.dataDir, "state_snapshot_metadata.json"), filepath.Join(db.dataDir, "state_wal_manifest.json")} {
		data, err := os.ReadFile(path)
		if err == nil {
			cp.replaceData[path] = data
		} else if os.IsNotExist(err) {
			cp.replaceMissing[path] = true
		} else {
			return FileCheckpoint{}, err
		}
	}
	walPaths, err := db.walSegmentPaths()
	if err != nil {
		return FileCheckpoint{}, err
	}
	for _, walPath := range walPaths {
		if info, err := os.Stat(walPath); err == nil {
			cp.walSizes[walPath] = info.Size()
		} else if os.IsNotExist(err) {
			cp.walSizes[walPath] = -1
		} else {
			return FileCheckpoint{}, err
		}
	}
	return cp, nil
}

func (db *DB) Rollback(checkpoint FileCheckpoint) error {
	path := filepath.Join(db.dataDir, "state_snapshot.json")
	if checkpoint.snapshotMissing {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	} else if err := os.WriteFile(path, checkpoint.snapshotData, 0o644); err != nil {
		return err
	}
	currentWALs, err := filepath.Glob(filepath.Join(db.dataDir, "state_delta*.wal"))
	if err != nil {
		return err
	}
	for _, path := range currentWALs {
		if _, ok := checkpoint.walSizes[path]; !ok {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	for path, size := range checkpoint.walSizes {
		if size == -1 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if err := f.Truncate(size); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	for path, data := range checkpoint.replaceData {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	for path := range checkpoint.replaceMissing {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (db *DB) Load() error {
	values := map[string]string{}
	snapshotMeta, err := db.loadSnapshotMetadata()
	if err != nil {
		return err
	}
	walManifest, err := db.loadWALManifest()
	if err != nil {
		return err
	}
	payload, err := os.ReadFile(filepath.Join(db.dataDir, "state_snapshot.json"))
	if err == nil {
		if err := json.Unmarshal(payload, &values); err != nil {
			return fmt.Errorf("decode state snapshot: %w", err)
		}
		if snapshotMeta.Version != "" {
			if got := Root(values); snapshotMeta.StateRoot != "" && got != snapshotMeta.StateRoot {
				return fmt.Errorf("snapshot metadata state root mismatch: got %s want %s", got, snapshotMeta.StateRoot)
			}
			db.walGeneration = snapshotMeta.WALGeneration
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if walManifest.ActiveGeneration > 0 {
		db.walGeneration = walManifest.ActiveGeneration
	}
	if err := db.replayWAL(values, snapshotMeta); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.values = values
	return nil
}

func (db *DB) loadSnapshotMetadata() (SnapshotMetadata, error) {
	payload, err := os.ReadFile(filepath.Join(db.dataDir, "state_snapshot_metadata.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return SnapshotMetadata{}, nil
		}
		return SnapshotMetadata{}, err
	}
	var meta SnapshotMetadata
	if err := json.Unmarshal(payload, &meta); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("decode snapshot metadata: %w", err)
	}
	if meta.Version != "state_snapshot_metadata_v1" || meta.Namespace != db.namespace {
		return SnapshotMetadata{}, fmt.Errorf("invalid snapshot metadata version=%q namespace=%q", meta.Version, meta.Namespace)
	}
	return meta, nil
}

func (db *DB) replayWAL(values map[string]string, snapshotMeta SnapshotMetadata) error {
	committedBlocks, err := committedBlockHashes(db.dataDir)
	if err != nil {
		return err
	}
	if len(committedBlocks) == 0 {
		return nil
	}
	applied := map[string]bool{}
	lastHeight := snapshotMeta.IncludedHeight
	lastHash := snapshotMeta.IncludedBlockHash
	paths, err := db.walSegmentPaths()
	if err != nil {
		return err
	}
	for _, path := range paths {
		lines, err := readWALLines(path)
		if err != nil {
			return err
		}
		for index, row := range lines {
			var record walRecord
			if err := json.Unmarshal(row, &record); err != nil {
				if index == len(lines)-1 {
					break
				}
				return fmt.Errorf("decode state wal %s line %d: %w", filepath.Base(path), index+1, err)
			}
			if record.Version != "state_delta_wal_v1" || record.Namespace != db.namespace {
				return fmt.Errorf("invalid state wal %s line %d: version=%q namespace=%q", filepath.Base(path), index+1, record.Version, record.Namespace)
			}
			if record.Checksum != walChecksum(record) {
				return fmt.Errorf("state wal checksum mismatch at %s line %d", filepath.Base(path), index+1)
			}
			if record.BlockHeight <= snapshotMeta.IncludedHeight {
				continue
			}
			if !committedBlocks[record.BlockHash] {
				continue
			}
			if applied[record.DeltaID] {
				continue
			}
			if lastHeight > 0 && record.BlockHeight != lastHeight+1 {
				return fmt.Errorf("state wal height gap: got %d after %d", record.BlockHeight, lastHeight)
			}
			if lastHash != "" && record.ParentHash != lastHash {
				return fmt.Errorf("state wal parent mismatch at height %d: got %s want %s", record.BlockHeight, record.ParentHash, lastHash)
			}
			if got := Root(values); record.StateRootBefore != "" && got != record.StateRootBefore {
				return fmt.Errorf("state wal root-before mismatch at height %d: got %s want %s", record.BlockHeight, got, record.StateRootBefore)
			}
			if err := applyStateKVs(values, record.StateUpdates, db.namespace); err != nil {
				return fmt.Errorf("apply state wal at height %d: %w", record.BlockHeight, err)
			}
			if got := Root(values); record.StateRootAfter != "" && got != record.StateRootAfter {
				return fmt.Errorf("state wal root-after mismatch at height %d: got %s want %s", record.BlockHeight, got, record.StateRootAfter)
			}
			applied[record.DeltaID] = true
			lastHeight = record.BlockHeight
			lastHash = record.BlockHash
		}
	}
	return nil
}

func (db *DB) walSegmentPaths() ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(db.dataDir, "state_delta.*.wal"))
	if err != nil {
		return nil, err
	}
	legacy := filepath.Join(db.dataDir, "state_delta.wal")
	if _, err := os.Stat(legacy); err == nil {
		paths = append(paths, legacy)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sort.Slice(paths, func(i, j int) bool {
		return walGenerationFromPath(paths[i]) < walGenerationFromPath(paths[j])
	})
	return paths, nil
}

func walGenerationFromPath(path string) int {
	base := filepath.Base(path)
	if base == "state_delta.wal" {
		return 0
	}
	parts := strings.Split(base, ".")
	if len(parts) >= 3 {
		value, _ := strconv.Atoi(parts[1])
		return value
	}
	return 0
}

func readWALLines(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var lines [][]byte
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, append([]byte(nil), scanner.Bytes()...))
	}
	return lines, scanner.Err()
}

func committedBlockHashes(dataDir string) (map[string]bool, error) {
	path := filepath.Join(dataDir, "commit_markers.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("open commit markers for state wal recovery: %w", err)
	}
	defer f.Close()
	hashes := map[string]bool{}
	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		var row struct {
			Version   string `json:"version"`
			BlockHash string `json:"block_hash"`
			Committed bool   `json:"committed"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("decode commit marker line %d for state wal recovery: %w", line, err)
		}
		if row.Version != "durable_commit_marker_v1" || row.BlockHash == "" || !row.Committed {
			return nil, fmt.Errorf("invalid commit marker line %d", line)
		}
		hashes[row.BlockHash] = true
	}
	return hashes, scanner.Err()
}

func (db *DB) appendWAL(record walRecord, opts PersistenceOptions) (int64, int64, error) {
	if err := os.MkdirAll(db.dataDir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("create state dir: %w", err)
	}
	if db.walGeneration <= 0 {
		if manifest, err := db.loadWALManifest(); err == nil && manifest.ActiveGeneration > 0 {
			db.walGeneration = manifest.ActiveGeneration
		} else {
			db.walGeneration = 1
		}
	}
	if _, err := os.Stat(filepath.Join(db.dataDir, "state_wal_manifest.json")); os.IsNotExist(err) {
		if err := db.writeWALManifest(db.walGeneration); err != nil {
			return 0, 0, fmt.Errorf("publish state wal manifest: %w", err)
		}
	}
	path := db.walPath(db.walGeneration)
	before := int64(0)
	if info, err := os.Stat(path); err == nil {
		before = info.Size()
	} else if !os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("stat state wal: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, 0, fmt.Errorf("open state wal: %w", err)
	}
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(record); err != nil {
		_ = f.Close()
		return 0, 0, fmt.Errorf("write state wal: %w", err)
	}
	syncStarted := timeNowMS()
	shouldSync := opts.DurabilityMode == "strict"
	db.mu.Lock()
	db.walSinceSync++
	if opts.DurabilityMode == "batched" && db.walSinceSync >= opts.FSyncCadence {
		shouldSync = true
	}
	if shouldSync {
		db.walSinceSync = 0
	}
	db.mu.Unlock()
	if shouldSync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return 0, 0, fmt.Errorf("sync state wal: %w", err)
		}
	}
	syncMS := int64(0)
	if shouldSync {
		syncMS = timeNowMS() - syncStarted
	}
	if err := f.Close(); err != nil {
		return 0, syncMS, fmt.Errorf("close state wal: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, syncMS, fmt.Errorf("stat written state wal: %w", err)
	}
	return info.Size() - before, syncMS, nil
}

func canonicalStateUpdates(updates []StateKV) []StateKV {
	return append([]StateKV(nil), updates...)
}

func walChecksum(record walRecord) string {
	copyRecord := record
	copyRecord.Checksum = ""
	payload, _ := json.Marshal(copyRecord)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func applyStateKVs(values map[string]string, updates []StateKV, namespace string) error {
	for _, item := range updates {
		key := item.Key
		if !strings.Contains(key, "::") {
			key = namespace + "::" + key
		}
		if err := applyStateKV(values, key, item); err != nil {
			return err
		}
	}
	return nil
}

func applyStateKV(values map[string]string, key string, item StateKV) error {
	if item.OrderingNoop {
		return nil
	}
	if item.UpdateSemantics == "commutative_delta" {
		currentText := values[key]
		current, _ := strconv.ParseInt(currentText, 10, 64)
		if currentText == "" && item.HasInitialValue {
			current = item.InitialValue
		}
		values[key] = strconv.FormatInt(current+item.Delta, 10)
		return nil
	}
	if item.ProducedVersion == 0 && item.BaseValueDigest != "" {
		actual := stateValueDigest(values[key])
		if actual != item.BaseValueDigest {
			return CASMismatchError{Key: key, ExpectedDigest: item.BaseValueDigest, ActualDigest: actual, DeltaID: item.DeltaID, TxIDs: append([]string(nil), item.TxIDs...), BlockHeight: item.BlockHeight}
		}
	}
	values[key] = item.Value
	return nil
}

func timeNowMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (db *DB) Balance(account string) int64 {
	value := db.Get("balance:" + account)
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func (db *DB) SetBalance(account string, balance int64) {
	db.Set("balance:"+account, strconv.FormatInt(balance, 10))
}

func (db *DB) Nonce(account string) uint64 {
	value := db.Get("nonce:" + account)
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

func (db *DB) SetNonce(account string, nonce uint64) {
	db.Set("nonce:"+account, strconv.FormatUint(nonce, 10))
}

func (db *DB) key(key string) string {
	if strings.Contains(key, "::") {
		return key
	}
	return db.namespace + "::" + key
}
