package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	mu        sync.Mutex
	namespace string
	dataDir   string
	values    map[string]string
}

type FileCheckpoint struct {
	data    []byte
	missing bool
}

func NewDB(dataDir, namespace string) *DB {
	if namespace == "" {
		namespace = "s0"
	}
	return &DB{dataDir: dataDir, namespace: namespace, values: map[string]string{}}
}

func Open(dataDir, namespace string) (*DB, error) {
	db := NewDB(dataDir, namespace)
	if err := db.Load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return db, nil
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

func (db *DB) Save() error {
	if err := os.MkdirAll(db.dataDir, 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	payload, err := json.MarshalIndent(db.Snapshot(), "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state snapshot: %w", err)
	}
	target := filepath.Join(db.dataDir, "state_snapshot.json")
	tmp, err := os.CreateTemp(db.dataDir, "state_snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create state snapshot temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmpName) }
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
		cleanup()
		return fmt.Errorf("write state snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync state snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close state snapshot: %w", err)
	}
	backup := target + ".bak"
	_ = os.Remove(backup)
	if _, err := os.Stat(target); err == nil {
		if err := os.Rename(target, backup); err != nil {
			_ = os.Remove(tmpName)
			return fmt.Errorf("backup state snapshot: %w", err)
		}
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(target)
		if _, backupErr := os.Stat(backup); backupErr == nil {
			_ = os.Rename(backup, target)
		}
		_ = os.Remove(tmpName)
		return fmt.Errorf("replace state snapshot: %w", err)
	}
	_ = os.Remove(backup)
	return nil
}

func (db *DB) Checkpoint() (FileCheckpoint, error) {
	path := filepath.Join(db.dataDir, "state_snapshot.json")
	data, err := os.ReadFile(path)
	if err == nil {
		return FileCheckpoint{data: data}, nil
	}
	if os.IsNotExist(err) {
		return FileCheckpoint{missing: true}, nil
	}
	return FileCheckpoint{}, err
}

func (db *DB) Rollback(checkpoint FileCheckpoint) error {
	path := filepath.Join(db.dataDir, "state_snapshot.json")
	if checkpoint.missing {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return os.WriteFile(path, checkpoint.data, 0o644)
}

func (db *DB) Load() error {
	payload, err := os.ReadFile(filepath.Join(db.dataDir, "state_snapshot.json"))
	if err != nil {
		return err
	}
	var values map[string]string
	if err := json.Unmarshal(payload, &values); err != nil {
		return fmt.Errorf("decode state snapshot: %w", err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.values = values
	return nil
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
