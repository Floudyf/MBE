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
	if err := os.WriteFile(filepath.Join(db.dataDir, "state_snapshot.json"), append(payload, '\n'), 0o644); err != nil {
		return fmt.Errorf("write state snapshot: %w", err)
	}
	return nil
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
