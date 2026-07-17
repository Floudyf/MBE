package blockstm

import (
	"sort"
	"sync"
)

type VersionedValue struct {
	Version  Version `json:"version"`
	Value    string  `json:"value"`
	Estimate bool    `json:"estimate"`
}

type MVMemory struct {
	mu     sync.RWMutex
	values map[string][]VersionedValue
}

func NewMVMemory() *MVMemory {
	return &MVMemory{values: map[string][]VersionedValue{}}
}

func (m *MVMemory) Write(key string, version Version, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsertLocked(key, VersionedValue{Version: version, Value: value})
}

func (m *MVMemory) MarkEstimate(key string, version Version) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsertLocked(key, VersionedValue{Version: version, Estimate: true})
}

func (m *MVMemory) upsertLocked(key string, item VersionedValue) {
	items := m.values[key]
	replaced := false
	for index := range items {
		if items[index].Version == item.Version {
			items[index] = item
			replaced = true
			break
		}
	}
	if !replaced {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Version.Less(items[j].Version)
	})
	m.values[key] = items
}

func (m *MVMemory) Read(key string, reader TxnIndex, base map[string]string) ReadDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := m.values[key]
	for index := len(items) - 1; index >= 0; index-- {
		item := items[index]
		if item.Version.Txn < reader {
			read := ReadDescriptor{Key: key, Version: item.Version, Value: item.Value, Estimate: item.Estimate}
			if item.Estimate {
				dependency := item.Version
				read.DependencyOn = &dependency
			}
			return read
		}
	}
	return ReadDescriptor{Key: key, FromBase: true, Value: base[key]}
}

func (m *MVMemory) Validate(reader TxnIndex, base map[string]string, captured CapturedReads) ValidationResult {
	for _, expected := range captured.Reads {
		observed := m.Read(expected.Key, reader, base)
		if observed.Estimate {
			return ValidationResult{Valid: false, FailedKey: expected.Key, Expected: expected, Observed: observed, Dependency: observed.DependencyOn}
		}
		if !sameRead(expected, observed) {
			return ValidationResult{Valid: false, FailedKey: expected.Key, Expected: expected, Observed: observed}
		}
	}
	return ValidationResult{Valid: true}
}

func (m *MVMemory) Snapshot() map[string][]VersionedValue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]VersionedValue, len(m.values))
	for key, items := range m.values {
		copied := append([]VersionedValue(nil), items...)
		out[key] = copied
	}
	return out
}

func sameRead(left, right ReadDescriptor) bool {
	if left.Key != right.Key || left.FromBase != right.FromBase || left.Value != right.Value || left.Estimate != right.Estimate {
		return false
	}
	if !left.FromBase && left.Version != right.Version {
		return false
	}
	return true
}
