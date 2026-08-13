package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadAcceptsLegacySnapshotRootWithoutVersionField(t *testing.T) {
	dir := t.TempDir()
	values := map[string]string{"s0::a": "1", "s0::b": "2"}
	payload, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state_snapshot.json"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{
		"version":             "state_snapshot_metadata_v1",
		"namespace":           "s0",
		"included_height":     0,
		"included_block_hash": "",
		"state_root":          LegacyRoot(values),
		"wal_generation":      1,
	}
	metaPayload, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state_snapshot_metadata.json"), metaPayload, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, "s0")
	if err != nil {
		t.Fatalf("open legacy snapshot: %v", err)
	}
	if got := db.Snapshot(); !reflect.DeepEqual(got, values) {
		t.Fatalf("legacy snapshot mismatch: got=%v want=%v", got, values)
	}
}
