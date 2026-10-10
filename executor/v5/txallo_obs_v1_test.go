package v5

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTxalloObsV1ExactEpochTimingRecord(t *testing.T) {
	root := t.TempDir()
	record := txalloEpochTimingV1{SchemaVersion: txalloEpochTimingSchemaV1, SourceEpoch: 2,
		RoutingEpochBefore: 2, RoutingEpochAfter: 3, TransactionCount: 7,
		CommitWaitNS: 500, ReplicaWaitNS: 300, AllocationUpdateNS: 40,
		MappingAckWaitNS: 60, TotalBarrierNS: 1100}
	txalloRecordEpochTimingV1(root, record, nil)
	raw, err := os.ReadFile(filepath.Join(root, "txallo_epoch_timing.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one row, got %d", len(lines))
	}
	var got txalloEpochTimingV1
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "passed" || got.TotalBarrierNS != 1100 || got.FailurePhase != "" || got.TimingBasis == "" {
		t.Fatalf("invalid stage evidence: %+v", got)
	}
}

func TestTxalloObsV1FailureMustKeepFailedPhase(t *testing.T) {
	root := t.TempDir()
	record := txalloEpochTimingV1{SchemaVersion: txalloEpochTimingSchemaV1, SourceEpoch: 0, FailurePhase: "replica_wait"}
	txalloRecordEpochTimingV1(root, record, os.ErrDeadlineExceeded)
	raw, err := os.ReadFile(filepath.Join(root, "txallo_epoch_timing.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var got txalloEpochTimingV1
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" || got.FailurePhase != "replica_wait" || got.Error == "" {
		t.Fatalf("lost failure evidence: %+v", got)
	}
}
