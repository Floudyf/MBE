package v5

// MBE_TXALLO_OBSERVABILITY_V1: client-only observations, not a protocol input.
// A failure to write optional evidence is reported on stderr; it never advances
// or changes a transaction, replica, or mapping barrier.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const txalloEpochTimingSchemaV1 = "mbe_txallo_epoch_timing_v1"

type txalloEpochTimingV1 struct {
	SchemaVersion      string `json:"schema_version"`
	SourceEpoch        int64  `json:"source_epoch"`
	RoutingEpochBefore uint64 `json:"routing_epoch_before"`
	RoutingEpochAfter  uint64 `json:"routing_epoch_after"`
	TransactionCount   int    `json:"transaction_count"`
	ReplicaTokenCount  int    `json:"replica_token_count"`
	CommitWaitNS       int64  `json:"commit_wait_ns"`
	ReplicaWaitNS      int64  `json:"replica_wait_ns"`
	AllocationUpdateNS int64  `json:"allocation_update_ns"`
	MappingAckWaitNS   int64  `json:"mapping_ack_wait_ns"`
	TotalBarrierNS     int64  `json:"total_barrier_ns"`
	Status             string `json:"status"`
	FailurePhase       string `json:"failure_phase,omitempty"`
	Error              string `json:"error,omitempty"`
	// These values describe monotonic durations, not synchronized node clocks.
	TimingBasis string `json:"timing_basis"`
}

func txalloWriteEpochTimingV1(outDir string, trace txalloEpochTimingV1) error {
	if trace.SchemaVersion != txalloEpochTimingSchemaV1 || trace.SourceEpoch < 0 || trace.TotalBarrierNS < 0 {
		return fmt.Errorf("invalid TxAllo observational epoch trace")
	}
	raw, err := json.Marshal(trace)
	if err != nil {
		return err
	}
	path := filepath.Join(outDir, "txallo_epoch_timing.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(raw, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func txalloRecordEpochTimingV1(outDir string, trace txalloEpochTimingV1, err error) {
	trace.TimingBasis = "client_local_monotonic_duration_ns_v1"
	if err != nil {
		trace.Status = "failed"
		trace.Error = err.Error()
	} else {
		trace.Status = "passed"
	}
	if writeErr := txalloWriteEpochTimingV1(outDir, trace); writeErr != nil {
		// Observability is not a participant in TxAllo's committed-only protocol.
		fmt.Fprintf(os.Stderr, "[TXALLO OBSERVABILITY WRITE FAILED] source_epoch=%d: %v\n", trace.SourceEpoch, writeErr)
	}
}

func txalloObserveDurationV1(start time.Time) int64 { return time.Since(start).Nanoseconds() }

// Capture the actual client executable identity at the time of the run.
// This does NOT identify each validator's binary; reports must not imply it does.
func txalloCaptureClientBuildV1(outDir string) {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO BUILD EVIDENCE FAILED] %v\n", err)
		return
	}
	f, err := os.Open(exe)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO BUILD EVIDENCE FAILED] %v\n", err)
		return
	}
	defer f.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, f); err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO BUILD EVIDENCE FAILED] %v\n", err)
		return
	}
	row := map[string]any{"schema_version": "mbe_txallo_client_binary_v1",
		"binary_sha256": hex.EncodeToString(digest.Sum(nil)), "binary_scope": "client_process_only",
		"executable_path": exe}
	raw, err := json.MarshalIndent(row, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(filepath.Join(outDir, "txallo_client_binary_v1.json"), append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO BUILD EVIDENCE FAILED] %v\n", err)
	}
}

// Make an explicit empty evidence artifact even when the entire evaluation
// occupies the final, intentionally untrained source epoch.
func txalloInitEpochTimingV1(outDir string) {
	f, err := os.OpenFile(filepath.Join(outDir, "txallo_epoch_timing.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO OBSERVABILITY INIT FAILED] %v\n", err)
		return
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "[TXALLO OBSERVABILITY INIT FAILED] %v\n", err)
	}
}
