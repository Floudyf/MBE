package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	RelayStateDetected                    = "Detected"
	RelayStateSourceLocked                = "SourceLocked"
	RelayStateCertificateGenerated        = "RelayCertificateGenerated"
	RelayStateProofVerified               = "ProofVerified"
	RelayStateProofFailed                 = "ProofFailed"
	RelayStateTargetVerified              = "TargetVerified"
	RelayStateTargetCommitted             = "TargetCommitted"
	RelayStateSourceFinalized             = "SourceFinalized"
	RelayStateTimeout                     = "Timeout"
	RelayStateRefunded                    = "Refunded"
	RelayStateAborted                     = "Aborted"
	defaultRelayMVPStateRootDigest        = "mvp_state_root_placeholder"
	defaultRelayMVPCertificateTruth       = "relay_certificate_mvp_no_byzantine_security"
	defaultRelayMVPCertificateVerifyTruth = "relay_certificate_mvp_deterministic_guard_not_byzantine_proof"
)

type RelayStateMachineRecord struct {
	TxID         string
	BlockHeight  int
	SourceShard  int
	TargetShards string
	FromState    string
	ToState      string
	Event        string
	EventTimeMS  int
	Success      bool
	Reason       string
}

type SourceLockRecord struct {
	TxID         string
	BlockHeight  int
	SourceShard  int
	TargetShards string
	SourceLockID string
	LockTimeMS   int
	Status       string
	Reason       string
}

type RelayCertificateRecord struct {
	TxID            string
	BlockHeight     int
	SourceShard     int
	TargetShards    string
	SourceLockID    string
	CertificateID   string
	CertificateHash string
	ProofDigest     string
	StateRootDigest string
	CreatedAtMS     int
	Status          string
	Truth           string
}

type RelayProofVerificationRecord struct {
	TxID               string
	CertificateID      string
	VerificationTimeMS int
	ProofDigest        string
	Verified           bool
	FailureReason      string
	Truth              string
}

type TargetVerificationRecord struct {
	TxID               string
	CertificateID      string
	TargetShard        int
	VerificationTimeMS int
	Accepted           bool
	Reason             string
}

type TargetCommitRecord struct {
	TxID          string
	CertificateID string
	TargetShard   int
	CommitTimeMS  int
	Committed     bool
	Reason        string
}

type SourceFinalizeRecord struct {
	TxID           string
	SourceLockID   string
	CertificateID  string
	FinalizeTimeMS int
	Finalized      bool
	Reason         string
}

type CrossShardTimeoutRefundRecord struct {
	TxID         string
	SourceLockID string
	EventTimeMS  int
	EventType    string
	Refunded     bool
	Reason       string
}

type CrossShardFailureRecord struct {
	TxID          string
	SourceLockID  string
	CertificateID string
	FailureTimeMS int
	FailureType   string
	Aborted       bool
	Refunded      bool
	Reason        string
}

type RelayMVPOutcome struct {
	TxStatus            string
	Completed           bool
	Failed              bool
	LatencyMS           int
	TargetReceiveCount  int
	MessageRows         []CrossShardMessageRecord
	StatusRows          []CrossShardStatusRecord
	TypedMessages       []MessageEnvelope
	SendRows            []NetworkSendRecord
	ReceiveRows         []NetworkReceiveRecord
	StateMachineRows    []RelayStateMachineRecord
	SourceLocks         []SourceLockRecord
	Certificates        []RelayCertificateRecord
	ProofVerifications  []RelayProofVerificationRecord
	TargetVerifications []TargetVerificationRecord
	TargetCommits       []TargetCommitRecord
	SourceFinalizes     []SourceFinalizeRecord
	TimeoutRefunds      []CrossShardTimeoutRefundRecord
	Failures            []CrossShardFailureRecord
}

type RelayMVPPreview struct {
	Enabled              bool
	Truth                string
	StateMachineRows     []RelayStateMachineRecord
	SourceLocks          []SourceLockRecord
	Certificates         []RelayCertificateRecord
	ProofVerifications   []RelayProofVerificationRecord
	TargetVerifications  []TargetVerificationRecord
	TargetCommits        []TargetCommitRecord
	SourceFinalizes      []SourceFinalizeRecord
	TimeoutRefunds       []CrossShardTimeoutRefundRecord
	Failures             []CrossShardFailureRecord
	TxCount              int
	SourceLockCount      int
	CertificateCount     int
	ProofVerifiedCount   int
	ProofFailedCount     int
	TargetVerifiedCount  int
	TargetCommitCount    int
	SourceFinalizedCount int
	TimeoutCount         int
	RefundCount          int
	AbortCount           int
	SuccessCount         int
	FailedCount          int
	AvgLatencyMS         float64
}

func BuildRelayMVPForTx(experiment ExperimentProfile, tx TxResult, record RoutingRecord, index, sourceShard, targetShard int, networkAdapter string) RelayMVPOutcome {
	targetShards := strconv.Itoa(targetShard)
	baseTime := tx.CommitTimeMS
	lockID := hashString(fmt.Sprintf("%s|%d|%s|%d", tx.TxID, sourceShard, targetShards, tx.BlockHeight))
	certID := "relay_cert_" + hashString("certificate_id|" + lockID)[:16]
	proofDigest := relayProofDigest(tx, record, lockID)
	stateRootDigest := defaultRelayMVPStateRootDigest
	certHash := hashString(strings.Join([]string{tx.TxID, lockID, certID, proofDigest, stateRootDigest}, "|"))
	messageID := fmt.Sprintf("cross_shard_relay_mvp_%06d", index+1)
	outcome := RelayMVPOutcome{TxStatus: "relay_mvp_started"}
	transition := func(from, to, event string, timeMS int, success bool, reason string) {
		outcome.StateMachineRows = append(outcome.StateMachineRows, RelayStateMachineRecord{
			TxID: tx.TxID, BlockHeight: tx.BlockHeight, SourceShard: sourceShard, TargetShards: targetShards,
			FromState: from, ToState: to, Event: event, EventTimeMS: timeMS, Success: success, Reason: reason,
		})
	}
	transition("", RelayStateDetected, "cross_shard_tx_detected", baseTime, true, "relay_mvp detected cross-shard tx")
	outcome.SourceLocks = append(outcome.SourceLocks, SourceLockRecord{
		TxID: tx.TxID, BlockHeight: tx.BlockHeight, SourceShard: sourceShard, TargetShards: targetShards,
		SourceLockID: lockID, LockTimeMS: baseTime + 1, Status: "locked", Reason: "deterministic MVP source lock",
	})
	transition(RelayStateDetected, RelayStateSourceLocked, "source_lock_created", baseTime+1, true, "SourceLock MVP record created")

	if shouldForceTimeout(experiment, index) {
		transition(RelayStateSourceLocked, RelayStateTimeout, "relay_timeout", baseTime+2, false, "deterministic relay timeout")
		transition(RelayStateTimeout, RelayStateRefunded, "source_refund", baseTime+3, true, "timeout refund path")
		outcome.TimeoutRefunds = append(outcome.TimeoutRefunds, CrossShardTimeoutRefundRecord{TxID: tx.TxID, SourceLockID: lockID, EventTimeMS: baseTime + 3, EventType: "timeout_refund", Refunded: true, Reason: "deterministic relay timeout"})
		outcome.Failures = append(outcome.Failures, CrossShardFailureRecord{TxID: tx.TxID, SourceLockID: lockID, FailureTimeMS: baseTime + 3, FailureType: "timeout", Refunded: true, Reason: "relay timeout triggered refund"})
		outcome.StatusRows = append(outcome.StatusRows, CrossShardStatusRecord{TxID: tx.TxID, Protocol: CrossShardProtocolRelayMVP, State: RelayStateRefunded, SourceShard: sourceShard, TargetShard: targetShard, Failed: true, Reason: "timeout refunded"})
		outcome.TxStatus = "timeout_refunded"
		outcome.Failed = true
		outcome.LatencyMS = 3
		return outcome
	}

	cert := RelayCertificateRecord{
		TxID: tx.TxID, BlockHeight: tx.BlockHeight, SourceShard: sourceShard, TargetShards: targetShards,
		SourceLockID: lockID, CertificateID: certID, CertificateHash: certHash, ProofDigest: proofDigest,
		StateRootDigest: stateRootDigest, CreatedAtMS: baseTime + 2, Status: "generated", Truth: defaultRelayMVPCertificateTruth,
	}
	outcome.Certificates = append(outcome.Certificates, cert)
	transition(RelayStateSourceLocked, RelayStateCertificateGenerated, "relay_certificate_generated", baseTime+2, true, "RelayCertificate MVP generated")

	verified, failureReason := verifyRelayCertificateMVP(cert, outcome.SourceLocks[0], tx, record, targetShards, shouldForceProofFail(experiment, index))
	outcome.ProofVerifications = append(outcome.ProofVerifications, RelayProofVerificationRecord{
		TxID: tx.TxID, CertificateID: certID, VerificationTimeMS: baseTime + 3, ProofDigest: proofDigest,
		Verified: verified, FailureReason: failureReason, Truth: defaultRelayMVPCertificateVerifyTruth,
	})
	if !verified {
		transition(RelayStateCertificateGenerated, RelayStateProofFailed, "relay_certificate_verification_failed", baseTime+3, false, failureReason)
		transition(RelayStateProofFailed, RelayStateRefunded, "source_refund", baseTime+4, true, "proof failure refund path")
		outcome.TimeoutRefunds = append(outcome.TimeoutRefunds, CrossShardTimeoutRefundRecord{TxID: tx.TxID, SourceLockID: lockID, EventTimeMS: baseTime + 4, EventType: "proof_fail_refund", Refunded: true, Reason: failureReason})
		outcome.Failures = append(outcome.Failures, CrossShardFailureRecord{TxID: tx.TxID, SourceLockID: lockID, CertificateID: certID, FailureTimeMS: baseTime + 4, FailureType: "proof_fail", Refunded: true, Reason: failureReason})
		outcome.StatusRows = append(outcome.StatusRows, CrossShardStatusRecord{TxID: tx.TxID, Protocol: CrossShardProtocolRelayMVP, State: RelayStateRefunded, SourceShard: sourceShard, TargetShard: targetShard, Failed: true, Reason: failureReason})
		outcome.TxStatus = "proof_failed_refunded"
		outcome.Failed = true
		outcome.LatencyMS = 4
		return outcome
	}

	transition(RelayStateCertificateGenerated, RelayStateProofVerified, "relay_certificate_verified", baseTime+3, true, "deterministic certificate guard passed")
	if relayFailureMode(experiment) == "target_reject" {
		transition(RelayStateProofVerified, RelayStateAborted, "target_rejected", baseTime+4, false, "deterministic target reject")
		outcome.Failures = append(outcome.Failures, CrossShardFailureRecord{TxID: tx.TxID, SourceLockID: lockID, CertificateID: certID, FailureTimeMS: baseTime + 4, FailureType: "target_reject", Aborted: true, Reason: "deterministic target reject"})
		outcome.StatusRows = append(outcome.StatusRows, CrossShardStatusRecord{TxID: tx.TxID, Protocol: CrossShardProtocolRelayMVP, State: RelayStateAborted, SourceShard: sourceShard, TargetShard: targetShard, Failed: true, Reason: "target rejected relay MVP certificate"})
		outcome.TxStatus = "target_rejected_aborted"
		outcome.Failed = true
		outcome.LatencyMS = 4
		return outcome
	}

	outcome.TargetVerifications = append(outcome.TargetVerifications, TargetVerificationRecord{TxID: tx.TxID, CertificateID: certID, TargetShard: targetShard, VerificationTimeMS: baseTime + 4, Accepted: true, Reason: "target accepted RelayCertificate MVP"})
	transition(RelayStateProofVerified, RelayStateTargetVerified, "target_verified", baseTime+4, true, "target verification MVP accepted")
	outcome.TargetCommits = append(outcome.TargetCommits, TargetCommitRecord{TxID: tx.TxID, CertificateID: certID, TargetShard: targetShard, CommitTimeMS: baseTime + 5, Committed: true, Reason: "target commit MVP record"})
	transition(RelayStateTargetVerified, RelayStateTargetCommitted, "target_commit", baseTime+5, true, "target commit MVP completed")
	outcome.SourceFinalizes = append(outcome.SourceFinalizes, SourceFinalizeRecord{TxID: tx.TxID, SourceLockID: lockID, CertificateID: certID, FinalizeTimeMS: baseTime + 6, Finalized: true, Reason: "source finalized after target commit MVP"})
	transition(RelayStateTargetCommitted, RelayStateSourceFinalized, "source_finalize", baseTime+6, true, "source finalize MVP completed")
	outcome.MessageRows = append(outcome.MessageRows, CrossShardMessageRecord{
		MessageID: messageID, MessageType: "cross_shard_relay_mvp", FromShard: sourceShard, ToShard: targetShard,
		TxID: tx.TxID, Protocol: CrossShardProtocolRelayMVP, NetworkAdapter: networkAdapter, TimestampMS: baseTime + 4, Status: "relay_mvp_delivered",
	})
	msg := NewMessageEnvelope(messageID, "cross_shard_relay_mvp", fmt.Sprintf("shard_%d", sourceShard), fmt.Sprintf("shard_%d", targetShard), sourceShard, "relay_mvp", tx.BlockHeight, index+1, fmt.Sprintf("tx=%s:certificate=%s:protocol=relay_mvp", tx.TxID, certID))
	outcome.TypedMessages = append(outcome.TypedMessages, msg)
	outcome.SendRows = append(outcome.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: crossShardSendStatus(networkAdapter)})
	outcome.ReceiveRows = append(outcome.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: crossShardReceiveStatus(networkAdapter)})
	outcome.StatusRows = append(outcome.StatusRows, CrossShardStatusRecord{TxID: tx.TxID, Protocol: CrossShardProtocolRelayMVP, State: RelayStateSourceFinalized, SourceShard: sourceShard, TargetShard: targetShard, Completed: true, Reason: "Relay MVP completed source lock, certificate, target commit, and source finalize"})
	outcome.TxStatus = "relay_mvp_completed"
	outcome.Completed = true
	outcome.LatencyMS = 6
	outcome.TargetReceiveCount = 1
	return outcome
}

func (preview *RelayMVPPreview) Append(outcome RelayMVPOutcome) {
	preview.Enabled = true
	preview.Truth = RelayMVPRuntimeTruth
	preview.TxCount++
	preview.StateMachineRows = append(preview.StateMachineRows, outcome.StateMachineRows...)
	preview.SourceLocks = append(preview.SourceLocks, outcome.SourceLocks...)
	preview.Certificates = append(preview.Certificates, outcome.Certificates...)
	preview.ProofVerifications = append(preview.ProofVerifications, outcome.ProofVerifications...)
	preview.TargetVerifications = append(preview.TargetVerifications, outcome.TargetVerifications...)
	preview.TargetCommits = append(preview.TargetCommits, outcome.TargetCommits...)
	preview.SourceFinalizes = append(preview.SourceFinalizes, outcome.SourceFinalizes...)
	preview.TimeoutRefunds = append(preview.TimeoutRefunds, outcome.TimeoutRefunds...)
	preview.Failures = append(preview.Failures, outcome.Failures...)
	if outcome.Completed {
		preview.SuccessCount++
	}
	if outcome.Failed {
		preview.FailedCount++
	}
}

func (preview *RelayMVPPreview) Finalize(avgLatency float64) {
	preview.SourceLockCount = len(preview.SourceLocks)
	preview.CertificateCount = len(preview.Certificates)
	for _, row := range preview.ProofVerifications {
		if row.Verified {
			preview.ProofVerifiedCount++
		} else {
			preview.ProofFailedCount++
		}
	}
	preview.TargetVerifiedCount = len(preview.TargetVerifications)
	preview.TargetCommitCount = len(preview.TargetCommits)
	preview.SourceFinalizedCount = len(preview.SourceFinalizes)
	for _, row := range preview.TimeoutRefunds {
		if row.EventType == "timeout_refund" {
			preview.TimeoutCount++
		}
		if row.Refunded {
			preview.RefundCount++
		}
	}
	for _, row := range preview.Failures {
		if row.Aborted {
			preview.AbortCount++
		}
	}
	preview.AvgLatencyMS = avgLatency
}

func relayProofDigest(tx TxResult, record RoutingRecord, lockID string) string {
	stateProofLink := "state_authenticity_mvp_linked"
	return hashString(strings.Join([]string{tx.TxID, strconv.Itoa(tx.BlockHeight), lockID, joinInts(record.TouchedShards), stateProofLink}, "|"))
}

func verifyRelayCertificateMVP(cert RelayCertificateRecord, lock SourceLockRecord, tx TxResult, record RoutingRecord, targetShards string, forceFailure bool) (bool, string) {
	if forceFailure {
		return false, "deterministic relay_force_proof_fail_every_n"
	}
	if cert.CertificateID == "" {
		return false, "certificate_id missing"
	}
	if cert.TxID != lock.TxID || cert.TxID != tx.TxID {
		return false, "tx_id mismatch"
	}
	if cert.SourceShard != lock.SourceShard {
		return false, "source_shard mismatch"
	}
	if cert.TargetShards != targetShards || lock.TargetShards != targetShards {
		return false, "target_shards mismatch"
	}
	if lock.Status != "locked" {
		return false, "source_lock not locked"
	}
	if cert.ProofDigest == "" {
		return false, "proof_digest missing"
	}
	if record.TxID != "" && record.PrimaryShard >= 0 && record.PrimaryShard != cert.SourceShard {
		return false, "routing source mismatch"
	}
	return true, ""
}

func relayFailureMode(experiment ExperimentProfile) string {
	if experiment.RelayFailureMode == "" {
		return "none"
	}
	return experiment.RelayFailureMode
}

func shouldForceProofFail(experiment ExperimentProfile, index int) bool {
	if relayFailureMode(experiment) == "proof_fail" {
		return true
	}
	return experiment.RelayForceProofFailEveryN > 0 && (index+1)%experiment.RelayForceProofFailEveryN == 0
}

func shouldForceTimeout(experiment ExperimentProfile, index int) bool {
	if relayFailureMode(experiment) == "timeout" {
		return true
	}
	return experiment.RelayForceTimeoutEveryN > 0 && (index+1)%experiment.RelayForceTimeoutEveryN == 0
}

func WriteRelayMVPArtifacts(out string, preview RelayMVPPreview) error {
	if err := writeRelayStateMachineCSV(filepath.Join(out, "relay_state_machine_log.csv"), preview.StateMachineRows); err != nil {
		return err
	}
	if err := writeSourceLockCSV(filepath.Join(out, "source_lock_log.csv"), preview.SourceLocks); err != nil {
		return err
	}
	if err := writeRelayCertificateCSV(filepath.Join(out, "relay_certificate_log.csv"), preview.Certificates); err != nil {
		return err
	}
	if err := writeRelayProofVerificationCSV(filepath.Join(out, "relay_proof_verification_log.csv"), preview.ProofVerifications); err != nil {
		return err
	}
	if err := writeTargetVerificationCSV(filepath.Join(out, "target_verification_log.csv"), preview.TargetVerifications); err != nil {
		return err
	}
	if err := writeTargetCommitCSV(filepath.Join(out, "target_commit_log.csv"), preview.TargetCommits); err != nil {
		return err
	}
	if err := writeSourceFinalizeCSV(filepath.Join(out, "source_finalize_log.csv"), preview.SourceFinalizes); err != nil {
		return err
	}
	if err := writeTimeoutRefundCSV(filepath.Join(out, "cross_shard_timeout_refund_log.csv"), preview.TimeoutRefunds); err != nil {
		return err
	}
	if err := writeCrossShardFailureCSV(filepath.Join(out, "cross_shard_failure_log.csv"), preview.Failures); err != nil {
		return err
	}
	return writeRelayMVPSummaryJSON(filepath.Join(out, "relay_mvp_summary.json"), preview)
}

func writeRelayStateMachineCSV(path string, rows []RelayStateMachineRecord) error {
	fields := []string{"tx_id", "block_height", "source_shard", "target_shards", "from_state", "to_state", "event", "event_time_ms", "success", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.Itoa(row.BlockHeight), strconv.Itoa(row.SourceShard), row.TargetShards, row.FromState, row.ToState, row.Event, strconv.Itoa(row.EventTimeMS), strconv.FormatBool(row.Success), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeSourceLockCSV(path string, rows []SourceLockRecord) error {
	fields := []string{"tx_id", "block_height", "source_shard", "target_shards", "source_lock_id", "lock_time_ms", "status", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.Itoa(row.BlockHeight), strconv.Itoa(row.SourceShard), row.TargetShards, row.SourceLockID, strconv.Itoa(row.LockTimeMS), row.Status, row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeRelayCertificateCSV(path string, rows []RelayCertificateRecord) error {
	fields := []string{"tx_id", "block_height", "source_shard", "target_shards", "source_lock_id", "certificate_id", "certificate_hash", "proof_digest", "state_root_digest", "created_at_ms", "status", "truth"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.Itoa(row.BlockHeight), strconv.Itoa(row.SourceShard), row.TargetShards, row.SourceLockID, row.CertificateID, row.CertificateHash, row.ProofDigest, row.StateRootDigest, strconv.Itoa(row.CreatedAtMS), row.Status, row.Truth})
	}
	return writeCSV(path, fields, out)
}

func writeRelayProofVerificationCSV(path string, rows []RelayProofVerificationRecord) error {
	fields := []string{"tx_id", "certificate_id", "verification_time_ms", "proof_digest", "verified", "failure_reason", "truth"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.CertificateID, strconv.Itoa(row.VerificationTimeMS), row.ProofDigest, strconv.FormatBool(row.Verified), row.FailureReason, row.Truth})
	}
	return writeCSV(path, fields, out)
}

func writeTargetVerificationCSV(path string, rows []TargetVerificationRecord) error {
	fields := []string{"tx_id", "certificate_id", "target_shard", "verification_time_ms", "accepted", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.CertificateID, strconv.Itoa(row.TargetShard), strconv.Itoa(row.VerificationTimeMS), strconv.FormatBool(row.Accepted), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeTargetCommitCSV(path string, rows []TargetCommitRecord) error {
	fields := []string{"tx_id", "certificate_id", "target_shard", "commit_time_ms", "committed", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.CertificateID, strconv.Itoa(row.TargetShard), strconv.Itoa(row.CommitTimeMS), strconv.FormatBool(row.Committed), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeSourceFinalizeCSV(path string, rows []SourceFinalizeRecord) error {
	fields := []string{"tx_id", "source_lock_id", "certificate_id", "finalize_time_ms", "finalized", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.SourceLockID, row.CertificateID, strconv.Itoa(row.FinalizeTimeMS), strconv.FormatBool(row.Finalized), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeTimeoutRefundCSV(path string, rows []CrossShardTimeoutRefundRecord) error {
	fields := []string{"tx_id", "source_lock_id", "event_time_ms", "event_type", "refunded", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.SourceLockID, strconv.Itoa(row.EventTimeMS), row.EventType, strconv.FormatBool(row.Refunded), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeCrossShardFailureCSV(path string, rows []CrossShardFailureRecord) error {
	fields := []string{"tx_id", "source_lock_id", "certificate_id", "failure_time_ms", "failure_type", "aborted", "refunded", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.SourceLockID, row.CertificateID, strconv.Itoa(row.FailureTimeMS), row.FailureType, strconv.FormatBool(row.Aborted), strconv.FormatBool(row.Refunded), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeRelayMVPSummaryJSON(path string, preview RelayMVPPreview) error {
	payload := map[string]any{
		"stage":                          "V3.11",
		"closure_stage":                  "V3.11 CrossShard Protocol Closure",
		"runtime_truth":                  RelayMVPRuntimeTruth,
		"relay_mvp_enabled":              preview.Enabled,
		"relay_mvp_tx_count":             preview.TxCount,
		"relay_source_lock_count":        preview.SourceLockCount,
		"relay_certificate_count":        preview.CertificateCount,
		"relay_proof_verified_count":     preview.ProofVerifiedCount,
		"relay_proof_failed_count":       preview.ProofFailedCount,
		"relay_target_verified_count":    preview.TargetVerifiedCount,
		"relay_target_commit_count":      preview.TargetCommitCount,
		"relay_source_finalized_count":   preview.SourceFinalizedCount,
		"relay_timeout_count":            preview.TimeoutCount,
		"relay_refund_count":             preview.RefundCount,
		"relay_abort_count":              preview.AbortCount,
		"relay_success_count":            preview.SuccessCount,
		"relay_failed_count":             preview.FailedCount,
		"relay_avg_latency_ms":           preview.AvgLatencyMS,
		"not_production_atomic_commit":   true,
		"not_complete_broker_or_2pc":     true,
		"not_byzantine_secure_relay":     true,
		"not_blockemulator_backend":      true,
		"not_fabric_or_evm_live_backend": true,
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}
