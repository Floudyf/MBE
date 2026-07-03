package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	CrossShardProtocolNone                  = "none"
	CrossShardProtocolRelayPreview          = "relay_preview"
	CrossShardProtocolRelayMVP              = "relay_mvp"
	CrossShardProtocolBrokerPreview         = "broker_preview"
	CrossShardProtocolTwoPhaseCommitPreview = "two_phase_commit_preview"
	CrossShardRuntimeTruth                  = "cross_shard_protocol_skeleton_not_atomic_cross_shard_commit"
	RelayMVPRuntimeTruth                    = "relay_mvp_not_production_atomic_commit"
)

type CrossShardTxRecord struct {
	TxID              string
	IsCrossShard      bool
	SourceShard       int
	TargetShard       int
	InvolvedStateKeys string
	Protocol          string
	DetectionRule     string
	Status            string
	LatencyMS         int
	SkippedReason     string
	ErrorMessage      string
}

type CrossShardMessageRecord struct {
	MessageID      string
	MessageType    string
	FromShard      int
	ToShard        int
	TxID           string
	Protocol       string
	NetworkAdapter string
	TimestampMS    int
	Status         string
	ErrorMessage   string
}

type RelayPreviewRecord struct {
	TxID             string
	SourceShard      int
	TargetShard      int
	RelayMessageID   string
	RelayEmitted     bool
	TargetReceived   bool
	PreviewCompleted bool
	SkippedReason    string
}

type CrossShardStatusRecord struct {
	TxID        string
	Protocol    string
	State       string
	SourceShard int
	TargetShard int
	Completed   bool
	Failed      bool
	Reason      string
}

type CrossShardProtocolPreview struct {
	ProtocolSelected       string
	NetworkAdapterSelected string
	TxRows                 []CrossShardTxRecord
	MessageRows            []CrossShardMessageRecord
	RelayRows              []RelayPreviewRecord
	StatusRows             []CrossShardStatusRecord
	TypedMessages          []MessageEnvelope
	SendRows               []NetworkSendRecord
	ReceiveRows            []NetworkReceiveRecord
	TxCount                int
	MessageCount           int
	RelayPreviewCount      int
	CompletedCount         int
	FailedCount            int
	AvgLatencyMS           float64
	SkippedCount           int
	DetectionPreviewCount  int
	RelayPreviewLatencyMS  int
	TargetReceiveCount     int
	RelayMVP               RelayMVPPreview
}

func RunCrossShardProtocolPreview(experiment ExperimentProfile, networkAdapter NetworkAdapterPreview, txResults []TxResult, routingLog []RoutingRecord) CrossShardProtocolPreview {
	protocol := firstNonEmpty(experiment.CrossShardProtocol, CrossShardProtocolNone)
	preview := CrossShardProtocolPreview{
		ProtocolSelected:       protocol,
		NetworkAdapterSelected: firstNonEmpty(networkAdapter.SelectedAdapter, NetworkAdapterInMemory),
	}
	routingByTx := map[string]RoutingRecord{}
	for _, record := range routingLog {
		if record.TxID != "" {
			routingByTx[record.TxID] = record
		}
	}
	totalLatency := 0
	for index, tx := range txResults {
		record := routingByTx[tx.TxID]
		sourceShard, targetShard := crossShardSourceTarget(tx, record)
		isCrossShard := record.CrossShard || tx.CrossStateUnitAccess
		status := "local_tx"
		skipped := ""
		latencyMS := 0
		if isCrossShard {
			status = "detected_no_protocol"
			preview.TxCount++
			preview.DetectionPreviewCount++
			if protocol == CrossShardProtocolRelayPreview {
				status = "preview_completed"
				latencyMS = 1
				totalLatency += latencyMS
				preview.RelayPreviewCount++
				preview.CompletedCount++
				preview.TargetReceiveCount++
				messageID := fmt.Sprintf("cross_shard_relay_%06d", preview.RelayPreviewCount)
				preview.MessageRows = append(preview.MessageRows, CrossShardMessageRecord{
					MessageID:      messageID,
					MessageType:    "cross_shard_relay",
					FromShard:      sourceShard,
					ToShard:        targetShard,
					TxID:           tx.TxID,
					Protocol:       protocol,
					NetworkAdapter: preview.NetworkAdapterSelected,
					TimestampMS:    tx.CommitTimeMS + latencyMS,
					Status:         "relay_preview_delivered",
				})
				preview.RelayRows = append(preview.RelayRows, RelayPreviewRecord{
					TxID:             tx.TxID,
					SourceShard:      sourceShard,
					TargetShard:      targetShard,
					RelayMessageID:   messageID,
					RelayEmitted:     true,
					TargetReceived:   true,
					PreviewCompleted: true,
				})
				preview.StatusRows = append(preview.StatusRows, CrossShardStatusRecord{
					TxID:        tx.TxID,
					Protocol:    protocol,
					State:       "preview_completed",
					SourceShard: sourceShard,
					TargetShard: targetShard,
					Completed:   true,
					Reason:      "relay_preview skeleton completed without atomic commit",
				})
				msg := NewMessageEnvelope(messageID, "cross_shard_relay", fmt.Sprintf("shard_%d", sourceShard), fmt.Sprintf("shard_%d", targetShard), sourceShard, "relay_preview", tx.BlockHeight, index+1, fmt.Sprintf("tx=%s:source=%d:target=%d:protocol=relay_preview", tx.TxID, sourceShard, targetShard))
				preview.TypedMessages = append(preview.TypedMessages, msg)
				preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: crossShardSendStatus(preview.NetworkAdapterSelected)})
				preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: crossShardReceiveStatus(preview.NetworkAdapterSelected)})
			} else if protocol == CrossShardProtocolRelayMVP {
				outcome := BuildRelayMVPForTx(experiment, tx, record, index, sourceShard, targetShard, preview.NetworkAdapterSelected)
				status = outcome.TxStatus
				latencyMS = outcome.LatencyMS
				if outcome.Completed {
					preview.CompletedCount++
					totalLatency += latencyMS
				}
				if outcome.Failed {
					preview.FailedCount++
				}
				preview.TargetReceiveCount += outcome.TargetReceiveCount
				preview.RelayMVP.Append(outcome)
				preview.MessageRows = append(preview.MessageRows, outcome.MessageRows...)
				preview.StatusRows = append(preview.StatusRows, outcome.StatusRows...)
				preview.TypedMessages = append(preview.TypedMessages, outcome.TypedMessages...)
				preview.SendRows = append(preview.SendRows, outcome.SendRows...)
				preview.ReceiveRows = append(preview.ReceiveRows, outcome.ReceiveRows...)
			} else if protocol == CrossShardProtocolNone {
				skipped = "cross-shard tx detected but cross_shard_protocol=none"
				preview.SkippedCount++
				preview.StatusRows = append(preview.StatusRows, CrossShardStatusRecord{
					TxID:        tx.TxID,
					Protocol:    protocol,
					State:       "detected_no_protocol",
					SourceShard: sourceShard,
					TargetShard: targetShard,
					Reason:      skipped,
				})
			} else {
				status = "planned_only_skipped"
				skipped = protocol + " is planned only and not runnable in V3.8"
				preview.SkippedCount++
				preview.StatusRows = append(preview.StatusRows, CrossShardStatusRecord{
					TxID:        tx.TxID,
					Protocol:    protocol,
					State:       "planned_only_skipped",
					SourceShard: sourceShard,
					TargetShard: targetShard,
					Reason:      skipped,
				})
			}
		}
		preview.TxRows = append(preview.TxRows, CrossShardTxRecord{
			TxID:              tx.TxID,
			IsCrossShard:      isCrossShard,
			SourceShard:       sourceShard,
			TargetShard:       targetShard,
			InvolvedStateKeys: crossShardInvolvedStateKeys(tx),
			Protocol:          protocol,
			DetectionRule:     crossShardDetectionRule(record),
			Status:            status,
			LatencyMS:         latencyMS,
			SkippedReason:     skipped,
		})
	}
	preview.MessageCount = len(preview.MessageRows)
	if preview.CompletedCount > 0 {
		preview.AvgLatencyMS = round(float64(totalLatency) / float64(preview.CompletedCount))
		preview.RelayPreviewLatencyMS = int(preview.AvgLatencyMS)
	}
	preview.RelayMVP.Finalize(preview.AvgLatencyMS)
	return preview
}

func crossShardSourceTarget(tx TxResult, record RoutingRecord) (int, int) {
	source := record.PrimaryShard
	if source < 0 {
		source = tx.ShardID
	}
	if source < 0 {
		source = 0
	}
	target := source
	for _, shard := range record.TouchedShards {
		if shard != source {
			target = shard
			break
		}
	}
	if target == source {
		for _, unit := range tx.AccessedStateUnitIDs {
			if unit != source {
				target = unit
				break
			}
		}
	}
	return source, target
}

func crossShardInvolvedStateKeys(tx TxResult) string {
	if len(tx.AccessedStateUnitIDs) == 0 {
		return "preview_state_units_unavailable"
	}
	return "preview_state_units=" + joinInts(tx.AccessedStateUnitIDs)
}

func crossShardDetectionRule(record RoutingRecord) string {
	if record.TxID == "" {
		return "tx_result_state_unit_preview_rule"
	}
	return "routing_log_touched_shards_preview_rule"
}

func crossShardSendStatus(adapter string) string {
	if adapter == NetworkAdapterLocalhostTCPPreview {
		return "cross_shard_tcp_preview_sent"
	}
	return "cross_shard_in_memory_sent_preview"
}

func crossShardReceiveStatus(adapter string) string {
	if adapter == NetworkAdapterLocalhostTCPPreview {
		return "cross_shard_tcp_preview_received"
	}
	return "cross_shard_in_memory_received_preview"
}

func (preview *NetworkAdapterPreview) AppendCrossShardProtocol(crossShard CrossShardProtocolPreview) {
	preview.TypedMessages = append(preview.TypedMessages, crossShard.TypedMessages...)
	preview.SendRows = append(preview.SendRows, crossShard.SendRows...)
	preview.ReceiveRows = append(preview.ReceiveRows, crossShard.ReceiveRows...)
}

func WriteCrossShardProtocolArtifacts(out string, preview CrossShardProtocolPreview) error {
	if err := writeCrossShardTxLogCSV(filepath.Join(out, "cross_shard_tx_log.csv"), preview.TxRows); err != nil {
		return err
	}
	if err := writeCrossShardMessageLogCSV(filepath.Join(out, "cross_shard_message_log.csv"), preview.MessageRows); err != nil {
		return err
	}
	if err := writeRelayPreviewLogCSV(filepath.Join(out, "relay_preview_log.csv"), preview.RelayRows); err != nil {
		return err
	}
	if err := writeCrossShardStatusCSV(filepath.Join(out, "cross_shard_status.csv"), preview.StatusRows); err != nil {
		return err
	}
	if err := WriteRelayMVPArtifacts(out, preview.RelayMVP); err != nil {
		return err
	}
	return writeCrossShardSummaryJSON(filepath.Join(out, "cross_shard_summary.json"), preview)
}

func writeCrossShardTxLogCSV(path string, rows []CrossShardTxRecord) error {
	fields := []string{"tx_id", "is_cross_shard", "source_shard", "target_shard", "involved_state_keys", "protocol", "detection_rule", "status", "latency_ms", "skipped_reason", "error_message"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.FormatBool(row.IsCrossShard), strconv.Itoa(row.SourceShard), strconv.Itoa(row.TargetShard), row.InvolvedStateKeys, row.Protocol, row.DetectionRule, row.Status, strconv.Itoa(row.LatencyMS), row.SkippedReason, row.ErrorMessage})
	}
	return writeCSV(path, fields, out)
}

func writeCrossShardMessageLogCSV(path string, rows []CrossShardMessageRecord) error {
	fields := []string{"message_id", "message_type", "from_shard", "to_shard", "tx_id", "protocol", "network_adapter", "timestamp_ms", "status", "error_message"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.MessageID, row.MessageType, strconv.Itoa(row.FromShard), strconv.Itoa(row.ToShard), row.TxID, row.Protocol, row.NetworkAdapter, strconv.Itoa(row.TimestampMS), row.Status, row.ErrorMessage})
	}
	return writeCSV(path, fields, out)
}

func writeRelayPreviewLogCSV(path string, rows []RelayPreviewRecord) error {
	fields := []string{"tx_id", "source_shard", "target_shard", "relay_message_id", "relay_emitted", "target_received", "preview_completed", "skipped_reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, strconv.Itoa(row.SourceShard), strconv.Itoa(row.TargetShard), row.RelayMessageID, strconv.FormatBool(row.RelayEmitted), strconv.FormatBool(row.TargetReceived), strconv.FormatBool(row.PreviewCompleted), row.SkippedReason})
	}
	return writeCSV(path, fields, out)
}

func writeCrossShardStatusCSV(path string, rows []CrossShardStatusRecord) error {
	fields := []string{"tx_id", "protocol", "state", "source_shard", "target_shard", "completed", "failed", "reason"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.TxID, row.Protocol, row.State, strconv.Itoa(row.SourceShard), strconv.Itoa(row.TargetShard), strconv.FormatBool(row.Completed), strconv.FormatBool(row.Failed), row.Reason})
	}
	return writeCSV(path, fields, out)
}

func writeCrossShardSummaryJSON(path string, preview CrossShardProtocolPreview) error {
	ratio := 0.0
	if len(preview.TxRows) > 0 {
		ratio = round(float64(preview.TxCount) / float64(len(preview.TxRows)))
	}
	payload := map[string]any{
		"stage":                               "V3.8",
		"closure_stage":                       "V3.8 CrossShardProtocol Skeleton Closure",
		"runtime_truth":                       CrossShardRuntimeTruth,
		"cross_shard_protocol_selected":       preview.ProtocolSelected,
		"network_adapter_selected":            preview.NetworkAdapterSelected,
		"cross_shard_tx_count":                preview.TxCount,
		"cross_shard_ratio":                   ratio,
		"cross_shard_message_count":           preview.MessageCount,
		"relay_preview_count":                 preview.RelayPreviewCount,
		"cross_shard_completed_count":         preview.CompletedCount,
		"cross_shard_failed_count":            preview.FailedCount,
		"cross_shard_avg_latency_ms":          preview.AvgLatencyMS,
		"cross_shard_skipped_count":           preview.SkippedCount,
		"cross_shard_detection_preview_count": preview.DetectionPreviewCount,
		"relay_preview_latency_ms":            preview.RelayPreviewLatencyMS,
		"cross_shard_target_receive_count":    preview.TargetReceiveCount,
		"relay_mvp_enabled":                   preview.RelayMVP.Enabled,
		"relay_mvp_tx_count":                  preview.RelayMVP.TxCount,
		"relay_source_lock_count":             preview.RelayMVP.SourceLockCount,
		"relay_certificate_count":             preview.RelayMVP.CertificateCount,
		"relay_proof_verified_count":          preview.RelayMVP.ProofVerifiedCount,
		"relay_proof_failed_count":            preview.RelayMVP.ProofFailedCount,
		"relay_target_verified_count":         preview.RelayMVP.TargetVerifiedCount,
		"relay_target_commit_count":           preview.RelayMVP.TargetCommitCount,
		"relay_source_finalized_count":        preview.RelayMVP.SourceFinalizedCount,
		"relay_timeout_count":                 preview.RelayMVP.TimeoutCount,
		"relay_refund_count":                  preview.RelayMVP.RefundCount,
		"relay_abort_count":                   preview.RelayMVP.AbortCount,
		"relay_success_count":                 preview.RelayMVP.SuccessCount,
		"relay_failed_count":                  preview.RelayMVP.FailedCount,
		"relay_avg_latency_ms":                preview.RelayMVP.AvgLatencyMS,
		"relay_mvp_truth":                     RelayMVPRuntimeTruth,
		"not_complete_relay":                  true,
		"not_complete_broker":                 true,
		"not_complete_2pc":                    true,
		"not_atomic_cross_shard_commit":       true,
		"not_cross_shard_state_proof":         true,
		"not_blockemulator_cross_shard":       true,
		"not_fabric_or_evm_live_backend":      true,
		"not_paper_grade_benchmark_output":    true,
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}

func (preview CrossShardProtocolPreview) SummaryLine() string {
	return fmt.Sprintf("cross_shard_protocol_selected=%s\ncross_shard_tx_count=%d\ncross_shard_message_count=%d\nrelay_preview_count=%d\ncross_shard_completed_count=%d\ncross_shard_failed_count=%d\ncross_shard_avg_latency_ms=%g\nrelay_mvp_enabled=%t\nrelay_mvp_tx_count=%d\nrelay_success_count=%d\nrelay_failed_count=%d\nrelay_mvp_truth=%s\n", preview.ProtocolSelected, preview.TxCount, preview.MessageCount, preview.RelayPreviewCount, preview.CompletedCount, preview.FailedCount, preview.AvgLatencyMS, preview.RelayMVP.Enabled, preview.RelayMVP.TxCount, preview.RelayMVP.SuccessCount, preview.RelayMVP.FailedCount, RelayMVPRuntimeTruth)
}
