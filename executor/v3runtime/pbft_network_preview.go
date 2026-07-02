package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	PBFTNetworkPathInMemory = "in_memory"
	PBFTNetworkPathTCP      = "localhost_tcp_preview"
)

type PBFTNetworkRecord struct {
	EventID          string
	TimeMS           int
	ConsensusRuntime string
	NetworkAdapter   string
	NetworkPath      string
	MessageID        string
	MessageType      string
	FromNodeID       string
	ToNodeID         string
	ShardID          int
	View             int
	SequenceID       int
	BlockHeight      int
	QuorumThreshold  int
	QuorumReached    bool
	Status           string
	Error            string
	Details          string
}

type PBFTNetworkPreview struct {
	Enabled                   bool
	ConsensusRuntimeSelected  string
	NetworkAdapterSelected    string
	NetworkPath               string
	MessageCount              int
	ErrorCount                int
	PrePrepareNetworkCount    int
	PrepareNetworkCount       int
	CommitNetworkCount        int
	FinalizedNetworkCount     int
	NetworkQuorumReachedCount int
	Records                   []PBFTNetworkRecord
	TypedMessages             []MessageEnvelope
	SendRows                  []NetworkSendRecord
	ReceiveRows               []NetworkReceiveRecord
	SkippedReason             string
}

func RunPBFTNetworkPreview(nodeRuntime NodeRuntimeArtifacts, launcher LauncherPreview, networkAdapter NetworkAdapterPreview, pbft PBFTPreview) PBFTNetworkPreview {
	selected := networkAdapter.SelectedAdapter
	if selected == "" {
		selected = selectedNetworkAdapter(launcher)
	}
	path := PBFTNetworkPathInMemory
	if selected == NetworkAdapterLocalhostTCPPreview {
		path = PBFTNetworkPathTCP
	}
	preview := PBFTNetworkPreview{
		Enabled:                   pbft.Enabled,
		ConsensusRuntimeSelected:  pbft.ConsensusRuntimeSelected,
		NetworkAdapterSelected:    selected,
		NetworkPath:               path,
		NetworkQuorumReachedCount: pbft.QuorumReachedCount,
		ErrorCount:                pbft.ErrorCount,
	}
	if !pbft.Enabled {
		preview.SkippedReason = firstNonEmpty(pbft.SkippedReason, "PBFT preview is not enabled for selected consensus runtime")
		preview.Records = append(preview.Records, PBFTNetworkRecord{
			EventID:          "pbft_net_000001",
			ConsensusRuntime: pbft.ConsensusRuntimeSelected,
			NetworkAdapter:   selected,
			NetworkPath:      path,
			Status:           "skipped",
			Details:          preview.SkippedReason,
		})
		return preview
	}

	nodeRoles := map[string]string{}
	for _, node := range nodeRuntime.Nodes {
		nodeRoles[node.NodeID] = node.Role
	}
	quorumReached := map[string]bool{}
	for _, row := range pbft.QuorumRows {
		key := fmt.Sprintf("%d:%d:%s", row.BlockHeight, row.SequenceID, row.QuorumType)
		quorumReached[key] = row.QuorumReached
	}
	for index, row := range pbft.MessageRows {
		payload := fmt.Sprintf("%s:block=%d:seq=%d:view=%d:path=%s", row.MessageType, row.BlockHeight, row.SequenceID, row.View, path)
		role := firstNonEmpty(nodeRoles[row.FromNodeID], "validator")
		msg := NewMessageEnvelope(row.MessageID, row.MessageType, row.FromNodeID, row.ToNodeID, row.ShardID, role, row.BlockHeight, row.SequenceID, payload)
		preview.TypedMessages = append(preview.TypedMessages, msg)
		preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: pbftNetworkSendStatus(path)})
		preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: pbftNetworkReceiveStatus(path)})
		reached := row.MessageType == "pbft_finalized"
		if row.MessageType == "pbft_prepare" {
			reached = quorumReached[fmt.Sprintf("%d:%d:prepare", row.BlockHeight, row.SequenceID)]
		}
		if row.MessageType == "pbft_commit" {
			reached = quorumReached[fmt.Sprintf("%d:%d:commit", row.BlockHeight, row.SequenceID)]
		}
		preview.Records = append(preview.Records, PBFTNetworkRecord{
			EventID:          fmt.Sprintf("pbft_net_%06d", index+1),
			TimeMS:           row.TimeMS,
			ConsensusRuntime: pbft.ConsensusRuntimeSelected,
			NetworkAdapter:   selected,
			NetworkPath:      path,
			MessageID:        row.MessageID,
			MessageType:      row.MessageType,
			FromNodeID:       row.FromNodeID,
			ToNodeID:         row.ToNodeID,
			ShardID:          row.ShardID,
			View:             row.View,
			SequenceID:       row.SequenceID,
			BlockHeight:      row.BlockHeight,
			QuorumThreshold:  pbft.QuorumThreshold,
			QuorumReached:    reached,
			Status:           "delivered_preview",
			Details:          "PBFT preview typed message over selected NetworkAdapter; preview only, not production PBFT",
		})
		preview.MessageCount++
		switch row.MessageType {
		case "pbft_preprepare":
			preview.PrePrepareNetworkCount++
		case "pbft_prepare":
			preview.PrepareNetworkCount++
		case "pbft_commit":
			preview.CommitNetworkCount++
		case "pbft_finalized":
			preview.FinalizedNetworkCount++
		}
	}
	return preview
}

func pbftNetworkSendStatus(path string) string {
	if path == PBFTNetworkPathTCP {
		return "pbft_tcp_preview_sent"
	}
	return "pbft_in_memory_sent_preview"
}

func pbftNetworkReceiveStatus(path string) string {
	if path == PBFTNetworkPathTCP {
		return "pbft_tcp_preview_received"
	}
	return "pbft_in_memory_received_preview"
}

func (preview *NetworkAdapterPreview) AppendPBFTNetwork(pbft PBFTNetworkPreview) {
	preview.TypedMessages = append(preview.TypedMessages, pbft.TypedMessages...)
	preview.SendRows = append(preview.SendRows, pbft.SendRows...)
	preview.ReceiveRows = append(preview.ReceiveRows, pbft.ReceiveRows...)
	preview.ErrorCount += pbft.ErrorCount
}

func WritePBFTNetworkArtifacts(out string, preview PBFTNetworkPreview) error {
	if err := writePBFTNetworkLogCSV(filepath.Join(out, "consensus_network_log.csv"), preview.Records); err != nil {
		return err
	}
	return writePBFTNetworkSummaryJSON(filepath.Join(out, "pbft_network_summary.json"), preview)
}

func writePBFTNetworkLogCSV(path string, rows []PBFTNetworkRecord) error {
	fields := []string{"event_id", "time_ms", "consensus_runtime", "network_adapter", "consensus_network_path", "message_id", "message_type", "from_node_id", "to_node_id", "shard_id", "view", "sequence_id", "block_height", "quorum_threshold", "quorum_reached", "status", "error", "details"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{
			row.EventID,
			strconv.Itoa(row.TimeMS),
			row.ConsensusRuntime,
			row.NetworkAdapter,
			row.NetworkPath,
			row.MessageID,
			row.MessageType,
			row.FromNodeID,
			row.ToNodeID,
			strconv.Itoa(row.ShardID),
			strconv.Itoa(row.View),
			strconv.Itoa(row.SequenceID),
			strconv.Itoa(row.BlockHeight),
			strconv.Itoa(row.QuorumThreshold),
			strconv.FormatBool(row.QuorumReached),
			row.Status,
			row.Error,
			row.Details,
		})
	}
	return writeCSV(path, fields, out)
}

func writePBFTNetworkSummaryJSON(path string, preview PBFTNetworkPreview) error {
	payload := map[string]any{
		"stage":                                "V3.7.2",
		"closure_stage":                        "V3.7.2 V3.7 Closure",
		"runtime_truth":                        "blockemulator_aligned_pbft_preview_over_network_not_production_pbft",
		"consensus_runtime_selected":           preview.ConsensusRuntimeSelected,
		"network_adapter_selected":             preview.NetworkAdapterSelected,
		"pbft_over_network_enabled":            preview.Enabled,
		"pbft_network_path":                    preview.NetworkPath,
		"pbft_network_message_count":           preview.MessageCount,
		"pbft_network_error_count":             preview.ErrorCount,
		"pbft_preprepare_network_count":        preview.PrePrepareNetworkCount,
		"pbft_prepare_network_count":           preview.PrepareNetworkCount,
		"pbft_commit_network_count":            preview.CommitNetworkCount,
		"pbft_finalized_network_count":         preview.FinalizedNetworkCount,
		"pbft_network_quorum_reached_count":    preview.NetworkQuorumReachedCount,
		"skipped_reason":                       preview.SkippedReason,
		"not_production_pbft":                  true,
		"not_full_byzantine_safety":            true,
		"not_view_change_hardening":            true,
		"not_checkpoint_or_stable_checkpoint":  true,
		"not_signature_verification_hardening": true,
		"not_blockemulator_backend":            true,
		"not_real_cross_shard_protocol":        true,
		"not_fabric_or_evm_live_backend":       true,
		"not_paper_grade_benchmark_output":     true,
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}

func (preview PBFTNetworkPreview) SummaryLine() string {
	return fmt.Sprintf("pbft_over_network_enabled=%t\npbft_network_path=%s\npbft_network_message_count=%d\npbft_network_error_count=%d\npbft_preprepare_network_count=%d\npbft_prepare_network_count=%d\npbft_commit_network_count=%d\npbft_finalized_network_count=%d\npbft_network_quorum_reached_count=%d\n", preview.Enabled, preview.NetworkPath, preview.MessageCount, preview.ErrorCount, preview.PrePrepareNetworkCount, preview.PrepareNetworkCount, preview.CommitNetworkCount, preview.FinalizedNetworkCount, preview.NetworkQuorumReachedCount)
}
