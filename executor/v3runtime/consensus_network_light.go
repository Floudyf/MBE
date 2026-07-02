package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	ConsensusNetworkPathInMemory = "in_memory_typed_message_bus"
	ConsensusNetworkPathTCP      = "localhost_tcp_typed_message_preview"
)

type ConsensusNetworkLightRecord struct {
	EventID            string
	TimeMS             int
	NetworkAdapter     string
	NetworkPath        string
	ConsensusRuntime   string
	ConsensusDomainID  string
	ShardID            int
	LeaderNodeID       string
	ValidatorNodeID    string
	MessageType        string
	BlockHeight        int
	SequenceID         int
	QuorumTarget       int
	VoteCount          int
	LightQuorumReached bool
	Status             string
	Details            string
}

type ConsensusNetworkLightPreview struct {
	Enabled                  bool
	ConsensusRuntimeSelected string
	NetworkAdapterSelected   string
	NetworkPath              string
	ProposalPreviewCount     int
	VotePreviewCount         int
	LightQuorumReachedCount  int
	ErrorCount               int
	Records                  []ConsensusNetworkLightRecord
	TypedMessages            []MessageEnvelope
	SendRows                 []NetworkSendRecord
	ReceiveRows              []NetworkReceiveRecord
}

func RunConsensusNetworkLightPreview(nodeRuntime NodeRuntimeArtifacts, launcher LauncherPreview, networkAdapter NetworkAdapterPreview, consensusLog []ConsensusRecord, consensusRuntime string) ConsensusNetworkLightPreview {
	selected := networkAdapter.SelectedAdapter
	if selected == "" {
		selected = selectedNetworkAdapter(launcher)
	}
	path := ConsensusNetworkPathInMemory
	if selected == NetworkAdapterLocalhostTCPPreview {
		path = ConsensusNetworkPathTCP
	}
	preview := ConsensusNetworkLightPreview{
		Enabled:                  true,
		ConsensusRuntimeSelected: consensusRuntime,
		NetworkAdapterSelected:   selected,
		NetworkPath:              path,
	}
	validatorsByShard := validatorsByShard(nodeRuntime.Nodes)
	records := consensusLog
	if len(records) == 0 {
		records = []ConsensusRecord{{BlockHeight: 1, SequenceID: 1, ViewID: 0, PluginID: consensusRuntime, Finalized: true}}
	}
	eventID := 1
	for _, record := range records {
		shardID := 0
		if nodeRuntime.Config.ShardCount > 0 {
			shardID = (record.BlockHeight - 1) % nodeRuntime.Config.ShardCount
		}
		validators := validatorsByShard[shardID]
		if len(validators) == 0 {
			preview.ErrorCount++
			preview.Records = append(preview.Records, ConsensusNetworkLightRecord{
				EventID:           fmt.Sprintf("consnet_%06d", eventID),
				TimeMS:            record.ConsensusStartTimeMS,
				NetworkAdapter:    selected,
				NetworkPath:       path,
				ConsensusRuntime:  consensusRuntime,
				ConsensusDomainID: consensusDomainID(shardID),
				ShardID:           shardID,
				MessageType:       "proposal_preview",
				BlockHeight:       record.BlockHeight,
				SequenceID:        record.SequenceID,
				Status:            "skipped",
				Details:           "no validators available for consensus-light preview",
			})
			eventID++
			continue
		}
		leader := validators[0]
		quorumTarget := lightQuorumTarget(len(validators))
		voteCount := 0
		for i, validator := range validators {
			proposalPayload := fmt.Sprintf("proposal_preview:block=%d:seq=%d:runtime=%s:path=%s", record.BlockHeight, record.SequenceID, consensusRuntime, path)
			proposal := NewMessageEnvelope(fmt.Sprintf("proposal-preview-%06d", preview.ProposalPreviewCount+1), "proposal_preview", leader.NodeID, validator.NodeID, shardID, leader.Role, record.BlockHeight, record.SequenceID, proposalPayload)
			preview.addTypedMessage(proposal, "proposal_preview_sent", "proposal_preview_received")
			preview.Records = append(preview.Records, ConsensusNetworkLightRecord{
				EventID:           fmt.Sprintf("consnet_%06d", eventID),
				TimeMS:            record.ConsensusStartTimeMS + i,
				NetworkAdapter:    selected,
				NetworkPath:       path,
				ConsensusRuntime:  consensusRuntime,
				ConsensusDomainID: consensusDomainID(shardID),
				ShardID:           shardID,
				LeaderNodeID:      leader.NodeID,
				ValidatorNodeID:   validator.NodeID,
				MessageType:       "proposal_preview",
				BlockHeight:       record.BlockHeight,
				SequenceID:        record.SequenceID,
				QuorumTarget:      quorumTarget,
				Status:            "delivered_preview",
				Details:           "consensus-light proposal over selected NetworkAdapter",
			})
			eventID++
			preview.ProposalPreviewCount++

			votePayload := fmt.Sprintf("vote_preview:block=%d:seq=%d:validator=%s", record.BlockHeight, record.SequenceID, validator.NodeID)
			vote := NewMessageEnvelope(fmt.Sprintf("vote-preview-%06d", preview.VotePreviewCount+1), "vote_preview", validator.NodeID, leader.NodeID, shardID, validator.Role, record.BlockHeight, record.SequenceID+i+1, votePayload)
			preview.addTypedMessage(vote, "vote_preview_sent", "vote_preview_received")
			voteCount++
			reached := voteCount >= quorumTarget
			if reached && voteCount == quorumTarget {
				preview.LightQuorumReachedCount++
			}
			preview.Records = append(preview.Records, ConsensusNetworkLightRecord{
				EventID:            fmt.Sprintf("consnet_%06d", eventID),
				TimeMS:             record.ConsensusOrderedTimeMS + i,
				NetworkAdapter:     selected,
				NetworkPath:        path,
				ConsensusRuntime:   consensusRuntime,
				ConsensusDomainID:  consensusDomainID(shardID),
				ShardID:            shardID,
				LeaderNodeID:       leader.NodeID,
				ValidatorNodeID:    validator.NodeID,
				MessageType:        "vote_preview",
				BlockHeight:        record.BlockHeight,
				SequenceID:         record.SequenceID,
				QuorumTarget:       quorumTarget,
				VoteCount:          voteCount,
				LightQuorumReached: reached,
				Status:             "delivered_preview",
				Details:            "consensus-light vote over selected NetworkAdapter; not PBFT prepare or commit",
			})
			eventID++
			preview.VotePreviewCount++
		}
	}
	return preview
}

func (preview *ConsensusNetworkLightPreview) addTypedMessage(msg MessageEnvelope, sendStatus, receiveStatus string) {
	preview.TypedMessages = append(preview.TypedMessages, msg)
	preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: sendStatus})
	preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: receiveStatus})
}

func (preview *NetworkAdapterPreview) AppendConsensusNetworkLight(consensus ConsensusNetworkLightPreview) {
	preview.TypedMessages = append(preview.TypedMessages, consensus.TypedMessages...)
	preview.SendRows = append(preview.SendRows, consensus.SendRows...)
	preview.ReceiveRows = append(preview.ReceiveRows, consensus.ReceiveRows...)
	preview.ErrorCount += consensus.ErrorCount
}

func WriteConsensusNetworkLightArtifacts(out string, preview ConsensusNetworkLightPreview) error {
	if err := writeConsensusNetworkLightLogCSV(filepath.Join(out, "consensus_network_light_log.csv"), preview.Records); err != nil {
		return err
	}
	return writeNetworkConsensusSummaryJSON(filepath.Join(out, "network_consensus_summary.json"), preview)
}

func writeConsensusNetworkLightLogCSV(path string, rows []ConsensusNetworkLightRecord) error {
	fields := []string{"event_id", "time_ms", "network_adapter", "consensus_network_path", "consensus_runtime", "consensus_domain_id", "shard_id", "leader_node_id", "validator_node_id", "message_type", "block_height", "sequence_id", "quorum_target", "vote_count", "light_quorum_reached", "status", "details"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{
			row.EventID,
			strconv.Itoa(row.TimeMS),
			row.NetworkAdapter,
			row.NetworkPath,
			row.ConsensusRuntime,
			row.ConsensusDomainID,
			strconv.Itoa(row.ShardID),
			row.LeaderNodeID,
			row.ValidatorNodeID,
			row.MessageType,
			strconv.Itoa(row.BlockHeight),
			strconv.Itoa(row.SequenceID),
			strconv.Itoa(row.QuorumTarget),
			strconv.Itoa(row.VoteCount),
			strconv.FormatBool(row.LightQuorumReached),
			row.Status,
			row.Details,
		})
	}
	return writeCSV(path, fields, out)
}

func writeNetworkConsensusSummaryJSON(path string, preview ConsensusNetworkLightPreview) error {
	payload := map[string]any{
		"stage":                            "V3.6.2",
		"closure_stage":                    "V3.6.2 V3.6 Closure",
		"runtime_truth":                    "network_adapter_consensus_light_preview_not_real_pbft",
		"consensus_over_network_enabled":   preview.Enabled,
		"consensus_runtime_selected":       preview.ConsensusRuntimeSelected,
		"network_adapter_selected":         preview.NetworkAdapterSelected,
		"consensus_network_path":           preview.NetworkPath,
		"proposal_preview_count":           preview.ProposalPreviewCount,
		"vote_preview_count":               preview.VotePreviewCount,
		"light_quorum_reached_count":       preview.LightQuorumReachedCount,
		"consensus_network_error_count":    preview.ErrorCount,
		"not_real_pbft":                    true,
		"not_blockemulator_aligned_pbft":   true,
		"not_hotstuff_or_raft":             true,
		"not_real_cross_shard_protocol":    true,
		"not_fabric_or_evm_live_backend":   true,
		"not_paper_grade_benchmark_output": true,
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}

func validatorsByShard(nodes []LogicalNode) map[int][]LogicalNode {
	result := map[int][]LogicalNode{}
	for _, node := range nodes {
		if node.Role == "validator" && node.ShardID >= 0 {
			result[node.ShardID] = append(result[node.ShardID], node)
		}
	}
	return result
}

func lightQuorumTarget(validatorCount int) int {
	if validatorCount <= 0 {
		return 0
	}
	f := (validatorCount - 1) / 3
	target := 2*f + 1
	if target > validatorCount {
		return validatorCount
	}
	return target
}

func (preview ConsensusNetworkLightPreview) SummaryLine() string {
	return fmt.Sprintf("consensus_over_network_enabled=%t\nconsensus_runtime_selected=%s\nproposal_preview_count=%d\nvote_preview_count=%d\nlight_quorum_reached_count=%d\nconsensus_network_error_count=%d\nconsensus_network_path=%s\n", preview.Enabled, preview.ConsensusRuntimeSelected, preview.ProposalPreviewCount, preview.VotePreviewCount, preview.LightQuorumReachedCount, preview.ErrorCount, preview.NetworkPath)
}
