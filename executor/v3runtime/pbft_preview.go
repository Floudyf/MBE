package v3runtime

import (
	"fmt"
	"path/filepath"
	"strconv"
)

const ConsensusRuntimeBlockEmulatorPBFTPreview = "blockemulator_aligned_pbft_preview"

type PBFTStateRecord struct {
	EventID             string
	TimeMS              int
	ConsensusRuntime    string
	ShardID             int
	NodeID              string
	LeaderNodeID        string
	View                int
	SequenceID          int
	BlockHeight         int
	RequestPoolSize     int
	PrepareConfirmCount int
	CommitConfirmCount  int
	PBFTStage           string
	QuorumThreshold     int
	QuorumReached       bool
	Status              string
	Details             string
}

type PBFTMessageRecord struct {
	MessageID     string
	TimeMS        int
	MessageType   string
	FromNodeID    string
	ToNodeID      string
	ShardID       int
	View          int
	SequenceID    int
	BlockHeight   int
	PayloadDigest string
	Status        string
}

type PBFTQuorumRecord struct {
	EventID         string
	ShardID         int
	LeaderNodeID    string
	View            int
	SequenceID      int
	BlockHeight     int
	QuorumType      string
	ValidatorCount  int
	FaultToleranceF int
	QuorumThreshold int
	ConfirmCount    int
	QuorumReached   bool
}

type PBFTFinalizedBlockRecord struct {
	BlockHeight     int
	BlockHash       string
	ShardID         int
	LeaderNodeID    string
	View            int
	SequenceID      int
	ValidatorCount  int
	QuorumThreshold int
	Finalized       bool
	Status          string
	RuntimeTruth    string
}

type PBFTPreview struct {
	Enabled                  bool
	ConsensusRuntimeSelected string
	View                     int
	Sequence                 int
	PrePrepareCount          int
	PrepareCount             int
	CommitCount              int
	QuorumReachedCount       int
	FinalizedBlockCount      int
	ConsensusLatencyMS       int
	QuorumThreshold          int
	StateRows                []PBFTStateRecord
	MessageRows              []PBFTMessageRecord
	QuorumRows               []PBFTQuorumRecord
	FinalizedRows            []PBFTFinalizedBlockRecord
	ErrorCount               int
	SkippedReason            string
}

func RunPBFTPreview(nodeRuntime NodeRuntimeArtifacts, consensusLog []ConsensusRecord, consensusRuntime string) PBFTPreview {
	preview := PBFTPreview{ConsensusRuntimeSelected: consensusRuntime}
	if consensusRuntime != ConsensusRuntimeBlockEmulatorPBFTPreview {
		preview.SkippedReason = "consensus runtime is not blockemulator_aligned_pbft_preview"
		preview.StateRows = append(preview.StateRows, PBFTStateRecord{
			EventID:          "pbft_state_000001",
			ConsensusRuntime: consensusRuntime,
			PBFTStage:        "Skipped",
			Status:           "skipped",
			Details:          preview.SkippedReason,
		})
		return preview
	}
	preview.Enabled = true
	validatorsByShard := validatorsByShard(nodeRuntime.Nodes)
	records := consensusLog
	if len(records) == 0 {
		records = []ConsensusRecord{{BlockHeight: 1, BlockHash: "block_preview_1", SequenceID: 1, ViewID: 0, ConsensusStartTimeMS: 0, ConsensusFinalizedTimeMS: 3}}
	}
	stateID := 1
	messageID := 1
	quorumID := 1
	latencies := []int{}
	for _, record := range records {
		shardID := 0
		if nodeRuntime.Config.ShardCount > 0 {
			shardID = (record.BlockHeight - 1) % nodeRuntime.Config.ShardCount
		}
		validators := validatorsByShard[shardID]
		if len(validators) == 0 {
			preview.ErrorCount++
			preview.StateRows = append(preview.StateRows, PBFTStateRecord{
				EventID:          fmt.Sprintf("pbft_state_%06d", stateID),
				TimeMS:           record.ConsensusStartTimeMS,
				ConsensusRuntime: consensusRuntime,
				ShardID:          shardID,
				View:             record.ViewID,
				SequenceID:       record.SequenceID,
				BlockHeight:      record.BlockHeight,
				PBFTStage:        "Skipped",
				Status:           "error",
				Details:          "no validators available for PBFT preview",
			})
			stateID++
			continue
		}
		leader := validators[record.ViewID%len(validators)]
		f := (len(validators) - 1) / 3
		quorumThreshold := min(len(validators), 2*f+1)
		preview.QuorumThreshold = quorumThreshold
		preview.View = record.ViewID
		preview.Sequence = record.SequenceID
		requestPoolSize := 1
		prepareConfirmCount := len(validators)
		commitConfirmCount := len(validators)

		preview.StateRows = append(preview.StateRows, PBFTStateRecord{
			EventID:             fmt.Sprintf("pbft_state_%06d", stateID),
			TimeMS:              record.ConsensusStartTimeMS,
			ConsensusRuntime:    consensusRuntime,
			ShardID:             shardID,
			NodeID:              leader.NodeID,
			LeaderNodeID:        leader.NodeID,
			View:                record.ViewID,
			SequenceID:          record.SequenceID,
			BlockHeight:         record.BlockHeight,
			RequestPoolSize:     requestPoolSize,
			PrepareConfirmCount: 0,
			CommitConfirmCount:  0,
			PBFTStage:           "PrePrepare",
			QuorumThreshold:     quorumThreshold,
			Status:              "preview",
			Details:             "leader selects request from request_pool and emits pbft_preprepare",
		})
		stateID++
		for _, validator := range validators {
			if validator.NodeID == leader.NodeID {
				continue
			}
			preview.MessageRows = append(preview.MessageRows, pbftMessage(messageID, record.ConsensusStartTimeMS, "pbft_preprepare", leader.NodeID, validator.NodeID, shardID, record))
			messageID++
			preview.PrePrepareCount++
		}

		prepareReached := prepareConfirmCount >= quorumThreshold
		if prepareReached {
			preview.QuorumReachedCount++
		}
		for _, validator := range validators {
			preview.MessageRows = append(preview.MessageRows, pbftMessage(messageID, record.ConsensusStartTimeMS+1, "pbft_prepare", validator.NodeID, leader.NodeID, shardID, record))
			messageID++
			preview.PrepareCount++
		}
		preview.QuorumRows = append(preview.QuorumRows, PBFTQuorumRecord{
			EventID:         fmt.Sprintf("pbft_quorum_%06d", quorumID),
			ShardID:         shardID,
			LeaderNodeID:    leader.NodeID,
			View:            record.ViewID,
			SequenceID:      record.SequenceID,
			BlockHeight:     record.BlockHeight,
			QuorumType:      "prepare",
			ValidatorCount:  len(validators),
			FaultToleranceF: f,
			QuorumThreshold: quorumThreshold,
			ConfirmCount:    prepareConfirmCount,
			QuorumReached:   prepareReached,
		})
		quorumID++
		preview.StateRows = append(preview.StateRows, PBFTStateRecord{
			EventID:             fmt.Sprintf("pbft_state_%06d", stateID),
			TimeMS:              record.ConsensusStartTimeMS + 1,
			ConsensusRuntime:    consensusRuntime,
			ShardID:             shardID,
			NodeID:              leader.NodeID,
			LeaderNodeID:        leader.NodeID,
			View:                record.ViewID,
			SequenceID:          record.SequenceID,
			BlockHeight:         record.BlockHeight,
			RequestPoolSize:     requestPoolSize,
			PrepareConfirmCount: prepareConfirmCount,
			CommitConfirmCount:  0,
			PBFTStage:           "Prepare",
			QuorumThreshold:     quorumThreshold,
			QuorumReached:       prepareReached,
			Status:              "preview",
			Details:             "prepare_confirm_map reaches lightweight preview quorum",
		})
		stateID++

		commitReached := commitConfirmCount >= quorumThreshold
		if commitReached {
			preview.QuorumReachedCount++
		}
		for _, validator := range validators {
			preview.MessageRows = append(preview.MessageRows, pbftMessage(messageID, record.ConsensusStartTimeMS+2, "pbft_commit", validator.NodeID, leader.NodeID, shardID, record))
			messageID++
			preview.CommitCount++
		}
		preview.QuorumRows = append(preview.QuorumRows, PBFTQuorumRecord{
			EventID:         fmt.Sprintf("pbft_quorum_%06d", quorumID),
			ShardID:         shardID,
			LeaderNodeID:    leader.NodeID,
			View:            record.ViewID,
			SequenceID:      record.SequenceID,
			BlockHeight:     record.BlockHeight,
			QuorumType:      "commit",
			ValidatorCount:  len(validators),
			FaultToleranceF: f,
			QuorumThreshold: quorumThreshold,
			ConfirmCount:    commitConfirmCount,
			QuorumReached:   commitReached,
		})
		quorumID++
		preview.StateRows = append(preview.StateRows, PBFTStateRecord{
			EventID:             fmt.Sprintf("pbft_state_%06d", stateID),
			TimeMS:              record.ConsensusStartTimeMS + 2,
			ConsensusRuntime:    consensusRuntime,
			ShardID:             shardID,
			NodeID:              leader.NodeID,
			LeaderNodeID:        leader.NodeID,
			View:                record.ViewID,
			SequenceID:          record.SequenceID,
			BlockHeight:         record.BlockHeight,
			RequestPoolSize:     requestPoolSize,
			PrepareConfirmCount: prepareConfirmCount,
			CommitConfirmCount:  commitConfirmCount,
			PBFTStage:           "Commit",
			QuorumThreshold:     quorumThreshold,
			QuorumReached:       commitReached,
			Status:              "preview",
			Details:             "commit_confirm_map reaches lightweight preview quorum",
		})
		stateID++
		finalized := prepareReached && commitReached
		if finalized {
			preview.FinalizedBlockCount++
		}
		preview.MessageRows = append(preview.MessageRows, pbftMessage(messageID, record.ConsensusStartTimeMS+3, "pbft_finalized", leader.NodeID, leader.NodeID, shardID, record))
		messageID++
		preview.StateRows = append(preview.StateRows, PBFTStateRecord{
			EventID:             fmt.Sprintf("pbft_state_%06d", stateID),
			TimeMS:              record.ConsensusStartTimeMS + 3,
			ConsensusRuntime:    consensusRuntime,
			ShardID:             shardID,
			NodeID:              leader.NodeID,
			LeaderNodeID:        leader.NodeID,
			View:                record.ViewID,
			SequenceID:          record.SequenceID,
			BlockHeight:         record.BlockHeight,
			RequestPoolSize:     requestPoolSize,
			PrepareConfirmCount: prepareConfirmCount,
			CommitConfirmCount:  commitConfirmCount,
			PBFTStage:           "Finalized",
			QuorumThreshold:     quorumThreshold,
			QuorumReached:       finalized,
			Status:              "preview",
			Details:             "finalized block preview after prepare and commit quorum",
		})
		stateID++
		preview.FinalizedRows = append(preview.FinalizedRows, PBFTFinalizedBlockRecord{
			BlockHeight:     record.BlockHeight,
			BlockHash:       record.BlockHash,
			ShardID:         shardID,
			LeaderNodeID:    leader.NodeID,
			View:            record.ViewID,
			SequenceID:      record.SequenceID,
			ValidatorCount:  len(validators),
			QuorumThreshold: quorumThreshold,
			Finalized:       finalized,
			Status:          "preview_finalized",
			RuntimeTruth:    "blockemulator_aligned_pbft_state_machine_preview_not_production_pbft",
		})
		latency := 3
		if record.ConsensusFinalizedTimeMS > record.ConsensusStartTimeMS {
			latency = record.ConsensusFinalizedTimeMS - record.ConsensusStartTimeMS
		}
		latencies = append(latencies, latency)
	}
	preview.ConsensusLatencyMS = int(avg(latencies))
	return preview
}

func pbftMessage(id int, timeMS int, messageType, fromNodeID, toNodeID string, shardID int, record ConsensusRecord) PBFTMessageRecord {
	payload := fmt.Sprintf("%s:block=%d:seq=%d:hash=%s", messageType, record.BlockHeight, record.SequenceID, record.BlockHash)
	return PBFTMessageRecord{
		MessageID:     fmt.Sprintf("pbft_msg_%06d", id),
		TimeMS:        timeMS,
		MessageType:   messageType,
		FromNodeID:    fromNodeID,
		ToNodeID:      toNodeID,
		ShardID:       shardID,
		View:          record.ViewID,
		SequenceID:    record.SequenceID,
		BlockHeight:   record.BlockHeight,
		PayloadDigest: payloadDigest(payload),
		Status:        "preview_delivered",
	}
}

func WritePBFTPreviewArtifacts(out string, preview PBFTPreview) error {
	if err := writePBFTStateLogCSV(filepath.Join(out, "pbft_state_log.csv"), preview.StateRows); err != nil {
		return err
	}
	if err := writePBFTMessageLogCSV(filepath.Join(out, "pbft_message_log.csv"), preview.MessageRows); err != nil {
		return err
	}
	if err := writePBFTQuorumLogCSV(filepath.Join(out, "quorum_log.csv"), preview.QuorumRows); err != nil {
		return err
	}
	return writePBFTFinalizedBlockLogCSV(filepath.Join(out, "finalized_block_log.csv"), preview.FinalizedRows)
}

func writePBFTStateLogCSV(path string, rows []PBFTStateRecord) error {
	fields := []string{"event_id", "time_ms", "consensus_runtime", "shard_id", "node_id", "leader_node_id", "view", "sequence_id", "block_height", "request_pool_size", "prepare_confirm_count", "commit_confirm_count", "pbft_stage", "quorum_threshold", "quorum_reached", "status", "details"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.EventID, strconv.Itoa(row.TimeMS), row.ConsensusRuntime, strconv.Itoa(row.ShardID), row.NodeID, row.LeaderNodeID, strconv.Itoa(row.View), strconv.Itoa(row.SequenceID), strconv.Itoa(row.BlockHeight), strconv.Itoa(row.RequestPoolSize), strconv.Itoa(row.PrepareConfirmCount), strconv.Itoa(row.CommitConfirmCount), row.PBFTStage, strconv.Itoa(row.QuorumThreshold), strconv.FormatBool(row.QuorumReached), row.Status, row.Details})
	}
	return writeCSV(path, fields, out)
}

func writePBFTMessageLogCSV(path string, rows []PBFTMessageRecord) error {
	fields := []string{"message_id", "time_ms", "message_type", "from_node_id", "to_node_id", "shard_id", "view", "sequence_id", "block_height", "payload_digest", "status"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.MessageID, strconv.Itoa(row.TimeMS), row.MessageType, row.FromNodeID, row.ToNodeID, strconv.Itoa(row.ShardID), strconv.Itoa(row.View), strconv.Itoa(row.SequenceID), strconv.Itoa(row.BlockHeight), row.PayloadDigest, row.Status})
	}
	return writeCSV(path, fields, out)
}

func writePBFTQuorumLogCSV(path string, rows []PBFTQuorumRecord) error {
	fields := []string{"event_id", "shard_id", "leader_node_id", "view", "sequence_id", "block_height", "quorum_type", "validator_count", "fault_tolerance_f", "quorum_threshold", "confirm_count", "quorum_reached"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.EventID, strconv.Itoa(row.ShardID), row.LeaderNodeID, strconv.Itoa(row.View), strconv.Itoa(row.SequenceID), strconv.Itoa(row.BlockHeight), row.QuorumType, strconv.Itoa(row.ValidatorCount), strconv.Itoa(row.FaultToleranceF), strconv.Itoa(row.QuorumThreshold), strconv.Itoa(row.ConfirmCount), strconv.FormatBool(row.QuorumReached)})
	}
	return writeCSV(path, fields, out)
}

func writePBFTFinalizedBlockLogCSV(path string, rows []PBFTFinalizedBlockRecord) error {
	fields := []string{"block_height", "block_hash", "shard_id", "leader_node_id", "view", "sequence_id", "validator_count", "quorum_threshold", "finalized", "status", "runtime_truth"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{strconv.Itoa(row.BlockHeight), row.BlockHash, strconv.Itoa(row.ShardID), row.LeaderNodeID, strconv.Itoa(row.View), strconv.Itoa(row.SequenceID), strconv.Itoa(row.ValidatorCount), strconv.Itoa(row.QuorumThreshold), strconv.FormatBool(row.Finalized), row.Status, row.RuntimeTruth})
	}
	return writeCSV(path, fields, out)
}

func (preview PBFTPreview) SummaryLine() string {
	return fmt.Sprintf("pbft_preview_enabled=%t\nconsensus_runtime_selected=%s\npbft_view=%d\npbft_sequence=%d\npbft_preprepare_count=%d\npbft_prepare_count=%d\npbft_commit_count=%d\npbft_quorum_reached_count=%d\npbft_finalized_block_count=%d\npbft_consensus_latency_ms=%d\npbft_quorum_threshold=%d\n", preview.Enabled, preview.ConsensusRuntimeSelected, preview.View, preview.Sequence, preview.PrePrepareCount, preview.PrepareCount, preview.CommitCount, preview.QuorumReachedCount, preview.FinalizedBlockCount, preview.ConsensusLatencyMS, preview.QuorumThreshold)
}
