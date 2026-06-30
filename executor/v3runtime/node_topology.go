package v3runtime

import (
	"fmt"
	"strconv"
)

type NodeTopologyConfig struct {
	ShardCount           int
	ValidatorsPerShard   int
	ExecutorsPerShard    int
	StorageNodesPerShard int
	SupervisorEnabled    bool
	NodeRuntimeMode      string
	NetworkMode          string
}

type LogicalNode struct {
	NodeID             string
	ShardID            int
	NodeIndex          int
	Role               string
	ConsensusDomainID  string
	ExecutionShardID   int
	StateStorageUnitID int
	LogicalAddress     string
	RuntimeMode        string
	NetworkMode        string
	Status             string
}

type NodeEvent struct {
	EventTimeMS int
	NodeID      string
	Role        string
	ShardID     int
	EventType   string
	BlockHeight int
	TxID        string
	Details     string
}

type NodeRuntimeArtifacts struct {
	Config            NodeTopologyConfig
	Nodes             []LogicalNode
	NodeEvents        []NodeEvent
	NetworkMessages   []NetworkMessage
	ConsensusMessages []ConsensusMessage
}

func DefaultNodeTopologyConfig() NodeTopologyConfig {
	return NodeTopologyConfig{
		ShardCount:           4,
		ValidatorsPerShard:   4,
		ExecutorsPerShard:    1,
		StorageNodesPerShard: 1,
		SupervisorEnabled:    true,
		NodeRuntimeMode:      "logical_single_process",
		NetworkMode:          "in_memory_message_bus",
	}
}

func topologyFromExperiment(exp ExperimentProfile) NodeTopologyConfig {
	cfg := DefaultNodeTopologyConfig()
	if exp.ShardCount > 0 {
		cfg.ShardCount = exp.ShardCount
	}
	if exp.ValidatorsPerShard > 0 {
		cfg.ValidatorsPerShard = exp.ValidatorsPerShard
	}
	if exp.ExecutorsPerShard >= 0 {
		cfg.ExecutorsPerShard = exp.ExecutorsPerShard
	}
	if exp.StorageNodesPerShard >= 0 {
		cfg.StorageNodesPerShard = exp.StorageNodesPerShard
	}
	cfg.SupervisorEnabled = exp.SupervisorEnabled
	if exp.NodeRuntimeMode != "" {
		cfg.NodeRuntimeMode = exp.NodeRuntimeMode
	}
	if exp.NetworkMode != "" {
		cfg.NetworkMode = exp.NetworkMode
	}
	return cfg
}

func GenerateLogicalNodes(cfg NodeTopologyConfig) []LogicalNode {
	nodes := []LogicalNode{}
	for shardID := 0; shardID < max(1, cfg.ShardCount); shardID++ {
		domainID := consensusDomainID(shardID)
		for i := 0; i < cfg.ValidatorsPerShard; i++ {
			nodes = append(nodes, logicalNode("validator", shardID, i, domainID, shardID, shardID, cfg))
		}
		for i := 0; i < cfg.ExecutorsPerShard; i++ {
			nodes = append(nodes, logicalNode("executor", shardID, i, domainID, shardID, shardID, cfg))
		}
		for i := 0; i < cfg.StorageNodesPerShard; i++ {
			nodes = append(nodes, logicalNode("storage", shardID, i, domainID, shardID, shardID, cfg))
		}
	}
	if cfg.SupervisorEnabled {
		nodes = append(nodes, LogicalNode{
			NodeID:             "supervisor-0",
			ShardID:            -1,
			NodeIndex:          0,
			Role:               "supervisor",
			ConsensusDomainID:  "global",
			ExecutionShardID:   -1,
			StateStorageUnitID: -1,
			LogicalAddress:     "logical://supervisor-0",
			RuntimeMode:        cfg.NodeRuntimeMode,
			NetworkMode:        cfg.NetworkMode,
			Status:             "logical_active",
		})
	}
	return nodes
}

func BuildLogicalNodeArtifacts(cfg NodeTopologyConfig, blocks []Block, consensusRecords []ConsensusRecord) NodeRuntimeArtifacts {
	nodes := GenerateLogicalNodes(cfg)
	bus := NewInMemoryMessageBus(cfg.NetworkMode)
	events := make([]NodeEvent, 0, len(nodes)+len(blocks))
	for _, node := range nodes {
		events = append(events, NodeEvent{EventTimeMS: 0, NodeID: node.NodeID, Role: node.Role, ShardID: node.ShardID, EventType: "logical_node_ready", BlockHeight: 0, Details: node.LogicalAddress})
	}
	validatorByShard := firstNodesByRole(nodes, "validator")
	executorByShard := firstNodesByRole(nodes, "executor")
	storageByShard := firstNodesByRole(nodes, "storage")
	supervisor := firstNodeByRole(nodes, "supervisor")
	for _, block := range blocks {
		shardID := 0
		if cfg.ShardCount > 0 {
			shardID = (block.Height - 1) % cfg.ShardCount
		}
		validator := validatorByShard[shardID]
		executor := executorByShard[shardID]
		storage := storageByShard[shardID]
		if executor.NodeID != "" {
			bus.SendNetwork(block.CutTimeMS, validator.NodeID, executor.NodeID, "block_proposal", shardID, block.Height, "", 256)
		}
		if storage.NodeID != "" && executor.NodeID != "" {
			bus.SendNetwork(block.CutTimeMS+1, executor.NodeID, storage.NodeID, "state_commit_notice", shardID, block.Height, "", 128)
		}
		if supervisor.NodeID != "" {
			bus.SendNetwork(block.CutTimeMS+2, validator.NodeID, supervisor.NodeID, "block_finalized_notice", shardID, block.Height, "", 96)
		}
		for _, record := range consensusRecords {
			if record.BlockHeight != block.Height {
				continue
			}
			for i := 0; i < min(cfg.ValidatorsPerShard, 4); i++ {
				to := fmt.Sprintf("shard-%d-validator-%d", shardID, i)
				bus.SendConsensus(record.ConsensusStartTimeMS+i, consensusDomainID(shardID), shardID, validator.NodeID, to, "consensus_light_vote", block.Height, record.ViewID, record.SequenceID)
			}
		}
		events = append(events, NodeEvent{EventTimeMS: block.CutTimeMS, NodeID: validator.NodeID, Role: "validator", ShardID: shardID, EventType: "block_observed", BlockHeight: block.Height, Details: "logical single-process event"})
	}
	return NodeRuntimeArtifacts{Config: cfg, Nodes: nodes, NodeEvents: events, NetworkMessages: bus.NetworkMessages(), ConsensusMessages: bus.ConsensusMessages()}
}

func (artifacts NodeRuntimeArtifacts) CountRole(role string) int {
	count := 0
	for _, node := range artifacts.Nodes {
		if node.Role == role {
			count++
		}
	}
	return count
}

func writeNodeTopologyCSV(path string, nodes []LogicalNode) error {
	fields := []string{"node_id", "shard_id", "node_index", "role", "consensus_domain_id", "execution_shard_id", "state_storage_unit_id", "logical_address", "runtime_mode", "network_mode", "status"}
	rows := [][]string{}
	for _, node := range nodes {
		rows = append(rows, []string{node.NodeID, strconv.Itoa(node.ShardID), strconv.Itoa(node.NodeIndex), node.Role, node.ConsensusDomainID, strconv.Itoa(node.ExecutionShardID), strconv.Itoa(node.StateStorageUnitID), node.LogicalAddress, node.RuntimeMode, node.NetworkMode, node.Status})
	}
	return writeCSV(path, fields, rows)
}

func writeNodeLogCSV(path string, events []NodeEvent) error {
	fields := []string{"event_time_ms", "node_id", "role", "shard_id", "event_type", "block_height", "tx_id", "details"}
	rows := [][]string{}
	for _, event := range events {
		rows = append(rows, []string{strconv.Itoa(event.EventTimeMS), event.NodeID, event.Role, strconv.Itoa(event.ShardID), event.EventType, strconv.Itoa(event.BlockHeight), event.TxID, event.Details})
	}
	return writeCSV(path, fields, rows)
}

func writeNetworkLogCSV(path string, messages []NetworkMessage) error {
	fields := []string{"message_id", "time_ms", "from_node", "to_node", "message_type", "shard_id", "block_height", "tx_id", "payload_size_bytes", "status", "network_mode"}
	rows := [][]string{}
	for _, message := range messages {
		rows = append(rows, []string{message.MessageID, strconv.Itoa(message.TimeMS), message.FromNode, message.ToNode, message.MessageType, strconv.Itoa(message.ShardID), strconv.Itoa(message.BlockHeight), message.TxID, strconv.Itoa(message.PayloadSizeBytes), message.Status, message.NetworkMode})
	}
	return writeCSV(path, fields, rows)
}

func writeConsensusMessageLogCSV(path string, messages []ConsensusMessage) error {
	fields := []string{"message_id", "time_ms", "consensus_domain_id", "shard_id", "from_node", "to_node", "message_type", "block_height", "view", "sequence", "status"}
	rows := [][]string{}
	for _, message := range messages {
		rows = append(rows, []string{message.MessageID, strconv.Itoa(message.TimeMS), message.ConsensusDomainID, strconv.Itoa(message.ShardID), message.FromNode, message.ToNode, message.MessageType, strconv.Itoa(message.BlockHeight), strconv.Itoa(message.View), strconv.Itoa(message.Sequence), message.Status})
	}
	return writeCSV(path, fields, rows)
}

func logicalNode(role string, shardID, index int, domainID string, executionShardID, stateStorageUnitID int, cfg NodeTopologyConfig) LogicalNode {
	nodeID := fmt.Sprintf("shard-%d-%s-%d", shardID, role, index)
	return LogicalNode{NodeID: nodeID, ShardID: shardID, NodeIndex: index, Role: role, ConsensusDomainID: domainID, ExecutionShardID: executionShardID, StateStorageUnitID: stateStorageUnitID, LogicalAddress: fmt.Sprintf("logical://shard-%d/%s-%d", shardID, role, index), RuntimeMode: cfg.NodeRuntimeMode, NetworkMode: cfg.NetworkMode, Status: "logical_active"}
}

func firstNodesByRole(nodes []LogicalNode, role string) map[int]LogicalNode {
	result := map[int]LogicalNode{}
	for _, node := range nodes {
		if node.Role == role {
			if _, exists := result[node.ShardID]; !exists {
				result[node.ShardID] = node
			}
		}
	}
	return result
}

func firstNodeByRole(nodes []LogicalNode, role string) LogicalNode {
	for _, node := range nodes {
		if node.Role == role {
			return node
		}
	}
	return LogicalNode{}
}
