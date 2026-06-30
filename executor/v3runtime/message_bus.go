package v3runtime

import "fmt"

type NetworkMessage struct {
	MessageID        string
	TimeMS           int
	FromNode         string
	ToNode           string
	MessageType      string
	ShardID          int
	BlockHeight      int
	TxID             string
	PayloadSizeBytes int
	Status           string
	NetworkMode      string
}

type ConsensusMessage struct {
	MessageID         string
	TimeMS            int
	ConsensusDomainID string
	ShardID           int
	FromNode          string
	ToNode            string
	MessageType       string
	BlockHeight       int
	View              int
	Sequence          int
	Status            string
}

type InMemoryMessageBus struct {
	NetworkMode       string
	networkMessages   []NetworkMessage
	consensusMessages []ConsensusMessage
}

func NewInMemoryMessageBus(networkMode string) *InMemoryMessageBus {
	if networkMode == "" {
		networkMode = "in_memory_message_bus"
	}
	return &InMemoryMessageBus{NetworkMode: networkMode}
}

func (bus *InMemoryMessageBus) SendNetwork(timeMS int, fromNode, toNode, messageType string, shardID, blockHeight int, txID string, payloadSizeBytes int) NetworkMessage {
	message := NetworkMessage{
		MessageID:        fmt.Sprintf("net_%06d", len(bus.networkMessages)+1),
		TimeMS:           timeMS,
		FromNode:         fromNode,
		ToNode:           toNode,
		MessageType:      messageType,
		ShardID:          shardID,
		BlockHeight:      blockHeight,
		TxID:             txID,
		PayloadSizeBytes: payloadSizeBytes,
		Status:           "delivered",
		NetworkMode:      bus.NetworkMode,
	}
	bus.networkMessages = append(bus.networkMessages, message)
	return message
}

func (bus *InMemoryMessageBus) SendConsensus(timeMS int, domainID string, shardID int, fromNode, toNode, messageType string, blockHeight, view, sequence int) ConsensusMessage {
	message := ConsensusMessage{
		MessageID:         fmt.Sprintf("cons_%06d", len(bus.consensusMessages)+1),
		TimeMS:            timeMS,
		ConsensusDomainID: domainID,
		ShardID:           shardID,
		FromNode:          fromNode,
		ToNode:            toNode,
		MessageType:       messageType,
		BlockHeight:       blockHeight,
		View:              view,
		Sequence:          sequence,
		Status:            "delivered",
	}
	bus.consensusMessages = append(bus.consensusMessages, message)
	return message
}

func (bus *InMemoryMessageBus) NetworkMessages() []NetworkMessage {
	return append([]NetworkMessage(nil), bus.networkMessages...)
}

func (bus *InMemoryMessageBus) ConsensusMessages() []ConsensusMessage {
	return append([]ConsensusMessage(nil), bus.consensusMessages...)
}
