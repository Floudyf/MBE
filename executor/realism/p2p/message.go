package p2p

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	MessageTXGossip       = "TX_GOSSIP"
	MessageBlockProposal  = "BLOCK_PROPOSAL"
	MessagePBFTPrePrepare = "PBFT_PRE_PREPARE"
	MessagePBFTPrepare    = "PBFT_PREPARE"
	MessagePBFTCommit     = "PBFT_COMMIT"
	MessagePBFTViewChange = "PBFT_VIEW_CHANGE"
	MessagePBFTNewView    = "PBFT_NEW_VIEW"
	MessageNodeHello      = "NODE_HELLO"
	MessageNodeShutdown   = "NODE_SHUTDOWN"
)

type MessageEnvelope struct {
	MessageID   string          `json:"message_id"`
	MessageType string          `json:"message_type"`
	FromNode    string          `json:"from_node"`
	ToNode      string          `json:"to_node,omitempty"`
	ShardID     string          `json:"shard_id"`
	Height      uint64          `json:"height"`
	View        uint64          `json:"view"`
	Sequence    uint64          `json:"sequence"`
	Timestamp   int64           `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
	Digest      string          `json:"digest"`
}

func NewEnvelope(messageType, fromNode, toNode, shardID string, height, view, sequence uint64, payload any) (MessageEnvelope, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return MessageEnvelope{}, err
	}
	msg := MessageEnvelope{
		MessageType: messageType,
		FromNode:    fromNode,
		ToNode:      toNode,
		ShardID:     shardID,
		Height:      height,
		View:        view,
		Sequence:    sequence,
		Timestamp:   time.Now().UnixMilli(),
		Payload:     payloadBytes,
	}
	msg.Digest = Digest(msg)
	msg.MessageID = MessageID(msg)
	return msg, nil
}

func DecodePayload[T any](msg MessageEnvelope) (T, error) {
	var out T
	if err := json.Unmarshal(msg.Payload, &out); err != nil {
		return out, fmt.Errorf("decode %s payload: %w", msg.MessageType, err)
	}
	return out, nil
}

func Digest(msg MessageEnvelope) string {
	core := struct {
		MessageType string          `json:"message_type"`
		FromNode    string          `json:"from_node"`
		ToNode      string          `json:"to_node,omitempty"`
		ShardID     string          `json:"shard_id"`
		Height      uint64          `json:"height"`
		View        uint64          `json:"view"`
		Sequence    uint64          `json:"sequence"`
		Payload     json.RawMessage `json:"payload"`
	}{msg.MessageType, msg.FromNode, msg.ToNode, msg.ShardID, msg.Height, msg.View, msg.Sequence, msg.Payload}
	payload, _ := json.Marshal(core)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func MessageID(msg MessageEnvelope) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s:%d:%d:%d:%s", msg.MessageType, msg.FromNode, msg.ToNode, msg.ShardID, msg.Height, msg.View, msg.Sequence, msg.Digest)))
	return hex.EncodeToString(sum[:])
}
