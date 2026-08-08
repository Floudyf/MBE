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
	MessagePBFTCheckpoint = "PBFT_CHECKPOINT"
	MessageXShardRelay    = "XSHARD_RELAY_CERTIFICATE"
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

// FinalizeEnvelope recomputes the per-recipient digest and message ID after
// addressing. Broadcast must call this after setting ToNode because ToNode is
// part of the authenticated envelope core.
func FinalizeEnvelope(msg MessageEnvelope) MessageEnvelope {
	msg.Digest = Digest(msg)
	msg.MessageID = MessageID(msg)
	return msg
}

// ValidateEnvelope checks transport-level message integrity. It is not a
// substitute for replica authentication/signatures, but it prevents corrupted
// or accidentally mutated envelopes from entering PBFT.
func ValidateEnvelope(msg MessageEnvelope) error {
	if msg.MessageType == "" || msg.FromNode == "" {
		return fmt.Errorf("invalid p2p envelope identity")
	}
	if msg.Digest == "" || msg.MessageID == "" {
		return fmt.Errorf("missing p2p envelope integrity fields")
	}
	expectedDigest := Digest(msg)
	if msg.Digest != expectedDigest {
		return fmt.Errorf("p2p envelope digest mismatch")
	}
	if msg.MessageID != MessageID(msg) {
		return fmt.Errorf("p2p envelope message id mismatch")
	}
	return nil
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
	// Digest identifies the protocol content. MessageID identifies one concrete
	// transmission instance. Including Timestamp lets protocol-level PBFT
	// retransmissions be sampled independently by deterministic fault injection,
	// while the three TCP retries inside Transport.Send keep the same ID.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s:%d:%d:%d:%d:%s", msg.MessageType, msg.FromNode, msg.ToNode, msg.ShardID, msg.Height, msg.View, msg.Sequence, msg.Timestamp, msg.Digest)))
	return hex.EncodeToString(sum[:])
}
