package v3runtime

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"
)

const (
	NetworkAdapterInMemory            = "in_memory_message_bus"
	NetworkAdapterLocalhostTCPPreview = "localhost_tcp_preview"
)

type MessageEnvelope struct {
	MessageID     string `json:"message_id"`
	MessageType   string `json:"message_type"`
	FromNodeID    string `json:"from_node_id"`
	ToNodeID      string `json:"to_node_id"`
	ShardID       int    `json:"shard_id"`
	Role          string `json:"role"`
	BlockHeight   int    `json:"block_height"`
	SequenceID    int    `json:"sequence_id"`
	PayloadDigest string `json:"payload_digest"`
	Payload       string `json:"payload"`
	TimestampMS   int    `json:"timestamp_ms"`
}

type TCPAdapterStatus struct {
	NodeID      string
	Role        string
	PreviewHost string
	PreviewPort int
	Adapter     string
	Action      string
	Status      string
	Error       string
}

type NetworkSendRecord struct {
	MessageEnvelope
	Status string
	Error  string
}

type NetworkReceiveRecord struct {
	MessageEnvelope
	Status string
	Error  string
}

type NetworkAdapterPreview struct {
	SelectedAdapter string
	TCPPreview      bool
	StatusRows      []TCPAdapterStatus
	SendRows        []NetworkSendRecord
	ReceiveRows     []NetworkReceiveRecord
	TypedMessages   []MessageEnvelope
	ErrorCount      int
}

func RunNetworkAdapterPreview(launcher LauncherPreview) NetworkAdapterPreview {
	selected := selectedNetworkAdapter(launcher)
	preview := NetworkAdapterPreview{SelectedAdapter: selected, TCPPreview: selected == NetworkAdapterLocalhostTCPPreview}
	if len(launcher.Addresses) == 0 {
		preview.ErrorCount = 1
		preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{Adapter: selected, Action: "select_adapter", Status: "error", Error: "no nodes available"})
		return preview
	}
	if selected != NetworkAdapterLocalhostTCPPreview {
		node := launcher.Addresses[0]
		msg := NewMessageEnvelope("node-ready-0001", "node_ready", node.NodeID, node.NodeID, node.ShardID, node.Role, 0, 1, "in_memory_message_bus")
		preview.TypedMessages = append(preview.TypedMessages, msg)
		preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: "preview_logged"})
		preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: "preview_logged"})
		preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: node.NodeID, Role: node.Role, PreviewHost: node.PreviewHost, PreviewPort: node.PreviewPort, Adapter: selected, Action: "in_memory_ready", Status: "ok"})
		return preview
	}
	return runLocalhostTCPHandshake(launcher)
}

func NewMessageEnvelope(messageID, messageType, fromNodeID, toNodeID string, shardID int, role string, blockHeight, sequenceID int, payload string) MessageEnvelope {
	return MessageEnvelope{
		MessageID:     messageID,
		MessageType:   messageType,
		FromNodeID:    fromNodeID,
		ToNodeID:      toNodeID,
		ShardID:       shardID,
		Role:          role,
		BlockHeight:   blockHeight,
		SequenceID:    sequenceID,
		PayloadDigest: payloadDigest(payload),
		Payload:       payload,
		TimestampMS:   sequenceID,
	}
}

func WriteNetworkAdapterPreviewArtifacts(out string, preview NetworkAdapterPreview) error {
	if err := writeTCPAdapterStatusCSV(filepath.Join(out, "tcp_adapter_status.csv"), preview.StatusRows); err != nil {
		return err
	}
	if err := writeNetworkSendLogCSV(filepath.Join(out, "network_send_log.csv"), preview.SendRows); err != nil {
		return err
	}
	if err := writeNetworkReceiveLogCSV(filepath.Join(out, "network_receive_log.csv"), preview.ReceiveRows); err != nil {
		return err
	}
	return writeTypedMessageLogCSV(filepath.Join(out, "typed_message_log.csv"), preview.TypedMessages)
}

func runLocalhostTCPHandshake(launcher LauncherPreview) NetworkAdapterPreview {
	preview := NetworkAdapterPreview{SelectedAdapter: NetworkAdapterLocalhostTCPPreview, TCPPreview: true}
	receiver := launcher.Addresses[0]
	sender := receiver
	if len(launcher.Addresses) > 1 {
		sender = launcher.Addresses[1]
	}
	address := net.JoinHostPort(receiver.PreviewHost, strconv.Itoa(receiver.PreviewPort))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		preview.ErrorCount++
		preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: receiver.NodeID, Role: receiver.Role, PreviewHost: receiver.PreviewHost, PreviewPort: receiver.PreviewPort, Adapter: NetworkAdapterLocalhostTCPPreview, Action: "listen", Status: "error", Error: err.Error()})
		return preview
	}
	defer listener.Close()
	preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: receiver.NodeID, Role: receiver.Role, PreviewHost: receiver.PreviewHost, PreviewPort: receiver.PreviewPort, Adapter: NetworkAdapterLocalhostTCPPreview, Action: "listen", Status: "ok"})

	received := make(chan MessageEnvelope, 1)
	responded := make(chan MessageEnvelope, 1)
	errors := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errors <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			errors <- err
			return
		}
		var msg MessageEnvelope
		if err := json.Unmarshal(line, &msg); err != nil {
			errors <- err
			return
		}
		received <- msg
		pong := NewMessageEnvelope("pong-0001", "pong", receiver.NodeID, sender.NodeID, receiver.ShardID, receiver.Role, msg.BlockHeight, msg.SequenceID+1, "pong")
		bytes, _ := json.Marshal(pong)
		if _, err := conn.Write(append(bytes, '\n')); err != nil {
			errors <- err
			return
		}
		responded <- pong
	}()

	ping := NewMessageEnvelope("ping-0001", "ping", sender.NodeID, receiver.NodeID, receiver.ShardID, sender.Role, 0, 1, "ping")
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		preview.ErrorCount++
		preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: sender.NodeID, Role: sender.Role, PreviewHost: receiver.PreviewHost, PreviewPort: receiver.PreviewPort, Adapter: NetworkAdapterLocalhostTCPPreview, Action: "dial", Status: "error", Error: err.Error()})
		return preview
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	bytes, _ := json.Marshal(ping)
	_, err = conn.Write(append(bytes, '\n'))
	if err != nil {
		preview.ErrorCount++
		preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: ping, Status: "error", Error: err.Error()})
		return preview
	}
	preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: sender.NodeID, Role: sender.Role, PreviewHost: receiver.PreviewHost, PreviewPort: receiver.PreviewPort, Adapter: NetworkAdapterLocalhostTCPPreview, Action: "dial_send", Status: "ok"})
	preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: ping, Status: "sent"})
	preview.TypedMessages = append(preview.TypedMessages, ping)

	pongLine, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		preview.ErrorCount++
		preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: ping, Status: "error", Error: err.Error()})
		return preview
	}
	var pong MessageEnvelope
	if err := json.Unmarshal(pongLine, &pong); err != nil {
		preview.ErrorCount++
		preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: ping, Status: "error", Error: err.Error()})
		return preview
	}
	preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: pong, Status: "received"})
	preview.TypedMessages = append(preview.TypedMessages, pong)

	select {
	case msg := <-received:
		preview.ReceiveRows = append(preview.ReceiveRows, NetworkReceiveRecord{MessageEnvelope: msg, Status: "received"})
	case err := <-errors:
		preview.ErrorCount++
		preview.StatusRows = append(preview.StatusRows, TCPAdapterStatus{NodeID: receiver.NodeID, Role: receiver.Role, PreviewHost: receiver.PreviewHost, PreviewPort: receiver.PreviewPort, Adapter: NetworkAdapterLocalhostTCPPreview, Action: "accept_receive", Status: "error", Error: err.Error()})
	default:
	}
	select {
	case msg := <-responded:
		preview.SendRows = append(preview.SendRows, NetworkSendRecord{MessageEnvelope: msg, Status: "sent"})
	default:
	}
	return preview
}

func selectedNetworkAdapter(launcher LauncherPreview) string {
	for _, address := range launcher.Addresses {
		if address.NetworkAdapter != "" {
			return address.NetworkAdapter
		}
		if address.NetworkMode != "" {
			return address.NetworkMode
		}
	}
	return NetworkAdapterInMemory
}

func payloadDigest(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func writeTCPAdapterStatusCSV(path string, rows []TCPAdapterStatus) error {
	fields := []string{"node_id", "role", "preview_host", "preview_port", "network_adapter", "action", "status", "error"}
	out := [][]string{}
	for _, row := range rows {
		out = append(out, []string{row.NodeID, row.Role, row.PreviewHost, strconv.Itoa(row.PreviewPort), row.Adapter, row.Action, row.Status, row.Error})
	}
	return writeCSV(path, fields, out)
}

func writeNetworkSendLogCSV(path string, rows []NetworkSendRecord) error {
	fields := append(messageEnvelopeFields(), "status", "error")
	out := [][]string{}
	for _, row := range rows {
		out = append(out, append(messageEnvelopeValues(row.MessageEnvelope), row.Status, row.Error))
	}
	return writeCSV(path, fields, out)
}

func writeNetworkReceiveLogCSV(path string, rows []NetworkReceiveRecord) error {
	fields := append(messageEnvelopeFields(), "status", "error")
	out := [][]string{}
	for _, row := range rows {
		out = append(out, append(messageEnvelopeValues(row.MessageEnvelope), row.Status, row.Error))
	}
	return writeCSV(path, fields, out)
}

func writeTypedMessageLogCSV(path string, rows []MessageEnvelope) error {
	out := [][]string{}
	for _, row := range rows {
		out = append(out, messageEnvelopeValues(row))
	}
	return writeCSV(path, messageEnvelopeFields(), out)
}

func messageEnvelopeFields() []string {
	return []string{"message_id", "message_type", "from_node_id", "to_node_id", "shard_id", "role", "block_height", "sequence_id", "payload_digest", "payload", "timestamp_ms"}
}

func messageEnvelopeValues(msg MessageEnvelope) []string {
	return []string{
		msg.MessageID,
		msg.MessageType,
		msg.FromNodeID,
		msg.ToNodeID,
		strconv.Itoa(msg.ShardID),
		msg.Role,
		strconv.Itoa(msg.BlockHeight),
		strconv.Itoa(msg.SequenceID),
		msg.PayloadDigest,
		msg.Payload,
		strconv.Itoa(msg.TimestampMS),
	}
}

func (preview NetworkAdapterPreview) SummaryLine() string {
	return fmt.Sprintf("network_adapter_selected=%s\ntcp_preview_enabled=%t\ntcp_listen_node_count=%d\ntcp_send_count=%d\ntcp_receive_count=%d\ntyped_message_count=%d\nnetwork_error_count=%d\n", preview.SelectedAdapter, preview.TCPPreview, preview.ListenNodeCount(), len(preview.SendRows), len(preview.ReceiveRows), len(preview.TypedMessages), preview.ErrorCount)
}

func (preview NetworkAdapterPreview) ListenNodeCount() int {
	count := 0
	for _, row := range preview.StatusRows {
		if row.Action == "listen" && row.Status == "ok" {
			count++
		}
	}
	return count
}
