package p2p

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/faults"
)

type Handler func(context.Context, MessageEnvelope) error

type Transport struct {
	NodeID     string
	ListenAddr string
	Peers      map[string]Peer
	Log        *NetworkLog
	handler    Handler
	listener   net.Listener
	cancel     context.CancelFunc
	faults     faults.Policy
	wg         sync.WaitGroup
	mu         sync.Mutex
}

func NewTransport(nodeID, listenAddr string, peers []Peer, handler Handler) *Transport {
	peerMap := map[string]Peer{}
	for _, p := range peers {
		if p.NodeID != "" && p.NodeID != nodeID {
			peerMap[p.NodeID] = p
		}
	}
	return &Transport{NodeID: nodeID, ListenAddr: listenAddr, Peers: peerMap, Log: &NetworkLog{}, handler: handler}
}

func (t *Transport) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listener != nil {
		return nil
	}
	if t.ListenAddr == "" {
		t.ListenAddr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return fmt.Errorf("start p2p listener: %w", err)
	}
	t.ListenAddr = ln.Addr().String()
	child, cancel := context.WithCancel(ctx)
	t.listener = ln
	t.cancel = cancel
	t.wg.Add(1)
	go t.acceptLoop(child, ln)
	return nil
}

func (t *Transport) SetPeers(peers []Peer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Peers = map[string]Peer{}
	for _, p := range peers {
		if p.NodeID != "" && p.NodeID != t.NodeID {
			t.Peers[p.NodeID] = p
		}
	}
}

func (t *Transport) SetFaultPolicy(policy faults.Policy) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.faults = policy
}

func (t *Transport) Stop() error {
	t.mu.Lock()
	cancel := t.cancel
	ln := t.listener
	t.cancel = nil
	t.listener = nil
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ln != nil {
		_ = ln.Close()
	}
	t.wg.Wait()
	return nil
}

func (t *Transport) acceptLoop(ctx context.Context, ln net.Listener) {
	defer t.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				return
			}
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.handleConn(ctx, conn)
		}()
	}
}

func (t *Transport) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	start := time.Now()
	msg, n, err := Decode(conn)
	entry := NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: msg.FromNode, Direction: "receive", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Bytes: n, Success: err == nil, LatencyMS: time.Since(start).Milliseconds()}
	if err != nil {
		entry.Error = err.Error()
		t.Log.Add(entry)
		return
	}
	if decision := t.faultDecision("receive", msg.FromNode, msg); decision.FaultEvent {
		if decision.Delay > 0 {
			time.Sleep(decision.Delay)
			t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: msg.FromNode, Direction: "fault_delay_receive", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: true, Error: decision.Reason, LatencyMS: decision.Delay.Milliseconds()})
		}
		if decision.Drop {
			t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: msg.FromNode, Direction: "fault_drop_receive", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: false, Error: decision.Reason})
			return
		}
	}
	t.Log.Add(entry)
	if t.handler != nil {
		if err := t.handler(ctx, msg); err != nil {
			t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: msg.FromNode, Direction: "handler", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: false, Error: err.Error()})
		}
	}
}

func (t *Transport) Send(ctx context.Context, peerID string, msg MessageEnvelope) error {
	peer, ok := t.Peers[peerID]
	if !ok {
		return fmt.Errorf("unknown peer %s", peerID)
	}
	msg.ToNode = peerID
	if msg.MessageID == "" || msg.Digest == "" {
		msg.Digest = Digest(msg)
		msg.MessageID = MessageID(msg)
	}
	if decision := t.faultDecision("send", peerID, msg); decision.FaultEvent {
		if decision.Delay > 0 {
			time.Sleep(decision.Delay)
			t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: peerID, Direction: "fault_delay_send", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: true, Error: decision.Reason, LatencyMS: decision.Delay.Milliseconds()})
		}
		if decision.Drop {
			t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: peerID, Direction: "fault_drop_send", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: false, Error: decision.Reason})
			return nil
		}
	}
	start := time.Now()
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", peer.ListenAddr)
	if err != nil {
		t.Log.Add(NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: peerID, Direction: "send", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Success: false, Error: err.Error(), LatencyMS: time.Since(start).Milliseconds()})
		return fmt.Errorf("send p2p to %s: %w", peerID, err)
	}
	defer conn.Close()
	err = Encode(conn, msg)
	bytes := len(msg.Payload)
	entry := NetworkLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: t.NodeID, PeerID: peerID, Direction: "send", MessageType: msg.MessageType, MessageID: msg.MessageID, Height: msg.Height, View: msg.View, Sequence: msg.Sequence, Bytes: bytes, Success: err == nil, LatencyMS: time.Since(start).Milliseconds()}
	if err != nil {
		entry.Error = err.Error()
	}
	t.Log.Add(entry)
	return err
}

func (t *Transport) Broadcast(ctx context.Context, msg MessageEnvelope) []error {
	var errs []error
	for peerID := range t.Peers {
		next := msg
		next.ToNode = peerID
		if err := t.Send(ctx, peerID, next); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (t *Transport) faultDecision(direction, peerID string, msg MessageEnvelope) faults.Decision {
	t.mu.Lock()
	policy := t.faults
	t.mu.Unlock()
	return policy.Decide(direction, peerID, msg.MessageType, msg.MessageID)
}
