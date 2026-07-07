package p2p

import (
	"bytes"
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/faults"
)

func TestMessageEncodeDecode(t *testing.T) {
	msg, err := NewEnvelope(MessageNodeHello, "n0", "n1", "s0", 1, 0, 1, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Encode(&buf, msg); err != nil {
		t.Fatal(err)
	}
	got, _, err := Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != msg.MessageID || got.MessageType != MessageNodeHello {
		t.Fatalf("unexpected decoded message: %+v", got)
	}
}

func TestP2PFaultPolicyDelayLogsRealDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan MessageEnvelope, 1)
	b := NewTransport("n1", "127.0.0.1:0", nil, func(ctx context.Context, msg MessageEnvelope) error {
		received <- msg
		return nil
	})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	a := NewTransport("n0", "127.0.0.1:0", []Peer{{NodeID: "n1", ListenAddr: b.ListenAddr}}, nil)
	a.SetFaultPolicy(faults.Policy{Enabled: true, DelayMS: 25, Seed: 1})
	msg, err := NewEnvelope(MessageNodeHello, "n0", "n1", "s0", 1, 0, 1, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := a.Send(ctx, "n1", msg); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("expected real delay, got %s", elapsed)
	}
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed send")
	}
	if !hasLogDirection(a.Log.Entries(), "fault_delay_send") {
		t.Fatalf("expected fault_delay_send log")
	}
}

func TestP2PFaultPolicyDropByMessageType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan MessageEnvelope, 1)
	b := NewTransport("n1", "127.0.0.1:0", nil, func(ctx context.Context, msg MessageEnvelope) error {
		received <- msg
		return nil
	})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	a := NewTransport("n0", "127.0.0.1:0", []Peer{{NodeID: "n1", ListenAddr: b.ListenAddr}}, nil)
	a.SetFaultPolicy(faults.Policy{Enabled: true, DropMessageTypes: []string{MessageNodeHello}, Seed: 1})
	msg, err := NewEnvelope(MessageNodeHello, "n0", "n1", "s0", 1, 0, 1, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Send(ctx, "n1", msg); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		t.Fatalf("message should have been dropped, got %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	if !hasLogDirection(a.Log.Entries(), "fault_drop_send") {
		t.Fatalf("expected fault_drop_send log")
	}
}

func hasLogDirection(entries []NetworkLogEntry, direction string) bool {
	for _, entry := range entries {
		if entry.Direction == direction {
			return true
		}
	}
	return false
}

func TestSendReceiveAndBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan MessageEnvelope, 2)
	b := NewTransport("n1", "127.0.0.1:0", nil, func(ctx context.Context, msg MessageEnvelope) error {
		received <- msg
		return nil
	})
	c := NewTransport("n2", "127.0.0.1:0", nil, func(ctx context.Context, msg MessageEnvelope) error {
		received <- msg
		return nil
	})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	a := NewTransport("n0", "127.0.0.1:0", []Peer{{NodeID: "n1", ListenAddr: b.ListenAddr}, {NodeID: "n2", ListenAddr: c.ListenAddr}}, nil)
	msg, err := NewEnvelope(MessageNodeHello, "n0", "", "s0", 1, 0, 1, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	if errs := a.Broadcast(ctx, msg); len(errs) != 0 {
		t.Fatalf("broadcast errors: %v", errs)
	}
	for i := 0; i < 2; i++ {
		select {
		case got := <-received:
			if got.MessageType != MessageNodeHello {
				t.Fatalf("unexpected message: %+v", got)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for broadcast")
		}
	}
	if len(a.Log.Entries()) < 2 || len(b.Log.Entries()) < 1 {
		t.Fatalf("expected network logs")
	}
}

func TestTransportStopDoesNotPanicAcceptLoop(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		tp := NewTransport("n0", "127.0.0.1:0", nil, nil)
		if err := tp.Start(ctx); err != nil {
			cancel()
			t.Fatalf("start transport: %v", err)
		}
		cancel()
		if err := tp.Stop(); err != nil {
			t.Fatalf("stop transport: %v", err)
		}
		if err := tp.Stop(); err != nil {
			t.Fatalf("second stop transport: %v", err)
		}
	}
}

func TestMultipleTransportConcurrentStopDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transports := make([]*Transport, 0, 20)
	for i := 0; i < 20; i++ {
		tp := NewTransport("n-stop", "127.0.0.1:0", nil, nil)
		if err := tp.Start(ctx); err != nil {
			t.Fatal(err)
		}
		transports = append(transports, tp)
	}
	done := make(chan struct{}, len(transports))
	for _, tp := range transports {
		go func(next *Transport) {
			_ = next.Stop()
			done <- struct{}{}
		}(tp)
	}
	for range transports {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for transport stop")
		}
	}
}
