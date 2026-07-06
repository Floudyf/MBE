package p2p

import (
	"bytes"
	"context"
	"testing"
	"time"
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
