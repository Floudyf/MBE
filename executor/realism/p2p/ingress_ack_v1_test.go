package p2p

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestIngressAckV1CapacityAndSuccess(t *testing.T) {
	var attempts int
	runtime := NewTransport("n0", "127.0.0.1:0", nil, func(_ context.Context, m MessageEnvelope) error {
		attempts++
		if attempts == 1 {
			return &testCapacityV1{}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Stop()
	conn, err := net.DialTimeout("tcp", runtime.ListenAddr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	msg, err := NewEnvelope(MessageTXGossip, ClientAdmissionOriginV1, "n0", "s0", 0, 0, 0, "sample")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := Encode(conn, msg); err != nil {
			t.Fatal(err)
		}
		reply, _, err := Decode(conn)
		if err != nil {
			t.Fatal(err)
		}
		if reply.MessageType != MessageClientAdmissionAckV1 {
			t.Fatalf("wrong reply: %s", reply.MessageType)
		}
		receipt, err := DecodePayload[ClientAdmissionReceiptV1](reply)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.ReferenceMessageID != msg.MessageID || receipt.Accepted != (i == 1) {
			t.Fatalf("wrong receipt: %+v", receipt)
		}
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d", attempts)
	}
}

type testCapacityV1 struct{}

func (*testCapacityV1) Error() string { return "admission capacity_exceeded" }
