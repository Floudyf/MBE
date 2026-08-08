package p2p

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestDecodeRejectsTamperedEnvelopePayload(t *testing.T) {
	msg, err := NewEnvelope(MessageNodeHello, "n0", "n1", "s0", 1, 0, 1, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	msg.Payload = []byte(`{"hello":"tampered"}`)
	var buf bytes.Buffer
	if err := Encode(&buf, msg); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Decode(&buf); err == nil {
		t.Fatal("tampered envelope payload was accepted")
	}
}

func TestSendFinalizesDigestAfterAddressingRecipient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan MessageEnvelope, 1)
	b := NewTransport("n1", "127.0.0.1:0", nil, func(_ context.Context, msg MessageEnvelope) error {
		received <- msg
		return nil
	})
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	a := NewTransport("n0", "127.0.0.1:0", []Peer{{NodeID: "n1", ShardID: "s0", ListenAddr: b.ListenAddr}}, nil)
	defer a.Stop()
	msg, err := NewEnvelope(MessagePBFTPrepare, "n0", "", "s0", 1, 0, 1, map[string]string{"block_hash": "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Send(ctx, "n1", msg); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got.ToNode != "n1" {
			t.Fatalf("recipient was not addressed: %+v", got)
		}
		if err := ValidateEnvelope(got); err != nil {
			t.Fatalf("addressed envelope failed integrity validation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for addressed message")
	}
}

func TestProtocolRetransmissionKeepsDigestButGetsNewMessageID(t *testing.T) {
	first, err := NewEnvelope(MessagePBFTPrePrepare, "n0", "n1", "s0", 9, 2, 9, map[string]string{"block_hash": "same"})
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Timestamp = first.Timestamp + 1
	second = FinalizeEnvelope(second)
	if first.Digest != second.Digest {
		t.Fatal("protocol retransmission changed the content digest")
	}
	if first.MessageID == second.MessageID {
		t.Fatal("protocol retransmission reused the same transmission message ID")
	}
	if err := ValidateEnvelope(second); err != nil {
		t.Fatalf("retransmitted envelope failed integrity validation: %v", err)
	}
}
