package v5

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
)

func TestClientAdmissionV1CapacityRetryAndPermanentRejection(t *testing.T) {
	run := func(reason string, shouldSucceed bool) {
		count := 0
		transport := p2p.NewTransport("n0", "127.0.0.1:0", nil, func(_ context.Context, msg p2p.MessageEnvelope) error {
			count++
			if count == 1 {
				return errors.New(reason)
			}
			return nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := transport.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer transport.Stop()
		msg, err := p2p.NewEnvelope(p2p.MessageTXGossip, p2p.ClientAdmissionOriginV1, "n0", "s0", 0, 0, 0, "sample")
		if err != nil {
			t.Fatal(err)
		}
		conns := map[string]net.Conn{}
		defer func() {
			for _, c := range conns {
				_ = c.Close()
			}
		}()
		retries, err := sendAdmissionVerifiedV1(ctx, conns, transport.ListenAddr, msg)
		if shouldSucceed && (err != nil || retries != 1 || count != 2) {
			t.Fatalf("capacity recovery: retries=%d count=%d err=%v", retries, count, err)
		}
		if !shouldSucceed && (err == nil || retries != 0 || count != 1 || !strings.Contains(err.Error(), reason)) {
			t.Fatalf("permanent rejection: retries=%d count=%d err=%v", retries, count, err)
		}
	}
	run("admission capacity_exceeded", true)
	run("admission nonce", false)
}
