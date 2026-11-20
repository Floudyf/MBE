package v5

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
)

const clientAdmissionAckTimeoutV1 = 15 * time.Second
const clientAdmissionCapacityWaitV1 = 120 * time.Second
const clientAdmissionRetryDelayV1 = 100 * time.Millisecond

type admissionRejectedV1 struct{ reason string }

func (e *admissionRejectedV1) Error() string { return "node admission rejected: " + e.reason }

// sendAdmissionVerifiedV1 never treats a successful socket write as an admission.
// It retries ONLY explicit capacity replies. An uncertain delivery is fatal so
// it cannot be quietly double-submitted or counted as a confirmed admission.
func sendAdmissionVerifiedV1(ctx context.Context, conns map[string]net.Conn, address string, message p2p.MessageEnvelope) (int, error) {
	start := time.Now()
	retries := 0
	for {
		err := sendAdmissionOneV1(ctx, conns, address, message)
		if err == nil {
			return retries, nil
		}
		var rejected *admissionRejectedV1
		if !errors.As(err, &rejected) || rejected.reason != "admission capacity_exceeded" {
			return retries, err
		}
		if time.Since(start) >= clientAdmissionCapacityWaitV1 {
			return retries, fmt.Errorf("admission capacity backpressure exceeded %s (transaction NOT confirmed): %w", clientAdmissionCapacityWaitV1, err)
		}
		retries++
		timer := time.NewTimer(clientAdmissionRetryDelayV1)
		select {
		case <-ctx.Done():
			timer.Stop()
			return retries, ctx.Err()
		case <-timer.C:
		}
	}
}

func sendAdmissionOneV1(ctx context.Context, conns map[string]net.Conn, address string, message p2p.MessageEnvelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn := conns[address]
	if conn == nil {
		var err error
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err = dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return fmt.Errorf("admission dial failed (transaction NOT confirmed): %w", err)
		}
		conns[address] = conn
	}
	// The client sends one transaction and waits for its matching receipt on
	// this connection. No other sender writes to this connection concurrently.
	deadline := time.Now().Add(clientAdmissionAckTimeoutV1)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	if err := p2p.Encode(conn, message); err != nil {
		delete(conns, address)
		_ = conn.Close()
		return fmt.Errorf("admission write failed, delivery uncertain: %w", err)
	}
	receiptEnvelope, _, err := p2p.DecodeReader(bufio.NewReader(conn))
	if err != nil {
		delete(conns, address)
		_ = conn.Close()
		return fmt.Errorf("admission receipt missing or corrupt, delivery uncertain: %w", err)
	}
	_ = conn.SetDeadline(time.Time{})
	if receiptEnvelope.MessageType != p2p.MessageClientAdmissionAckV1 || receiptEnvelope.FromNode != message.ToNode || receiptEnvelope.ShardID != message.ShardID || receiptEnvelope.ToNode != p2p.ClientAdmissionOriginV1 {
		return fmt.Errorf("unrecognized admission receipt, delivery uncertain: type=%s", receiptEnvelope.MessageType)
	}
	receipt, err := p2p.DecodePayload[p2p.ClientAdmissionReceiptV1](receiptEnvelope)
	if err != nil || receipt.ReferenceMessageID != message.MessageID {
		return fmt.Errorf("admission receipt correlation mismatch, delivery uncertain: %v", err)
	}
	if !receipt.Accepted {
		return &admissionRejectedV1{reason: receipt.Reason}
	}
	return nil
}
