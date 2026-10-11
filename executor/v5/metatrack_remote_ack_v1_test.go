package v5

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// MBE_MT_TAPOS_V1: pure transport-contract regressions, independent of PBFT.
func mtTaposAckV1(r StateDeltaApplyRequest) StateDeltaApplyAck {
	a := stateDeltaApplyAckFromRequest(r, r.HomeShard+"::"+r.Key, stableTextDigest("queued:"+r.Value), "root")
	a.WitnessDigest = stateDeltaApplyWitnessDigest(a)
	return a
}
func TestMTTaposV1RetrySingleAckAndPreserveRequestIdentity(t *testing.T) {
	req := StateDeltaApplyRequest{RequestID: "stable-id", TxID: "a", BlockHash: "block", Key: "k", Value: "1", HomeShard: "s1", ExecutionShard: "s0", UpdateSemantics: "read_write"}
	replies := make(chan StateDeltaApplyAck, 4)
	sendCount := 0
	ack, attempts, err := metaTrackWaitSingleAckV1(context.Background(), req, replies, func(ctx context.Context) error {
		sendCount++
		if sendCount == 2 {
			replies <- mtTaposAckV1(req)
		}
		return nil
	}, 2*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if sendCount != 2 || attempts != 2 || ack.RequestID != req.RequestID {
		t.Fatalf("send=%d attempts=%d ack=%#v", sendCount, attempts, ack)
	}
}
func TestMTTaposV1RejectBadAckAndBoundedWait(t *testing.T) {
	req := StateDeltaApplyRequest{RequestID: "one", BlockHash: "hash", TxID: "t", Key: "k", Value: "9", HomeShard: "s1", ExecutionShard: "s0"}
	ack := mtTaposAckV1(req)
	ack.HomeShard = "other"
	ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
	if err := metaTrackValidateSingleAckV1(req, ack); err == nil {
		t.Fatal("wrong Home accepted")
	}
	ack = mtTaposAckV1(req)
	ack.WitnessDigest = "forged"
	if err := metaTrackValidateSingleAckV1(req, ack); err == nil {
		t.Fatal("invalid witness accepted")
	}
	ack = mtTaposAckV1(req)
	ack.Success = false
	ack.Error = "wrong_version"
	ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
	if err := metaTrackValidateSingleAckV1(req, ack); err == nil {
		t.Fatal("Home rejection ignored")
	}
	sends := 0
	_, attempts, err := metaTrackWaitSingleAckV1(context.Background(), req, make(chan StateDeltaApplyAck), func(context.Context) error { sends++; return nil }, time.Millisecond, 7*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "deadline") || attempts < 2 || sends != attempts {
		t.Fatalf("timeout=%v attempts=%d sends=%d", err, attempts, sends)
	}
}
func TestMTTaposV1CancellationAndSendFailure(t *testing.T) {
	req := StateDeltaApplyRequest{RequestID: "one"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sends := 0
	_, _, err := metaTrackWaitSingleAckV1(ctx, req, make(chan StateDeltaApplyAck), func(context.Context) error { sends++; return nil }, time.Millisecond, 10*time.Millisecond)
	if !errors.Is(err, context.Canceled) || sends != 0 {
		t.Fatalf("cancellation ignored: %v %d", err, sends)
	}
	wanted := errors.New("network closed")
	_, _, err = metaTrackWaitSingleAckV1(context.Background(), req, make(chan StateDeltaApplyAck), func(context.Context) error { return wanted }, time.Millisecond, 10*time.Millisecond)
	if !errors.Is(err, wanted) {
		t.Fatalf("send error ignored: %v", err)
	}
}
