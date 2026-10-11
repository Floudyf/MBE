package v5

import (
	"context"
	"strings"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMTFixV1MixedCommutativeCanonicalMaterialization(t *testing.T) {
	working := map[string]string{"s0::market:hot": "0", "s0::balance:a": "10"}
	item := tx.SignedTransaction{TxID: "one", SchedulingAccessList: []tx.AccessItem{{Key: "market:hot", Mode: tx.AccessCommutativeDelta}}, AccessList: []tx.AccessItem{
		{Key: "balance:a", Mode: tx.AccessReadWrite},
		{Key: "market:hot", Mode: tx.AccessCommutativeDelta, Delta: 1},
	}}
	first, err := metaTrackCanonicalizeTxDeltaV1(working, "s0", item, map[string]string{"balance:a": "9", "market:hot": "1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if first["balance:a"] != "9" || first["market:hot"] != "1" {
		t.Fatalf("bad first mixed delta: %#v", first)
	}
	for k, v := range first {
		working[qualifyStateKey("s0", k)] = v
	}
	// The worker's speculative absolute value is still "1"; canonical value must be 2.
	second, err := metaTrackCanonicalizeTxDeltaV1(working, "s0", item, map[string]string{"balance:a": "8", "market:hot": "1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if second["balance:a"] != "8" || second["market:hot"] != "2" {
		t.Fatalf("lost a commutative update: %#v", second)
	}
	// A failing transaction must not apply a speculative increment.
	failed, err := metaTrackCanonicalizeTxDeltaV1(working, "s0", item, map[string]string{}, false)
	if err != nil || len(failed) != 0 {
		t.Fatalf("failed transaction changed state: %#v, %v", failed, err)
	}
}

func TestMTFixV1CommutativeInvalidAndDuplicateFailClosed(t *testing.T) {
	item := tx.SignedTransaction{TxID: "bad", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessCommutativeDelta, Delta: 1}}}
	if _, err := metaTrackCanonicalizeTxDeltaV1(map[string]string{"s0::k": "not-a-number"}, "s0", item, map[string]string{"k": "1"}, true); err == nil {
		t.Fatal("invalid base silently accepted")
	}
	item.AccessList = append(item.AccessList, item.AccessList[0])
	if _, err := metaTrackCanonicalizeTxDeltaV1(map[string]string{"s0::k": "0"}, "s0", item, map[string]string{}, true); err == nil {
		t.Fatal("duplicate commutative declaration accepted")
	}
}

func mtFixAck(req StateDeltaApplyRequest) StateDeltaApplyAck {
	ack := stateDeltaApplyAckFromRequest(req, req.HomeShard+"::"+req.Key, stableTextDigest("queued:"+req.Value), "root")
	ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
	return ack
}
func TestMTFixV1BatchAckRequiresAllAndRetriesIdempotentRequests(t *testing.T) {
	reqs := []StateDeltaApplyRequest{
		{RequestID: "id1", BlockHash: "block", TxID: "a", Key: "k1", HomeShard: "s1", ExecutionShard: "s0", UpdateSemantics: "set"},
		{RequestID: "id2", BlockHash: "block", TxID: "b", Key: "k2", HomeShard: "s1", ExecutionShard: "s0", UpdateSemantics: "set"},
	}
	incoming := make(chan StateDeltaApplyAck, 8)
	incoming <- mtFixAck(reqs[0])
	incoming <- mtFixAck(reqs[0]) // duplicate ACK does not release id2.
	retries := 0
	accepted := map[int]int{}
	err := metaTrackAwaitBatchAcksV1(context.Background(), reqs, incoming, 2*time.Millisecond, 200*time.Millisecond,
		func() error { retries++; incoming <- mtFixAck(reqs[1]); return nil },
		func(i int, ack StateDeltaApplyAck) error { accepted[i]++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if retries < 1 || accepted[0] != 1 || accepted[1] != 1 {
		t.Fatalf("retries=%d accepted=%v", retries, accepted)
	}
}
func TestMTFixV1BatchAckMismatchAndDeadline(t *testing.T) {
	req := StateDeltaApplyRequest{RequestID: "one", BlockHash: "block", TxID: "a", Key: "k", HomeShard: "s1", ExecutionShard: "s0", UpdateSemantics: "set"}
	ack := mtFixAck(req)
	ack.HomeShard = "other"
	ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
	if err := metaTrackValidateBatchAckV1(req, ack); err == nil {
		t.Fatal("mismatched Home ACK accepted")
	}
	err := metaTrackAwaitBatchAcksV1(context.Background(), []StateDeltaApplyRequest{req}, make(chan StateDeltaApplyAck), time.Millisecond, 6*time.Millisecond,
		func() error { return nil }, func(int, StateDeltaApplyAck) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("no bounded timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := metaTrackAwaitBatchAcksV1(ctx, []StateDeltaApplyRequest{req}, make(chan StateDeltaApplyAck), time.Millisecond, 5*time.Millisecond,
		func() error { return nil }, func(int, StateDeltaApplyAck) error { return nil }); err != context.Canceled {
		t.Fatalf("no cancellation: %v", err)
	}
}
