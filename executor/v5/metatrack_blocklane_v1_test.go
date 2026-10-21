package v5

// MBE_MT_BLOCKLANE_V1: behavior test must run in detached worktree before install.
import (
	"context"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMTBlockingLaneStableTopoOrder(t *testing.T) {
	items := []tx.SignedTransaction{{TxID: "consumer"}, {TxID: "independent"}, {TxID: "producer"}}
	got, err := metaTrackBlockingOrderV1(items, map[string][]string{"consumer": {"producer"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"independent", "producer", "consumer"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stable dependency-safe FIFO=%v expected %v", got, want)
	}
	_, err = metaTrackBlockingOrderV1(items, map[string][]string{"consumer": {"producer"}, "producer": {"consumer"}})
	if err == nil {
		t.Fatal("cycle must fail closed")
	}
}

func TestMTBlockingLaneHeadOfLineWithoutWorkerSerialization(t *testing.T) {
	remote := tx.SignedTransaction{TxID: "remote", AccessList: []tx.AccessItem{{Key: "asset:remote", Mode: tx.AccessRead}}}
	local := tx.SignedTransaction{TxID: "local", AccessList: []tx.AccessItem{{Key: "asset:local", Mode: tx.AccessRead}}}
	block := realblock.Block{Height: 1, ShardID: "s1", TxList: []tx.SignedTransaction{remote, local}}
	classification := BatchClassificationResult{
		Decisions:     map[string]ExecutionDecision{"remote": {Track: "fast", Reason: "safe"}, "local": {Track: "fast", Reason: "safe"}},
		Dependencies:  map[string][]string{"remote": {}, "local": {}},
		StateWaitKeys: map[string][]string{"remote": {"k:asset:remote"}},
	}
	schedule := ScheduleResult{Ordered: []tx.SignedTransaction{remote, local}}
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		select {
		case <-ctx.Done():
			return RemoteStateReadyEvent{}, ctx.Err()
		case <-time.After(30 * time.Millisecond):
		}
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: stateReadinessToken(item, access), Value: "7", HomeShard: "s0", LatencyMS: 30}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, metrics, outcomes, _, err := executeMetaTrackScheduleWithBlockingLaneV1(ctx, schedule, classification, block, map[string]string{"s1::asset:local": "3"}, 8, 0, fetch, nil, nil, false, false, false, false, false, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("completed %d", len(outcomes))
	}
	remoteStateReady, localDispatch := -1, -1
	for i, event := range events {
		if event.TxID == "remote" && strings.HasPrefix(event.DecisionReason, "actual_state_ready:") {
			remoteStateReady = i
		}
		if event.TxID == "local" && strings.HasPrefix(event.DecisionReason, "actual_dispatch") {
			localDispatch = i
		}
	}
	if remoteStateReady < 0 || localDispatch <= remoteStateReady {
		t.Fatalf("local dispatch must be held until independent head state is Ready: ready=%d dispatch=%d", remoteStateReady, localDispatch)
	}
	seenGate := false
	for _, event := range events {
		if event.TxID == "local" && strings.HasPrefix(event.DecisionReason, "blocklane_wait_for:remote") {
			seenGate = true
		}
	}
	if !seenGate {
		t.Fatalf("expected no-bypass gate event; events=%#v", events)
	}
	if metrics["metatrack_blocklane_fifo_gate_enabled"] != true {
		t.Fatal("gate was not enabled")
	}
	if got := metrics["configured_worker_count"]; got != 8 {
		t.Fatalf("worker count changed: %v", got)
	}
}
