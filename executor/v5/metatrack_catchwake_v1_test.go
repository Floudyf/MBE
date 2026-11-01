package v5

import "testing"

// Regression: FW 100K n3 was committed at 323 with a stale checkpoint <=320,
// but retained future PRE-PREPARE proposals. Previously normal retry slept 60 s.
func TestMTCatchWakeV1FutureProposalsTriggerUrgency(t *testing.T) {
	deferred := map[uint64]deferredPrePrepare{
		325: {View: 0},
		331: {View: 0},
	}
	urgent, target := mtCatchupFutureUrgencyV1(323, 320, 0, deferred)
	if !urgent || target != 331 {
		t.Fatalf("expected urgent catch-up to 331; got urgent=%t target=%d", urgent, target)
	}
}

func TestMTCatchWakeV1UnchangedWhenNoFuture(t *testing.T) {
	urgent, target := mtCatchupFutureUrgencyV1(323, 320, 0, nil)
	if urgent || target != 320 {
		t.Fatalf("unexpected change: urgent=%t target=%d", urgent, target)
	}
}

func TestMTCatchWakeV1RejectsStaleViewHint(t *testing.T) {
	deferred := map[uint64]deferredPrePrepare{331: {View: 1}}
	urgent, target := mtCatchupFutureUrgencyV1(323, 320, 0, deferred)
	if urgent || target != 320 {
		t.Fatalf("stale view must not accelerate: urgent=%t target=%d", urgent, target)
	}
}

func TestMTCatchWakeV1PreservesCheckpointTarget(t *testing.T) {
	deferred := map[uint64]deferredPrePrepare{325: {View: 0}}
	urgent, target := mtCatchupFutureUrgencyV1(323, 340, 0, deferred)
	if !urgent || target != 340 {
		t.Fatalf("checkpoint should dominate: urgent=%t target=%d", urgent, target)
	}
}

func TestMTCatchWakeV1IgnoresOldDeferredHeight(t *testing.T) {
	deferred := map[uint64]deferredPrePrepare{323: {View: 0}, 322: {View: 0}}
	urgent, target := mtCatchupFutureUrgencyV1(323, 320, 0, deferred)
	if urgent || target != 320 {
		t.Fatalf("old heights must be ignored: urgent=%t target=%d", urgent, target)
	}
}

func TestMTCatchWakeV1DoesNotMutateInput(t *testing.T) {
	deferred := map[uint64]deferredPrePrepare{331: {View: 0}}
	_, _ = mtCatchupFutureUrgencyV1(323, 320, 0, deferred)
	if len(deferred) != 1 || deferred[331].View != 0 {
		t.Fatal("deferred proposals mutated")
	}
}
