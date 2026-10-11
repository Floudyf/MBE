package v5

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
)

func mtAckV2Envelope(i byte) p2p.MessageEnvelope {
	return p2p.MessageEnvelope{MessageID: string([]byte{i})}
}

func TestMTACKV2NonBlockingAndPerPeerFIFO(t *testing.T) {
	started := make(chan string, 4)
	release := make(chan struct{})
	completed := make(chan string, 4)
	d := newMetaTrackHomeAckDispatcherV2(func(_ context.Context, peer string, msg p2p.MessageEnvelope) error {
		started <- peer + msg.MessageID
		if msg.MessageID == "A" {
			<-release
		}
		completed <- peer + msg.MessageID
		return nil
	}, nil)
	ctx := context.Background()
	if err := d.enqueue(ctx, "peer", mtAckV2Envelope('A')); err != nil {
		t.Fatal(err)
	}
	select {
	case x := <-started:
		if x != "peerA" {
			t.Fatalf("first ACK %q", x)
		}
	case <-time.After(time.Second):
		t.Fatal("first ACK did not start")
	}
	start := time.Now()
	if err := d.enqueue(ctx, "peer", mtAckV2Envelope('B')); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("read callback blocked by outbound ACK")
	}
	select {
	case x := <-started:
		t.Fatalf("same peer ACK overtook blocked predecessor %q", x)
	case <-time.After(30 * time.Millisecond):
	}
	// A blocked Home peer must not delay an unrelated peer.
	if err := d.enqueue(ctx, "other", mtAckV2Envelope('C')); err != nil {
		t.Fatal(err)
	}
	select {
	case x := <-completed:
		if x != "otherC" {
			t.Fatalf("unrelated peer blocked: %q", x)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated peer stalled")
	}
	close(release)
	for _, expect := range []string{"peerA", "peerB"} {
		select {
		case x := <-completed:
			if x != expect {
				t.Fatalf("got %q want %q", x, expect)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing ACK %q", expect)
		}
	}
}

func TestMTACKV2CancelFailureAndBoundedQueue(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	var mu sync.Mutex
	failures := 0
	d := newMetaTrackHomeAckDispatcherV2(func(ctx context.Context, _ string, _ p2p.MessageEnvelope) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
		return errors.New("synthetic state transport error")
	}, func(err error) {
		mu.Lock()
		if err != nil {
			failures++
		}
		mu.Unlock()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.enqueue(ctx, "peer", mtAckV2Envelope('A')); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel got %v", err)
	}
	if err := d.enqueue(context.Background(), "peer", mtAckV2Envelope('A')); err != nil {
		t.Fatal(err)
	}
	<-started
	for i := 0; i < metaTrackHomeAckQueueLimitV2; i++ {
		if err := d.enqueue(context.Background(), "peer", mtAckV2Envelope('B')); err != nil {
			t.Fatalf("premature queue full: %v", err)
		}
	}
	if err := d.enqueue(context.Background(), "peer", mtAckV2Envelope('C')); err == nil {
		t.Fatal("unbounded ACK queue accepted over-limit task")
	}
	close(gate)
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := failures
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("failed ACK was not observed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
