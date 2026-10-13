package p2p

import (
	"context"
	"sync"
	"testing"
	"time"
)

func awaitAdmissionV2(t *testing.T, g *stateAccessWriteAdmissionV2, high, background int) {
	t.Helper()
	limit := time.Now().Add(3 * time.Second)
	for time.Now().Before(limit) {
		g.mu.Lock()
		found := len(g.critical) >= high && len(g.background) >= background
		g.mu.Unlock()
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing waiting writes: critical=%d background=%d", high, background)
}

func TestStateAccessAdmissionV2CriticalWinsOverUnstartedBackground(t *testing.T) {
	g := &stateAccessWriteAdmissionV2{}
	leave, queued, err := g.enter(context.Background(), false)
	if err != nil || queued {
		t.Fatal("idle background must start immediately")
	}
	var mu sync.Mutex
	order := []string{}
	done := make(chan bool, 2)
	go func() {
		release, _, e := g.enter(context.Background(), false)
		if e == nil {
			mu.Lock()
			order = append(order, "background")
			mu.Unlock()
			release()
		}
		done <- true
	}()
	awaitAdmissionV2(t, g, 0, 1)
	go func() {
		release, _, e := g.enter(context.Background(), true)
		if e == nil {
			mu.Lock()
			order = append(order, "critical")
			mu.Unlock()
			release()
		}
		done <- true
	}()
	awaitAdmissionV2(t, g, 1, 1)
	leave()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("deadlock")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "critical" || order[1] != "background" {
		t.Fatalf("priority mismatch %v", order)
	}
}

func TestStateAccessAdmissionV2BoundedBackground(t *testing.T) {
	g := &stateAccessWriteAdmissionV2{}
	leave, _, _ := g.enter(context.Background(), false)
	out := make(chan string, 7)
	for i := 0; i < 5; i++ {
		go func() {
			rel, _, e := g.enter(context.Background(), true)
			if e == nil {
				out <- "critical"
				rel()
			}
		}()
	}
	awaitAdmissionV2(t, g, 5, 0)
	go func() {
		rel, _, e := g.enter(context.Background(), false)
		if e == nil {
			out <- "background"
			rel()
		}
	}()
	awaitAdmissionV2(t, g, 5, 1)
	leave()
	order := []string{}
	for i := 0; i < 6; i++ {
		select {
		case x := <-out:
			order = append(order, x)
		case <-time.After(3 * time.Second):
			t.Fatal("deadlock")
		}
	}
	first := -1
	for i, v := range order {
		if v == "background" {
			first = i
			break
		}
	}
	if first < 0 || first > stateAccessMaxCriticalBurstV2 {
		t.Fatalf("background starved %v", order)
	}
}

func TestStateAccessAdmissionV2CancelledWaiterCannotBlockLane(t *testing.T) {
	g := &stateAccessWriteAdmissionV2{}
	leave, _, _ := g.enter(context.Background(), false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		rel, _, e := g.enter(ctx, true)
		if rel != nil {
			rel()
		}
		done <- e
	}()
	awaitAdmissionV2(t, g, 1, 0)
	cancel()
	select {
	case e := <-done:
		if e != context.Canceled {
			t.Fatalf("bad cancellation %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock")
	}
	g.mu.Lock()
	count := len(g.critical)
	g.mu.Unlock()
	if count != 0 {
		t.Fatalf("stale waiter=%d", count)
	}
	leave()
	rel, _, e := g.enter(context.Background(), false)
	if e != nil {
		t.Fatal(e)
	}
	rel()
}
