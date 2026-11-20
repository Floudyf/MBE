package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMTLifeNodeLeaseOutlivesSubmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease")
	if err := os.WriteFile(path, []byte("lease"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel, err := mtLifeWatchContextWithConfig(context.Background(), path, 400*time.Millisecond, 15*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	// Simulate an extended client submission: refreshing the lease keeps the
	// node alive independently of an elapsed drain-duration-sized interval.
	for i := 0; i < 6; i++ {
		now := time.Now()
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatal(err)
		}
		time.Sleep(75 * time.Millisecond)
		if ctx.Err() != nil {
			t.Fatal("node expired while supervisor heartbeat remained fresh")
		}
	}
	select {
	case <-ctx.Done():
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("orphan node lease expiration did not cancel context")
	}
}

func TestMTLifeNodeRejectsStaleLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease")
	if err := os.WriteFile(path, []byte("lease"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mtLifeWatchContextWithConfig(context.Background(), path, time.Minute, time.Millisecond); err == nil {
		t.Fatal("stale lease accepted")
	}
}

func TestMTLifeNodePropagatesParentCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease")
	if err := os.WriteFile(path, []byte("lease"), 0o600); err != nil {
		t.Fatal(err)
	}
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel, err := mtLifeWatchContextWithConfig(parent, path, time.Minute, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent cancellation not propagated")
	}
}
