package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMTLifeSupervisorLeaseHeartbeatAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node_lease")
	lease, err := mtLifeStartLeaseWithInterval(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.ModTime().After(old.Add(time.Minute)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor lease heartbeat did not refresh")
		}
		time.Sleep(10 * time.Millisecond)
	}
	lease.Close()
	old = time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().After(old.Add(time.Minute)) {
		t.Fatal("heartbeat continued after close")
	}
}

func TestMTLifeSupervisorEnvironmentSingleLease(t *testing.T) {
	got := mtLifeNodeEnvironment([]string{"A=1", "mbe_v5_supervisor_lease=wrong", "B=2"}, "new")
	joined := strings.Join(got, "|")
	if strings.Contains(joined, "wrong") || !strings.Contains(joined, "MBE_V5_SUPERVISOR_LEASE=new") || len(got) != 3 {
		t.Fatalf("lease env not unique: %v", got)
	}
}

func TestMTLifeSupervisorStaleStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	if mtLifeNodeStatusStale(path, time.Now()) {
		t.Fatal("missing status must be handled by prior gate")
	}
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if mtLifeNodeStatusStale(path, time.Now()) {
		t.Fatal("fresh status marked stale")
	}
	old := time.Now().Add(-5 * time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if !mtLifeNodeStatusStale(path, time.Now()) {
		t.Fatal("stale status not detected")
	}
}
