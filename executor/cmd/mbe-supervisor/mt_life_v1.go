package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// mtLifeLease ties the lifetime of supervisor-managed V5 nodes to the active
// supervisor. The Drain deadline itself remains exclusively in drainV5.
type mtLifeLease struct {
	path string
	stop chan struct{}
	done chan struct{}
}

const mtLifeLeaseEnvironment = "MBE_V5_SUPERVISOR_LEASE"
const mtLifeHeartbeatInterval = 10 * time.Second
const mtLifeNodeStatusMaxAge = 2 * time.Minute

func mtLifeStartLease(path string) (*mtLifeLease, error) {
	return mtLifeStartLeaseWithInterval(path, mtLifeHeartbeatInterval)
}

func mtLifeStartLeaseWithInterval(path string, interval time.Duration) (*mtLifeLease, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("invalid supervisor heartbeat interval")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	// Avoid reusing a symlink to a file outside this run's private directory.
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("supervisor lease symlink refused")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.WriteFile(path, []byte("mbe-v5-supervisor-lease-v1\n"), 0o600); err != nil {
		return nil, err
	}
	lease := &mtLifeLease{path: path, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(lease.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-lease.stop:
				return
			case <-ticker.C:
				now := time.Now()
				if err := os.Chtimes(path, now, now); err != nil {
					// The node-side lease guard will cancel the runtime if
					// refreshing the heartbeat becomes impossible.
					fmt.Fprintln(os.Stderr, "[MTLIFE LEASE ERROR]", err)
				}
			}
		}
	}()
	return lease, nil
}

func (l *mtLifeLease) Path() string { return l.path }
func (l *mtLifeLease) Close() {
	if l == nil {
		return
	}
	close(l.stop)
	<-l.done
}

func mtLifeNodeEnvironment(in []string, leasePath string) []string {
	out := make([]string, 0, len(in)+1)
	for _, variable := range in {
		name, _, _ := strings.Cut(variable, "=")
		if strings.EqualFold(name, mtLifeLeaseEnvironment) {
			continue
		}
		out = append(out, variable)
	}
	return append(out, mtLifeLeaseEnvironment+"="+leasePath)
}

func mtLifeNodeStatusStale(path string, now time.Time) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	} // missing status is handled by existing readiness/no-progress gates
	return now.Sub(info.ModTime()) > mtLifeNodeStatusMaxAge
}
