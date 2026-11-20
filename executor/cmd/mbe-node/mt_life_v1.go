package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Independent fallback: if the V5 supervisor disappears without issuing
// stop.request, its heartbeat eventually expires and nodes exit on ctx.Done.
const mtLifeNodeLeaseMaxAge = 5 * time.Minute
const mtLifeNodeLeasePollInterval = 10 * time.Second

func mtLifeWatchContext(parent context.Context, leasePath string) (context.Context, context.CancelFunc, error) {
	return mtLifeWatchContextWithConfig(parent, leasePath, mtLifeNodeLeaseMaxAge, mtLifeNodeLeasePollInterval)
}

func mtLifeCheckLease(path string, ttl time.Duration) error {
	if path == "" {
		return fmt.Errorf("empty supervisor lease path")
	}
	if ttl <= 0 {
		return fmt.Errorf("invalid supervisor lease ttl")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("supervisor lease unavailable: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("supervisor lease is not a regular file")
	}
	if time.Since(info.ModTime()) > ttl {
		return fmt.Errorf("supervisor lease expired")
	}
	return nil
}

func mtLifeWatchContextWithConfig(parent context.Context, path string, ttl, interval time.Duration) (context.Context, context.CancelFunc, error) {
	if interval <= 0 {
		return nil, nil, fmt.Errorf("invalid supervisor lease poll interval")
	}
	if err := mtLifeCheckLease(path, ttl); err != nil {
		return nil, nil, err
	}
	child, cancel := context.WithCancel(parent)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-child.Done():
				return
			case <-ticker.C:
				if err := mtLifeCheckLease(path, ttl); err != nil {
					fmt.Fprintln(os.Stderr, "[MTLIFE SUPERVISOR LOST]", err)
					cancel()
					return
				}
			}
		}
	}()
	return child, cancel, nil
}
