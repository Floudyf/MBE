package v5

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFixedBlockWorkerPoolHonorsRequestedConcurrency(t *testing.T) {
	for _, workers := range []int{2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pool := newFixedBlockWorkerPool(ctx, workers)
			defer pool.Close()

			started := make(chan struct{}, workers)
			release := make(chan struct{})
			var once sync.Once
			tasks := make([]func(), workers)
			for i := range tasks {
				tasks[i] = func() {
					started <- struct{}{}
					<-release
				}
			}
			done := make(chan struct{})
			var maximum int
			var runErr error
			go func() {
				defer close(done)
				maximum, runErr = pool.Run(tasks)
			}()

			for i := 0; i < workers; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					once.Do(func() { close(release) })
					t.Fatalf("only %d/%d workers entered concurrently", i, workers)
				}
			}
			once.Do(func() { close(release) })
			<-done
			if runErr != nil {
				t.Fatalf("pool run failed: %v", runErr)
			}
			if maximum != workers {
				t.Fatalf("observed concurrency=%d want=%d", maximum, workers)
			}
		})
	}
}

func TestConfiguredWorkerCountHonorsSweepValue(t *testing.T) {
	for _, workers := range []int{2, 4, 8, 16, 32} {
		got := configuredWorkerCount(map[string]any{"worker_count": workers}, 1)
		if got != workers {
			t.Fatalf("configuredWorkerCount(%d)=%d", workers, got)
		}
	}
	if got := cgPlanningWorkerCount(map[string]any{"worker_count": 32}); got != 1 {
		t.Fatalf("CG planning worker count=%d, want source-faithful 1", got)
	}
}

func TestWorkerBatchTrackerIsRaceSafeUnderConcurrentEntry(t *testing.T) {
	var tracker fixedBlockWorkerBatchTracker
	var wg sync.WaitGroup
	var active int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.enter()
			atomic.AddInt64(&active, 1)
			time.Sleep(time.Millisecond)
			atomic.AddInt64(&active, -1)
			tracker.leave()
		}()
	}
	wg.Wait()
	if tracker.max() < 1 || atomic.LoadInt64(&active) != 0 {
		t.Fatalf("invalid tracker state max=%d active=%d", tracker.max(), active)
	}
}
