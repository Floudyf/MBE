package v5

import (
	"context"
	"strings"
	"testing"
	"time"
)

func porygonV55RootWaitFixture() *NodeRuntime {
	r := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0"}, runtimeMetricCounts: map[string]int64{}}
	porygonPaperRuntimeStates.Store(r, &porygonPaperRuntimeState{baselineHeight: 0, certifiedPartitionRoots: map[uint64]map[string]string{}})
	return r
}

func TestPorygonV55ActivePredecessorCanExceedProposalTimeoutWithoutFalseFatal(t *testing.T) {
	r := porygonV55RootWaitFixture()
	p := r.porygonPipelineRuntime()
	p.mu.Lock()
	p.blocks[9] = &porygonPipelineBlock{Phase: porygonPipelineOrdered}
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(110 * time.Millisecond)
		paper := r.porygonPaperRuntimeState()
		paper.mu.Lock()
		if paper.certifiedPartitionRoots == nil {
			paper.certifiedPartitionRoots = map[uint64]map[string]string{}
		}
		paper.certifiedPartitionRoots[9] = map[string]string{"s0": "certified-h9-root"}
		paper.mu.Unlock()
	}()
	h, root, err := r.porygonV55AwaitCanonicalBase(ctx, 10, "s0", 20*time.Millisecond)
	if err != nil || h != 9 || root != "certified-h9-root" {
		t.Fatalf("active predecessor false fatal or wrong root: height=%d root=%q err=%v", h, root, err)
	}
}

func TestPorygonV55PredecessorFailurePropagatesInsteadOfTimingOut(t *testing.T) {
	r := porygonV55RootWaitFixture()
	p := r.porygonPipelineRuntime()
	p.mu.Lock()
	p.blocks[9] = &porygonPipelineBlock{Phase: porygonPipelineFailed}
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := r.porygonV55AwaitCanonicalBase(ctx, 10, "s0", 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "preceding execution failed") {
		t.Fatalf("missing predecessor-failure propagation: %v", err)
	}
}

func TestPorygonV55MissingPredecessorStillFailsClosed(t *testing.T) {
	r := porygonV55RootWaitFixture()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, _, err := r.porygonV55AwaitCanonicalBase(ctx, 10, "s0", 40*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "local base timeout") {
		t.Fatalf("missing predecessor unexpectedly accepted: %v", err)
	}
}

func TestPorygonV55ContextCancellationStopsLocalWait(t *testing.T) {
	r := porygonV55RootWaitFixture()
	p := r.porygonPipelineRuntime()
	p.mu.Lock()
	p.blocks[9] = &porygonPipelineBlock{Phase: porygonPipelineOrdered}
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := r.porygonV55AwaitCanonicalBase(ctx, 10, "s0", time.Second)
	if err != context.Canceled {
		t.Fatalf("cancelled predecessor wait must exit: %v", err)
	}
}
