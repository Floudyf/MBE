package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
)

func TestMTWatchFixRetryDelayBounded(t *testing.T) {
	checks := []struct {
		attempts int
		expected time.Duration
	}{
		{-1, 2 * time.Second}, {0, 2 * time.Second},
		{1, 4 * time.Second}, {2, 8 * time.Second},
		{3, 8 * time.Second}, {1000, 8 * time.Second},
	}
	for _, c := range checks {
		if got := statelessVersionAdmissionWatchRetryDelay(c.attempts); got != c.expected {
			t.Fatalf("attempt=%d delay=%s expected=%s", c.attempts, got, c.expected)
		}
	}
}

func TestMTWatchFixP4FilterHonorsBackoff(t *testing.T) {
	_, consumer, key := statelessVersionAdmissionTestRuntimes(t)
	token := statelessVersionAdmissionToken(key, 987)
	reqs := []statelessVersionAdmissionRequirement{{token: token, key: key, version: 987, homeShard: "s0"}}
	for _, test := range []struct {
		retryCount int
		age        time.Duration
		pending    bool
	}{
		{0, 1 * time.Second, true}, {0, 3 * time.Second, false},
		{1, 3 * time.Second, true}, {1, 5 * time.Second, false},
		{2, 7 * time.Second, true}, {2, 9 * time.Second, false},
	} {
		consumer.mu.Lock()
		consumer.stateVersionAdmissionWatches[token] = stateVersionAdmissionWatch{
			Token: token, Key: key, Version: 987, HomeShard: "s0", TargetNode: "n-s0",
			StartedAt: time.Now().Add(-test.age), RetryCount: test.retryCount,
		}
		consumer.mu.Unlock()
		ready := map[string]bool{}
		unknown, _ := consumer.mtP4FilterKnownAdmission(reqs, ready)
		isPending := len(unknown) == 0
		if isPending != test.pending || ready[token] {
			t.Fatalf("count=%d age=%s pending=%t ready=%v", test.retryCount, test.age, isPending, ready)
		}
	}
}

func TestMTWatchFixRetryAndImmediatePublish(t *testing.T) {
	home, consumer, key := statelessVersionAdmissionTestRuntimes(t)
	token := statelessVersionAdmissionToken(key, 1)
	requirement := statelessVersionAdmissionRequirement{
		token: token, key: key, version: 1, homeShard: "s0",
		transaction: statelessVersionAdmissionItem("watch-fix", "s1", key, 2, 1),
	}
	candidate := realblock.Block{BlockHash: "watch-retry-test", Height: 1, ShardID: "s1"}
	now := func() stateVersionAdmissionWatch {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return consumer.stateVersionAdmissionWatches[token]
	}
	expire := func(age time.Duration) {
		consumer.mu.Lock()
		watch := consumer.stateVersionAdmissionWatches[token]
		watch.StartedAt = time.Now().Add(-age)
		consumer.stateVersionAdmissionWatches[token] = watch
		consumer.mu.Unlock()
	}
	call := func() {
		t.Helper()
		ok, err := consumer.ensureStatelessVersionAdmissionWatch(context.Background(), candidate, requirement)
		if err != nil || ok {
			t.Fatalf("unexpected readiness=%v error=%v", ok, err)
		}
	}
	call()
	assertRetries := func(want int64) {
		consumer.mu.Lock()
		got := consumer.runtimeMetricCounts["stateless_version_admission_watch_retry_count"]
		consumer.mu.Unlock()
		if got != want {
			t.Fatalf("retry events=%d expected=%d", got, want)
		}
	}
	assertRetries(0)
	first := now()
	if first.RetryCount != 0 || first.RequestID == "" {
		t.Fatalf("first=%+v", first)
	}
	expire(3 * time.Second)
	call()
	second := now()
	assertRetries(1)
	if second.RetryCount != 1 || first.RequestID != second.RequestID {
		t.Fatalf("second=%+v", second)
	}
	expire(3 * time.Second)
	call() // No retry before four-second deadline.
	if got := now().RetryCount; got != 1 {
		t.Fatalf("premature 4s retry count %d", got)
	}
	assertRetries(1)
	expire(5 * time.Second)
	call()
	if got := now().RetryCount; got != 2 {
		t.Fatalf("expected capped level: %d", got)
	}
	assertRetries(2)
	expire(7 * time.Second)
	call() // No retry before eight-second deadline.
	if got := now().RetryCount; got != 2 {
		t.Fatalf("premature 8s retry count %d", got)
	}
	assertRetries(2)
	expire(9 * time.Second)
	call() // Retry still happens after eight seconds: lost response recoverable.
	if got := now().RetryCount; got != 2 {
		t.Fatalf("cap changed: %d", got)
	}
	assertRetries(3)
	home.publishStateVersion(key, 1, "v1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		consumer.mu.Lock()
		ready := consumer.stateVersionAdmissionReady[token]
		consumer.mu.Unlock()
		if ready {
			ok, err := consumer.ensureStatelessVersionAdmissionWatch(context.Background(), candidate, requirement)
			if err != nil || !ok {
				t.Fatalf("ready cache not consumed: %v %v", ok, err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Home publication did not immediately release pending Watch")
}

func TestMTWatchFixHomeDeduplicatesStableRequestID(t *testing.T) {
	home, _, key := statelessVersionAdmissionTestRuntimes(t)
	request := StateFetchRequest{RequestID: "stable-id", Key: key, RequiredVersion: 77, HomeShard: "s0", ExecutionShard: "s1", Versioned: true}
	home.mu.Lock()
	for i := 0; i < 10; i++ {
		home.registerStateVersionRemoteSubscriptionLocked("n-s1", request)
	}
	waitKey := stateVersionWaitKey(key, 77)
	got := len(home.stateVersionRemoteSubscriptions[waitKey])
	registered := home.runtimeMetricCounts["state_version_subscription_registered_count"]
	home.mu.Unlock()
	if got != 1 || registered != 1 {
		t.Fatalf("duplicate Home subscription: size=%d events=%d", got, registered)
	}
}
