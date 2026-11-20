package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
)

func TestMTAdmitP4FilterKnownAndExpiry(t *testing.T) {
	_, s1, key := statelessVersionAdmissionTestRuntimes(t)
	remoteToken := statelessVersionAdmissionToken(key, 2)
	localToken := statelessVersionAdmissionToken(key, 3)
	pendingToken := statelessVersionAdmissionToken(key, 4)
	missingToken := statelessVersionAdmissionToken("m.missing", 7)
	s1.mu.Lock()
	s1.stateVersionAdmissionReady[remoteToken] = true
	s1.stateVersionAdmissionWatches[pendingToken] = stateVersionAdmissionWatch{Token: pendingToken, StartedAt: time.Now()}
	s1.mu.Unlock()
	s1.publishStateVersion(key, 3, "v3")
	reqs := []statelessVersionAdmissionRequirement{
		{token: remoteToken, key: key, version: 2, homeShard: "s0"},
		{token: localToken, key: key, version: 3, homeShard: "s1"},
		{token: pendingToken, key: key, version: 4, homeShard: "s0"},
		{token: missingToken, key: "m.missing", version: 7, homeShard: "s0"},
	}
	ready := map[string]bool{}
	unknown, notReady := s1.mtP4FilterKnownAdmission(reqs, ready)
	if len(unknown) != 1 || unknown[0].token != missingToken || notReady != 1 {
		t.Fatalf("unexpected pending/notReady: %+v %d", unknown, notReady)
	}
	if !ready[remoteToken] || !ready[localToken] || ready[pendingToken] {
		t.Fatalf("wrong fast readiness: %+v", ready)
	}
	// Expired watch must still go through the unchanged retry path.
	s1.mu.Lock()
	entry := s1.stateVersionAdmissionWatches[pendingToken]
	entry.StartedAt = time.Now().Add(-3 * time.Second)
	s1.stateVersionAdmissionWatches[pendingToken] = entry
	s1.mu.Unlock()
	ready = map[string]bool{}
	unknown, notReady = s1.mtP4FilterKnownAdmission(reqs, ready)
	if len(unknown) != 2 || notReady != 0 {
		t.Fatalf("expired watch suppressed retry: %+v, notReady=%d", unknown, notReady)
	}
}

func TestMTAdmitP4ProbeLogicalCounterUnchanged(t *testing.T) {
	_, s1, key := statelessVersionAdmissionTestRuntimes(t)
	readyToken := statelessVersionAdmissionToken(key, 1)
	pendingToken := statelessVersionAdmissionToken(key, 2)
	s1.mu.Lock()
	s1.stateVersionAdmissionReady[readyToken] = true
	s1.stateVersionAdmissionWatches[pendingToken] = stateVersionAdmissionWatch{Token: pendingToken, StartedAt: time.Now()}
	s1.mu.Unlock()
	reqs := map[string]statelessVersionAdmissionRequirement{
		readyToken:   {token: readyToken, key: key, version: 1, homeShard: "s0"},
		pendingToken: {token: pendingToken, key: key, version: 2, homeShard: "s0"},
	}
	block := realblock.Block{BlockHash: "mt-p4", Height: 1, ShardID: "s1"}
	for i := 0; i < 3; i++ {
		got, err := s1.probeStatelessVersionAdmissionRequirements(context.Background(), block, reqs)
		if err != nil || !got[readyToken] || got[pendingToken] {
			t.Fatalf("round=%d readiness=%v error=%v", i, got, err)
		}
	}
	s1.mu.Lock()
	checks := s1.runtimeMetricCounts["stateless_version_admission_probe_count"]
	unready := s1.runtimeMetricCounts["stateless_version_admission_probe_not_ready_count"]
	hits := s1.runtimeMetricCounts["stateless_version_admission_watch_cache_hit_count"]
	s1.mu.Unlock()
	if checks != 6 || unready != 3 || hits != 3 {
		t.Fatalf("logical counters changed: probes=%d notReady=%d cacheHits=%d", checks, unready, hits)
	}
}
