package v5

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiveAndFallback(t *testing.T) {
	now := time.Now().UnixMilli()
	items := []metaTrackIncrementalCandidateV650{{Shard: "s0", ReadyRank: 0, ExactCross: 0, RemoteCost: 9}, {Shard: "s1", ReadyRank: 1, ExactCross: 1, RemoteCost: 1}}
	if _, ok := mtOnlineRankCandidatesV1(items, nil, now); ok {
		t.Fatal("missing feedback must fallback")
	}
	f := &mtOnlineFeedbackV1{Shards: map[string]mtOnlineShardFeedbackV1{
		"s0": {Shard: "s0", SampledMS: now, RemoteLatencyMS: 3},
		"s1": {Shard: "s1", SampledMS: now, RemoteLatencyMS: 3}}}
	rows, ok := mtOnlineRankCandidatesV1(items, f, now)
	if !ok {
		t.Fatal("fresh feedback not used")
	}
	if rows[0].WorstRank <= rows[1].WorstRank {
		t.Fatalf("expected remote-heavy s0 worse than s1: %+v", rows)
	}
	if _, ok := mtOnlineRankCandidatesV1(items, f, now+mtOnlineFreshV1.Milliseconds()+1); ok {
		t.Fatal("stale feedback accepted")
	}
	if !strings.Contains(mtOnlinePlacementReasonV1(true, true, rows[1]), "online_joint_ready") {
		t.Fatal("missing routed evidence")
	}
	if !strings.Contains(mtOnlinePlacementReasonV1(true, false, rows[0]), "fallback") {
		t.Fatal("missing fallback evidence")
	}
}
func TestSnapshotIsReadOnlyAndFailClosed(t *testing.T) {
	d := t.TempDir()
	now := time.Now().UnixMilli()
	p := filepath.Join(d, "metatrack_online_runtime.json") // MBE_MT_ONLINE_FIX_V1
	status := map[string]interface{}{"execution_shard_id": "s1", "committed_height": 8, "mempool_depth": 7, "pending_state_fetch_count": 2, "pending_state_version_subscription_count": 3, "node_id": "n4", "sampled_at_ms": now, "remote_latency_valid": true, "remote_latency_ms": 7.0, "remote_sample_at_ms": now}
	b, _ := json.Marshal(status)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	v, ok := mtOnlineReadStatusV1(p, "s1", "n4", time.Now().UnixMilli())
	if !ok || v.RemoteLatencyMS != 7 || v.PendingFetch != 2 {
		t.Fatalf("bad status %+v %v", v, ok)
	}
	if _, ok := mtOnlineReadStatusV1(p, "s0", "n4", time.Now().UnixMilli()); ok {
		t.Fatal("wrong shard accepted")
	}
	if err := os.WriteFile(p, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := mtOnlineReadStatusV1(p, "s1", "n4", time.Now().UnixMilli()); ok {
		t.Fatal("partial JSON accepted")
	}
}

func TestOnlineFixRejectsServiceOnlyAndStaleSample(t *testing.T) {
	p := filepath.Join(t.TempDir(), "metatrack_online_runtime.json")
	now := time.Now().UnixMilli()
	serviceOnly := map[string]any{"node_id": "n4", "execution_shard_id": "s1", "sampled_at_ms": now, "last_state_fetch_service": map[string]any{"finished_at_ms": now, "stage": "response_sent"}}
	b, _ := json.Marshal(serviceOnly)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	x, ok := mtOnlineReadStatusV1(p, "s1", "n4", now)
	if !ok || x.RemoteLatencyMS != 0 {
		t.Fatalf("service-only fabricated RTT: %+v %v", x, ok)
	}
	stale := map[string]any{"node_id": "n4", "execution_shard_id": "s1", "sampled_at_ms": now - 10000, "remote_latency_valid": true, "remote_latency_ms": 7, "remote_sample_at_ms": now - 10000}
	b, _ = json.Marshal(stale)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok = mtOnlineReadStatusV1(p, "s1", "n4", now); ok {
		t.Fatal("stale publication accepted")
	}
}
