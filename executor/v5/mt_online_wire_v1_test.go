package v5

// MTOnlineWire_v1: online-sidecar data wiring ONLY. No routing or consensus changes.
import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/mempool"
)

func TestMTOnlineWireV1TelemetryReportsMeasuredQueueAndWorkers(t *testing.T) {
	dir := t.TempDir()
	r := &NodeRuntime{
		node:            NodePlan{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: dir},
		pool:            mempool.New("n0", "s0", mempool.DefaultPolicy(), nil),
		stateFetchTasks: make(chan stateFetchTask, 16),
	}
	r.blockExecutionProgress.ActiveTaskCount = 5
	r.blockExecutionProgress.SchedulerQueueLen = 7
	for i := 0; i < 9; i++ {
		r.stateFetchTasks <- stateFetchTask{}
	}
	if err := r.mtOnlineWriteTelemetryV1(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "metatrack_online_runtime.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatal(err)
	}
	var requestQueue int
	if err := json.Unmarshal(data["state_fetch_request_queue_depth"], &requestQueue); err != nil || requestQueue != 9 {
		t.Fatalf("real request queue expected 9, got %d: %v", requestQueue, err)
	}
	var progress map[string]int
	if err := json.Unmarshal(data["block_execution_progress"], &progress); err != nil {
		t.Fatal(err)
	}
	if progress["active_task_count"] != 5 || progress["scheduler_queue_length"] != 7 {
		t.Fatalf("unexpected progress fields: %+v", progress)
	}
	row, ok := mtOnlineReadStatusV1(path, "s0", "n0", time.Now().UnixMilli())
	if !ok || row.ActiveTasks != 5 || row.RequestQueue != 9 || row.SchedulerQueue != 7 {
		t.Fatalf("real sidecar parser lost measured values: %+v, ok=%v", row, ok)
	}
	// True zero must remain a numeric 0 after queues drain.
	for i := 0; i < 9; i++ {
		<-r.stateFetchTasks
	}
	r.blockExecutionProgress.ActiveTaskCount = 0
	if err := r.mtOnlineWriteTelemetryV1(); err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data["state_fetch_request_queue_depth"], &requestQueue); err != nil || requestQueue != 0 {
		t.Fatalf("true zero not emitted: %d %v", requestQueue, err)
	}
	if err := json.Unmarshal(data["block_execution_progress"], &progress); err != nil || progress["active_task_count"] != 0 {
		t.Fatalf("true active zero missing: %+v %v", progress, err)
	}
}

func TestMTOnlineWireV1LiveFieldsAffectExistingScorer(t *testing.T) {
	now := time.Now().UnixMilli()
	items := []metaTrackIncrementalCandidateV650{{Shard: "s0", ReadyRank: 1, RemoteCost: 2}, {Shard: "s1", ReadyRank: 1, RemoteCost: 2}}
	f := &mtOnlineFeedbackV1{Shards: map[string]mtOnlineShardFeedbackV1{
		"s0": {Shard: "s0", SampledMS: now, RemoteLatencyMS: 7},
		"s1": {Shard: "s1", SampledMS: now, RemoteLatencyMS: 7},
	}}
	winner := func() string {
		ranked, ok := mtOnlineRankCandidatesV1(items, f, now)
		if !ok {
			t.Fatal("fresh feedback unexpectedly rejected")
		}
		if metaTrackIncrementalCandidateLessV650(ranked[1], ranked[0]) {
			return "s1"
		}
		return "s0"
	}
	if got := winner(); got != "s0" {
		t.Fatalf("expected deterministic baseline s0, got %s", got)
	}
	a := f.Shards["s0"]
	a.ActiveTasks = 8
	f.Shards["s0"] = a
	if got := winner(); got != "s0" {
		t.Fatalf("8 active workers should remain masked by two remote accesses, got %s", got)
	}
	a.ActiveTasks = 0
	a.RequestQueue = 8
	f.Shards["s0"] = a
	if got := winner(); got != "s1" {
		t.Fatalf("request queue should shift selection to s1, got %s", got)
	}
	a.RequestQueue = 0
	f.Shards["s0"] = a
	if got := winner(); got != "s0" {
		t.Fatalf("zero backlog should restore stable baseline, got %s", got)
	}
	// 8 active tasks can change choice when there is no larger static term.
	items = []metaTrackIncrementalCandidateV650{{Shard: "s0"}, {Shard: "s1"}}
	a.ActiveTasks = 8
	f.Shards["s0"] = a
	if got := winner(); got != "s1" {
		t.Fatalf("8 active workers should shift zero-static-cost candidate, got %s", got)
	}
}
