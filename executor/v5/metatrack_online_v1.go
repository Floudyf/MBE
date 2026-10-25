package v5

// MBE_MT_ONLINE_V1: client-side, non-blocking read-only feedback + joint
// version/state/worker placement. No PBFT, Home ownership, StateReady or
// version-signature semantics are changed by this file.
import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const mtOnlinePolicyV1 = "online_version_state_ready_v1"
const mtOnlinePollV1 = 500 * time.Millisecond
const mtOnlineFreshV1 = 3 * time.Second // lightweight runtime sidecar emitted every 1s
const mtOnlineStatusMaxV1 = 64 << 10    // MBE_MT_ONLINE_FIX_V1 compact telemetry, no tx-id arrays

type mtOnlineShardFeedbackV1 struct {
	Shard           string
	Node            string
	SampledMS       int64
	RemoteSampleMS  int64
	CommittedHeight int64
	MempoolDepth    int
	ActiveTasks     int
	SchedulerQueue  int
	PendingFetch    int
	RequestQueue    int
	PendingVersion  int
	RemoteLatencyMS float64
	BlockServiceMS  float64
}
type mtOnlineFeedbackV1 struct {
	SampledMS int64
	Shards    map[string]mtOnlineShardFeedbackV1
}

func (f *mtOnlineFeedbackV1) valid(shard string, nowMS int64) (mtOnlineShardFeedbackV1, bool) {
	if f == nil || f.Shards == nil {
		return mtOnlineShardFeedbackV1{}, false
	}
	row, ok := f.Shards[shard]
	if !ok || row.SampledMS <= 0 || nowMS < row.SampledMS || nowMS-row.SampledMS > mtOnlineFreshV1.Milliseconds() {
		return mtOnlineShardFeedbackV1{}, false
	}
	return row, true
}

// This sampler only reads status produced by real node processes. Sampling
// never blocks submission and corrupt/partially-written status is ignored.
// Leaders only: never sum four PBFT replicas into a fake execution queue.
type mtOnlineSamplerV1 struct {
	mu       sync.RWMutex
	nodes    []NodePlan
	feedback mtOnlineFeedbackV1
	stop     chan struct{}
	done     chan struct{}
}

func newMTOnlineSamplerV1(nodes []NodePlan) *mtOnlineSamplerV1 {
	s := &mtOnlineSamplerV1{nodes: append([]NodePlan(nil), nodes...), stop: make(chan struct{}), done: make(chan struct{})}
	s.feedback.Shards = map[string]mtOnlineShardFeedbackV1{}
	go func() {
		defer close(s.done)
		s.poll()
		ticker := time.NewTicker(mtOnlinePollV1)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.poll()
			}
		}
	}()
	return s
}
func (s *mtOnlineSamplerV1) close() {
	if s == nil {
		return
	}
	close(s.stop)
	<-s.done
}
func (s *mtOnlineSamplerV1) snapshot() *mtOnlineFeedbackV1 {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := &mtOnlineFeedbackV1{SampledMS: s.feedback.SampledMS, Shards: make(map[string]mtOnlineShardFeedbackV1, len(s.feedback.Shards))}
	for k, v := range s.feedback.Shards {
		out.Shards[k] = v
	}
	return out
}
func mtOnlineJSONIntV1(m map[string]json.RawMessage, k string) int {
	var i int
	_ = json.Unmarshal(m[k], &i)
	if i < 0 {
		return 0
	}
	return i
}
func mtOnlineJSONInt64V1(m map[string]json.RawMessage, k string) int64 {
	var i int64
	_ = json.Unmarshal(m[k], &i)
	if i < 0 {
		return 0
	}
	return i
}
func mtOnlineJSONFloatV1(m map[string]json.RawMessage, k string) float64 {
	var v float64
	_ = json.Unmarshal(m[k], &v)
	if v < 0 || math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return v
}
func mtOnlineReadStatusV1(path, shard, node string, nowMS int64) (mtOnlineShardFeedbackV1, bool) {
	info, err := os.Stat(path)
	if err != nil || info.Size() <= 0 || info.Size() > mtOnlineStatusMaxV1 {
		return mtOnlineShardFeedbackV1{}, false
	}
	// File mtime is not used as evidence of a successful operation; only for
	// snapshot freshness. Reject stale artifacts from a previous run.
	if age := time.Since(info.ModTime()); age < -time.Second || age > mtOnlineFreshV1 {
		return mtOnlineShardFeedbackV1{}, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return mtOnlineShardFeedbackV1{}, false
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(b, &top) != nil {
		return mtOnlineShardFeedbackV1{}, false
	}
	var actualShard string
	_ = json.Unmarshal(top["execution_shard_id"], &actualShard)
	if actualShard != "" && actualShard != shard {
		return mtOnlineShardFeedbackV1{}, false
	}
	// Use the node's real publication timestamp, not the moment the client read it.
	sampledMS := mtOnlineJSONInt64V1(top, "sampled_at_ms")
	if sampledMS <= 0 || sampledMS > nowMS+1000 || nowMS-sampledMS > mtOnlineFreshV1.Milliseconds() {
		return mtOnlineShardFeedbackV1{}, false
	}
	var actualNode string
	_ = json.Unmarshal(top["node_id"], &actualNode)
	if actualNode != node {
		return mtOnlineShardFeedbackV1{}, false
	}
	v := mtOnlineShardFeedbackV1{Shard: shard, Node: node, SampledMS: sampledMS,
		CommittedHeight: mtOnlineJSONInt64V1(top, "committed_height"),
		MempoolDepth:    mtOnlineJSONIntV1(top, "mempool_depth"),
		PendingFetch:    mtOnlineJSONIntV1(top, "pending_state_fetch_count"),
		RequestQueue:    mtOnlineJSONIntV1(top, "state_fetch_request_queue_depth"),
		PendingVersion:  mtOnlineJSONIntV1(top, "pending_state_version_subscription_count")}
	var prog map[string]json.RawMessage
	if json.Unmarshal(top["block_execution_progress"], &prog) == nil {
		v.ActiveTasks = mtOnlineJSONIntV1(prog, "active_task_count")
		v.SchedulerQueue = mtOnlineJSONIntV1(prog, "scheduler_queue_length")
	}
	// Completed requester-side RPCs only. Never infer a duration from
	// last_state_fetch_service: its StartedAtMS is never populated.
	// Sub-millisecond completed operations are rounded up to 1ms because
	// the runtime stores latency as integer milliseconds.
	var remoteValid bool
	_ = json.Unmarshal(top["remote_latency_valid"], &remoteValid)
	if remoteValid {
		latency := mtOnlineJSONFloatV1(top, "remote_latency_ms")
		finished := mtOnlineJSONInt64V1(top, "remote_sample_at_ms")
		if finished > 0 && finished <= sampledMS && sampledMS-finished <= 30_000 && latency <= 60_000 {
			if latency < 1 {
				latency = 1
			}
			v.RemoteLatencyMS = latency
			v.RemoteSampleMS = finished
		}
	}
	// Do not manufacture per-version readiness; this is aggregate telemetry.
	return v, true
}
func (s *mtOnlineSamplerV1) poll() {
	now := time.Now().UnixMilli()
	fresh := map[string]mtOnlineShardFeedbackV1{}
	for _, n := range s.nodes {
		if !n.Leader {
			continue
		}
		shard := effectiveExecutionShardID(n)
		if shard == "" {
			continue
		}
		path := filepath.Join(n.DataDir, "metatrack_online_runtime.json")
		if v, ok := mtOnlineReadStatusV1(path, shard, n.NodeID, now); ok {
			fresh[shard] = v
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Drop missing/stale shards; never silently retain a previous-run sample.
	s.feedback = mtOnlineFeedbackV1{SampledMS: now, Shards: fresh}
}

// The estimate is expressed in relative milliseconds using completed remote
// service telemetry. Without a fresh per-shard observation, return false and
// run the unchanged legacy five-criterion selector. Version readiness stays
// solely under the execution-side exact StateReady oracle.
func mtOnlineRankCandidatesV1(items []metaTrackIncrementalCandidateV650, f *mtOnlineFeedbackV1, nowMS int64) ([]metaTrackIncrementalCandidateV650, bool) {
	if len(items) == 0 {
		return items, false
	}
	observations := make([]mtOnlineShardFeedbackV1, len(items))
	for i, c := range items {
		v, ok := f.valid(c.Shard, nowMS)
		if !ok {
			return items, false
		}
		observations[i] = v
	}
	// Use common observed remote service scale, no dataset-specific training.
	remoteSamples := []float64{}
	for _, v := range observations {
		if v.RemoteLatencyMS > 0 {
			remoteSamples = append(remoteSamples, v.RemoteLatencyMS)
		}
	}
	if len(remoteSamples) == 0 {
		return items, false
	}
	sort.Float64s(remoteSamples)
	remoteMS := remoteSamples[len(remoteSamples)/2]
	if remoteMS < 1 {
		remoteMS = 1
	}
	// Conservative surrogate: execution service is not currently recorded per
	// tx in the status file. One remote service interval is the sole time unit,
	// not a claimed measured worker execution duration.
	execMS := remoteMS
	ranked := append([]metaTrackIncrementalCandidateV650(nil), items...)
	for i, c := range items {
		st := observations[i]
		// Version rank is a dependency-depth proxy, NOT confirmed per-key ready.
		dependencyReady := float64(c.ReadyRank)*execMS + float64(c.ExactCross)*remoteMS
		// Fetch requests may overlap. In-flight fetch/subscriber backlog is a
		// conservative aggregate proxy; never interpreted as safety readiness.
		stateReady := float64(c.RemoteCost+st.PendingFetch+st.RequestQueue+st.PendingVersion) * remoteMS
		workers := float64(st.MempoolDepth+st.SchedulerQueue+st.ActiveTasks) * execMS / 8.0
		predictedFinish := math.Max(dependencyReady, math.Max(stateReady, workers)) + execMS
		if predictedFinish < 0 || math.IsNaN(predictedFinish) || math.IsInf(predictedFinish, 0) {
			return items, false
		}
		scaled := predictedFinish * 1000
		if scaled > float64(int(^uint(0)>>1)/4) {
			return items, false
		}
		ranked[i].WorstRank = int(math.Round(scaled))
		// Deterministic tie-break: actual remote accesses, then exact-cross,
		// then prior rank and shard ID from the legacy comparator.
		ranked[i].RankSum = c.RemoteCost*1000 + c.ExactCross
	}
	return ranked, true
}

func mtOnlinePlacementReasonV1(enabled, used bool, c metaTrackIncrementalCandidateV650) string {
	if enabled {
		if used {
			return fmt.Sprintf("online_joint_ready:depth=%d:cross=%d:remote=%d:load=%d:cost_us=%d", c.ReadyRank, c.ExactCross, c.RemoteCost, c.Load, c.WorstRank)
		}
		return fmt.Sprintf("online_feedback_fallback:depth=%d:cross=%d:remote=%d:load=%d", c.ReadyRank, c.ExactCross, c.RemoteCost, c.Load)
	}
	return fmt.Sprintf("incremental_exact_ready_balanced:ready_rank=%d:exact_cross=%d:coaccess=%d:remote=%d:load=%d:worst_rank=%d:rank_sum=%d", c.ReadyRank, c.ExactCross, c.CoaccessLocality, c.RemoteCost, c.Load, c.WorstRank, c.RankSum)
}
