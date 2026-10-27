package v5

// MBE_MT_ONLINE_FIX_V1
// Lightweight runtime performance hint, visible only to the newer online
// router. Never affects PBFT quorum, signature validation, StateReady, or
// commit decisions. One small file per execution-shard leader per second.
import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

func (r *NodeRuntime) mtOnlineWriteTelemetryV1() error {
	if r == nil || !r.node.Leader {
		return nil
	}
	now := time.Now().UnixMilli()
	r.mu.Lock()
	last := r.lastStateFetch
	committedHeight := r.committedHeight
	pendingFetch := len(r.pendingStateFetches)
	pendingVersion := len(r.stateVersionRemoteSubscriptions)
	pendingCommit := len(r.pendingCommits)
	queue := r.blockExecutionProgress.SchedulerQueueLen
	activeTasks := r.blockExecutionProgress.ActiveTaskCount
	requestQueue := len(r.stateFetchTasks)
	r.mu.Unlock()

	// Request-side success is the only valid duration source. The service
	// record does not contain a starting timestamp and must never be used.
	good := last.Stage == "response_received" && last.Error == "" &&
		last.StartedAtMS > 0 && last.FinishedAtMS >= last.StartedAtMS &&
		last.FinishedAtMS <= now && now-last.FinishedAtMS <= 30000 &&
		last.LatencyMS >= 0 && last.LatencyMS <= 60000
	remote := int64(0)
	finished := int64(0)
	if good {
		remote = last.LatencyMS
		if remote < 1 {
			remote = 1
		} // successful sub-ms operation
		finished = last.FinishedAtMS
	}
	if queue < 0 {
		queue = 0
	}
	if activeTasks < 0 {
		activeTasks = 0
	}
	status := map[string]any{
		"schema_version":                           "mbe_metatrack_online_runtime_v1",
		"node_id":                                  r.node.NodeID,
		"execution_shard_id":                       effectiveExecutionShardID(r.node),
		"sampled_at_ms":                            now,
		"committed_height":                         committedHeight,
		"mempool_depth":                            r.pool.Len(),
		"pending_state_fetch_count":                pendingFetch,
		"state_fetch_request_queue_depth":          requestQueue,
		"pending_state_version_subscription_count": pendingVersion,
		"block_execution_progress":                 map[string]any{"scheduler_queue_length": queue, "active_task_count": activeTasks},
		"pending_commit_count":                     pendingCommit,
		"remote_latency_valid":                     good,
		"remote_latency_ms":                        remote,
		"remote_sample_at_ms":                      finished,
	}
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	dir := r.node.DataDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, "metatrack_online_runtime.json")
	tmp := filepath.Join(dir, ".metatrack_online_runtime.tmp")
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
