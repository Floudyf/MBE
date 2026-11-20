package v5

import "time"

// mtP4FilterKnownAdmission batches lookups of monotonic success cache,
// local exact versions, and unexpired remote-watch entries under one lock.
// Only unknown/expired requirements proceed to the original probe/retry path.
// This does not change the exact-version predicate, candidate closure, FIFO
// window, PBFT, or Block-STM. Watch retries use bounded 2/4/8s backoff.
// The TxAllo source-local write-behind mode keeps its full original path.
func (r *NodeRuntime) mtP4FilterKnownAdmission(items []statelessVersionAdmissionRequirement, ready map[string]bool) ([]statelessVersionAdmissionRequirement, int64) {
	if r.methodPreservingExactVersionTransportEnabled() {
		return items, 0
	}
	var pending []statelessVersionAdmissionRequirement
	now := time.Now()
	r.mu.Lock()
	cacheHits := int64(0)
	notReady := int64(0)
	for _, item := range items {
		switch {
		case item.version == 0:
			ready[item.token] = true
		case item.homeShard == r.node.ShardID:
			_, ok := r.stateVersionValueLocked(item.key, item.version)
			ready[item.token] = ok
			if !ok {
				notReady++
			}
		case r.stateVersionAdmissionReady[item.token]:
			ready[item.token] = true
			cacheHits++
		case r.mtP4WatchStillPendingLocked(item.token, now):
			ready[item.token] = false
			notReady++
		default:
			pending = append(pending, item)
		}
	}
	if cacheHits > 0 {
		r.runtimeMetricCounts["stateless_version_admission_watch_cache_hit_count"] += cacheHits
	}
	r.mu.Unlock()
	return pending, notReady
}

func (r *NodeRuntime) mtP4WatchStillPendingLocked(token string, now time.Time) bool {
	watch, exists := r.stateVersionAdmissionWatches[token]
	return exists && now.Sub(watch.StartedAt) < statelessVersionAdmissionWatchRetryDelay(watch.RetryCount)
}
