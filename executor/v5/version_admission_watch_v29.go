package v5

import (
	"context"
	"fmt"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const statelessVersionAdmissionWatchAccessKind = "state_version_admission_watch"
const statelessVersionAdmissionWatchRetryInterval = 2 * time.Second

type stateVersionAdmissionWatch struct {
	Token      string
	RequestID  string
	Key        string
	Version    uint64
	HomeShard  string
	TargetNode string
	StartedAt  time.Time
}

// selectMetaTrackLivenessSafePBFTProjection deliberately keeps one complete
// signed routing projection per PBFT block. MetaTrack classifies dependencies
// inside one signed micro-batch window; aggregating later projection windows
// into the same consensus-bound block leaves cross-window exact-version edges
// to StateReady only and can create cross-shard head-of-line cycles.
func selectMetaTrackLivenessSafePBFTProjection(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, error) {
	return selectMetaTrackBatchProjection(items, limit, shardID)
}

func (r *NodeRuntime) ensureStatelessVersionAdmissionWatch(ctx context.Context, block realblock.Block, requirement statelessVersionAdmissionRequirement) (bool, error) {
	if requirement.version == 0 {
		return true, nil
	}
	if requirement.homeShard == r.node.ShardID {
		_, ready := r.stateVersionValue(requirement.key, requirement.version)
		return ready, nil
	}
	targetNode := r.leaderID(requirement.homeShard)
	if targetNode == "" {
		return false, fmt.Errorf("stateless version admission home leader missing for %s", requirement.homeShard)
	}
	now := time.Now()
	r.mu.Lock()
	if r.stateVersionAdmissionReady == nil {
		r.stateVersionAdmissionReady = map[string]bool{}
	}
	if r.stateVersionAdmissionWatches == nil {
		r.stateVersionAdmissionWatches = map[string]stateVersionAdmissionWatch{}
	}
	if r.stateVersionAdmissionRequestTokens == nil {
		r.stateVersionAdmissionRequestTokens = map[string]string{}
	}
	if r.stateVersionAdmissionReady[requirement.token] {
		r.incrementRuntimeMetricLocked("stateless_version_admission_watch_cache_hit_count")
		r.mu.Unlock()
		return true, nil
	}
	if current, exists := r.stateVersionAdmissionWatches[requirement.token]; exists {
		if now.Sub(current.StartedAt) < statelessVersionAdmissionWatchRetryInterval {
			r.mu.Unlock()
			return false, nil
		}
		delete(r.stateVersionAdmissionRequestTokens, current.RequestID)
		delete(r.stateVersionAdmissionWatches, requirement.token)
		r.incrementRuntimeMetricLocked("stateless_version_admission_watch_retry_count")
	}
	requestID := stableTextDigest(strings.Join([]string{
		"admission-watch",
		r.node.NodeID,
		requirement.token,
		requirement.homeShard,
	}, "|"))
	watch := stateVersionAdmissionWatch{
		Token: requirement.token, RequestID: requestID, Key: requirement.key,
		Version: requirement.version, HomeShard: requirement.homeShard,
		TargetNode: targetNode, StartedAt: now,
	}
	r.stateVersionAdmissionWatches[requirement.token] = watch
	r.stateVersionAdmissionRequestTokens[requestID] = requirement.token
	r.incrementRuntimeMetricLocked("stateless_version_admission_watch_registered_count")
	r.mu.Unlock()

	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{
		RequestID: requestID, TxID: requirement.transaction.TxID,
		BlockHash: block.BlockHash, Key: requirement.key,
		HomeShard: requirement.homeShard, ExecutionShard: r.node.ShardID,
		AccessKind:      statelessVersionAdmissionWatchAccessKind,
		RequiredVersion: requirement.version, Versioned: true,
	})
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
	if err != nil {
		r.clearStatelessVersionAdmissionWatch(requirement.token, requestID)
		return false, err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		r.clearStatelessVersionAdmissionWatch(requirement.token, requestID)
		return false, err
	}
	// In focused/in-memory transports an already-ready response may be delivered
	// synchronously by the send hook. Re-check the cache so that path does not
	// require an artificial extra producer tick.
	r.mu.Lock()
	ready := r.stateVersionAdmissionReady[requirement.token]
	r.mu.Unlock()
	return ready, nil
}

func (r *NodeRuntime) clearStatelessVersionAdmissionWatch(token, requestID string) {
	r.mu.Lock()
	if r.stateVersionAdmissionWatches == nil || r.stateVersionAdmissionRequestTokens == nil {
		r.mu.Unlock()
		return
	}
	if current, ok := r.stateVersionAdmissionWatches[token]; ok && current.RequestID == requestID {
		delete(r.stateVersionAdmissionWatches, token)
	}
	delete(r.stateVersionAdmissionRequestTokens, requestID)
	r.mu.Unlock()
}

// handleStateVersionAdmissionWatchResponse consumes only responses correlated
// with the pre-consensus admission watch table. Normal StateReady fetch waiters
// continue through handleStateFetchResponse unchanged.
func (r *NodeRuntime) handleStateVersionAdmissionWatchResponse(response StateFetchResponse) bool {
	r.mu.Lock()
	if r.stateVersionAdmissionRequestTokens == nil {
		r.mu.Unlock()
		return false
	}
	token, watched := r.stateVersionAdmissionRequestTokens[response.RequestID]
	if !watched {
		r.mu.Unlock()
		return false
	}
	watch, exists := r.stateVersionAdmissionWatches[token]
	valid := exists && watch.RequestID == response.RequestID && response.Success && response.Versioned && response.Key == watch.Key && response.HomeShard == watch.HomeShard && response.StateVersion == watch.Version
	delete(r.stateVersionAdmissionRequestTokens, response.RequestID)
	if exists && watch.RequestID == response.RequestID {
		delete(r.stateVersionAdmissionWatches, token)
	}
	if valid {
		if r.stateVersionAdmissionReady == nil {
			r.stateVersionAdmissionReady = map[string]bool{}
		}
		r.stateVersionAdmissionReady[token] = true
		r.incrementRuntimeMetricLocked("stateless_version_admission_watch_wakeup_count")
		r.lastProgressAt = time.Now().UnixMilli()
	} else {
		r.incrementRuntimeMetricLocked("stateless_version_admission_watch_invalid_response_count")
	}
	r.mu.Unlock()
	return true
}

func (r *NodeRuntime) stateVersionRemoteSubscriptionCountLocked() int {
	total := 0
	for _, bucket := range r.stateVersionRemoteSubscriptions {
		total += len(bucket)
	}
	return total
}
