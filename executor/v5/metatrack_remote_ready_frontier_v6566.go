package v5

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	metaTrackRemoteReadyPolicyV6566    = "signed_transaction_remote_ready_frontier_v6566"
	metaTrackRemoteReadyWatchKindV6566 = "metatrack_frontier_remote_exact_ready_v6566"
	metaTrackRemoteReadyRequestV6566   = "mtfr6566:"
)

type metaTrackExactVersionReadyV6566 func(tx.SignedTransaction, tx.StateVersionDependency) bool

type metaTrackRemoteReadyKeyV6566 struct {
	key     string
	version uint64
}

type metaTrackRemoteReadyRuntimeStateV6566 struct {
	mu       sync.Mutex
	ready    map[metaTrackRemoteReadyKeyV6566]bool
	lastSent map[metaTrackRemoteReadyKeyV6566]time.Time
}

var (
	metaTrackRemoteReadyRuntimeCacheV6566 sync.Map // *NodeRuntime -> *metaTrackRemoteReadyRuntimeStateV6566
	metaTrackRemoteReadySendSlotsV6566    = make(chan struct{}, stateFetchWorkerCount)
)

func metaTrackRemoteReadyStateV6566(r *NodeRuntime) *metaTrackRemoteReadyRuntimeStateV6566 {
	raw, _ := metaTrackRemoteReadyRuntimeCacheV6566.LoadOrStore(r, &metaTrackRemoteReadyRuntimeStateV6566{
		ready:    map[metaTrackRemoteReadyKeyV6566]bool{},
		lastSent: map[metaTrackRemoteReadyKeyV6566]time.Time{},
	})
	return raw.(*metaTrackRemoteReadyRuntimeStateV6566)
}

func metaTrackExactAccessForDependencyV6566(item tx.SignedTransaction, key string) bool {
	for _, access := range item.AccessList {
		if access.Key == key {
			return requiresExactStateValue(access)
		}
	}
	return false
}

func metaTrackLocalExecutionPredecessorsV6566(item tx.SignedTransaction) map[uint64]bool {
	out := map[uint64]bool{}
	if item.ExecutionRouting == nil {
		return out
	}
	for _, ordinal := range item.ExecutionRouting.ConsensusExecutionPredecessorOrdinals {
		if ordinal > 0 {
			out[ordinal] = true
		}
	}
	return out
}

// metaTrackRemoteExactDependenciesReadyV6566 gates only exact-value predecessors
// that are not represented by the signed same-execution-shard predecessor set.
// Such predecessors must have a published exact version before the consumer can
// enter PBFT. This converts StateReady from an unbounded post-consensus wait into
// a proposal-time non-blocking readiness filter while preserving cross-RouteBatch
// packing for genuinely independent transactions.
func metaTrackRemoteExactDependenciesReadyV6566(item tx.SignedTransaction, ready metaTrackExactVersionReadyV6566) bool {
	if item.ExecutionRouting == nil {
		return false
	}
	local := metaTrackLocalExecutionPredecessorsV6566(item)
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.Key == "" || dependency.RequiredVersion == 0 || !metaTrackExactAccessForDependencyV6566(item, dependency.Key) {
			continue
		}
		if local[dependency.RequiredVersion] {
			continue
		}
		if ready == nil || !ready(item, dependency) {
			return false
		}
	}
	return true
}

// Leader-side selection for v6.5.6.6. It is the v6.5.6 transaction frontier
// plus one liveness rule: an external exact-version predecessor must already be
// published. The readiness callback is non-blocking; a miss defers the consumer
// and lets the scan continue so later independent work can still fill the block.
func selectMetaTrackRemoteReadyTransactionFrontierV6566(items []tx.SignedTransaction, limit int, shardID string, _ *mempool.Mempool, ready metaTrackExactVersionReadyV6566) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.6 requires positive block limit")
	}
	ordered := append([]tx.SignedTransaction(nil), items...)
	for _, item := range ordered {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.6 missing execution routing")
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].ExecutionRouting.RoutingOrdinal < ordered[j].ExecutionRouting.RoutingOrdinal
	})
	active := map[uint64]bool{}
	for _, item := range ordered {
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, nil, nil, err
		}
		if item.ExecutionRouting.ExecutionShard != shardID || item.ExecutionRouting.ConsensusExecutionDepth <= 0 {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.6 invalid signed transaction-frontier metadata")
		}
		active[item.ExecutionRouting.RoutingOrdinal] = true
	}

	selectedOrd := map[uint64]bool{}
	depth := map[uint64]int{}
	selected := make([]tx.SignedTransaction, 0, limit)
	selectedIDs := map[string]bool{}
	for _, item := range ordered {
		if len(selected) >= limit {
			break
		}
		routing := item.ExecutionRouting
		blocked := false
		for _, pred := range routing.ConsensusOrderingPredecessorOrdinals {
			if active[pred] && !selectedOrd[pred] {
				blocked = true
				break
			}
		}
		candidateDepth := 1
		if !blocked {
			for _, pred := range routing.ConsensusExecutionPredecessorOrdinals {
				if active[pred] && !selectedOrd[pred] {
					blocked = true
					break
				}
				if selectedOrd[pred] && depth[pred]+1 > candidateDepth {
					candidateDepth = depth[pred] + 1
				}
			}
		}
		if blocked || candidateDepth > routing.ConsensusExecutionDepth {
			continue
		}
		if !metaTrackRemoteExactDependenciesReadyV6566(item, ready) {
			continue
		}
		ordinal := routing.RoutingOrdinal
		selectedOrd[ordinal] = true
		depth[ordinal] = candidateDepth
		selected = append(selected, item)
		selectedIDs[item.TxID] = true
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.6.6 found no remote-ready dependency-closed transaction frontier")
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if !selectedIDs[item.TxID] {
			deferred = append(deferred, item)
		}
	}
	summaries, err := metaTrackProjectionSummariesV656(realblock.Block{ShardID: shardID, TxList: selected})
	return selected, deferred, summaries, err
}

func metaTrackRemoteReadyRequestIDV6566(nodeID, executionShard, homeShard, key string, version uint64) string {
	return metaTrackRemoteReadyRequestV6566 + stableTextDigest(strings.Join([]string{
		nodeID, executionShard, homeShard, key, fmt.Sprint(version),
	}, "|"))
}

func (r *NodeRuntime) metaTrackExactVersionReadyV6566(item tx.SignedTransaction, dependency tx.StateVersionDependency) bool {
	if r == nil || dependency.Key == "" || dependency.RequiredVersion == 0 {
		return dependency.RequiredVersion == 0
	}
	shardIDs := r.shardIDs()
	homeShard := r.stateHomeShardForKey(dependency.Key, shardIDs)
	if homeShard == "" {
		return false
	}
	if homeShard == r.node.ShardID {
		r.mu.Lock()
		_, ready := r.stateVersionValueLocked(dependency.Key, dependency.RequiredVersion)
		r.mu.Unlock()
		return ready
	}

	key := metaTrackRemoteReadyKeyV6566{key: dependency.Key, version: dependency.RequiredVersion}
	state := metaTrackRemoteReadyStateV6566(r)
	state.mu.Lock()
	ready := state.ready[key]
	lastSent := state.lastSent[key]
	state.mu.Unlock()
	if ready {
		return true
	}

	// Reuse the configured block interval as the subscription refresh cadence;
	// no new empirical timing threshold is introduced by this closure.
	retryAfter := r.blockInterval()
	if retryAfter <= 0 {
		retryAfter = time.Millisecond
	}
	now := time.Now()
	if !lastSent.IsZero() && now.Sub(lastSent) < retryAfter {
		return false
	}
	select {
	case metaTrackRemoteReadySendSlotsV6566 <- struct{}{}:
		state.mu.Lock()
		if state.ready[key] {
			state.mu.Unlock()
			<-metaTrackRemoteReadySendSlotsV6566
			return true
		}
		state.lastSent[key] = now
		state.mu.Unlock()
		go func() {
			defer func() { <-metaTrackRemoteReadySendSlotsV6566 }()
			if err := r.sendMetaTrackRemoteReadyWatchV6566(item, dependency, homeShard); err != nil {
				state.mu.Lock()
				// Permit a later proposal attempt to retry immediately after a failed send.
				delete(state.lastSent, key)
				state.mu.Unlock()
				r.incrementFullLocalityMetric("metatrack_remote_ready_watch_send_error_v6566_count", 1)
			}
		}()
	default:
		// Backpressure is fail-closed: defer this consumer and let a later proposal
		// retry instead of blocking the proposer or creating unbounded goroutines.
	}
	return false
}

func (r *NodeRuntime) sendMetaTrackRemoteReadyWatchV6566(item tx.SignedTransaction, dependency tx.StateVersionDependency, homeShard string) error {
	targetNode := r.leaderID(homeShard)
	if targetNode == "" {
		return fmt.Errorf("metatrack v6.5.6.6 remote-ready home leader missing for %s", homeShard)
	}
	r.mu.Lock()
	ctx := r.stateFetchWorkerContext
	r.mu.Unlock()
	if ctx == nil {
		return fmt.Errorf("metatrack v6.5.6.6 state fetch service is not running")
	}
	requestID := metaTrackRemoteReadyRequestIDV6566(r.node.NodeID, r.node.ShardID, homeShard, dependency.Key, dependency.RequiredVersion)
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{
		RequestID:       requestID,
		TxID:            item.TxID,
		BlockHash:       metaTrackRemoteReadyPolicyV6566,
		Key:             dependency.Key,
		HomeShard:       homeShard,
		ExecutionShard:  r.node.ShardID,
		AccessKind:      metaTrackRemoteReadyWatchKindV6566,
		RequiredVersion: dependency.RequiredVersion,
		Versioned:       true,
	})
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, 0, 0, 0, request)
	if err != nil {
		return err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		return err
	}
	r.incrementFullLocalityMetric("metatrack_remote_ready_watch_sent_v6566_count", 1)
	return nil
}

func metaTrackRemoteReadyResponseValidV6566(nodeID, executionShard, expectedHome string, response StateFetchResponse) bool {
	if response.Key == "" || response.StateVersion == 0 || expectedHome == "" {
		return false
	}
	expectedRequestID := metaTrackRemoteReadyRequestIDV6566(nodeID, executionShard, expectedHome, response.Key, response.StateVersion)
	return response.Success && response.Versioned && response.BlockHash == metaTrackRemoteReadyPolicyV6566 &&
		response.ExecutionShard == executionShard && response.HomeShard == expectedHome && response.RequestID == expectedRequestID
}

// Called before the normal request waiter dispatch. v6.5.6.6 watch responses do
// not own a blocking waiter; they only advance the local proposal-readiness cache.
func (r *NodeRuntime) noteMetaTrackRemoteReadyResponseV6566(response StateFetchResponse) bool {
	if r == nil || !strings.HasPrefix(response.RequestID, metaTrackRemoteReadyRequestV6566) {
		return false
	}
	expectedHome := r.stateHomeShardForKey(response.Key, r.shardIDs())
	if !metaTrackRemoteReadyResponseValidV6566(r.node.NodeID, r.node.ShardID, expectedHome, response) {
		r.incrementFullLocalityMetric("metatrack_remote_ready_watch_response_error_v6566_count", 1)
		return true
	}
	key := metaTrackRemoteReadyKeyV6566{key: response.Key, version: response.StateVersion}
	state := metaTrackRemoteReadyStateV6566(r)
	state.mu.Lock()
	wasReady := state.ready[key]
	state.ready[key] = true
	delete(state.lastSent, key)
	state.mu.Unlock()
	if !wasReady {
		r.incrementFullLocalityMetric("metatrack_remote_ready_version_observed_v6566_count", 1)
		r.signalMetaTrackDurableProposalWakeupV654()
	}
	return true
}
