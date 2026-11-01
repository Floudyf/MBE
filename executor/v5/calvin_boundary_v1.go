package v5

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
)

// Calvin H reads the immutable home-partition state at H-1.  This is separate
// from RequiredVersion, which is zero for a key with no writer inside H.
// Only Stateless Calvin emits/consumes this access-kind namespace.
const calvinBoundaryAccessPrefixV1 = "calvin_boundary_h:"

func calvinBoundaryAccessKindV1(predecessor uint64) string {
	return calvinBoundaryAccessPrefixV1 + strconv.FormatUint(predecessor, 10)
}

func calvinBoundaryRequiredHeightV1(kind string) (uint64, bool) {
	if !strings.HasPrefix(kind, calvinBoundaryAccessPrefixV1) {
		return 0, false
	}
	h, err := strconv.ParseUint(strings.TrimPrefix(kind, calvinBoundaryAccessPrefixV1), 10, 64)
	return h, err == nil
}

// Called solely by the Calvin-gated hook in commitOnce, while its stable
// stateBefore is still the pre-H snapshot.  Never sample the live DB on fetch.
func (r *NodeRuntime) primeCalvinBoundarySnapshotV1(block realblock.Block, stateBefore map[string]string) {
	if !r.statelessCalvinRemoteStateEnabled() || block.BlockHash == "" || block.Height == 0 {
		return
	}
	home := r.stateAccessPartitionID()
	if home == "" {
		return
	}
	frozen := copyStringMap(stateBefore)
	root := state.RootOfSnapshot(frozen)
	requesters := r.stateAccessShardIDs()
	if len(requesters) == 0 {
		requesters = []string{home}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stateFetchSnapshots == nil {
		r.stateFetchSnapshots = map[string]map[string]string{}
	}
	if r.stateFetchSnapshotRoots == nil {
		r.stateFetchSnapshotRoots = map[string]string{}
	}
	for _, requester := range requesters {
		key := stateFetchSnapshotKey(StateFetchRequest{BlockHash: block.BlockHash, HomeShard: home, ExecutionShard: requester})
		if r.stateFetchSnapshots[key] != nil {
			continue
		}
		r.stateFetchSnapshots[key] = frozen
		r.stateFetchSnapshotRoots[key] = root
		r.stateFetchSnapshotOrder = append(r.stateFetchSnapshotOrder, key)
	}
	for len(r.stateFetchSnapshotOrder) > stateFetchSnapshotCacheLimit {
		oldest := r.stateFetchSnapshotOrder[0]
		r.stateFetchSnapshotOrder = r.stateFetchSnapshotOrder[1:]
		delete(r.stateFetchSnapshots, oldest)
		delete(r.stateFetchSnapshotRoots, oldest)
	}
}

func (r *NodeRuntime) calvinBoundaryLocalReadV1(input CalvinStatelessFetchInput) (CalvinStatelessFetchResult, error) {
	home := r.stateAccessPartitionID()
	key := stateFetchSnapshotKey(StateFetchRequest{BlockHash: input.Block.BlockHash, HomeShard: home, ExecutionShard: home})
	r.mu.Lock()
	frozen, ok := r.stateFetchSnapshots[key]
	root := r.stateFetchSnapshotRoots[key]
	r.mu.Unlock()
	if !ok || frozen == nil || root == "" {
		return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_boundary_local_snapshot_not_ready: block=%d home=%s", input.Block.Height, home)
	}
	return CalvinStatelessFetchResult{Value: frozen[qualifyStateKey(home, input.Access.Key)], BlockStart: true}, nil
}

// A stale Home must wait until H's pre-state is primed.  An advanced Home
// without the cached boundary must fail closed, never return its live state.
func (r *NodeRuntime) handleCalvinBoundaryFetchRequestV1(ctx context.Context, requester string, request StateFetchRequest) (bool, error) {
	if !strings.HasPrefix(request.AccessKind, calvinBoundaryAccessPrefixV1) {
		return false, nil
	}
	predecessor, recognized := calvinBoundaryRequiredHeightV1(request.AccessKind)
	if !recognized {
		return true, fmt.Errorf("calvin_boundary_malformed_height")
	}
	qualified := request.HomeShard + "::" + request.Key
	respond := func(success bool, value, root, reason string) error {
		response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash,
			Key: request.Key, QualifiedKey: qualified, Value: value, HomeShard: request.HomeShard,
			ExecutionShard: request.ExecutionShard, StateRoot: root, StateVersion: predecessor,
			Versioned: false, Success: success, Error: reason}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		return r.enqueueStateFetchResponse(ctx, requester, response)
	}
	if !r.statelessCalvinRemoteStateEnabled() || request.HomeShard == "" || request.HomeShard != r.stateAccessPartitionID() || request.BlockHash == "" || request.Versioned || request.RequiredVersion != predecessor {
		return true, respond(false, "", "", "calvin_boundary_invalid_request")
	}
	cacheKey := stateFetchSnapshotKey(request)
	r.mu.Lock()
	cached, ok := r.stateFetchSnapshots[cacheKey]
	root := r.stateFetchSnapshotRoots[cacheKey]
	committedHeight := r.committedHeight
	r.mu.Unlock()
	if ok && cached != nil && root != "" {
		return true, respond(true, cached[qualified], root, "")
	}
	if committedHeight > predecessor {
		return true, respond(false, "", "", "calvin_boundary_snapshot_unavailable")
	}
	return true, respond(false, "", "", "calvin_boundary_home_not_ready")
}

// Exact RequiredVersion>0 fetches continue through the untouched existing
// StateVersion transport.  Only block-boundary reads use this causal handshake.
func (r *NodeRuntime) calvinBoundaryFetchStateV1(ctx context.Context, input CalvinStatelessFetchInput) (CalvinStatelessFetchResult, error) {
	if input.Block.Height == 0 || input.Block.BlockHash == "" {
		return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_boundary_missing_block_identity")
	}
	started := time.Now()
	local := r.stateAccessPartitionID()
	if input.HomeShard == local {
		value, err := r.calvinBoundaryLocalReadV1(input)
		value.WaitMS = time.Since(started).Milliseconds()
		return value, err
	}
	target := r.stateAccessLeaderID(input.HomeShard)
	if target == "" {
		return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_boundary_missing_home_leader: %s", input.HomeShard)
	}
	height := input.Block.Height - 1
	kind := calvinBoundaryAccessKindV1(height)
	requestID := stableTextDigest(strings.Join([]string{"calvin-boundary-v1", r.node.NodeID, input.Item.TxID, input.Block.BlockHash, input.Access.Key, input.HomeShard, local}, "|"))
	waiter := make(chan StateFetchResponse, 1)
	r.mu.Lock()
	if r.stateFetchWaiters == nil {
		r.stateFetchWaiters = map[string]chan StateFetchResponse{}
	}
	r.stateFetchWaiters[requestID] = waiter
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.stateFetchWaiters, requestID); r.mu.Unlock() }()
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: input.Item.TxID, BlockHash: input.Block.BlockHash,
		Key: input.Access.Key, HomeShard: input.HomeShard, ExecutionShard: local, AccessKind: kind, RequiredVersion: height, Versioned: false})
	request.AccessKind, request.RequiredVersion, request.Versioned = kind, height, false
	for {
		envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, target, r.node.ShardID, input.Block.Height, 0, input.Block.Height, request)
		if err != nil {
			return CalvinStatelessFetchResult{}, err
		}
		if err = r.sendStateAccessToNode(ctx, target, envelope); err != nil {
			return CalvinStatelessFetchResult{}, err
		}
		select {
		case response := <-waiter:
			if response.Success {
				if response.StateVersion != height || response.StateRoot == "" || response.BlockHash != input.Block.BlockHash {
					return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_boundary_response_identity_mismatch")
				}
				r.recordRemoteStateAccess(input.Block, input.Item, input.Access, response, time.Since(started))
				return CalvinStatelessFetchResult{Value: response.Value, Remote: true, BlockStart: true, WaitMS: time.Since(started).Milliseconds(), PhysicalFetch: 1}, nil
			}
			if response.Error != "calvin_boundary_home_not_ready" {
				return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_boundary_fetch_failed: %s", response.Error)
			}
			select {
			case <-ctx.Done():
				return CalvinStatelessFetchResult{}, ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		case <-ctx.Done():
			return CalvinStatelessFetchResult{}, ctx.Err()
		}
	}
}
