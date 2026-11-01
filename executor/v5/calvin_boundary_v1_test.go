package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const calvinBoundaryTestKeyV1 = "asset:calvin-boundary"

// The only substituted component is physical networking. The production
// sender, Home handler, queued response, and requester callback remain live.
func calvinBoundaryHarnessV1(t *testing.T) (*NodeRuntime, *NodeRuntime, context.Context, chan StateFetchRequest) {
	t.Helper()
	clientNode := NodePlan{NodeID: "n-client", ShardID: "calvin-global", ExecutionShardID: "s0"}
	homeNode := NodePlan{NodeID: "n-home", ShardID: "calvin-global", ExecutionShardID: "s1"}
	plan := Plan{NodeConfigs: []NodePlan{clientNode, homeNode}}
	store := calvinPartitionStateStore{builtinStateStorage{makeBasic("state_storage", calvinStateStorageID, nil)}}
	executor := calvinBlockExecutor{basicPlugin: makeBasic("block_executor", calvinStatelessExecutorID, nil), mode: calvinStatelessMode}
	home := &NodeRuntime{plan: plan, node: homeNode, db: state.NewDB(t.TempDir(), "s1"),
		plugins:                 RuntimePlugins{StateStorage: store, BlockExecutor: executor},
		stateFetchResponseTasks: make(chan stateFetchResponseTask, 32)}
	client := &NodeRuntime{plan: plan, node: clientNode, db: state.NewDB(t.TempDir(), "s0"),
		plugins: RuntimePlugins{StateAccess: builtinStateAccess{makeBasic("state_access", calvinStatelessStateAccessID, nil)},
			StateStorage: store, BlockExecutor: executor}, stateFetchWaiters: map[string]chan StateFetchResponse{}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	requests := make(chan StateFetchRequest, 128)
	client.sendToNodeHook = func(sendCtx context.Context, nodeID string, env p2p.MessageEnvelope) error {
		if nodeID != "n-home" || env.MessageType != stateFetchRequestMessage {
			return fmt.Errorf("unexpected Calvin transport target=%s type=%s", nodeID, env.MessageType)
		}
		var req StateFetchRequest
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			return err
		}
		select {
		case requests <- req:
		default:
			return fmt.Errorf("Calvin request overflow")
		}
		return home.handleStateFetchRequest(sendCtx, "n-client", req)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-home.stateFetchResponseTasks:
				if task.requester == "n-client" {
					client.handleStateFetchResponse(task.response)
				}
			}
		}
	}()
	return client, home, ctx, requests
}

func calvinBoundaryFetchV1(client *NodeRuntime, ctx context.Context, version uint64) (CalvinStatelessFetchResult, error) {
	item := tx.SignedTransaction{TxID: fmt.Sprintf("calvin-boundary-%d", version),
		AccessList: []tx.AccessItem{{Key: calvinBoundaryTestKeyV1, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{ExecutionShard: "s0",
			StateVersions: []tx.StateVersionDependency{{Key: calvinBoundaryTestKeyV1, RequiredVersion: version}}}}
	block := realblock.Block{ShardID: "calvin-global", BlockHash: "calvin-b35-boundary", Height: 35, TxList: []tx.SignedTransaction{item}}
	return client.calvinStatelessFetchState(ctx, CalvinStatelessFetchInput{Block: block, Item: item,
		Access: item.AccessList[0], HomeShard: "s1"})
}

func TestMTCalvinFixV1BoundaryNormal(t *testing.T) {
	client, home, ctx, requests := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at34")
	home.committedHeight = 34
	home.primeCalvinBoundarySnapshotV1(realblock.Block{BlockHash: "calvin-b35-boundary", Height: 35}, home.db.Snapshot())
	got, err := calvinBoundaryFetchV1(client, ctx, 0)
	if err != nil || !got.Remote || !got.BlockStart || got.Value != "at34" {
		t.Fatalf("boundary normal got=%+v err=%v", got, err)
	}
	select {
	case req := <-requests:
		if req.AccessKind != calvinBoundaryAccessKindV1(34) || req.Versioned || req.RequiredVersion != 34 {
			t.Fatalf("wrong boundary metadata: %+v", req)
		}
	default:
		t.Fatal("missing boundary request")
	}
}

func TestMTCalvinFixV1BoundaryAheadWithFrozenSnapshot(t *testing.T) {
	client, home, ctx, _ := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at34")
	home.committedHeight = 34
	home.primeCalvinBoundarySnapshotV1(realblock.Block{BlockHash: "calvin-b35-boundary", Height: 35}, home.db.Snapshot())
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at36")
	home.committedHeight = 36
	got, err := calvinBoundaryFetchV1(client, ctx, 0)
	if err != nil || got.Value != "at34" {
		t.Fatalf("MT_CALVIN_BOUNDARY_FUTURE_VALUE: %+v err=%v", got, err)
	}
}

func TestMTCalvinFixV1BoundaryAheadMissingSnapshotFailsClosed(t *testing.T) {
	client, home, ctx, _ := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at36")
	home.committedHeight = 36
	got, err := calvinBoundaryFetchV1(client, ctx, 0)
	if err == nil || !strings.Contains(err.Error(), "calvin_boundary_snapshot_unavailable") || got.Value != "" {
		t.Fatalf("missing boundary read did not fail closed: value=%+v err=%v", got, err)
	}
}

func TestMTCalvinFixV1BoundaryBehindWaits(t *testing.T) {
	client, home, _, _ := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at33")
	home.committedHeight = 33
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	got, err := calvinBoundaryFetchV1(client, ctx, 0)
	if err != context.DeadlineExceeded || got.Value != "" {
		t.Fatalf("lagging Home returned stale value: got=%+v err=%v", got, err)
	}
}

func TestMTCalvinFixV1ExactVersionPreserved(t *testing.T) {
	client, home, ctx, requests := calvinBoundaryHarnessV1(t)
	version := uint64(34)<<32 | 1
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at36")
	home.committedHeight = 36
	home.publishStateVersion(calvinBoundaryTestKeyV1, version, "exact34")
	got, err := calvinBoundaryFetchV1(client, ctx, version)
	if err != nil || !got.Exact || got.Value != "exact34" {
		t.Fatalf("exact-version regression: got=%+v err=%v", got, err)
	}
	select {
	case req := <-requests:
		if !req.Versioned || req.RequiredVersion != version {
			t.Fatalf("exact version request altered: %+v", req)
		}
	default:
		t.Fatal("missing exact request")
	}
}

func TestMTCalvinFixV1LocalBlockStartUsesFrozen(t *testing.T) {
	_, home, ctx, _ := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at34")
	home.primeCalvinBoundarySnapshotV1(realblock.Block{BlockHash: "calvin-b35-boundary", Height: 35}, home.db.Snapshot())
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at36")
	item := tx.SignedTransaction{TxID: "local", AccessList: []tx.AccessItem{{Key: calvinBoundaryTestKeyV1, Mode: tx.AccessReadWrite}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: calvinBoundaryTestKeyV1, RequiredVersion: 0}}}}
	block := realblock.Block{BlockHash: "calvin-b35-boundary", Height: 35}
	got, err := home.calvinStatelessFetchState(ctx, CalvinStatelessFetchInput{Block: block, Item: item, Access: item.AccessList[0], HomeShard: "s1"})
	if err != nil || !got.BlockStart || got.Value != "at34" {
		t.Fatalf("Calvin local boundary incorrect: got=%+v err=%v", got, err)
	}
}

func TestMTCalvinFixV1LocalMissingSnapshotFailsClosed(t *testing.T) {
	_, home, ctx, _ := calvinBoundaryHarnessV1(t)
	home.db.Set("s1::"+calvinBoundaryTestKeyV1, "at36")
	item := tx.SignedTransaction{TxID: "local", AccessList: []tx.AccessItem{{Key: calvinBoundaryTestKeyV1, Mode: tx.AccessReadWrite}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: calvinBoundaryTestKeyV1, RequiredVersion: 0}}}}
	got, err := home.calvinStatelessFetchState(ctx, CalvinStatelessFetchInput{Block: realblock.Block{BlockHash: "calvin-b35-boundary", Height: 35}, Item: item, Access: item.AccessList[0], HomeShard: "s1"})
	if err == nil || !strings.Contains(err.Error(), "calvin_boundary_local_snapshot_not_ready") || got.Value != "" {
		t.Fatalf("unsafe local boundary fallback: got=%+v err=%v", got, err)
	}
}
