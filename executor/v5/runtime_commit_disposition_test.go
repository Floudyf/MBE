package v5

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
)

func TestFutureBlockIsNotReportedAsSuccessfulCommit(t *testing.T) {
	runtime := &NodeRuntime{
		committed:       map[string]bool{},
		committing:      map[string]bool{},
		pendingCommits:  map[uint64]realblock.Block{},
		committedHeight: 5,
		committedHash:   "h5",
	}
	block := realblock.Block{ShardID: "s0", Height: 7, PreviousHash: "h6", BlockHash: "h7"}
	if err := runtime.commit(context.Background(), block); err == nil {
		t.Fatal("future block must not be reported as a successful commit")
	}
	if _, ok := runtime.pendingCommits[7]; !ok || runtime.committedHeight != 5 {
		t.Fatalf("future block was not deferred safely: pending=%v height=%d", runtime.pendingCommits, runtime.committedHeight)
	}
	result, err := runtime.commitWithDisposition(context.Background(), block)
	if err != nil || result.Disposition != CommitDeferred {
		t.Fatalf("expected deferred disposition, got result=%+v err=%v", result, err)
	}
}

func TestConflictingStaleBlockIsNotReportedAsSuccessfulCommit(t *testing.T) {
	runtime := &NodeRuntime{
		committed:       map[string]bool{"h5": true},
		committing:      map[string]bool{},
		pendingCommits:  map[uint64]realblock.Block{},
		committedHeight: 5,
		committedHash:   "h5",
	}
	block := realblock.Block{ShardID: "s0", Height: 5, PreviousHash: "h4", BlockHash: "other-h5"}
	if err := runtime.commit(context.Background(), block); err == nil {
		t.Fatal("conflicting stale block must not be reported as a successful commit")
	}
}

func TestAlreadyAppliedBlockIsIdempotent(t *testing.T) {
	runtime := &NodeRuntime{
		committed:       map[string]bool{"h5": true},
		committing:      map[string]bool{},
		pendingCommits:  map[uint64]realblock.Block{},
		committedHeight: 5,
		committedHash:   "h5",
	}
	result, err := runtime.commitWithDisposition(context.Background(), realblock.Block{ShardID: "s0", Height: 5, PreviousHash: "h4", BlockHash: "h5"})
	if err != nil || result.Disposition != CommitAlreadyApplied {
		t.Fatalf("expected already-applied disposition, got result=%+v err=%v", result, err)
	}
}

func TestAppliedBlockResultIsNotOverwrittenByPendingFailure(t *testing.T) {
	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	plugins, err := InstantiatePlugins(profile)
	if err != nil {
		t.Fatal(err)
	}
	db := state.NewDB(root, "s0")
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}
	runtime := &NodeRuntime{
		node:            NodePlan{NodeID: "n0", ShardID: "s0", DataDir: root},
		pool:            mempool.New("n0", "s0", mempool.DefaultPolicy(), account.NewNonceManager()),
		db:              db,
		store:           storage.NewBlockStore(storeDir, "n0", "s0"),
		engine:          execution.NewEngine(),
		committed:       map[string]bool{},
		committing:      map[string]bool{},
		pendingCommits:  map[uint64]realblock.Block{},
		committedHeight: 5,
		committedHash:   "h5",
		pluginSnapshot:  profile,
		plugins:         plugins,
	}
	current := realblock.Block{ShardID: "s0", Height: 6, PreviousHash: "h5", BlockHash: "h6"}
	pending := realblock.Block{ShardID: "s0", Height: 7, PreviousHash: "wrong-parent", BlockHash: "h7"}
	runtime.pendingCommits[7] = pending
	result, err := runtime.commitWithDisposition(context.Background(), current)
	if err != nil || result.Disposition != CommitApplied {
		t.Fatalf("current block was not applied independently: result=%+v err=%v", result, err)
	}
	if runtime.committedHeight != 6 || !runtime.committed[current.BlockHash] {
		t.Fatalf("current commit was overwritten by pending failure: height=%d committed=%v", runtime.committedHeight, runtime.committed)
	}
	if runtime.pendingCommitErrors[7] == "" {
		t.Fatal("pending failure was not recorded independently")
	}
}
