package v5

import (
	"fmt"
	"testing"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// porygonV392KeyForShard chooses a real Porygon state key for a target shard
// without assuming anything about stableKey parity. The bound makes fixture
// failure explicit instead of allowing a non-terminating search.
func porygonV392KeyForShard(t *testing.T, prefix string, targetShard, shardCount int) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("%s:%d", prefix, i)
		if porygonStateShard(key, shardCount) == targetShard {
			return key
		}
	}
	t.Fatalf("unable to find Porygon fixture key for shard=%d count=%d", targetShard, shardCount)
	return ""
}

func TestPorygonV39OrderedCollapsePreservesProtocolLastWriter(t *testing.T) {
	updates := []PorygonStateUpdate{
		{TxID: "u-late-index", OriginalIndex: 100, Key: "asset:k", Value: "from-u"},
		{TxID: "itx-early-index", OriginalIndex: 1, Key: "asset:k", Value: "from-itx"},
	}
	got := porygonPaperCollapseOrderedStateUpdates(updates)
	if len(got) != 1 || got[0].Value != "from-itx" || got[0].TxID != "itx-early-index" {
		t.Fatalf("protocol-stage last writer lost: %#v", got)
	}
}

func TestPorygonV39DurableDeltaRetainsWriteEqualToOldTValue(t *testing.T) {
	key := porygonV392KeyForShard(t, "v39-stale", 0, 2)
	proposal := PorygonProposalBody{}
	assignment := porygonTxAssignment{TxID: "itx", OriginalIndex: 7, ExecutionShard: 0, CrossShard: false}
	delta := execution.TxDelta{TxID: "itx", OriginalIndex: 7, WriteSet: map[string]string{key: "A"}, Success: true}
	got, err := porygonPaperDurableStateDelta(proposal, []execution.TxDelta{delta}, []porygonTxAssignment{assignment}, "porygon-global", "s0", 2)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := qualifyStateKey("s0", key)
	if len(got) != 1 || got[0].Key != wantKey || got[0].Value != "A" {
		t.Fatalf("real write intent was dropped or routed to the wrong Storage Role: got=%#v want_key=%s", got, wantKey)
	}
}

func TestPorygonV39DurableDeltaAppliesUThenCurrentITxRegardlessOfOriginalIndex(t *testing.T) {
	key := porygonV392KeyForShard(t, "v39-stage-order", 0, 2)
	proposal := PorygonProposalBody{U: []PorygonProposalUpdate{{
		TxID: "prior-ctx", OriginHeight: 2,
		Updates: []PorygonStateUpdate{{TxID: "prior-ctx", OriginalIndex: 100, Key: key, Value: "from-u"}},
	}}}
	assignment := porygonTxAssignment{TxID: "current-itx", OriginalIndex: 1, ExecutionShard: 0, CrossShard: false}
	delta := execution.TxDelta{TxID: "current-itx", OriginalIndex: 1, WriteSet: map[string]string{key: "from-itx"}, Success: true}
	got, err := porygonPaperDurableStateDelta(proposal, []execution.TxDelta{delta}, []porygonTxAssignment{assignment}, "porygon-global", "s0", 2)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := qualifyStateKey("s0", key)
	if len(got) != 1 || got[0].Key != wantKey || got[0].Value != "from-itx" {
		t.Fatalf("OriginalIndex reordered ITx ahead of Proposal.U: got=%#v want_key=%s", got, wantKey)
	}
}

func TestPorygonV39ProposalUpdateOrderUsesOriginalOCIndexBeforeTxID(t *testing.T) {
	z := PorygonProposalUpdate{TxID: "tx-z", OriginHeight: 3, Updates: []PorygonStateUpdate{{TxID: "tx-z", OriginalIndex: 5, Key: "k", Value: "A"}}}
	a := PorygonProposalUpdate{TxID: "tx-a", OriginHeight: 3, Updates: []PorygonStateUpdate{{TxID: "tx-a", OriginalIndex: 6, Key: "k", Value: "B"}}}
	if porygonPaperProposalUpdateOrderIndex(z) >= porygonPaperProposalUpdateOrderIndex(a) {
		t.Fatalf("original OC order not recovered: z=%d a=%d", porygonPaperProposalUpdateOrderIndex(z), porygonPaperProposalUpdateOrderIndex(a))
	}
}

func TestPorygonV39LogicalRootUpdatesUseUThenITxBeforeCollapse(t *testing.T) {
	key := porygonV392KeyForShard(t, "v39-root-order", 0, 2)
	proposal := PorygonProposalBody{U: []PorygonProposalUpdate{{
		TxID: "prior-ctx", OriginHeight: 2,
		Updates: []PorygonStateUpdate{{TxID: "prior-ctx", OriginalIndex: 100, Key: key, Value: "from-u"}},
	}}}
	local := []porygonWaveResult{{
		Item:    tx.SignedTransaction{TxID: "current-itx"},
		Receipt: execution.Receipt{TxID: "current-itx", Success: true},
		Delta:   execution.TxDelta{TxID: "current-itx", OriginalIndex: 1, WriteSet: map[string]string{key: "from-itx"}, Success: true},
	}}
	assignments := map[string]porygonTxAssignment{
		"current-itx": {TxID: "current-itx", OriginalIndex: 1, ExecutionShard: 0, CrossShard: false},
	}
	got := porygonPaperLogicalPartitionUpdates(proposal, local, assignments, "s0", 2)
	if len(got) != 1 || got[0].Key != key || got[0].Value != "from-itx" {
		t.Fatalf("Storage Role root update order differs from Paper2 materialization order: %#v", got)
	}
}

func TestPorygonV39UpdatesForProposalUsesOCIndexBeforeTxID(t *testing.T) {
	runtime := &NodeRuntime{}
	state := &porygonPaperRuntimeState{
		proposalUpdates: map[uint64][]PorygonProposalUpdate{
			5: {
				{TxID: "tx-a", OriginHeight: 3, Updates: []PorygonStateUpdate{{TxID: "tx-a", OriginalIndex: 6, Key: "k", Value: "B"}}},
				{TxID: "tx-z", OriginHeight: 3, Updates: []PorygonStateUpdate{{TxID: "tx-z", OriginalIndex: 5, Key: "k", Value: "A"}}},
			},
		},
	}
	porygonPaperRuntimeStates.Store(runtime, state)
	defer porygonPaperRuntimeStates.Delete(runtime)
	got := runtime.porygonPaperUpdatesForProposal(5)
	if len(got) != 2 || got[0].TxID != "tx-z" || got[1].TxID != "tx-a" {
		t.Fatalf("Proposal.U reordered OC transactions by TxID: %#v", got)
	}
}

func TestPorygonV392MaterializedDeltaProjectsToExecutionStateUpdate(t *testing.T) {
	materialized := []state.StateKV{{Key: "s1:asset:k", Value: "A"}}
	got := porygonPaperExecutionStateDelta(materialized)
	if len(got) != 1 || got[0].Key != "s1:asset:k" || got[0].Value != "A" {
		t.Fatalf("durable StateKV projection changed execution StateUpdate truth: %#v", got)
	}
}

func TestPorygonV392ShardFixtureSearchTerminatesAndHitsBothPartitions(t *testing.T) {
	for target := 0; target < 2; target++ {
		key := porygonV392KeyForShard(t, "v392-shard-fixture", target, 2)
		if got := porygonStateShard(key, 2); got != target {
			t.Fatalf("fixture key routed to shard=%d want=%d key=%q", got, target, key)
		}
	}
}
