package v5

import (
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func withMetaTrackV6567Round(t *testing.T, item tx.SignedTransaction, round int, dependency *tx.StateVersionDependency) tx.SignedTransaction {
	t.Helper()
	routing := *item.ExecutionRouting
	routing.ConsensusExecutionDepth = 1
	routing.ConsensusExecutionRound = round
	if dependency != nil {
		routing.StateVersions = []tx.StateVersionDependency{*dependency}
		item.StateKeys = []string{dependency.Key}
		item.AccessList = []tx.AccessItem{{Key: dependency.Key, Mode: tx.AccessRead, UpdateSemantics: "validate"}}
	}
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	if err := tx.ValidateExecutionRouting(item); err != nil {
		t.Fatal(err)
	}
	return item
}

func makeRoundRowsV6567(t *testing.T, shard string, round, count int, ordinalStart uint64) []tx.SignedTransaction {
	t.Helper()
	out := make([]tx.SignedTransaction, 0, count)
	for i := 0; i < count; i++ {
		item := signedMetaTrackBatchTestTx(t, fmt.Sprintf("%s-r%d-%d", shard, round, i), ordinalStart+uint64(i), uint64(round), fmt.Sprintf("p%d", round), shard, count, count)
		out = append(out, withMetaTrackV6567Round(t, item, round, nil))
	}
	return out
}

func TestMetaTrackV6567AnnotateBindsCrossShardExactPredecessorRound(t *testing.T) {
	tracker := newMetaTrackConsensusPredecessorTrackerV656()
	producer := WorkloadRecord{RoutingOrdinal: 1, RouteBatchSequence: 1, ExecutionShard: "s1", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}, StateVersions: []tx.StateVersionDependency{{Key: "k", ProducedVersion: 1}}}
	if err := tracker.annotate("s1", "alice", &producer); err != nil {
		t.Fatal(err)
	}
	if producer.ConsensusExecutionRound != 1 {
		t.Fatalf("producer round=%d", producer.ConsensusExecutionRound)
	}

	consumer := WorkloadRecord{RoutingOrdinal: 2, RouteBatchSequence: 2, ExecutionShard: "s0", AccessList: []tx.AccessItem{{Key: "k", Mode: tx.AccessRead, UpdateSemantics: "validate"}}, StateVersions: []tx.StateVersionDependency{{Key: "k", RequiredVersion: 1}}}
	if err := tracker.annotate("s0", "bob", &consumer); err != nil {
		t.Fatal(err)
	}
	if consumer.ConsensusExecutionRound != 2 {
		t.Fatalf("consumer round=%d", consumer.ConsensusExecutionRound)
	}
	if got := consumer.StateVersions[0].RequiredExecutionRound; got != 1 {
		t.Fatalf("required round=%d", got)
	}
	if len(consumer.ConsensusExecutionPredecessorOrdinals) != 0 {
		t.Fatalf("remote exact predecessor leaked into local predecessor set: %v", consumer.ConsensusExecutionPredecessorOrdinals)
	}
}

func TestMetaTrackV6567DenseTwoRoundBandPacksWithoutPublishedRemoteVersion(t *testing.T) {
	items := makeRoundRowsV6567(t, "s0", 1, 8, 1)
	for i := 0; i < 8; i++ {
		dep := tx.StateVersionDependency{Key: fmt.Sprintf("remote-%d", i), RequiredVersion: uint64(100 + i), RequiredExecutionRound: 1}
		item := signedMetaTrackBatchTestTx(t, fmt.Sprintf("consumer-%d", i), uint64(200+i), 2, "p2", "s0", 8, 8)
		items = append(items, withMetaTrackV6567Round(t, item, 2, &dep))
	}
	selected, deferred, _, err := selectMetaTrackRoundBandedTransactionFrontierV6567(items, 1000, "s0", nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 16 || len(deferred) != 0 {
		t.Fatalf("selected=%d deferred=%d", len(selected), len(deferred))
	}
}

func TestMetaTrackV6567ThinLongChainIsNotSwallowed(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 10)
	for round := 1; round <= 10; round++ {
		item := signedMetaTrackBatchTestTx(t, fmt.Sprintf("chain-%d", round), uint64(round), uint64(round), fmt.Sprintf("p%d", round), "s0", 1, 1)
		items = append(items, withMetaTrackV6567Round(t, item, round, nil))
	}
	selected, deferred, _, err := selectMetaTrackRoundBandedTransactionFrontierV6567(items, 1000, "s0", nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].ExecutionRouting.ConsensusExecutionRound != 1 {
		t.Fatalf("thin chain selected=%v", txIDsV6566(selected))
	}
	if len(deferred) != 9 {
		t.Fatalf("deferred=%d", len(deferred))
	}
}

func TestMetaTrackV6567BandNeverExceedsWorkerDerivedDepth(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 80)
	ordinal := uint64(1)
	for round := 1; round <= 10; round++ {
		rows := makeRoundRowsV6567(t, "s0", round, 8, ordinal)
		ordinal += 8
		items = append(items, rows...)
	}
	selected, _, _, err := selectMetaTrackRoundBandedTransactionFrontierV6567(items, 1000, "s0", nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	minRound, maxRound := 0, 0
	for _, item := range selected {
		r := item.ExecutionRouting.ConsensusExecutionRound
		if minRound == 0 || r < minRound {
			minRound = r
		}
		if r > maxRound {
			maxRound = r
		}
	}
	if span := metaTrackRoundSpanV6567(minRound, maxRound); span > 8 {
		t.Fatalf("span=%d", span)
	}
	if span := metaTrackRoundSpanV6567(minRound, maxRound); span > metaTrackRoundBandAllowanceV6567(len(selected), 8) {
		t.Fatalf("span=%d count=%d", span, len(selected))
	}
}

func TestMetaTrackV6567ValidatorRejectsOverwideRoundBand(t *testing.T) {
	a := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "a", 1, 1, "p1", "s0", 1, 1), 1, nil)
	b := withMetaTrackV6567Round(t, signedMetaTrackBatchTestTx(t, "b", 2, 5, "p5", "s0", 1, 1), 5, nil)
	runtime := &NodeRuntime{pluginSnapshot: map[string]PluginConfig{"block_executor": {Config: map[string]any{"worker_count": 8}}}}
	if _, err := runtime.validateMetaTrackRoundBandedTransactionFrontierV6567(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{a, b}}); err == nil {
		t.Fatal("overwide round band must be rejected")
	}
}

func TestMetaTrackV6567ValidatorRejectsNonDecreasingExactRound(t *testing.T) {
	item := signedMetaTrackBatchTestTx(t, "bad", 2, 2, "p2", "s0", 1, 1)
	item.StateKeys = []string{"remote"}
	item.AccessList = []tx.AccessItem{{Key: "remote", Mode: tx.AccessRead, UpdateSemantics: "validate"}}
	routing := *item.ExecutionRouting
	routing.ConsensusExecutionDepth = 1
	routing.ConsensusExecutionRound = 2
	routing.StateVersions = []tx.StateVersionDependency{{Key: "remote", RequiredVersion: 1, RequiredExecutionRound: 2}}
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	runtime := &NodeRuntime{pluginSnapshot: map[string]PluginConfig{"block_executor": {Config: map[string]any{"worker_count": 8}}}}
	if _, err := runtime.validateMetaTrackRoundBandedTransactionFrontierV6567(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{item}}); err == nil {
		t.Fatal("non-decreasing remote exact round must be rejected")
	}
}
