package v5

import (
	"context"
	"sync/atomic"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

type metaTrackLatestV3Fixture struct {
	items          []tx.SignedTransaction
	classification BatchClassificationResult
	token          string
	expectedWriter string
	expectedReader string
}

func buildMetaTrackLatestV3Fixture(t *testing.T) metaTrackLatestV3Fixture {
	t.Helper()
	key := "asset:k"
	writer := tx.SignedTransaction{
		TxID:             "writer",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "metatrack_latest_v3_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
			RoutingOrdinal: 2,
			StateVersions:  []tx.StateVersionDependency{{Key: key, ProducedVersion: 2}},
		},
	}
	reader := tx.SignedTransaction{
		TxID:             "reader",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "metatrack_latest_v3_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
			RoutingOrdinal: 4,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 2, ProducedVersion: 4}},
		},
	}
	token := stateReadinessToken(reader, reader.AccessList[0])
	classification := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			writer.TxID: {Track: "fast", Reason: "test"},
			reader.TxID: {Track: "fast", Reason: "test"},
		},
		Dependencies:  map[string][]string{},
		ReasonCodes:   map[string][]string{},
		StateWaitKeys: map[string][]string{reader.TxID: {token}},
	}
	items := []tx.SignedTransaction{writer, reader}
	bindMetaTrackInBlockVersionHandoffs(items, &classification)

	referenceBlock := realblock.Block{ShardID: "s0", Height: 1, TxIDs: []string{writer.TxID, reader.TxID}, TxList: items}
	serial := execution.NewSerialExecutor()
	_, writerDelta := serial.ExecuteTransaction(referenceBlock, writer, map[string]string{}, 0)
	expectedWriter, ok := writeSetLogicalValue(writerDelta.WriteSet, key)
	if !ok {
		t.Fatalf("reference writer did not produce %s: %#v", key, writerDelta.WriteSet)
	}
	referenceSnapshot := map[string]string{qualifyStateKey(referenceBlock.ShardID, key): expectedWriter}
	_, readerDelta := serial.ExecuteTransaction(referenceBlock, reader, referenceSnapshot, 1)
	expectedReader, ok := writeSetLogicalValue(readerDelta.WriteSet, key)
	if !ok {
		t.Fatalf("reference reader did not produce %s: %#v", key, readerDelta.WriteSet)
	}
	return metaTrackLatestV3Fixture{items: items, classification: classification, token: token, expectedWriter: expectedWriter, expectedReader: expectedReader}
}

func outcomeLogicalValue(t *testing.T, outcomes []metaTrackExecutionOutcome, txID, key string) string {
	t.Helper()
	for _, outcome := range outcomes {
		if outcome.TxID != txID {
			continue
		}
		if !outcome.Receipt.Success {
			t.Fatalf("%s failed: %#v", txID, outcome.Receipt)
		}
		value, ok := writeSetLogicalValue(outcome.Delta.WriteSet, key)
		if !ok {
			t.Fatalf("%s did not write %s: %#v", txID, key, outcome.Delta.WriteSet)
		}
		return value
	}
	t.Fatalf("missing outcome for %s", txID)
	return ""
}

func TestMetaTrackLatestV3SameBlockExactVersionHandoffSuppressesFetchAndPreservesValue(t *testing.T) {
	fixture := buildMetaTrackLatestV3Fixture(t)
	var fetchCalls atomic.Int64
	fetch := func(_ context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		fetchCalls.Add(1)
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: fixture.token, Value: fixture.expectedWriter, StateVersion: 2}, nil
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxIDs: []string{fixture.items[0].TxID, fixture.items[1].TxID}, TxList: fixture.items}
	_, metrics, outcomes, _, err := executeMetaTrackScheduleWithOptions(
		context.Background(), ScheduleResult{Ordered: fixture.items}, fixture.classification, block, map[string]string{}, 1, 0, fetch, nil, false, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("latest same-block exact version used %d fetches; want 0", got)
	}
	if got := intFromAny(metrics["metatrack_local_version_remote_fetch_suppressed_count"]); got != 1 {
		t.Fatalf("suppressed=%d metrics=%#v", got, metrics)
	}
	if got := intFromAny(metrics["metatrack_local_version_handoff_ready_count"]); got != 1 {
		t.Fatalf("handoff_ready=%d metrics=%#v", got, metrics)
	}
	if enabled, _ := metrics["metatrack_local_exact_version_handoff_enabled"].(bool); !enabled {
		t.Fatalf("handoff flag missing: %#v", metrics)
	}
	if got := outcomeLogicalValue(t, outcomes, fixture.items[1].TxID, "asset:k"); got != fixture.expectedReader {
		t.Fatalf("reader value=%s want=%s", got, fixture.expectedReader)
	}
}

func TestMetaTrackCurrentV3KeepsLegacyFetchAndMatchesLatestSemantics(t *testing.T) {
	fixture := buildMetaTrackLatestV3Fixture(t)
	var fetchCalls atomic.Int64
	fetch := func(_ context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		fetchCalls.Add(1)
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: fixture.token, Value: fixture.expectedWriter, StateVersion: 2}, nil
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxIDs: []string{fixture.items[0].TxID, fixture.items[1].TxID}, TxList: fixture.items}
	_, metrics, outcomes, _, err := executeMetaTrackScheduleWithOptions(
		context.Background(), ScheduleResult{Ordered: fixture.items}, fixture.classification, block, map[string]string{}, 1, 0, fetch, nil, false, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("current MetaTrack fetch calls=%d want=1", got)
	}
	if got := intFromAny(metrics["metatrack_local_version_remote_fetch_suppressed_count"]); got != 0 {
		t.Fatalf("current unexpectedly suppressed=%d", got)
	}
	if enabled, _ := metrics["metatrack_local_exact_version_handoff_enabled"].(bool); enabled {
		t.Fatal("current MetaTrack unexpectedly enabled local handoff")
	}
	if got := outcomeLogicalValue(t, outcomes, fixture.items[1].TxID, "asset:k"); got != fixture.expectedReader {
		t.Fatalf("reader value=%s want=%s", got, fixture.expectedReader)
	}
}

func TestMetaTrackLatestV3HistoricalExactVersionStillUsesFetch(t *testing.T) {
	key := "asset:history"
	reader := tx.SignedTransaction{
		TxID:             "reader-history",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "metatrack_latest_v3_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{ControlPolicy: metaTrackDeclaredAccessFrontierPolicy, RoutingOrdinal: 9, StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: 3, ProducedVersion: 9}}},
	}
	token := stateReadinessToken(reader, reader.AccessList[0])
	classification := BatchClassificationResult{Decisions: map[string]ExecutionDecision{reader.TxID: {Track: "fast", Reason: "test"}}, Dependencies: map[string][]string{}, ReasonCodes: map[string][]string{}, StateWaitKeys: map[string][]string{reader.TxID: {token}}}
	var fetchCalls atomic.Int64
	fetch := func(_ context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		fetchCalls.Add(1)
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, Value: "3", StateVersion: 3}, nil
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxIDs: []string{reader.TxID}, TxList: []tx.SignedTransaction{reader}}
	_, metrics, _, _, err := executeMetaTrackScheduleWithOptions(context.Background(), ScheduleResult{Ordered: []tx.SignedTransaction{reader}}, classification, block, map[string]string{}, 1, 0, fetch, nil, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("historical exact version fetch calls=%d want=1", got)
	}
	if got := intFromAny(metrics["metatrack_local_version_remote_fetch_suppressed_count"]); got != 0 {
		t.Fatalf("historical version unexpectedly suppressed=%d", got)
	}
}
