package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV10InBlockHandoffKeepsExactValueToken(t *testing.T) {
	key := "asset:k"
	writer := tx.SignedTransaction{
		TxID:       "writer",
		AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 2,
			StateVersions:  []tx.StateVersionDependency{{Key: key, ProducedVersion: 2}},
		},
	}
	reader := tx.SignedTransaction{
		TxID:       "reader",
		AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 4,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 2, ProducedVersion: 4}},
		},
	}
	token := stateReadinessToken(reader, reader.AccessList[0])
	result := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			writer.TxID: {Track: "fast", Reason: "test"},
			reader.TxID: {Track: "fast", Reason: "test"},
		},
		Dependencies:  map[string][]string{},
		ReasonCodes:   map[string][]string{},
		StateWaitKeys: map[string][]string{reader.TxID: {token}},
	}
	bindMetaTrackInBlockVersionHandoffs([]tx.SignedTransaction{writer, reader}, &result)
	if len(result.Dependencies[reader.TxID]) != 1 || result.Dependencies[reader.TxID][0] != writer.TxID {
		t.Fatalf("in-block owner must remain an execution predecessor: %#v", result.Dependencies)
	}
	waits := result.StateWaitKeys[reader.TxID]
	if len(waits) != 1 || waits[0] != token {
		t.Fatalf("exact-version token was lost during in-block handoff: %#v", result.StateWaitKeys)
	}
}

func TestMetaTrackV10RequiredVersionOverridesCompletionOrderWorkingState(t *testing.T) {
	key := "asset:k"
	v2 := tx.SignedTransaction{
		TxID:             "v2",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "v10_determinism_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 2,
			StateVersions:  []tx.StateVersionDependency{{Key: key, ProducedVersion: 2}},
		},
	}
	v3 := tx.SignedTransaction{
		TxID:             "v3",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "v10_determinism_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 3,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 2, ProducedVersion: 3}},
		},
	}
	consumer := tx.SignedTransaction{
		TxID:             "consumer",
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "v10_determinism_test",
		AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 4,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 2, ProducedVersion: 4}},
		},
	}

	token := stateReadinessToken(consumer, consumer.AccessList[0])
	classification := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			v2.TxID:       {Track: "fast", Reason: "test"},
			v3.TxID:       {Track: "fast", Reason: "test"},
			consumer.TxID: {Track: "fast", Reason: "test"},
		},
		Dependencies: map[string][]string{
			v3.TxID:       {v2.TxID},
			consumer.TxID: {v2.TxID, v3.TxID},
		},
		ReasonCodes:   map[string][]string{},
		StateWaitKeys: map[string][]string{consumer.TxID: {token}},
	}
	items := []tx.SignedTransaction{v2, consumer, v3}
	bindMetaTrackInBlockVersionHandoffs(items, &classification)

	// Derive the exact values through the public serial execution contract.
	// Do not call or copy the execution package's private directAccessValue helper.
	referenceBlock := realblock.Block{ShardID: "s0", Height: 1, TxIDs: []string{v2.TxID, consumer.TxID}, TxList: []tx.SignedTransaction{v2, consumer}}
	serial := execution.NewSerialExecutor()
	_, v2Delta := serial.ExecuteTransaction(referenceBlock, v2, map[string]string{}, 0)
	expectedV2, ok := writeSetLogicalValue(v2Delta.WriteSet, key)
	if !ok {
		t.Fatalf("reference v2 did not write %s: %#v", key, v2Delta.WriteSet)
	}
	referenceSnapshot := map[string]string{qualifyStateKey(referenceBlock.ShardID, key): expectedV2}
	_, consumerDelta := serial.ExecuteTransaction(referenceBlock, consumer, referenceSnapshot, 1)
	expectedConsumer, ok := writeSetLogicalValue(consumerDelta.WriteSet, key)
	if !ok {
		t.Fatalf("reference consumer did not write %s: %#v", key, consumerDelta.WriteSet)
	}

	fetch := func(_ context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		return RemoteStateReadyEvent{
			TxID:           item.TxID,
			Key:            access.Key,
			ReadinessToken: token,
			Value:          expectedV2,
			StateVersion:   2,
		}, nil
	}

	block := realblock.Block{
		ShardID: "s0",
		Height:  1,
		TxIDs:   []string{v2.TxID, consumer.TxID, v3.TxID},
		TxList:  items,
	}
	// v3 is forced to complete after v2 and before consumer. Therefore the
	// completion-order workingSnapshot contains v3 when consumer dispatches.
	// RequiredVersion=2 must still override that newer completion-order value.
	schedule := ScheduleResult{Ordered: []tx.SignedTransaction{v2, v3, consumer}}
	_, _, outcomes, _, err := executeMetaTrackScheduleWithPolicy(
		context.Background(),
		schedule,
		classification,
		block,
		map[string]string{},
		2,
		0,
		fetch,
		nil,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	got := ""
	for _, outcome := range outcomes {
		if outcome.TxID != consumer.TxID {
			continue
		}
		value, ok := writeSetLogicalValue(outcome.Delta.WriteSet, key)
		if !ok {
			t.Fatalf("consumer did not write %s: %#v", key, outcome.Delta.WriteSet)
		}
		got = value
	}
	if got == "" {
		t.Fatal("consumer outcome missing")
	}
	if got != expectedConsumer {
		t.Fatalf("consumer read completion-order state instead of RequiredVersion=2: got=%s want=%s", got, expectedConsumer)
	}
}
