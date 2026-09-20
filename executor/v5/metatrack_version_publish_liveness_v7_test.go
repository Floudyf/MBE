package v5

import (
	"context"
	"sync"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV7CompletionPublicationReleasesRemoteStateReadyBeforeBlockFinish(t *testing.T) {
	producer := tx.SignedTransaction{
		TxID:       "producer",
		Sender:     "alice",
		Receiver:   "bob",
		Value:      1,
		AccessList: []tx.AccessItem{{Key: "asset:producer", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
	}
	consumer := tx.SignedTransaction{
		TxID:       "consumer",
		Sender:     "carol",
		Receiver:   "dave",
		Value:      1,
		AccessList: []tx.AccessItem{{Key: "asset:remote", Mode: tx.AccessRead, UpdateSemantics: "validate"}},
	}
	token := stateReadinessToken(consumer, consumer.AccessList[0])
	classification := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			producer.TxID: {Track: "fast", Reason: "test"},
			consumer.TxID: {Track: "fast", Reason: "test"},
		},
		Dependencies:  map[string][]string{},
		ReasonCodes:   map[string][]string{},
		StateWaitKeys: map[string][]string{consumer.TxID: {token}},
	}
	schedule := ScheduleResult{Ordered: []tx.SignedTransaction{producer, consumer}}
	block := realblock.Block{
		ShardID: "s0",
		Height:  1,
		TxIDs:   []string{producer.TxID, consumer.TxID},
		TxList:  []tx.SignedTransaction{producer, consumer},
	}

	published := make(chan struct{})
	var once sync.Once
	publish := func(ctx context.Context, item tx.SignedTransaction, _ execution.TxDelta, _ map[string]string) error {
		if item.TxID == producer.TxID {
			once.Do(func() { close(published) })
		}
		return nil
	}
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		select {
		case <-published:
			return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, Value: "ready"}, nil
		case <-ctx.Done():
			return RemoteStateReadyEvent{}, ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, metrics, outcomes, _, err := executeMetaTrackScheduleWithPolicy(
		ctx,
		schedule,
		classification,
		block,
		map[string]string{},
		2,
		0,
		fetch,
		publish,
		false,
	)
	if err != nil {
		t.Fatalf("completion-triggered publication should release StateReady before block finish: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("completed outcomes=%d, want 2", len(outcomes))
	}
	if got := metrics["metatrack_version_publish_policy"]; got != "completion_triggered_independent_v1" {
		t.Fatalf("unexpected publish policy: %#v", got)
	}
}
