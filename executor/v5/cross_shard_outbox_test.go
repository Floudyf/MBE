package v5

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func newOutboxTestRuntime(
	nodeID string,
	shardID string,
) *NodeRuntime {
	return &NodeRuntime{
		plan: Plan{
			NodeConfigs: []NodePlan{
				{
					NodeID:  "n0",
					ShardID: "s0",
					Leader:  true,
				},
				{
					NodeID:  "n1",
					ShardID: "s1",
					Leader:  true,
				},
			},
		},
		node: NodePlan{
			NodeID:  nodeID,
			ShardID: shardID,
			Leader:  true,
		},
		plugins: RuntimePlugins{
			BlockProducer: builtinBlockProducer{
				makeBasic(
					"block_producer",
					"time_or_count_block_producer",
					nil,
				),
			},
			Consensus: builtinConsensus{
				makeBasic(
					"consensus",
					"pbft_style_consensus",
					nil,
				),
			},
		},
		relaySource:             map[string]Relay{},
		pendingOutboundRelays:   map[string]Relay{},
		pendingFinalizeMessages: map[string]Finalize{},
		outboundRelaySendErrors: map[string]string{},
		finalizeSendErrors:      map[string]string{},
		crossEventSeen:          map[string]bool{},
		relayAdmissionFailures:  map[string]string{},
	}
}

func countCrossShardStage(
	runtime *NodeRuntime,
	stage string,
) int {
	count := 0

	for _, event := range runtime.events {
		if strings.EqualFold(event.Stage, stage) {
			count++
		}
	}

	return count
}

func TestOutboundRelayRetriesUntilFinalize(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n0", "s0")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n1" {
			t.Fatalf(
				"relay destination = %q, want n1",
				nodeID,
			)
		}

		if sendCount <= 2 &&
			envelope.MessageType != p2p.MessageXShardRelay {
			t.Fatalf(
				"message type = %q, want relay",
				envelope.MessageType,
			)
		}

		if sendCount == 1 {
			return errors.New(
				"forced relay send failure",
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-1",
		},
		LogicalTxID: "logical-1",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueOutboundRelay(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial relay send count = %d, want 1",
			sendCount,
		)
	}

	if len(runtime.pendingOutboundRelays) != 1 {
		t.Fatalf(
			"pending outbound relay count = %d, want 1",
			len(runtime.pendingOutboundRelays),
		)
	}

	if runtime.outboundRelaySendErrors["logical-1"] == "" {
		t.Fatal(
			"relay send failure was not retained",
		)
	}

	runtime.lastCrossShardRetry = time.Time{}

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 2 {
		t.Fatalf(
			"relay send count after retry = %d, want 2",
			sendCount,
		)
	}

	if len(runtime.pendingOutboundRelays) != 1 {
		t.Fatal(
			"relay was cleared before SourceFinalize",
		)
	}

	if _, exists :=
		runtime.outboundRelaySendErrors["logical-1"]; exists {
		t.Fatal(
			"relay send error remained after successful retry",
		)
	}

	finish := Finalize{
		TxID:        "physical-1",
		LogicalTxID: "logical-1",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	envelope, err := p2p.NewEnvelope(
		finalizeMessage,
		"n1",
		"n0",
		"s1",
		0,
		0,
		0,
		finish,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.handle(
		context.Background(),
		envelope,
	); err != nil {
		t.Fatal(err)
	}

	if len(runtime.pendingOutboundRelays) != 0 {
		t.Fatal(
			"SourceFinalize did not clear outbound relay",
		)
	}

	if countCrossShardStage(
		runtime,
		"SourceFinalize",
	) != 1 {
		t.Fatal(
			"SourceFinalize was not recorded exactly once",
		)
	}

	if sendCount != 3 {
		t.Fatalf(
			"send count after FinalizeAck = %d, want 3",
			sendCount,
		)
	}

	// Duplicate Finalize remains idempotent and returns ACK again.
	if err := runtime.handle(
		context.Background(),
		envelope,
	); err != nil {
		t.Fatal(err)
	}

	if countCrossShardStage(
		runtime,
		"SourceFinalize",
	) != 1 {
		t.Fatal(
			"duplicate Finalize created duplicate terminal event",
		)
	}

	if sendCount != 4 {
		t.Fatalf(
			"send count after duplicate Finalize = %d, want 4",
			sendCount,
		)
	}
}

func TestFinalizeOutboxRetriesUntilAck(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n1", "s1")

	sendCount := 0

	runtime.sendToNodeHook = func(
		_ context.Context,
		nodeID string,
		envelope p2p.MessageEnvelope,
	) error {
		sendCount++

		if nodeID != "n0" {
			t.Fatalf(
				"finalize destination = %q, want n0",
				nodeID,
			)
		}

		if envelope.MessageType != finalizeMessage {
			t.Fatalf(
				"message type = %q, want Finalize",
				envelope.MessageType,
			)
		}

		if sendCount == 1 {
			return errors.New(
				"forced finalize send failure",
			)
		}

		return nil
	}

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-2",
		},
		LogicalTxID: "logical-2",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.queueFinalize(
		context.Background(),
		relay,
	)

	if sendCount != 1 {
		t.Fatalf(
			"initial finalize send count = %d, want 1",
			sendCount,
		)
	}

	if len(runtime.pendingFinalizeMessages) != 1 {
		t.Fatalf(
			"pending finalize count = %d, want 1",
			len(runtime.pendingFinalizeMessages),
		)
	}

	if runtime.finalizeSendErrors["logical-2"] == "" {
		t.Fatal(
			"finalize send failure was not retained",
		)
	}

	runtime.lastCrossShardRetry = time.Time{}

	runtime.retryPendingCrossShardMessages(
		context.Background(),
	)

	if sendCount != 2 {
		t.Fatalf(
			"finalize send count after retry = %d, want 2",
			sendCount,
		)
	}

	if len(runtime.pendingFinalizeMessages) != 1 {
		t.Fatal(
			"Finalize was cleared before FinalizeAck",
		)
	}

	if _, exists :=
		runtime.finalizeSendErrors["logical-2"]; exists {
		t.Fatal(
			"finalize send error remained after successful retry",
		)
	}

	finish := Finalize{
		TxID:        "physical-2",
		LogicalTxID: "logical-2",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	ackEnvelope, err := p2p.NewEnvelope(
		finalizeAckMessage,
		"n0",
		"n1",
		"s0",
		0,
		0,
		0,
		finish,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.handle(
		context.Background(),
		ackEnvelope,
	); err != nil {
		t.Fatal(err)
	}

	if len(runtime.pendingFinalizeMessages) != 0 {
		t.Fatal(
			"FinalizeAck did not clear pending Finalize",
		)
	}

	// Duplicate ACK must be harmless.
	if err := runtime.handle(
		context.Background(),
		ackEnvelope,
	); err != nil {
		t.Fatal(err)
	}

	if len(runtime.pendingFinalizeMessages) != 0 {
		t.Fatal(
			"duplicate FinalizeAck changed cleared state",
		)
	}
}

func crossShardStatusInt(
	t *testing.T,
	status map[string]any,
	key string,
) int {
	t.Helper()

	raw, exists := status[key]
	if !exists {
		t.Fatalf(
			"runtime status missing key %q",
			key,
		)
	}

	value, ok := raw.(float64)
	if !ok {
		t.Fatalf(
			"runtime status %s type = %T, want float64",
			key,
			raw,
		)
	}

	return int(value)
}

func crossShardStatusStrings(
	t *testing.T,
	status map[string]any,
	key string,
) []string {
	t.Helper()

	raw, exists := status[key]
	if !exists {
		t.Fatalf(
			"runtime status missing key %q",
			key,
		)
	}

	values, ok := raw.([]any)
	if !ok {
		t.Fatalf(
			"runtime status %s type = %T, want []any",
			key,
			raw,
		)
	}

	result := make([]string, 0, len(values))

	for _, value := range values {
		item, ok := value.(string)
		if !ok {
			t.Fatalf(
				"runtime status %s item type = %T",
				key,
				value,
			)
		}

		result = append(result, item)
	}

	return result
}

func TestRuntimeStatusIncludesAllCrossShardOutboxes(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime("n0", "s0")
	runtime.node.DataDir = t.TempDir()

	runtime.pool = mempool.New(
		"n0",
		"s0",
		mempool.DefaultPolicy(),
		nil,
	)

	runtime.relaySource["relay-in"] = Relay{
		Tx: tx.SignedTransaction{
			TxID: "relay-in",
		},
		LogicalTxID: "relay-in",
		SourceShard: "s1",
		TargetShard: "s0",
	}

	runtime.pendingOutboundRelays["relay-out"] = Relay{
		Tx: tx.SignedTransaction{
			TxID: "relay-out",
		},
		LogicalTxID: "relay-out",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.pendingFinalizeMessages["finalize-out"] = Finalize{
		TxID:        "finalize-out",
		LogicalTxID: "finalize-out",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.outboundRelaySendErrors["relay-out"] =
		"forced relay send failure"

	runtime.finalizeSendErrors["finalize-out"] =
		"forced finalize send failure"

	if err := runtime.writeRuntimeStatus(); err != nil {
		t.Fatal(err)
	}

	statusPath := filepath.Join(
		runtime.node.DataDir,
		"node_runtime_status.json",
	)

	raw, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatal(err)
	}

	var status map[string]any

	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}

	expectedCounts := map[string]int{
		"pending_cross_shard_count":      3,
		"pending_relay_source_count":     1,
		"pending_outbound_relay_count":   1,
		"pending_finalize_message_count": 1,
	}

	for key, expected := range expectedCounts {
		actual := crossShardStatusInt(
			t,
			status,
			key,
		)

		if actual != expected {
			t.Fatalf(
				"%s = %d, want %d",
				key,
				actual,
				expected,
			)
		}
	}

	expectedIDs := map[string][]string{
		"pending_cross_shard_ids": {
			"finalize-out",
			"relay-in",
			"relay-out",
		},
		"pending_relay_source_ids": {
			"relay-in",
		},
		"pending_outbound_relay_ids": {
			"relay-out",
		},
		"pending_finalize_message_ids": {
			"finalize-out",
		},
	}

	for key, expected := range expectedIDs {
		actual := crossShardStatusStrings(
			t,
			status,
			key,
		)

		if len(actual) != len(expected) {
			t.Fatalf(
				"%s length = %d, want %d: %#v",
				key,
				len(actual),
				len(expected),
				actual,
			)
		}

		for index := range expected {
			if actual[index] != expected[index] {
				t.Fatalf(
					"%s[%d] = %q, want %q",
					key,
					index,
					actual[index],
					expected[index],
				)
			}
		}
	}

	outboundErrors, ok :=
		status["outbound_relay_send_errors"].(map[string]any)

	if !ok {
		t.Fatalf(
			"outbound_relay_send_errors type = %T",
			status["outbound_relay_send_errors"],
		)
	}

	if outboundErrors["relay-out"] !=
		"forced relay send failure" {
		t.Fatalf(
			"unexpected outbound relay errors: %#v",
			outboundErrors,
		)
	}

	finalizeErrors, ok :=
		status["finalize_send_errors"].(map[string]any)

	if !ok {
		t.Fatalf(
			"finalize_send_errors type = %T",
			status["finalize_send_errors"],
		)
	}

	if finalizeErrors["finalize-out"] !=
		"forced finalize send failure" {
		t.Fatalf(
			"unexpected finalize errors: %#v",
			finalizeErrors,
		)
	}
}

func TestFollowerClearsCommittedRelayReplica(
	t *testing.T,
) {
	runtime := newOutboxTestRuntime(
		"n1",
		"s1",
	)

	runtime.node.Leader = false

	relay := Relay{
		Tx: tx.SignedTransaction{
			TxID: "physical-replica",
		},
		LogicalTxID: "logical-replica",
		SourceShard: "s0",
		TargetShard: "s1",
	}

	runtime.relaySource["logical-replica"] = relay

	runtime.relaySource["physical-replica"] = relay

	runtime.relayAdmissionFailures["logical-replica"] = "logical_failure"

	runtime.relayAdmissionFailures["physical-replica"] = "physical_failure"

	sendCalled := false

	runtime.sendToNodeHook = func(
		_ context.Context,
		_ string,
		_ p2p.MessageEnvelope,
	) error {
		sendCalled = true
		return nil
	}

	runtime.onCommittedTxWithOrigin(
		context.Background(),
		relay.Tx,
		relay,
		CommitOriginConsensus,
	)

	if len(runtime.relaySource) != 0 {
		t.Fatalf(
			"follower relay replicas remain: %#v",
			runtime.relaySource,
		)
	}

	if len(runtime.relayAdmissionFailures) != 0 {
		t.Fatalf(
			"follower relay failures remain: %#v",
			runtime.relayAdmissionFailures,
		)
	}

	if len(runtime.pendingFinalizeMessages) != 0 {
		t.Fatal(
			"follower created a Finalize outbox entry",
		)
	}

	if sendCalled {
		t.Fatal(
			"follower sent a cross-shard message",
		)
	}
}
