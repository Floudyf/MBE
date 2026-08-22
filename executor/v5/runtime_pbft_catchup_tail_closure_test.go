package v5

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
)

func TestCatchupBusyResponderReturnsExplicitUnavailable(t *testing.T) {
	r := pbftUnitRuntime("n1")
	r.catchupResponsesInFlight = maxConcurrentCatchupResponses
	var got CatchupUnavailable
	r.sendToNodeHook = func(_ context.Context, to string, msg p2p.MessageEnvelope) error {
		if to != "n0" || msg.MessageType != catchupUnavailableMessage {
			t.Fatalf("unexpected response to=%s type=%s", to, msg.MessageType)
		}
		item, err := p2p.DecodePayload[CatchupUnavailable](msg)
		if err != nil {
			t.Fatal(err)
		}
		got = item
		return nil
	}
	if err := r.handleCertifiedCatchupRequest(context.Background(), "n0", CatchupRequest{ShardID: "s0", FromHeight: 10, ToHeight: 20}); err != nil {
		t.Fatal(err)
	}
	if got.Reason != "responder_busy" || got.RequestedFromHeight != 10 {
		t.Fatalf("unexpected unavailable hint: %+v", got)
	}
}

func TestCatchupBehindSourceReturnsExplicitUnavailable(t *testing.T) {
	r := pbftUnitRuntime("n1")
	r.committedHeight = 5
	var got CatchupUnavailable
	r.sendToNodeHook = func(_ context.Context, _ string, msg p2p.MessageEnvelope) error {
		item, err := p2p.DecodePayload[CatchupUnavailable](msg)
		if err != nil {
			t.Fatal(err)
		}
		got = item
		return nil
	}
	if err := r.handleCertifiedCatchupRequest(context.Background(), "n0", CatchupRequest{ShardID: "s0", FromHeight: 6, ToHeight: 8}); err != nil {
		t.Fatal(err)
	}
	if got.Reason != "source_behind_request" || got.CommittedHeight != 5 {
		t.Fatalf("unexpected source-behind hint: %+v", got)
	}
}

func TestCatchupUnavailablePreservesRetryPacingAndRejectsSpoof(t *testing.T) {
	r := pbftUnitRuntime("n3")
	lastRequest := time.Now()
	r.lastCatchupRequest = lastRequest
	r.catchupTargetHeight = 25
	item := CatchupUnavailable{
		SourceNode: "n1", RequestedFromHeight: 8, CommittedHeight: 30,
		StableCheckpointHeight: 20, Reason: "responder_busy",
	}
	if err := r.handleCatchupUnavailable("n1", "s0", item); err != nil {
		t.Fatal(err)
	}
	if !r.lastCatchupRequest.Equal(lastRequest) {
		t.Fatal("negative response bypassed the existing bounded retry interval")
	}
	if r.catchupTargetHeight != 25 {
		t.Fatalf("unsigned negative hint advanced target: got=%d want=25", r.catchupTargetHeight)
	}
	spoof := item
	spoof.SourceNode = "n2"
	if err := r.handleCatchupUnavailable("n1", "s0", spoof); err == nil {
		t.Fatal("spoofed unavailable source was accepted")
	}
}

func TestStaleCertifiedCatchupBlockDropsBeforeExpensiveValidation(t *testing.T) {
	r := pbftUnitRuntime("n3")
	r.committedHeight = 10
	r.committedHash = "height-10"
	item := CatchupBlock{Block: pbftUnitBlock(), SourceNode: "n1"}
	item.Block.Height = 9
	item.Block.BlockHash = "malformed-stale"
	if err := r.handleCertifiedCatchupBlock(context.Background(), "n1", item); err != nil {
		t.Fatalf("stale duplicate should be ignored cheaply: %v", err)
	}
	if r.runtimeMetricCounts["pbft_catchup_stale_block_drop_count"] != 1 {
		t.Fatalf("stale drop metric=%d", r.runtimeMetricCounts["pbft_catchup_stale_block_drop_count"])
	}
}

func TestCatchupBusyResponderWithoutTransportPreservesHistoricalNoErrorContract(t *testing.T) {
	r := pbftUnitRuntime("n1")
	r.catchupResponsesInFlight = maxConcurrentCatchupResponses
	r.transport = nil
	r.sendToNodeHook = nil

	err := r.handleCertifiedCatchupRequest(
		context.Background(),
		"n0",
		CatchupRequest{ShardID: "s0", FromHeight: 10, ToHeight: 20},
	)
	if err != nil {
		t.Fatalf("busy responder must remain a non-error guard path: %v", err)
	}
	if r.runtimeMetricCounts["pbft_catchup_response_busy_count"] != 1 {
		t.Fatalf("busy metric=%d", r.runtimeMetricCounts["pbft_catchup_response_busy_count"])
	}
	if r.runtimeMetricCounts["pbft_catchup_unavailable_send_failure_count"] != 1 {
		t.Fatalf("negative-hint send-failure metric=%d", r.runtimeMetricCounts["pbft_catchup_unavailable_send_failure_count"])
	}
}

func TestCatchupSourceBehindWithoutTransportPreservesHistoricalNoErrorContract(t *testing.T) {
	r := pbftUnitRuntime("n1")
	r.committedHeight = 5
	r.transport = nil
	r.sendToNodeHook = nil

	err := r.handleCertifiedCatchupRequest(
		context.Background(),
		"n0",
		CatchupRequest{ShardID: "s0", FromHeight: 6, ToHeight: 8},
	)
	if err != nil {
		t.Fatalf("source-behind hint must remain best-effort: %v", err)
	}
	if r.runtimeMetricCounts["pbft_catchup_unavailable_send_failure_count"] != 1 {
		t.Fatalf("negative-hint send-failure metric=%d", r.runtimeMetricCounts["pbft_catchup_unavailable_send_failure_count"])
	}
}

func TestCatchupUnavailableRejectsCheckpointAboveSourceCommit(t *testing.T) {
	r := pbftUnitRuntime("n3")
	item := CatchupUnavailable{
		SourceNode: "n1", RequestedFromHeight: 8, CommittedHeight: 20,
		StableCheckpointHeight: 21, Reason: "responder_busy",
	}
	if err := r.handleCatchupUnavailable("n1", "s0", item); err == nil {
		t.Fatal("impossible negative-hint checkpoint metadata was accepted")
	}
}
