package v5

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
)

// Construct the same verified Stateless Hash + Block-STM production profile
// used by the existing admission regression fixture, not a helper-only test.
func mtSTMAckFixture(t *testing.T) (*NodeRuntime, *NodeRuntime, string, realblock.Block) {
	t.Helper()
	home, source, key := statelessVersionAdmissionTestRuntimes(t)
	block := realblock.Block{BlockHash: "ack-12614-verified", Height: 12614, ShardID: "s1", ProposerID: "n-s1"}
	return home, source, key, block
}

func mtSTMAckItem(shard, key, value string, prev, produced uint64) state.StateKV {
	return state.StateKV{
		Key:             shard + "::" + key,
		Value:           value,
		TxIDs:           []string{fmt.Sprintf("tx-%d", produced)},
		ApplyOrigin:     "versioned_remote_home",
		RoutingOrdinal:  produced,
		PreviousVersion: prev,
		ProducedVersion: produced,
	}
}

func TestMTSTMAckV1DelayedHomeConfirmation(t *testing.T) {
	home, source, key, block := mtSTMAckFixture(t)
	item := mtSTMAckItem("s1", key, "v91876", 91875, 91876)
	if !source.statelessHashSTMExactAckV1(item) {
		t.Fatal("stateless hash Block-STM exact-version publication must use bounded reliable ACK")
	}
	var mu sync.Mutex
	var requestIDs, wireIDs []string
	delivered := make(chan struct{})
	source.sendToNodeHook = func(_ context.Context, target string, envelope p2p.MessageEnvelope) error {
		if target != "n-s0" || envelope.MessageType != stateDeltaApplyMessage {
			return fmt.Errorf("unexpected message to %s: %s", target, envelope.MessageType)
		}
		req, err := p2p.DecodePayload[StateDeltaApplyRequest](envelope)
		if err != nil {
			return err
		}
		if err := p2p.ValidateEnvelope(envelope); err != nil {
			return err
		}
		ack := home.handleStateDeltaApplyRequest(req)
		mu.Lock()
		requestIDs = append(requestIDs, req.RequestID)
		wireIDs = append(wireIDs, envelope.MessageID)
		n := len(requestIDs)
		mu.Unlock()
		if n == 1 {
			// Reproduce the 2026-11-07 S0/12614 Home-ACK timeline: Home
			// accepted before the old 2s deadline but ACK arrived ~2.224s later.
			go func() {
				time.Sleep(2240 * time.Millisecond)
				source.handleStateDeltaApplyAck(ack)
				close(delivered)
			}()
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	started := time.Now()
	acks, _, err := source.applyRemoteStateDelta(ctx, block, item, key, "s0", nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("late valid ACK caused fatal committed-block failure: %v", err)
	}
	<-delivered
	if elapsed < 2*time.Second || elapsed >= 8*time.Second {
		t.Fatalf("ACK timing unexpected: %v", elapsed)
	}
	if len(acks) != 1 || acks[0].ProducedVersion != 91876 {
		t.Fatalf("wrong acknowledgement: %+v", acks)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requestIDs) < 2 {
		t.Fatalf("missed bounded retry: request count=%d", len(requestIDs))
	}
	for i := 1; i < len(requestIDs); i++ {
		if requestIDs[i] != requestIDs[0] {
			t.Fatalf("retry changed logical RequestID: %#v", requestIDs)
		}
		if wireIDs[i] == wireIDs[i-1] {
			t.Fatalf("retransmit must get a fresh P2P message ID: %#v", wireIDs)
		}
	}
	home.mu.Lock()
	pending := len(home.pendingStateDeltas)
	home.mu.Unlock()
	if pending != 1 {
		t.Fatalf("Home duplicate writebacks: pending deltas=%d", pending)
	}
	value, ok := home.stateVersionValue(key, 91876)
	if !ok || value != "v91876" {
		t.Fatalf("Home lost exact version: %q %v", value, ok)
	}
}

func TestMTSTMAckV1RejectCorruptedAndNegativeAcks(t *testing.T) {
	for _, scenario := range []string{"wrong_home", "bad_witness", "home_reject"} {
		t.Run(scenario, func(t *testing.T) {
			_, source, key, block := mtSTMAckFixture(t)
			item := mtSTMAckItem("s1", key, "v91876", 91875, 91876)
			source.sendToNodeHook = func(_ context.Context, _ string, envelope p2p.MessageEnvelope) error {
				req, err := p2p.DecodePayload[StateDeltaApplyRequest](envelope)
				if err != nil {
					return err
				}
				ack := stateDeltaApplyAckFromRequest(req, req.HomeShard+"::"+req.Key, stableTextDigest("queued:"+req.Value), "root")
				switch scenario {
				case "wrong_home":
					ack.HomeShard = "s9"
					ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
				case "bad_witness":
					ack.WitnessDigest = "bad-proof"
				case "home_reject":
					ack.Success = false
					ack.Error = "rejected"
					ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
				}
				source.handleStateDeltaApplyAck(ack)
				return nil
			}
			_, _, err := source.applyRemoteStateDelta(context.Background(), block, item, key, "s0", nil)
			if err == nil {
				t.Fatalf("invalid Home confirmation accepted: %s", scenario)
			}
		})
	}
}

func TestMTSTMAckV1BoundedFailureRemovesWaiter(t *testing.T) {
	_, source, key, block := mtSTMAckFixture(t)
	item := mtSTMAckItem("s1", key, "v91876", 91875, 91876)
	sent := 0
	source.sendToNodeHook = func(_ context.Context, _ string, _ p2p.MessageEnvelope) error { sent++; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, _, err := source.applyRemoteStateDelta(ctx, block, item, key, "s0", nil)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("no bounded failure: %v", err)
	}
	if sent != 1 {
		t.Fatalf("unexpected attempt count: %d", sent)
	}
	source.mu.Lock()
	pending := len(source.stateApplyWaiters)
	source.mu.Unlock()
	if pending != 0 {
		t.Fatalf("outstanding ACK waiter leaked: %d", pending)
	}
}

func TestMTSTMAckV1OrderedExactVersionsAndIdempotentHome(t *testing.T) {
	home, source, key, block := mtSTMAckFixture(t)
	source.sendToNodeHook = func(_ context.Context, target string, envelope p2p.MessageEnvelope) error {
		if target != "n-s0" {
			return fmt.Errorf("unknown target: %s", target)
		}
		req, err := p2p.DecodePayload[StateDeltaApplyRequest](envelope)
		if err != nil {
			return err
		}
		ack := home.handleStateDeltaApplyRequest(req)
		source.handleStateDeltaApplyAck(ack)
		return nil
	}
	for _, row := range []struct {
		previous, version uint64
		value             string
	}{
		{91875, 91876, "v91876"}, {91876, 91877, "v91877"},
	} {
		item := mtSTMAckItem("s1", key, row.value, row.previous, row.version)
		acks, _, err := source.applyRemoteStateDelta(context.Background(), block, item, key, "s0", nil)
		if err != nil || len(acks) != 1 || acks[0].PreviousVersion != row.previous || acks[0].ProducedVersion != row.version {
			t.Fatalf("version %d failed or changed ordering: acks=%+v err=%v", row.version, acks, err)
		}
		value, ok := home.stateVersionValue(key, row.version)
		if !ok || value != row.value {
			t.Fatalf("historical exact version %d missing: %q", row.version, value)
		}
	}
	home.mu.Lock()
	pending := len(home.pendingStateDeltas)
	home.mu.Unlock()
	if pending != 2 {
		t.Fatalf("sequential exact updates lost or duplicated: %d", pending)
	}
}

func TestMTSTMAckV1MethodIsolation(t *testing.T) {
	_, source, key, _ := mtSTMAckFixture(t)
	exact := mtSTMAckItem("s1", key, "v", 1, 2)
	if !source.statelessHashSTMExactAckV1(exact) {
		t.Fatal("targeted baseline not matched")
	}
	cases := []struct {
		name   string
		change func()
	}{
		{"generic_update", func() { exact.ApplyOrigin = "" }},
		{"zero_version", func() { exact.ProducedVersion = 0 }},
		{"other_routing", func() {
			source.plugins.Routing = statelessHashRouting{basicPlugin: makeBasic("routing", "not_stateless_hash_routing", nil)}
		}},
		{"other_executor", func() {
			source.plugins.BlockExecutor = blockSTMBlockExecutor{basicPlugin: makeBasic("block_executor", "not_block_stm_block_executor", nil)}
		}},
	}
	for _, row := range cases {
		exact = mtSTMAckItem("s1", key, "v", 1, 2)
		// Restore the exact profile between independent method-isolation checks.
		source.plugins.Routing = statelessHashRouting{basicPlugin: makeBasic("routing", "stateless_hash_routing", nil)}
		source.plugins.BlockExecutor = blockSTMBlockExecutor{basicPlugin: makeBasic("block_executor", "block_stm_block_executor", nil)}
		row.change()
		if source.statelessHashSTMExactAckV1(exact) {
			t.Errorf("scope escaped for %s", row.name)
		}
	}
}
