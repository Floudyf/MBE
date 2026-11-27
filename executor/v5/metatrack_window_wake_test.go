package v5

import (
	"errors"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
)

func TestMetaTrackWindowWakeUsesTheCompleteProjection(t *testing.T) {
	first := completeWindowV671Tx(t, "wake-first", 1, 1)
	last := completeWindowV671Tx(t, "wake-last", 2, 2)
	producer := newCompleteWindowV671Producer()

	for _, arriveBeforeArm := range []bool{false, true} {
		pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
		wake := metaTrackWindowWake{ready: make(chan struct{}, 1)}
		input := BlockProductionInput{
			Pool: pool, Proposer: realblock.NewProposer("n0", "s0"),
			Limit: 1000, Now: time.UnixMilli(10), RoutingPluginID: "metatrack_coaccess_routing",
		}
		if !pool.Admit(last).Accepted {
			t.Fatal("out-of-order arrival was rejected")
		}
		_, err := producer.BuildCandidate(input)
		var incomplete *metaTrackIncompleteWindow
		if !errors.As(err, &incomplete) {
			t.Fatalf("expected an incomplete projection, got %v", err)
		}
		if arriveBeforeArm && !pool.Admit(first).Accepted {
			t.Fatal("arrival between selection and watch was rejected")
		}
		wake.arm(pool, incomplete.projection)
		wake.observe(last)
		unrelated := *first.ExecutionRouting
		unrelated.ConsensusWindowSequence++
		other := first
		other.ExecutionRouting = &unrelated
		wake.observe(other)
		if !arriveBeforeArm {
			if len(wake.ready) != 0 {
				t.Fatal("duplicate or unrelated transaction woke the proposer")
			}
			if !pool.Admit(first).Accepted {
				t.Fatal("last missing transaction was rejected")
			}
			wake.observe(first)
		}
		select {
		case <-wake.ready:
		default:
			t.Fatal("complete projection did not wake the proposer")
		}
		wake.observe(first)
		if len(wake.ready) != 0 {
			t.Fatal("completion signaled more than once")
		}
		block, err := producer.BuildCandidate(input)
		if err != nil || len(block.TxList) != 2 || block.TxList[0].TxID != first.TxID || block.TxList[1].TxID != last.TxID {
			t.Fatalf("normal producer changed selection: transactions=%v, error=%v", len(block.TxList), err)
		}
	}
}
