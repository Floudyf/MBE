package v5

import (
	"context"
	"encoding/json"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
)

func TestPorygonCertifiedCatchupUsesOrderingPipeline(t *testing.T) {
	r := pbftUnitRuntime("n0")
	r.plugins.BlockExecutor = porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	state := r.porygonPipelineRuntime()
	state.started = true
	porygonV38InstallPaperState(r, &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{}}, state)
	slots := r.porygonV50ExecutionSlots()
	for i := range slots.queues {
		slots.queues[i] = make(chan realblock.Block, 2)
	}
	t.Cleanup(func() {
		porygonV38CleanupRuntime(r)
		porygonV50ExecutionSlotStates.Delete(r)
	})
	makeBlock := func(height uint64, parent string) realblock.Block {
		b := pbftUnitBlock()
		b.Height, b.PreviousHash = height, parent
		plan := porygonExecutionPlan{BlockHeight: height, PlanDigest: "test-plan"}
		payload, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		b.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
			AlgorithmID: porygonPlanAlgorithmID, PlanDigest: plan.PlanDigest, Payload: payload,
		}
		realblock.AssignHash(&b)
		return b
	}
	catchup := func(b realblock.Block) error {
		cert := recoveryCertificateAt(b.Height, b.BlockHash)
		return r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{
			Block: b, SourceNode: "n1", Certificate: cert,
		})
	}
	first := makeBlock(1, r.committedHash)
	second := makeBlock(2, first.BlockHash)
	if err := catchup(second); err == nil {
		t.Fatal("out-of-order catch-up advanced past its missing parent")
	}
	for _, b := range []realblock.Block{first, second, second} {
		if err := catchup(b); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []realblock.Block{first, second} {
		queue := slots.queues[porygonV50ECSlotForHeight(b.Height)]
		if len(queue) != 1 {
			t.Fatalf("expected one execution of height %d, got %d", b.Height, len(queue))
		}
		if got := <-queue; got.BlockHash != b.BlockHash {
			t.Fatalf("execution queue got %s, want %s", got.BlockHash, b.BlockHash)
		}
	}
	if r.committedHeight != 0 || len(r.commitTasks) != 0 {
		t.Fatal("catch-up bypassed Porygon execution and queued a durable commit")
	}
	if state.orderedHeight != 2 || state.orderedHash != second.BlockHash {
		t.Fatal("Porygon ordering cursor did not advance through the certified chain")
	}
}
