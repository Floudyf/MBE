package v5

import (
	"context"
	"fmt"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
)

type fixedBlockWorkerTask struct {
	run     func()
	done    *sync.WaitGroup
	tracker *fixedBlockWorkerBatchTracker
}

type fixedBlockWorkerBatchTracker struct {
	mu      sync.Mutex
	active  int
	maximum int
}

func (t *fixedBlockWorkerBatchTracker) enter() {
	t.mu.Lock()
	t.active++
	if t.active > t.maximum {
		t.maximum = t.active
	}
	t.mu.Unlock()
}

func (t *fixedBlockWorkerBatchTracker) leave() {
	t.mu.Lock()
	t.active--
	t.mu.Unlock()
}

func (t *fixedBlockWorkerBatchTracker) max() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.maximum
}

// fixedBlockWorkerPool keeps one bounded worker set alive for the whole block.
// Algorithms still synchronize at their original batch/wave barriers; this
// removes only repeated goroutine/channel construction between barriers.
type fixedBlockWorkerPool struct {
	ctx  context.Context
	jobs chan fixedBlockWorkerTask
	wg   sync.WaitGroup
}

func newFixedBlockWorkerPool(ctx context.Context, workerCount int) *fixedBlockWorkerPool {
	if workerCount < 1 {
		workerCount = 1
	}
	p := &fixedBlockWorkerPool{ctx: ctx, jobs: make(chan fixedBlockWorkerTask, workerCount)}
	for worker := 0; worker < workerCount; worker++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for task := range p.jobs {
				task.tracker.enter()
				task.run()
				task.tracker.leave()
				task.done.Done()
			}
		}()
	}
	return p
}

func (p *fixedBlockWorkerPool) Run(tasks []func()) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}
	tracker := &fixedBlockWorkerBatchTracker{}
	var done sync.WaitGroup
	for _, fn := range tasks {
		if err := p.ctx.Err(); err != nil {
			done.Wait()
			return tracker.max(), err
		}
		done.Add(1)
		task := fixedBlockWorkerTask{run: fn, done: &done, tracker: tracker}
		select {
		case p.jobs <- task:
		case <-p.ctx.Done():
			done.Done()
			done.Wait()
			return tracker.max(), p.ctx.Err()
		}
	}
	done.Wait()
	if err := p.ctx.Err(); err != nil {
		return tracker.max(), err
	}
	return tracker.max(), nil
}

func (p *fixedBlockWorkerPool) Close() {
	if p == nil {
		return
	}
	close(p.jobs)
	p.wg.Wait()
}

// verifyPreverifiedLiteratureGraphProjection is intentionally lightweight.
// It may be used only after the runtime has already fully verified this exact
// immutable consensus-bound block/plan pair and remembered that provenance.
// Recovery, catch-up and direct execution continue to use each algorithm's
// complete semantic verifier.
func verifyPreverifiedLiteratureGraphProjection(block realblock.Block, plan literatureGraphPlan, algorithmID string) error {
	if block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID != algorithmID {
		return fmt.Errorf("%s execution plan is missing", algorithmID)
	}
	if plan.AlgorithmID != algorithmID || plan.PlanDigest == "" {
		return fmt.Errorf("%s parsed plan identity mismatch", algorithmID)
	}
	if block.ExecutionPlan.PlanDigest != plan.PlanDigest || block.ExecutionPlan.PayloadDigest != stableTextDigest(string(block.ExecutionPlan.Payload)) {
		return fmt.Errorf("%s execution plan envelope mismatch", algorithmID)
	}
	if plan.BlockHeight != block.Height {
		return fmt.Errorf("%s block height mismatch", algorithmID)
	}
	candidateIDs := transactionIDs(block.TxList)
	if !sameStringList(plan.CandidateTransactionIDs, candidateIDs) {
		return fmt.Errorf("%s candidate transaction projection mismatch", algorithmID)
	}
	if plan.Metrics.TransactionCount != len(candidateIDs) {
		return fmt.Errorf("%s transaction metric mismatch", algorithmID)
	}

	flattened := make([]string, 0, len(candidateIDs))
	seen := make(map[string]bool, len(candidateIDs))
	candidateSet := make(map[string]bool, len(candidateIDs))
	for _, id := range candidateIDs {
		candidateSet[id] = true
	}
	for _, wave := range plan.Waves {
		for _, id := range wave {
			if !candidateSet[id] || seen[id] {
				return fmt.Errorf("%s invalid wave transaction %s", algorithmID, id)
			}
			seen[id] = true
			flattened = append(flattened, id)
		}
	}
	if !sameStringList(flattened, plan.SerializationOrder) {
		return fmt.Errorf("%s serialization projection mismatch", algorithmID)
	}
	for _, id := range plan.AbortedTransactionIDs {
		if !candidateSet[id] || seen[id] {
			return fmt.Errorf("%s invalid aborted transaction %s", algorithmID, id)
		}
		seen[id] = true
	}
	if len(seen) != len(candidateIDs) {
		return fmt.Errorf("%s plan does not cover every candidate transaction", algorithmID)
	}
	return nil
}
