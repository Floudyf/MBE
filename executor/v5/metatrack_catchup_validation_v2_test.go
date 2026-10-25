package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
)

// MBE_MT_CATCHFIX_V1: even a future-height block must have a valid body and
// certificate before it is enqueued. Full dependency checks run before commit.
func TestPBFTCatchupFutureHeightCertifiedStructuralGateV2(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	b.Height = 2
	realblock.AssignHash(&b)
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	cert := recoveryCertificateAt(2, b.BlockHash)
	if err := r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{Block: b, SourceNode: "n1", Certificate: cert}); err != nil {
		t.Fatalf("future certified structural catch-up rejected: %v", err)
	}
	if r.committedHeight != 0 {
		t.Fatal("future certified catch-up must not advance durable height at ingress")
	}
	select {
	case task := <-r.commitTasks:
		if task.kind != commitTaskCatchUp || task.block.Height != 2 {
			t.Fatalf("unexpected deferred commit task: %#v", task)
		}
	default:
		t.Fatal("future certified block must be queued for ordered verification")
	}
}

func TestPBFTCatchupFutureHeightRejectsInvalidBodyV2(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	b.Height = 2
	realblock.AssignHash(&b)
	// AssignHash recomputes TxRoot. Corrupt the root only after assigning
	// a valid block, then recompute the enclosing block hash without resetting
	// TxRoot. This isolates the transaction-root verifier from the hash gate.
	b.TxRoot = "wrong-root"
	b.BlockHash = realblock.Hash(b)
	if b.TxRoot == realblock.TxRoot(b.TxIDs) {
		t.Fatal("corrupt test fixture has a valid transaction root")
	}
	if b.BlockHash != realblock.Hash(b) {
		t.Fatal("corrupt test fixture has an invalid enclosing block hash")
	}
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	err := r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{Block: b, SourceNode: "n1", Certificate: recoveryCertificateAt(2, b.BlockHash)})
	if err == nil {
		t.Fatal("invalid future-height transaction root passed structural gate")
	}
	if len(r.commitTasks) != 0 {
		t.Fatal("invalid block was queued")
	}
}

func TestPBFTCatchupFutureHeightRejectsTamperedBlockHashV2(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	b.Height = 2
	realblock.AssignHash(&b)
	b.TxRoot = "wrong-root"
	if b.BlockHash == realblock.Hash(b) {
		t.Fatal("tampered fixture unexpectedly has a valid block hash")
	}
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	err := r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{Block: b, SourceNode: "n1", Certificate: recoveryCertificateAt(2, b.BlockHash)})
	if err == nil {
		t.Fatal("tampered future-height block hash passed structural gate")
	}
	if len(r.commitTasks) != 0 {
		t.Fatal("tampered block was queued")
	}
}

func TestPBFTCatchupFutureHeightRejectsInsufficientCertificateV2(t *testing.T) {
	r := pbftUnitRuntime("n0")
	b := pbftUnitBlock()
	b.Height = 2
	realblock.AssignHash(&b)
	r.commitWorkerContext = context.Background()
	r.commitTasks = make(chan commitTask, 2)
	cert := recoveryCertificateAt(2, b.BlockHash)
	cert.Commits = cert.Commits[:2]
	err := r.handleCertifiedCatchupBlock(context.Background(), "n1", CatchupBlock{Block: b, SourceNode: "n1", Certificate: cert})
	if err == nil {
		t.Fatal("insufficient certificate must fail closed")
	}
	if len(r.commitTasks) != 0 {
		t.Fatal("uncertified block queued")
	}
}
