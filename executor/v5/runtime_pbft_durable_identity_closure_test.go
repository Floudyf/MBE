package v5

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/storage"
)

func TestDurableCommitPreservesCertifiedConsensusBodyAndExecutedMarkerRoots(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewBlockStore(dir, "n0", "s0")
	proposed := realblock.Block{
		ShardID: "s0", Height: 7, PreviousHash: "parent-6", ProposerID: "n0", Timestamp: 1234,
		StateRootBefore: "proposal-before", StateRootAfter: "proposal-after", ReceiptRoot: "proposal-receipt",
	}
	realblock.AssignHash(&proposed)
	originalHash := proposed.BlockHash
	result := execution.Result{BlockHash: proposed.BlockHash, Height: proposed.Height, StateRootBefore: "executed-before", StateRootAfter: "executed-after", ReceiptRoot: "executed-receipt"}
	if _, err := store.DurableCommitWithMetrics(proposed, result); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.ReadCommittedAtHeight(proposed.Height)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("durably committed block missing")
	}
	if got.BlockHash != originalHash || realblock.Hash(got) != got.BlockHash {
		t.Fatalf("durable consensus identity changed: stored=%s recomputed=%s want=%s", got.BlockHash, realblock.Hash(got), originalHash)
	}
	if got.StateRootBefore != proposed.StateRootBefore || got.StateRootAfter != proposed.StateRootAfter || got.ReceiptRoot != proposed.ReceiptRoot {
		t.Fatalf("durable block rewrote consensus-hashed roots: %+v", got)
	}
	f, err := os.Open(filepath.Join(dir, "commit_markers.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var marker storage.CommitMarker
	if err := json.NewDecoder(f).Decode(&marker); err != nil {
		t.Fatal(err)
	}
	if marker.BlockHash != originalHash || marker.StateRoot != result.StateRootAfter || marker.ReceiptRoot != result.ReceiptRoot {
		t.Fatalf("executed roots missing from commit marker: %+v", marker)
	}
}

func TestGenericDurableStoreRetainsSyntheticHashFixtureCompatibility(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewBlockStore(dir, "n0", "s0")
	item := realblock.Block{ShardID: "s0", Height: 6, PreviousHash: "h5", BlockHash: "h6"}
	if _, err := store.DurableCommitWithMetrics(item, execution.Result{BlockHash: "h6", Height: 6}); err != nil {
		t.Fatalf("generic storage API rejected historical synthetic-hash fixture: %v", err)
	}
}

func TestCertifiedCatchupSourceRejectsHashInconsistentDurableBlockBeforeSend(t *testing.T) {
	r := pbftUnitRuntime("n1")
	item := pbftUnitBlock()
	cert := recoveryCertificate(item.BlockHash)
	item.StateRootAfter = "mutated-after-pbft-certificate"
	err := r.sendCertifiedCatchupBlock(context.Background(), "n0", item, cert)
	if err == nil || !strings.Contains(err.Error(), "source block identity mismatch") {
		t.Fatalf("hash-inconsistent durable block was not rejected before send: %v", err)
	}
	if r.runtimeMetricCounts["pbft_catchup_source_block_identity_failure_count"] != 1 {
		t.Fatalf("source identity failure metric=%d", r.runtimeMetricCounts["pbft_catchup_source_block_identity_failure_count"])
	}
}
