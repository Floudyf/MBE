package v5

import "testing"

// Paper2 regression for the real-cluster failure where one dynamic logical ESC
// spanned validators co-located with different physical Storage Roles.  The ESC
// batch digest must be derived from the logical partition's Storage-Role-
// certified root; requester-local physical storage identity is not an input.
func TestPorygonPaper2LogicalPartitionRootIndependentOfRequesterStorageRole(t *testing.T) {
	partitionID := "s0"
	base := map[string]string{
		qualifyStateKey(partitionID, "asset:a"): "old-a",
		qualifyStateKey(partitionID, "asset:b"): "old-b",
	}
	updates := []PorygonStateUpdate{
		{TxID: "tx-2", OriginalIndex: 2, Key: "asset:b", Value: "new-b"},
		{TxID: "tx-1", OriginalIndex: 1, Key: "asset:a", Value: "new-a"},
	}

	rootFromMemberOnPhysicalS0 := porygonPaperProspectivePartitionRoot(base, partitionID, updates)
	rootFromMemberOnPhysicalS1 := porygonPaperProspectivePartitionRoot(base, partitionID, append([]PorygonStateUpdate(nil), updates...))
	if rootFromMemberOnPhysicalS0 == "" || rootFromMemberOnPhysicalS0 != rootFromMemberOnPhysicalS1 {
		t.Fatalf("logical partition root diverged across requester physical roles: s0=%s s1=%s", rootFromMemberOnPhysicalS0, rootFromMemberOnPhysicalS1)
	}

	left := porygonSealBatchResult(PorygonESCBatchResult{BlockHash: "b4", Height: 4, ExecutionShardID: partitionID, StateRoot: rootFromMemberOnPhysicalS0})
	right := porygonSealBatchResult(PorygonESCBatchResult{BlockHash: "b4", Height: 4, ExecutionShardID: partitionID, StateRoot: rootFromMemberOnPhysicalS1})
	if left.ResultDigest != right.ResultDigest {
		t.Fatalf("same logical ESC/storage-certified root produced different batch digests: %s != %s", left.ResultDigest, right.ResultDigest)
	}
}

func TestPorygonPaper2ProspectiveRootCanonicalizesUpdateOrder(t *testing.T) {
	base := map[string]string{qualifyStateKey("s0", "k"): "0"}
	a := []PorygonStateUpdate{
		{TxID: "b", OriginalIndex: 2, Key: "z", Value: "2"},
		{TxID: "a", OriginalIndex: 1, Key: "k", Value: "1"},
	}
	b := []PorygonStateUpdate{a[1], a[0]}
	if got, want := porygonPaperProspectivePartitionRoot(base, "s0", a), porygonPaperProspectivePartitionRoot(base, "s0", b); got != want {
		t.Fatalf("canonical Paper2 update order changed prospective root: %s != %s", got, want)
	}
}
