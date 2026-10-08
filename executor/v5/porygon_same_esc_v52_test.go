package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonV52CertifiedResult(item tx.SignedTransaction, index int, writes map[string]string) porygonWaveResult {
	return porygonWaveResult{
		Item:  item,
		Delta: execution.TxDelta{TxID: item.TxID, OriginalIndex: index, Success: true, WriteSet: writes},
	}
}

func TestPorygonV52SameESCEarlierCTxLaterITxWriteHazardIsQuarantined(t *testing.T) {
	localKey := porygonFixtureKeyOnShard(t, "v52-local", 0, 4)
	remoteKey := porygonFixtureKeyOnShard(t, "v52-remote", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx-first", 0,
		tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		tx.AccessItem{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "none"},
	)
	itx := porygonBaselineRoutedTx("itx-later", 0, tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, itx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Assignments[0].CrossShard || plan.Assignments[1].CrossShard || plan.Assignments[0].ExecutionShard != plan.Assignments[1].ExecutionShard {
		t.Fatalf("fixture does not form same-ESC CTx->ITx: %#v", plan.Assignments)
	}
	crossPreview, _, crossCount, err := porygonPostExecutionConflictPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if crossCount != 0 || crossPreview[ctx.TxID] {
		t.Fatalf("same-ESC hazard leaked into OC cross-ESC preview: %v count=%d", crossPreview, crossCount)
	}
	samePreview, conflicts, sameCount, err := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if sameCount != 1 || !samePreview[ctx.TxID] || len(conflicts[ctx.TxID]) != 1 || conflicts[ctx.TxID][0] != itx.TxID {
		t.Fatalf("same-ESC deferred-write preview mismatch: preview=%v conflicts=%v count=%d", samePreview, conflicts, sameCount)
	}
	view := map[string]string{qualifyStateKey(block.ShardID, localKey): "base"}
	candidate := porygonV52CertifiedResult(ctx, plan.Assignments[0].OriginalIndex, map[string]string{localKey: "stale-u"})
	if porygonPublishSpeculativeOverlay(view, block.ShardID, candidate, porygonMergeTxBoolSets(crossPreview, samePreview)) {
		t.Fatal("same-ESC deferred-write CTx unexpectedly published speculative S")
	}
	if got := view[qualifyStateKey(block.ShardID, localKey)]; got != "base" {
		t.Fatalf("quarantined CTx polluted later ITx view: got=%q", got)
	}

	certified := map[string]porygonWaveResult{
		ctx.TxID: candidate,
		itx.TxID: porygonV52CertifiedResult(itx, plan.Assignments[1].OriginalIndex, map[string]string{localKey: "itx-current-round"}),
	}
	final, abandoned, count, remaining, err := porygonApplySameESCDeferredWriteHazards(plan.Assignments, block.TxList, certified)
	if err != nil {
		t.Fatal(err)
	}
	if !final[0].Abandoned || final[1].Abandoned || !abandoned[ctx.TxID] || count != 1 || remaining != 0 {
		t.Fatalf("same-ESC deferred-write closure mismatch: final=%#v abandoned=%v count=%d remaining=%d", final, abandoned, count, remaining)
	}
	truth, err := porygonBuildCertifiedExecutionTruth(block.BlockHash, block.Height, plan, final, certified, map[string]string{"s0": "esc-s0"})
	if err != nil {
		t.Fatal(err)
	}
	if truth.FutureProposalHeight != block.Height+2 {
		t.Fatalf("future Proposal.U height=%d want=%d", truth.FutureProposalHeight, block.Height+2)
	}
	for _, update := range truth.ProposalU {
		if update.TxID == ctx.TxID {
			t.Fatalf("abandoned same-ESC CTx leaked into h+2 Proposal.U: %+v", update)
		}
	}
}

func TestPorygonV52SameESCITxBeforeCTxRemainsValid(t *testing.T) {
	localKey := porygonFixtureKeyOnShard(t, "v52-direction-local", 0, 4)
	remoteKey := porygonFixtureKeyOnShard(t, "v52-direction-remote", 1, 4)
	itx := porygonBaselineRoutedTx("itx-first", 0, tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	ctx := porygonBaselineRoutedTx("ctx-later", 0,
		tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		tx.AccessItem{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "none"},
	)
	block := porygonFixtureBlock(t, itx, ctx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	preview, _, count, err := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || preview[ctx.TxID] {
		t.Fatalf("valid ITx->CTx direction was over-abandoned: preview=%v count=%d", preview, count)
	}
	certified := map[string]porygonWaveResult{
		itx.TxID: porygonV52CertifiedResult(itx, plan.Assignments[0].OriginalIndex, map[string]string{localKey: "itx"}),
		ctx.TxID: porygonV52CertifiedResult(ctx, plan.Assignments[1].OriginalIndex, map[string]string{localKey: "ctx-after-itx"}),
	}
	final, abandoned, abandonedCount, remaining, err := porygonApplySameESCDeferredWriteHazards(plan.Assignments, block.TxList, certified)
	if err != nil {
		t.Fatal(err)
	}
	if final[0].Abandoned || final[1].Abandoned || len(abandoned) != 0 || abandonedCount != 0 || remaining != 0 {
		t.Fatalf("valid ITx->CTx direction changed: final=%#v abandoned=%v", final, abandoned)
	}
	truth, err := porygonBuildCertifiedExecutionTruth(block.BlockHash, block.Height, plan, final, certified, map[string]string{"s0": "esc-s0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(truth.ProposalU) != 1 || truth.ProposalU[0].TxID != ctx.TxID {
		t.Fatalf("valid later CTx lost its h+2 Proposal.U: %+v", truth.ProposalU)
	}
}

func TestPorygonV52SameESCReadOnlyCTxKeyDoesNotTriggerWriteHazard(t *testing.T) {
	localKey := porygonFixtureKeyOnShard(t, "v52-readonly-local", 0, 4)
	remoteKey := porygonFixtureKeyOnShard(t, "v52-readonly-remote", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx-read-local", 0,
		tx.AccessItem{Key: localKey, Mode: tx.AccessRead, UpdateSemantics: "none"},
		tx.AccessItem{Key: remoteKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
	)
	itx := porygonBaselineRoutedTx("itx-write-local", 0, tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, itx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	preview, _, count, err := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || preview[ctx.TxID] {
		t.Fatalf("read-only local CTx access incorrectly treated as stale delayed write: preview=%v count=%d", preview, count)
	}
}

func TestPorygonV52SameESCDisjointWritesRemainRetained(t *testing.T) {
	ctxLocal := porygonFixtureKeyOnShard(t, "v52-disjoint-ctx", 0, 4)
	itxLocal := porygonFixtureKeyOnShard(t, "v52-disjoint-itx", 0, 4)
	remoteKey := porygonFixtureKeyOnShard(t, "v52-disjoint-remote", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx-disjoint", 0,
		tx.AccessItem{Key: ctxLocal, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		tx.AccessItem{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "none"},
	)
	itx := porygonBaselineRoutedTx("itx-disjoint", 0, tx.AccessItem{Key: itxLocal, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, itx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	preview, _, count, err := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || preview[ctx.TxID] {
		t.Fatalf("disjoint same-ESC writes incorrectly quarantined: preview=%v count=%d", preview, count)
	}
}

func TestPorygonV52CrossESCAndSameESCHazardOverlapDoesNotDoubleCount(t *testing.T) {
	localKey := porygonFixtureKeyOnShard(t, "v52-overlap-local", 0, 4)
	remoteKey := porygonFixtureKeyOnShard(t, "v52-overlap-remote", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx-overlap", 0,
		tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		tx.AccessItem{Key: remoteKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
	)
	localITx := porygonBaselineRoutedTx("itx-local", 0, tx.AccessItem{Key: localKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	remoteITx := porygonBaselineRoutedTx("itx-remote", 1, tx.AccessItem{Key: remoteKey, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, localITx, remoteITx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	crossPreview, _, crossCount, err := porygonPostExecutionConflictPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	samePreview, _, sameCount, err := porygonSameESCDeferredWriteHazardPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if crossCount != 1 || sameCount != 1 || !crossPreview[ctx.TxID] || !samePreview[ctx.TxID] {
		t.Fatalf("overlap fixture did not hit both closures: cross=%v/%d same=%v/%d", crossPreview, crossCount, samePreview, sameCount)
	}
	certified := map[string]porygonWaveResult{
		ctx.TxID:       porygonV52CertifiedResult(ctx, plan.Assignments[0].OriginalIndex, map[string]string{localKey: "ctx-local", remoteKey: "ctx-remote"}),
		localITx.TxID:  porygonV52CertifiedResult(localITx, plan.Assignments[1].OriginalIndex, map[string]string{localKey: "local-itx"}),
		remoteITx.TxID: porygonV52CertifiedResult(remoteITx, plan.Assignments[2].OriginalIndex, map[string]string{remoteKey: "remote-itx"}),
	}
	crossFinal, crossAbandoned, crossAbandonedCount, _, err := porygonPostExecutionConcurrencyControl(plan.Assignments, block.TxList, certified)
	if err != nil {
		t.Fatal(err)
	}
	if crossAbandonedCount != 1 || !crossAbandoned[ctx.TxID] || !crossFinal[0].Abandoned {
		t.Fatalf("cross-ESC closure did not abandon overlap CTx: final=%#v abandoned=%v count=%d", crossFinal, crossAbandoned, crossAbandonedCount)
	}
	final, sameAbandoned, newSameCount, remaining, err := porygonApplySameESCDeferredWriteHazards(crossFinal, block.TxList, certified)
	if err != nil {
		t.Fatal(err)
	}
	if newSameCount != 0 || len(sameAbandoned) != 0 || remaining != 0 {
		t.Fatalf("same-ESC closure double-counted already-abandoned overlap CTx: abandoned=%v new=%d remaining=%d", sameAbandoned, newSameCount, remaining)
	}
	if !porygonPreviewAbandonmentClosed(samePreview, final) {
		t.Fatal("same-ESC preview was not satisfied by prior cross-ESC terminal abandonment")
	}
}
