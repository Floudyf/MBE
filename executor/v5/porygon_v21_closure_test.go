package v5

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"testing"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonBaselineRoutedTx(id string, executionShard int, accesses ...tx.AccessItem) tx.SignedTransaction {
	item := porygonFixtureTx(id, id+"-sender", accesses...)
	item.ExecutionRouting = &tx.ExecutionRoutingMetadata{ExecutionShard: fmt.Sprintf("s%d", executionShard), RoutingReason: "porygon_initiating_account_route"}
	return item
}

func porygonSuccessfulCertified(items ...tx.SignedTransaction) map[string]porygonWaveResult {
	out := map[string]porygonWaveResult{}
	for _, item := range items {
		out[item.TxID] = porygonWaveResult{Item: item, Delta: execution.TxDelta{TxID: item.TxID, Success: true, WriteSet: map[string]string{}}}
	}
	return out
}

func TestPorygonBaselineOrderingLeavesCTxITxForPostExecutionControl(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-ctx-itx", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx", 0, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	itx := porygonBaselineRoutedTx("itx", 1, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, itx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Assignments[0].CrossShard || plan.Assignments[0].Abandoned || plan.Assignments[1].CrossShard || plan.Assignments[1].Abandoned {
		t.Fatalf("Ordering must not pre-abandon CTx/ITx: %#v", plan.Assignments)
	}
	if plan.AbandonedCTxITxConflictCnt != 0 || !plan.ConflictClosureVerified {
		t.Fatalf("Ordering conflict boundary mismatch: %+v", plan)
	}
	final, abandoned, count, pairs, err := porygonPostExecutionConcurrencyControl(plan.Assignments, block.TxList, porygonSuccessfulCertified(ctx, itx))
	if err != nil {
		t.Fatal(err)
	}
	if !final[0].Abandoned || final[1].Abandoned || !abandoned[ctx.TxID] || count != 1 || pairs != 0 {
		t.Fatalf("post-execution CTx/ITx control mismatch: final=%#v abandoned=%#v count=%d pairs=%d", final, abandoned, count, pairs)
	}
}

func TestPorygonBaselinePostExecutionControlIsITxFirstIndependentOfPBFTPosition(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-itx-ctx", 1, 4)
	itx := porygonBaselineRoutedTx("itx-first", 1, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	ctx := porygonBaselineRoutedTx("ctx-later", 0, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, itx, ctx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Assignments[0].Abandoned || plan.Assignments[1].Abandoned {
		t.Fatalf("Ordering must leave CTx/ITx to post-execution control: %#v", plan.Assignments)
	}
	final, abandoned, count, pairs, err := porygonPostExecutionConcurrencyControl(plan.Assignments, block.TxList, porygonSuccessfulCertified(itx, ctx))
	if err != nil {
		t.Fatal(err)
	}
	if final[0].Abandoned || !final[1].Abandoned || !abandoned[ctx.TxID] || count != 1 || pairs != 0 {
		t.Fatalf("ITx-first post-execution control mismatch: final=%#v abandoned=%#v count=%d pairs=%d", final, abandoned, count, pairs)
	}
}

func TestPorygonBaselineQuarantinesPostExecutionAbandonedS3FromSuccessors(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-s3-quarantine", 1, 4)
	ctx := porygonBaselineRoutedTx("ctx-quarantine", 0, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	itx := porygonBaselineRoutedTx("itx-conflict", 1, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, ctx, itx)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	preview, conflicts, count, err := porygonPostExecutionConflictPreview(plan.Assignments, block.TxList)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !preview[ctx.TxID] || len(conflicts[ctx.TxID]) != 1 || conflicts[ctx.TxID][0] != itx.TxID {
		t.Fatalf("post-execution preview mismatch: preview=%v conflicts=%v count=%d", preview, conflicts, count)
	}
	view := map[string]string{qualifyStateKey(block.ShardID, key): "base"}
	candidate := porygonWaveResult{Item: ctx, Delta: execution.TxDelta{TxID: ctx.TxID, Success: true, WriteSet: map[string]string{key: "ghost"}}}
	if porygonPublishSpeculativeOverlay(view, block.ShardID, candidate, preview) {
		t.Fatal("post-execution-abandoned CTx unexpectedly published speculative S3")
	}
	if got := view[qualifyStateKey(block.ShardID, key)]; got != "base" {
		t.Fatalf("quarantined CTx polluted successor execution view: got=%q", got)
	}
	retained := porygonWaveResult{Item: itx, Delta: execution.TxDelta{TxID: itx.TxID, Success: true, WriteSet: map[string]string{key: "committed"}}}
	if !porygonPublishSpeculativeOverlay(view, block.ShardID, retained, preview) {
		t.Fatal("retained transaction was incorrectly quarantined")
	}
	if got := view[qualifyStateKey(block.ShardID, key)]; got != "committed" {
		t.Fatalf("retained transaction did not update successor execution view: got=%q", got)
	}
}

func TestPorygonBaselineOrderingPreservesEarlierCTxAndAbandonsLaterCTx(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-ctx-ctx", 1, 4)
	left := porygonBaselineRoutedTx("ctx-left", 0, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	right := porygonBaselineRoutedTx("ctx-right", 2, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	plan, err := buildPorygonPlan(porygonFixtureBlock(t, left, right), porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Assignments[0].Abandoned || !plan.Assignments[1].Abandoned || plan.AbandonedCTxCTxConflictCnt != 1 {
		t.Fatalf("CTx/CTx Ordering closure mismatch: %#v", plan.Assignments)
	}
	if !plan.ConflictClosureVerified || porygonNonAbandonedCrossESCCTxConflictPairs(plan.Assignments, []tx.SignedTransaction{left, right}) != 0 {
		t.Fatalf("retained CTx conflict remained: %+v", plan)
	}
}

func TestPorygonBaselinePartitionMaterializationIncludesT3AndU4AndBindsOrder(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-materialize", 1, 4)
	assignments := []porygonTxAssignment{
		{TxID: "itx", OriginalIndex: 1, ExecutionShard: 1, CrossShard: false},
		{TxID: "ctx", OriginalIndex: 2, ExecutionShard: 0, CrossShard: true},
	}
	certified := map[string]porygonWaveResult{
		"itx": {Delta: execution.TxDelta{TxID: "itx", Success: true, WriteSet: map[string]string{key: "A"}}},
		"ctx": {Delta: execution.TxDelta{TxID: "ctx", Success: true, WriteSet: map[string]string{key: "B"}}},
	}
	updates, count := porygonPartitionMaterializationUpdates(assignments, 4, certified)
	items := updates["s1"]
	if count != 2 || len(items) != 2 || items[0].TxID != "itx" || items[1].TxID != "ctx" {
		t.Fatalf("ordered T3+U4 materialization missing: count=%d updates=%+v", count, updates)
	}
	root := porygonProspectivePartitionRoot(map[string]string{}, "s1", []PorygonStateUpdate{items[1], items[0]})
	expected := state.RootOfSnapshot(map[string]string{qualifyStateKey("s1", key): "B"})
	if root != expected {
		t.Fatalf("canonical materialization root=%s want=%s", root, expected)
	}
	if porygonUpdateDigest([]PorygonStateUpdate{items[1], items[0]}) != porygonUpdateDigest(items) {
		t.Fatal("update digest is not canonical-order invariant")
	}
	orderChanged := []PorygonStateUpdate{{TxID: "itx", OriginalIndex: 2, Key: key, Value: "A"}, {TxID: "ctx", OriginalIndex: 1, Key: key, Value: "B"}}
	if porygonUpdateDigest(orderChanged) == porygonUpdateDigest(items) {
		t.Fatal("update digest failed to bind original transaction order")
	}
}

func TestPorygonBaselinePostExecutionAbandonedCTxIsExcludedFromU4(t *testing.T) {
	key := porygonFixtureKeyOnShard(t, "baseline-u4-filter", 1, 4)
	assignments := []porygonTxAssignment{
		{TxID: "itx", OriginalIndex: 0, ExecutionShard: 1, CrossShard: false},
		{TxID: "ctx", OriginalIndex: 1, ExecutionShard: 0, CrossShard: true, Abandoned: true, ConflictReason: "oc_post_execution_ctx_itx_conflict_abandoned"},
	}
	certified := map[string]porygonWaveResult{
		"itx": {Delta: execution.TxDelta{TxID: "itx", Success: true, WriteSet: map[string]string{key: "A"}}},
		"ctx": {Delta: execution.TxDelta{TxID: "ctx", Success: true, WriteSet: map[string]string{key: "B"}}},
	}
	updates, count := porygonPartitionMaterializationUpdates(assignments, 4, certified)
	if count != 1 || len(updates["s1"]) != 1 || updates["s1"][0].TxID != "itx" {
		t.Fatalf("abandoned CTx leaked into U4/materialization: count=%d updates=%+v", count, updates)
	}
}

func TestPorygonBaselineMultiShardCertificateBindsMajorityACKs(t *testing.T) {
	runtime, privateKeys := porygonV26AuthenticatedRuntime(t)
	updates := []PorygonStateUpdate{{TxID: "ordered", OriginalIndex: 1, Key: "asset:baseline-cert", Value: "ok"}}
	updateDigest := porygonUpdateDigest(updates)
	root := porygonProspectivePartitionRoot(map[string]string{}, "s0", updates)
	acks := make([]PorygonPartitionUpdateAck, 0, 3)
	voters := []string{"n0", "n1", "n2"}
	for _, nodeID := range voters {
		ack := PorygonPartitionUpdateAck{BlockHash: "baseline-cert-block", Height: 21, PartitionID: "s0", UpdateDigest: updateDigest, ProspectiveRoot: root, NodeID: nodeID, Attempt: 1}
		ack.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKeys[nodeID], porygonUpdateAckBytes(ack)))
		acks = append(acks, ack)
	}
	cert := PorygonMultiShardUpdateCertificate{BlockHash: "baseline-cert-block", Height: 21, Attempt: 1, Partitions: []PorygonPartitionRootCertificate{{PartitionID: "s0", UpdateDigest: updateDigest, ProspectiveRoot: root, Voters: append([]string(nil), voters...), Acks: acks}}}
	cert.GlobalStateRoot = porygonGlobalRootFromPartitions(cert.Partitions)
	cert.CertificateDigest = porygonMultiShardCertDigest(cert)
	if err := runtime.validatePorygonMultiShardCertificate(cert); err != nil {
		t.Fatalf("valid strict-majority root certificate rejected: %v", err)
	}
	tampered := cert
	tampered.Partitions = append([]PorygonPartitionRootCertificate(nil), cert.Partitions...)
	tampered.Partitions[0].Acks = append([]PorygonPartitionUpdateAck(nil), cert.Partitions[0].Acks...)
	tampered.Partitions[0].Acks[1].ProspectiveRoot = "tampered-root"
	tampered.CertificateDigest = porygonMultiShardCertDigest(tampered)
	if err := runtime.validatePorygonMultiShardCertificate(tampered); err == nil {
		t.Fatal("certificate accepted an ACK whose prospective root was not bound to the partition entry")
	}
}
