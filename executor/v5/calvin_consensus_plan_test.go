package v5

import (
	"encoding/json"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func calvinV34TestTx(id string, mode tx.AccessMode, clientRequired, clientProduced uint64) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID:       id,
		AccessList: []tx.AccessItem{{Key: "asset:k", Mode: mode, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			ExecutionShard: "s0",
			StateVersions:  []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: clientRequired, ProducedVersion: clientProduced}},
		},
	}
}

func TestCalvinConsensusVersionPlanUsesBlockOrderNotClientMetadata(t *testing.T) {
	block := realblock.Block{
		ShardID: "calvin-global",
		Height:  7,
		TxList: []tx.SignedTransaction{
			calvinV34TestTx("T2", tx.AccessReadWrite, 999, 1000),
			calvinV34TestTx("T1", tx.AccessReadWrite, 0, 998),
		},
	}
	plan, err := buildCalvinConsensusPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	v0, _ := calvinConsensusVersion(7, 0)
	v1, _ := calvinConsensusVersion(7, 1)
	if got := plan.VersionBindings[0].Dependencies[0]; got.RequiredVersion != 0 || got.ProducedVersion != v0 {
		t.Fatalf("first consensus transaction must start from block-boundary value and produce its consensus version: %#v", got)
	}
	if got := plan.VersionBindings[1].Dependencies[0]; got.RequiredVersion != v0 || got.ProducedVersion != v1 {
		t.Fatalf("second consensus transaction must depend on the first consensus writer, not client metadata: %#v", got)
	}
	if plan.OrderedTransactionIDs[0] != "T2" || plan.OrderedTransactionIDs[1] != "T1" {
		t.Fatalf("plan did not preserve block order: %#v", plan.OrderedTransactionIDs)
	}
}

func TestCalvinConsensusVersionPlanBlockBoundaryUsesCurrentCommittedValue(t *testing.T) {
	block7 := realblock.Block{ShardID: "calvin-global", Height: 7, TxList: []tx.SignedTransaction{calvinV34TestTx("A", tx.AccessReadWrite, 0, 0)}}
	block8 := realblock.Block{ShardID: "calvin-global", Height: 8, TxList: []tx.SignedTransaction{calvinV34TestTx("B", tx.AccessReadWrite, 123, 456)}}
	plan7, err := buildCalvinConsensusPlan(block7)
	if err != nil {
		t.Fatal(err)
	}
	plan8, err := buildCalvinConsensusPlan(block8)
	if err != nil {
		t.Fatal(err)
	}
	d7 := plan7.VersionBindings[0].Dependencies[0]
	d8 := plan8.VersionBindings[0].Dependencies[0]
	if d7.RequiredVersion != 0 || d8.RequiredVersion != 0 {
		t.Fatalf("the first access to a key in each block must read current committed block-start state: d7=%#v d8=%#v", d7, d8)
	}
	if d8.ProducedVersion <= d7.ProducedVersion {
		t.Fatalf("consensus versions must remain globally monotonic across block heights: d7=%d d8=%d", d7.ProducedVersion, d8.ProducedVersion)
	}
}

func TestCalvinSchedulerBindsAndVerifiesConsensusVersionPlan(t *testing.T) {
	block := realblock.Block{ShardID: "calvin-global", Height: 11, TxList: []tx.SignedTransaction{
		calvinV34TestTx("A", tx.AccessReadWrite, 0, 0),
		calvinV34TestTx("B", tx.AccessRead, 0, 0),
	}}
	scheduler := statelessCalvinScheduler{calvinScheduler{basicPlugin: makeBasic("scheduler", calvinStatelessSchedulerID, nil)}}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if planned.Block.ExecutionPlan == nil || planned.Block.ExecutionPlan.AlgorithmID != calvinConsensusPlanAlgorithmID {
		t.Fatalf("consensus version plan not attached: %#v", planned.Block.ExecutionPlan)
	}
	if err := scheduler.VerifyBlockPlan(planned.Block); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}

	var payload calvinConsensusPlan
	if err := json.Unmarshal(planned.Block.ExecutionPlan.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	payload.VersionBindings[1].Dependencies[0].RequiredVersion++
	raw, _ := json.Marshal(payload)
	tampered := planned.Block
	tampered.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   calvinConsensusPlanAlgorithmID,
		PayloadDigest: stableTextDigest(string(raw)),
		PlanDigest:    payload.PlanDigest,
		Payload:       raw,
	}
	if err := scheduler.VerifyBlockPlan(tampered); err == nil {
		t.Fatal("tampered consensus version semantics were accepted")
	}
}

func TestCalvinBindConsensusVersionsDoesNotMutateSignedTransaction(t *testing.T) {
	item := calvinV34TestTx("A", tx.AccessReadWrite, 91, 92)
	before := append([]tx.StateVersionDependency(nil), item.ExecutionRouting.StateVersions...)
	bound, err := calvinBindConsensusVersions(item, []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: 7, ProducedVersion: 8}})
	if err != nil {
		t.Fatal(err)
	}
	if item.ExecutionRouting.StateVersions[0] != before[0] {
		t.Fatalf("original signed transaction metadata was mutated: before=%#v after=%#v", before, item.ExecutionRouting.StateVersions)
	}
	got := bound.ExecutionRouting.StateVersions[0]
	if got.RequiredVersion != 7 || got.ProducedVersion != 8 {
		t.Fatalf("bound execution copy did not receive consensus versions: %#v", got)
	}
}
