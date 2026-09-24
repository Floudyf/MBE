package v5

import (
	"context"
	"sync"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func calvinTestTx(id string, accesses ...tx.AccessItem) tx.SignedTransaction {
	return tx.SignedTransaction{TxID: id, AccessList: accesses}
}

func TestCalvinSharedReadersAreReadyTogether(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("t1", tx.AccessItem{Key: "k", Mode: tx.AccessRead}),
		calvinTestTx("t2", tx.AccessItem{Key: "k", Mode: tx.AccessRead}),
	}
	manager, err := newCalvinLockManager(items, "s0", func(string) string { return "s0" })
	if err != nil {
		t.Fatal(err)
	}
	ready := manager.initialReady()
	if len(ready) != 2 || ready[0] != 0 || ready[1] != 1 {
		t.Fatalf("shared readers should be ready together: %v", ready)
	}
}

func TestCalvinWriterPreventsLaterReaderBypass(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("r1", tx.AccessItem{Key: "k", Mode: tx.AccessRead}),
		calvinTestTx("w2", tx.AccessItem{Key: "k", Mode: tx.AccessWrite}),
		calvinTestTx("r3", tx.AccessItem{Key: "k", Mode: tx.AccessRead}),
	}
	manager, err := newCalvinLockManager(items, "s0", func(string) string { return "s0" })
	if err != nil {
		t.Fatal(err)
	}
	ready := manager.initialReady()
	if len(ready) != 1 || ready[0] != 0 {
		t.Fatalf("initial ready=%v", ready)
	}
	ready = manager.release(0)
	if len(ready) != 1 || ready[0] != 1 {
		t.Fatalf("writer should wake next: %v", ready)
	}
	ready = manager.release(1)
	if len(ready) != 1 || ready[0] != 2 {
		t.Fatalf("later reader must wait behind writer: %v", ready)
	}
}

func TestCalvinPureWriteSerializes(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("w1", tx.AccessItem{Key: "k", Mode: tx.AccessWrite}),
		calvinTestTx("w2", tx.AccessItem{Key: "k", Mode: tx.AccessWrite}),
	}
	manager, err := newCalvinLockManager(items, "s0", func(string) string { return "s0" })
	if err != nil {
		t.Fatal(err)
	}
	ready := manager.initialReady()
	if len(ready) != 1 || ready[0] != 0 {
		t.Fatalf("initial ready=%v", ready)
	}
	ready = manager.release(0)
	if len(ready) != 1 || ready[0] != 1 {
		t.Fatalf("second writer should wake after first: %v", ready)
	}
}

func TestCalvinWaitsForAllRequiredLocks(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("a", tx.AccessItem{Key: "x", Mode: tx.AccessWrite}),
		calvinTestTx("b", tx.AccessItem{Key: "y", Mode: tx.AccessWrite}),
		calvinTestTx("c", tx.AccessItem{Key: "x", Mode: tx.AccessRead}, tx.AccessItem{Key: "y", Mode: tx.AccessRead}),
	}
	manager, err := newCalvinLockManager(items, "s0", func(string) string { return "s0" })
	if err != nil {
		t.Fatal(err)
	}
	ready := manager.initialReady()
	if len(ready) != 2 {
		t.Fatalf("expected two independent writers ready, got %v", ready)
	}
	if got := manager.release(0); len(got) != 0 {
		t.Fatalf("c must still wait for y: %v", got)
	}
	got := manager.release(1)
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("c should wake only after both locks: %v", got)
	}
}

func TestCalvinCommutativeDeltaUsesExclusiveLock(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("d1", tx.AccessItem{Key: "hot", Mode: tx.AccessCommutativeDelta, Delta: 1}),
		calvinTestTx("d2", tx.AccessItem{Key: "hot", Mode: tx.AccessCommutativeDelta, Delta: 1}),
	}
	manager, err := newCalvinLockManager(items, "s0", func(string) string { return "s0" })
	if err != nil {
		t.Fatal(err)
	}
	if manager.exclCount != 2 || manager.sharedCount != 0 {
		t.Fatalf("delta locks shared=%d exclusive=%d", manager.sharedCount, manager.exclCount)
	}
	ready := manager.initialReady()
	if len(ready) != 1 || ready[0] != 0 {
		t.Fatalf("delta transactions must serialize: %v", ready)
	}
}

func TestCalvinRejectsRuntimeAccessBeyondSignedDeclaration(t *testing.T) {
	item := calvinTestTx("t", tx.AccessItem{Key: "declared", Mode: tx.AccessRead})
	delta := execution.TxDelta{TxID: "t", ReadSet: []execution.ReadObservation{{Key: "hidden"}}}
	if err := calvinValidateActualAccess(item, delta); err == nil {
		t.Fatal("Calvin must reject undeclared runtime reads because they were never locked")
	}
}

func TestCalvinCanonicalAccessesRejectDuplicateAndUnknownMode(t *testing.T) {
	if _, err := calvinCanonicalAccesses([]tx.AccessItem{{Key: "k", Mode: tx.AccessRead}, {Key: "k", Mode: tx.AccessWrite}}); err == nil {
		t.Fatal("duplicate declared key must fail closed")
	}
	if _, err := calvinCanonicalAccesses([]tx.AccessItem{{Key: "k", Mode: tx.AccessMode("future")}}); err == nil {
		t.Fatal("unsupported access mode must fail closed")
	}
}

func TestCalvinReadOnlyTransactionGetsDeterministicExecutionHome(t *testing.T) {
	item := calvinTestTx("r", tx.AccessItem{Key: "a", Mode: tx.AccessRead}, tx.AccessItem{Key: "b", Mode: tx.AccessRead})
	home := func(key string) string {
		if key == "a" {
			return "s1"
		}
		return "s0"
	}
	topology, err := calvinTopology(item, home)
	if err != nil {
		t.Fatal(err)
	}
	if len(topology.ActiveHomes) != 0 {
		t.Fatalf("read-only txn must have no writer homes: %v", topology.ActiveHomes)
	}
	if len(topology.ExecutionHomes) != 1 || topology.ExecutionHomes[0] != "s0" || topology.OutcomeHome != "s0" {
		t.Fatalf("read-only deterministic execution home mismatch: %#v", topology)
	}
}

func TestCalvinReadResultRequiresExactDeclaredKeySet(t *testing.T) {
	row := sealCalvinReadResult(CalvinReadResult{BlockHash: "b", Height: 1, TxID: "t", ExecutionShardID: "s0", SenderNodeID: "n0", ReadKeys: []string{"k1", "k2"}, Values: map[string]string{"k1": "v1"}})
	if err := validateCalvinReadResult(row); err == nil {
		t.Fatal("READ_RESULT missing a declared key must be rejected")
	}
	row = sealCalvinReadResult(CalvinReadResult{BlockHash: "b", Height: 1, TxID: "t", ExecutionShardID: "s0", SenderNodeID: "n0", ReadKeys: []string{"k1"}, Values: map[string]string{"k1": "v1", "hidden": "x"}})
	if err := validateCalvinReadResult(row); err == nil {
		t.Fatal("READ_RESULT with an undeclared extra key must be rejected")
	}
}

func TestCalvinOutcomeCarriesFailureAcrossNonParticipants(t *testing.T) {
	receipt := execution.Receipt{TxID: "t", BlockHash: "b", Height: 7, Success: false, Error: "business_failure", ExecutionCost: 3, StateKeys: []string{"k"}}
	outcome := sealCalvinOutcome(calvinOutcomeFromReceipt("b", 7, "s1", "n4", receipt))
	if err := validateCalvinOutcome(outcome); err != nil {
		t.Fatal(err)
	}
	rebuilt := calvinReceiptFromOutcome(outcome)
	if rebuilt.Success || rebuilt.Error != receipt.Error || rebuilt.ExecutionCost != receipt.ExecutionCost {
		t.Fatalf("canonical outcome lost terminal semantics: %#v", rebuilt)
	}
}

func TestCalvinPlanDigestBindsStateHomeMapping(t *testing.T) {
	b := realblock.Block{BlockHash: "b", Height: 1, TxList: []tx.SignedTransaction{
		calvinTestTx("t", tx.AccessItem{Key: "k", Mode: tx.AccessReadWrite}),
	}}
	left, leftDigest := calvinExecutionPlan(b, 4, calvinStatefulExecutorID, func(string) string { return "s0" })
	right, rightDigest := calvinExecutionPlan(b, 4, calvinStatefulExecutorID, func(string) string { return "s1" })
	if leftDigest == rightDigest || left.PlanDigest == right.PlanDigest {
		t.Fatal("Calvin plan digest must bind key-to-state-home mapping")
	}
}

func TestCalvinStateHomeRespectsExplicitExecutionShardPrefix(t *testing.T) {
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	shards := []string{"s0", "s1"}
	if got := calvinStateHomeForKey(sharding, "s1::asset:7", shards); got != "s1" {
		t.Fatalf("qualified state key moved across partitions: got=%s want=s1", got)
	}
	if got := calvinStateHomeForKey(sharding, "external::asset:7", shards); got == "external" {
		t.Fatalf("unknown namespace prefix must not manufacture an execution shard: %s", got)
	}
}

func TestCalvinRemoteReadWaitDoesNotStarveSingleBusinessWorker(t *testing.T) {
	activeWaits := calvinTestTx("active-waits", tx.AccessItem{Key: "remote-read", Mode: tx.AccessRead}, tx.AccessItem{Key: "local-write", Mode: tx.AccessWrite})
	activeWaits.AccessListSchema = "calvin_test_access_v1"
	passiveSupplies := calvinTestTx("passive-supplies", tx.AccessItem{Key: "trigger", Mode: tx.AccessRead}, tx.AccessItem{Key: "remote-write", Mode: tx.AccessWrite})
	passiveSupplies.AccessListSchema = "calvin_test_access_v1"
	block := realblock.Block{BlockHash: "calvin-starvation", Height: 9, TxList: []tx.SignedTransaction{activeWaits, passiveSupplies}}
	home := func(key string) string {
		switch key {
		case "remote-read", "remote-write":
			return "s1"
		default:
			return "s0"
		}
	}
	trigger := make(chan struct{})
	var triggerOnce sync.Once
	readExchange := func(ctx context.Context, input CalvinReadExchangeInput) (CalvinReadExchangeResult, error) {
		switch input.Result.TxID {
		case "active-waits":
			select {
			case <-trigger:
				return CalvinReadExchangeResult{Values: map[string]string{"remote-read": "rv"}, RemoteReadCount: 1}, nil
			case <-ctx.Done():
				return CalvinReadExchangeResult{}, ctx.Err()
			}
		case "passive-supplies":
			triggerOnce.Do(func() { close(trigger) })
			return CalvinReadExchangeResult{Values: input.Result.Values}, nil
		default:
			return CalvinReadExchangeResult{Values: input.Result.Values}, nil
		}
	}
	outcomeExchange := func(_ context.Context, input CalvinOutcomeExchangeInput) (CalvinOutcomeExchangeResult, error) {
		if input.Publish {
			return CalvinOutcomeExchangeResult{Outcome: input.Outcome}, nil
		}
		return CalvinOutcomeExchangeResult{Outcome: CalvinTxOutcome{
			BlockHash: input.Outcome.BlockHash, Height: input.Outcome.Height, TxID: input.Outcome.TxID,
			OutcomeShardID: input.OutcomeShard, SenderNodeID: "remote-leader", Success: true,
		}}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	executor := calvinBlockExecutor{
		basicPlugin: makeBasic("block_executor", calvinStatefulExecutorID, map[string]any{"worker_count": 1}),
		mode:        calvinStatefulMode,
	}
	result, err := executor.ExecuteBlock(ctx, BlockExecutionInput{
		Block:                 block,
		BaseStateSnapshot:     map[string]string{"s0::trigger": "ready"},
		NodeID:                "n0",
		ExecutionShardID:      "s0",
		WorkerCount:           1,
		CalvinStateHome:       home,
		CalvinExecutionShards: []string{"s0", "s1"},
		CalvinReadExchange:    readExchange,
		CalvinOutcomeExchange: outcomeExchange,
	})
	if err != nil {
		t.Fatalf("remote-read coordination starved the single business worker: %v", err)
	}
	if result.ActualMetrics["calvin_coordination_waits_consume_worker_slots"] != false {
		t.Fatalf("coordination/worker separation evidence missing: %#v", result.ActualMetrics)
	}
}

func TestStatelessCalvinGlobalLockTableIncludesAllStateHomes(t *testing.T) {
	items := []tx.SignedTransaction{
		calvinTestTx("t1", tx.AccessItem{Key: "left", Mode: tx.AccessWrite}),
		calvinTestTx("t2", tx.AccessItem{Key: "left", Mode: tx.AccessRead}, tx.AccessItem{Key: "right", Mode: tx.AccessWrite}),
	}
	home := func(key string) string {
		if key == "left" {
			return "s0"
		}
		return "s1"
	}
	manager, err := newCalvinLockManager(items, "", home)
	if err != nil {
		t.Fatal(err)
	}
	if manager.lockCount != 3 || manager.participantCount() != 2 {
		t.Fatalf("global locks=%d participants=%d", manager.lockCount, manager.participantCount())
	}
	ready := manager.initialReady()
	if len(ready) != 1 || ready[0] != 0 {
		t.Fatalf("global Calvin order not enforced: %v", ready)
	}
	ready = manager.release(0)
	if len(ready) != 1 || ready[0] != 1 {
		t.Fatalf("dependent transaction did not wake: %v", ready)
	}
}

func TestStatelessCalvinFinalAccessListVersionChainIncludesDefaultAccountKeys(t *testing.T) {
	items := tx.DefaultTransferAccessList("alice", "bob")
	items = append(items, tx.AccessItem{Key: "asset:7", Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	last := map[string]uint64{}
	first := calvinStateVersionDependenciesForAccessList(items, 7, last)
	if len(first) != len(items) {
		t.Fatalf("dependencies=%d accesses=%d", len(first), len(items))
	}
	byKey := map[string]tx.StateVersionDependency{}
	for _, dep := range first {
		byKey[dep.Key] = dep
	}
	for _, key := range []string{"balance:alice", "nonce:alice", "balance:bob", "nonce:bob", "asset:7"} {
		if _, ok := byKey[key]; !ok {
			t.Fatalf("final signed AccessList key missing version dependency: %s", key)
		}
	}
	if byKey["nonce:bob"].ProducedVersion != 0 {
		t.Fatal("read-only receiver nonce must not produce a version")
	}
	second := calvinStateVersionDependenciesForAccessList(items, 9, last)
	byKey = map[string]tx.StateVersionDependency{}
	for _, dep := range second {
		byKey[dep.Key] = dep
	}
	if byKey["balance:alice"].RequiredVersion != 7 || byKey["asset:7"].RequiredVersion != 7 {
		t.Fatalf("predecessor chain lost: %#v", byKey)
	}
}
