package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestPorygonFixV34StateProjectionPreservesGenesisTAnchor(t *testing.T) {
	access := tx.AccessItem{Key: "asset:genesis", Mode: tx.AccessRead}
	kind := porygonStateProjectionAccessKindForProposal(access, 0, "genesis-partition-root")
	height, root, ok := porygonStateProjectionAnchor(kind)
	if !ok || height != 0 || root != "genesis-partition-root" {
		t.Fatalf("Proposal.T@0 anchor round trip failed: %d %q %t kind=%q", height, root, ok, kind)
	}
}

func TestPorygonFixV503PendingEvidenceProtectsAllUncommittedPriorTransactions(t *testing.T) {
	runtime := &NodeRuntime{}
	state := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"ctx": {TxID: "ctx", OriginHeight: 10, CrossShard: true, Status: porygonPaperTxOrdered, Accesses: []tx.AccessItem{{Key: "asset:a", Mode: tx.AccessWrite}}, LockedKeys: []string{"asset:a"}},
		"itx": {TxID: "itx", OriginHeight: 10, CrossShard: false, Status: porygonPaperTxOrdered, Accesses: []tx.AccessItem{{Key: "asset:b", Mode: tx.AccessWrite}}, LockedKeys: []string{"asset:b"}},
	}}
	porygonPaperRuntimeStates.Store(runtime, state)
	defer porygonPaperRuntimeStates.Delete(runtime)
	defer porygonV50RecoveryStates.Delete(runtime)

	first := runtime.porygonPaperPendingEvidenceForProposal(11)
	if len(first) != 2 || first[0].TxID != "ctx" || first[1].TxID != "itx" {
		t.Fatalf("proposal B11 pending set=%#v want uncommitted CTx+ITx", first)
	}
	firstDigest := stableJSONDigest(first)
	state.mu.Lock()
	state.txs["ctx"].Status = porygonPaperTxUpdateApplied
	state.txs["itx"].Status = porygonPaperTxITxExecuted
	state.mu.Unlock()
	second := runtime.porygonPaperPendingEvidenceForProposal(11)
	if stableJSONDigest(second) != firstDigest {
		t.Fatalf("local execution timing changed B(h+1) consensus pending evidence: first=%#v second=%#v", first, second)
	}
	atITxCommit := runtime.porygonPaperPendingEvidenceForProposal(12)
	if len(atITxCommit) != 1 || atITxCommit[0].TxID != "ctx" {
		t.Fatalf("B12 pending set=%#v want only still-uncommitted CTx", atITxCommit)
	}
	if later := runtime.porygonPaperPendingEvidenceForProposal(14); len(later) != 0 {
		t.Fatalf("B14 retained normal CTx beyond deterministic h+4 release: %#v", later)
	}
}

func TestPorygonFixV34AdmissionRequiresSignedDeclaredAccess(t *testing.T) {
	plugin := porygonAdmission{makeBasic("transaction_admission", porygonAdmissionID, nil)}
	_, privateKey := tx.DeterministicKeyPair("porygon-admission")
	valid := tx.SignedTransaction{LogicalTxID: "admit-ok", Receiver: "receiver", Value: 1, AccessList: []tx.AccessItem{{Key: "asset:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}, StateKeys: []string{"asset:alice"}}
	if err := tx.Sign(&valid, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Admit(valid); err != nil {
		t.Fatalf("valid Porygon declared-access transaction rejected: %v", err)
	}

	bad := tx.SignedTransaction{LogicalTxID: "admit-bad", Receiver: "receiver", Value: 1, AccessList: []tx.AccessItem{{Key: "asset:alice", Mode: tx.AccessRead}, {Key: "asset:alice", Mode: tx.AccessWrite}}, StateKeys: []string{"asset:alice"}}
	if err := tx.Sign(&bad, privateKey); err != nil {
		t.Fatal(err)
	}
	if err := plugin.Admit(bad); err == nil {
		t.Fatal("duplicate Porygon access key was admitted")
	}
}

func TestPorygonFixV34ObjectShardingKeepsRelatedObjectFieldsTogether(t *testing.T) {
	plugin := porygonObjectSharding{makeBasic("sharding", porygonShardingID, nil)}
	shards := []string{"s0", "s1", "s2", "s3"}
	left := plugin.ShardFor([]string{"m.federation/miners/alice"}, shards)
	right := plugin.ShardFor([]string{"m.federation/bags/alice"}, shards)
	if left == "" || left != right {
		t.Fatalf("related Porygon account/object state split across homes: %q != %q", left, right)
	}
}

func TestPorygonFixV34TransactionBlockDurableSourcesStayInOneFixedStoragePartition(t *testing.T) {
	runtime := &NodeRuntime{plan: Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ExecutionShardID: "s0"}, {NodeID: "n1", ExecutionShardID: "s0"},
		{NodeID: "n2", ExecutionShardID: "s1"}, {NodeID: "n3", ExecutionShardID: "s1"},
	}}, pluginSnapshot: map[string]PluginConfig{
		"block_executor": {PluginID: porygonBlockExecutorID, Config: map[string]any{"execution_shard_count": 2}},
	}}
	body := porygonBuildTransactionBlock(1, "porygon-global", []tx.SignedTransaction{{TxID: "tb"}}, "")
	nodes := runtime.porygonTransactionBlockStorageNodeIDs(body)
	if len(nodes) != 2 {
		t.Fatalf("durable TransactionBlock replicas=%v want exactly one two-replica fixed partition", nodes)
	}
	partition := ""
	for _, nodeID := range nodes {
		for _, node := range runtime.plan.NodeConfigs {
			if node.NodeID == nodeID {
				if partition == "" {
					partition = effectiveExecutionShardID(node)
				}
				if effectiveExecutionShardID(node) != partition {
					t.Fatalf("TransactionBlock durable sources crossed fixed Storage Roles: %v", nodes)
				}
			}
		}
	}
}
