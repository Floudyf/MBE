package v5

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestPorygonV46NilAndEmptyProposalUHaveOneSemanticDigest(t *testing.T) {
	var nilU []PorygonProposalUpdate
	emptyU := []PorygonProposalUpdate{}
	if porygonProposalUSemanticDigest(nilU) != porygonProposalUSemanticDigest(emptyU) {
		t.Fatal("nil and empty Proposal.U changed semantic digest")
	}
	if porygonCanonicalProposalUpdates(emptyU) != nil {
		t.Fatal("zero-entry Proposal.U is not canonical nil")
	}
}

func TestPorygonV46CertifiedTruthTracksFinalProtocolObligations(t *testing.T) {
	plan := porygonExecutionPlan{BlockHeight: 6, ExecutionShardCount: 2, Assignments: []porygonTxAssignment{
		{TxID: "itx", OriginalIndex: 0, ExecutionShard: 0, CrossShard: false},
		{TxID: "ctx", OriginalIndex: 1, ExecutionShard: 1, CrossShard: true, InvolvedShards: []int{0, 1}},
	}}
	certified := map[string]porygonWaveResult{
		"itx": {Delta: executionDeltaForV46("itx", 0, map[string]string{"a": "1"})},
		"ctx": {Delta: executionDeltaForV46("ctx", 1, map[string]string{"b": "2"})},
	}
	truth, err := porygonBuildCertifiedExecutionTruth("b6", 6, plan, plan.Assignments, certified, map[string]string{"s0": "r0", "s1": "r1"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"itx|final_commit|8": true, "ctx|proposal_u|8": true, "ctx|final_commit|10": true}
	for _, row := range truth.FutureObligations {
		key := row.TxID + "|" + row.Kind + "|" + fmtUintV46(row.TargetHeight)
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing future obligations: %v", want)
	}
}

func TestPorygonV46MaintenanceCertifiedTruthAllowsUOnlyESCs(t *testing.T) {
	plan := porygonExecutionPlan{BlockHeight: 5, ExecutionShardCount: 2}
	truth, err := porygonBuildCertifiedExecutionTruth("maintenance-b5", 5, plan, nil, map[string]porygonWaveResult{}, map[string]string{"s1": "u-root-result"})
	if err != nil {
		t.Fatal(err)
	}
	if len(truth.RequiredExecutionShards) != 1 || truth.RequiredExecutionShards[0] != "s1" {
		t.Fatalf("maintenance ESC coverage lost certified U-only shard: %+v", truth.RequiredExecutionShards)
	}
}

func executionDeltaForV46(txID string, index int, writes map[string]string) execution.TxDelta {
	return execution.TxDelta{TxID: txID, OriginalIndex: index, Success: true, WriteSet: writes}
}

func fmtUintV46(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func TestPorygonV46ProposalReservationLedgerSurvivesCompactBody(t *testing.T) {
	r := &NodeRuntime{pool: mempool.New("n0", "porygon-global", mempool.DefaultPolicy(), nil)}
	items := []tx.SignedTransaction{{TxID: "t1"}, {TxID: "t2"}}
	r.porygonRememberProposalReservation("compact-hash", 6, items)
	if got := r.porygonProposalReservationCount(); got != 1 {
		t.Fatalf("reservation ledger count=%d want=1", got)
	}
	stored, ok := r.porygonTakeProposalReservation("compact-hash")
	if !ok || len(stored) != 2 || stored[0].TxID != "t1" || stored[1].TxID != "t2" {
		t.Fatalf("reservation ledger lost compact proposal items: %+v", stored)
	}
}

func TestPorygonV46TransactionBlockBytesIgnoreWitnessCertificate(t *testing.T) {
	items := []tx.SignedTransaction{{TxID: "t1"}}
	left := porygonBuildTransactionBlock(6, "porygon-global", items, "witness-a")
	right := porygonBuildTransactionBlock(6, "porygon-global", items, "witness-b")
	if left.TransactionBlockID != right.TransactionBlockID {
		t.Fatal("witness certificate changed content-addressed TransactionBlockID")
	}
	leftRaw, err := json.Marshal(left)
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := json.Marshal(right)
	if err != nil {
		t.Fatal(err)
	}
	if string(leftRaw) != string(rightRaw) {
		t.Fatal("witness certificate changed persisted TransactionBlock bytes")
	}
}

func TestPorygonV46CertificateDigestFunctionsArePure(t *testing.T) {
	witness := PorygonWitnessCertificate{CommitteeMembers: []string{"n2", "n1"}, Votes: []PorygonWitnessVote{{NodeID: "n2"}, {NodeID: "n1"}}}
	witnessBefore := deepCopyV46(t, witness)
	_ = porygonWitnessCertificateDigest(witness)
	if !reflect.DeepEqual(witness, witnessBefore) {
		t.Fatal("witness digest mutated live certificate")
	}

	handoff := PorygonUpdateHandoffCertificate{Committee: []string{"n2", "n1"}, Votes: []PorygonUpdateHandoffVote{{NodeID: "n2"}, {NodeID: "n1"}}}
	handoffBefore := deepCopyV46(t, handoff)
	_ = porygonHandoffCertificateDigest(handoff)
	if !reflect.DeepEqual(handoff, handoffBefore) {
		t.Fatal("handoff digest mutated live certificate")
	}

	multi := PorygonMultiShardUpdateCertificate{Partitions: []PorygonPartitionRootCertificate{
		{PartitionID: "s1", Voters: []string{"n7", "n6"}, Acks: []PorygonPartitionUpdateAck{{NodeID: "n7"}, {NodeID: "n6"}}, HandoffCertificate: handoff},
		{PartitionID: "s0", Voters: []string{"n3", "n2"}, Acks: []PorygonPartitionUpdateAck{{NodeID: "n3"}, {NodeID: "n2"}}, HandoffCertificate: handoff},
	}}
	multiBefore := deepCopyV46(t, multi)
	_ = porygonMultiShardCertDigest(multi)
	if !reflect.DeepEqual(multi, multiBefore) {
		t.Fatal("multi-shard digest mutated live certificate")
	}

	rollback := PorygonRollbackCertificate{Partitions: []PorygonRollbackPartitionCertificate{
		{PartitionID: "s1", Voters: []string{"n7", "n6"}, Acks: []PorygonRollbackAck{{NodeID: "n7"}, {NodeID: "n6"}}},
		{PartitionID: "s0", Voters: []string{"n3", "n2"}, Acks: []PorygonRollbackAck{{NodeID: "n3"}, {NodeID: "n2"}}},
	}}
	rollbackBefore := deepCopyV46(t, rollback)
	_ = porygonRollbackCertificateDigest(rollback)
	if !reflect.DeepEqual(rollback, rollbackBefore) {
		t.Fatal("rollback digest mutated live certificate")
	}
}

func deepCopyV46[T any](t *testing.T, in T) T {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
