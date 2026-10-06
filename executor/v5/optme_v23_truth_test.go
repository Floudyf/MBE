package v5

import (
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestOptMEV23ObservedRWMustStayInsideOwnSignedAccessList(t *testing.T) {
	b := realblock.Block{TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessReadWrite}}},
		{TxID: "t1", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessReadWrite}}},
	}}
	good := []execution.TxDelta{{TxID: "t0", OriginalIndex: 0, ReadSet: []execution.ReadObservation{{Key: "a"}}, WriteSet: map[string]string{"a": "1"}}}
	if err := validateOptMEV23ObservedAccessCoverage(b, good); err != nil {
		t.Fatalf("valid observed RW rejected: %v", err)
	}
	bad := []execution.TxDelta{{TxID: "t0", OriginalIndex: 0, ReadSet: []execution.ReadObservation{{Key: "b"}}, WriteSet: map[string]string{}}}
	if err := validateOptMEV23ObservedAccessCoverage(b, bad); err == nil || !strings.Contains(err.Error(), "escaped signed AccessList") {
		t.Fatalf("cross-transaction envelope borrowing was not rejected: %v", err)
	}
}

func TestOptMEV23QualifiedAccessListKeyFailsClosed(t *testing.T) {
	b := realblock.Block{TxList: []tx.SignedTransaction{{TxID: "t0", AccessList: []tx.AccessItem{{Key: "s0::a", Mode: tx.AccessRead}}}}}
	if err := validateOptMEV23ObservedAccessCoverage(b, nil); err == nil {
		t.Fatal("qualified signed AccessList key must fail closed")
	}
}
