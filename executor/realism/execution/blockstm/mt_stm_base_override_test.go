package blockstm

import "testing"

// MT_STM_SINGLE_CONTEXT_V1: two consumers of the same key may request
// different already-certified exact external versions. Never mutate the
// single shared base snapshot to accommodate one of them.
func TestMTSTMPerTransactionExactBaseOverride(t *testing.T) {
	m := NewMVMemory()
	base := map[string]string{"x": "base"}
	five := map[string]string{"x": "v5"}
	seven := map[string]string{"x": "v7"}
	a := m.ReadWithBaseOverride("x", 1, base, five)
	b := m.ReadWithBaseOverride("x", 2, base, seven)
	if a.Value != "v5" || b.Value != "v7" || !a.FromBase || !b.FromBase || base["x"] != "base" {
		t.Fatalf("exact versions clobbered each other: a=%+v b=%+v base=%v", a, b, base)
	}
	if v := m.ValidateWithBaseOverrides(1, base, five, CapturedReads{Reads: []ReadDescriptor{a}}); !v.Valid {
		t.Fatalf("correct exact base read rejected: %+v", v)
	}
	if v := m.ValidateWithBaseOverrides(1, base, seven, CapturedReads{Reads: []ReadDescriptor{a}}); v.Valid {
		t.Fatalf("wrong exact version accepted: %+v", v)
	}
	m.Write("x", Version{Txn: 0, Incarnation: 0}, "internal")
	observed := m.ReadWithBaseOverride("x", 1, base, five)
	if !observed.FromBase || observed.Value != "v5" {
		t.Fatalf("signed exact version must supersede speculative MV write: %+v", observed)
	}
	native := m.Read("x", 1, base)
	if native.FromBase || native.Value != "internal" {
		t.Fatalf("ordinary MV read changed unexpectedly: %+v", native)
	}
	if valid := m.ValidateWithBaseOverrides(1, base, five, CapturedReads{Reads: []ReadDescriptor{observed}}); !valid.Valid {
		t.Fatalf("exact version read rejected after unrelated MV publication: %+v", valid)
	}
}
