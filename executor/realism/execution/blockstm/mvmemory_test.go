package blockstm

import "testing"

func TestMVMemoryReadsLatestLowerVersion(t *testing.T) {
	memory := NewMVMemory()
	base := map[string]string{"balance:a": "10"}
	memory.Write("balance:a", Version{Txn: 0, Incarnation: 0}, "9")
	memory.Write("balance:a", Version{Txn: 2, Incarnation: 0}, "7")

	read := memory.Read("balance:a", 2, base)
	if read.FromBase || read.Version != (Version{Txn: 0, Incarnation: 0}) || read.Value != "9" {
		t.Fatalf("unexpected read: %+v", read)
	}
}

func TestMVMemoryFallsBackToBase(t *testing.T) {
	memory := NewMVMemory()
	read := memory.Read("balance:a", 0, map[string]string{"balance:a": "10"})
	if !read.FromBase || read.Value != "10" {
		t.Fatalf("expected base read, got %+v", read)
	}
}

func TestCapturedReadsValidateUntilLowerWriteChanges(t *testing.T) {
	memory := NewMVMemory()
	base := map[string]string{"nonce:a": "0"}
	captured := CapturedReads{}
	captured.Add(memory.Read("nonce:a", 3, base))
	if result := memory.Validate(3, base, captured); !result.Valid {
		t.Fatalf("expected valid captured read: %+v", result)
	}

	memory.Write("nonce:a", Version{Txn: 1, Incarnation: 0}, "1")
	result := memory.Validate(3, base, captured)
	if result.Valid || result.FailedKey != "nonce:a" || result.Observed.Value != "1" {
		t.Fatalf("expected validation failure after lower write: %+v", result)
	}
}

func TestEstimateCreatesDependencyInsteadOfStableRead(t *testing.T) {
	memory := NewMVMemory()
	base := map[string]string{"balance:a": "10"}
	memory.MarkEstimate("balance:a", Version{Txn: 1, Incarnation: 2})

	read := memory.Read("balance:a", 4, base)
	if !read.Estimate || read.DependencyOn == nil || *read.DependencyOn != (Version{Txn: 1, Incarnation: 2}) {
		t.Fatalf("expected estimate dependency, got %+v", read)
	}
	captured := CapturedReads{}
	captured.Add(ReadDescriptor{Key: "balance:a", FromBase: true, Value: "10"})
	result := memory.Validate(4, base, captured)
	if result.Valid || result.Dependency == nil {
		t.Fatalf("expected dependency validation failure, got %+v", result)
	}
}

func TestAbortCanReplaceOldVersionWithNewIncarnation(t *testing.T) {
	memory := NewMVMemory()
	base := map[string]string{}
	memory.Write("balance:a", Version{Txn: 1, Incarnation: 0}, "9")
	memory.MarkEstimate("balance:a", Version{Txn: 1, Incarnation: 0})
	memory.Write("balance:a", Version{Txn: 1, Incarnation: 1}, "8")

	read := memory.Read("balance:a", 2, base)
	if read.Estimate || read.Version != (Version{Txn: 1, Incarnation: 1}) || read.Value != "8" {
		t.Fatalf("expected latest reincarnation read, got %+v", read)
	}
}
