package blockstm

import "testing"

// MBE_V37_BLOCKSTM_SCHEDULER_ISOLATION
func TestV37PrioritySchedulerAndSameIncarnationWait(t *testing.T) {
	s := NewPrioritySchedulerWithOrder(4, []TxnIndex{3, 1, 2, 0})
	first, ok := s.Next()
	if !ok || first.Version.Txn != 0 || first.Kind != TaskExecute {
		t.Fatalf("priority scheduler must choose lowest transaction index, got=%+v ok=%v", first, ok)
	}
	// Dependency suspension is not an abort: Wait/Resume keeps the exact version.
	s.Wait(first.Version)
	if s.Status(first.Version.Txn) != StatusWaiting {
		t.Fatalf("expected waiting status, got %s", s.Status(first.Version.Txn))
	}
	if s.AbortCount() != 0 {
		t.Fatalf("dependency wait must not increment abort count: %d", s.AbortCount())
	}
	s.Resume(first.Version)
	resumed, ok := s.Next()
	if !ok || resumed.Version != first.Version || resumed.Kind != TaskExecute {
		t.Fatalf("same incarnation was not resumed: first=%+v resumed=%+v ok=%v", first, resumed, ok)
	}
	if s.AbortCount() != 0 {
		t.Fatalf("resume changed abort count: %d", s.AbortCount())
	}
}
