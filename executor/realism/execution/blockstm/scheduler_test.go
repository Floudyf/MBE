package blockstm

import "testing"

func TestSchedulerAbortIncrementsIncarnationAndRequeues(t *testing.T) {
	scheduler := NewScheduler(1)
	task, ok := scheduler.Next()
	if !ok || task.Kind != TaskExecute || task.Version != (Version{Txn: 0, Incarnation: 0}) {
		t.Fatalf("unexpected first task: %+v ok=%v", task, ok)
	}
	next := scheduler.Abort(task.Version)
	if next != (Version{Txn: 0, Incarnation: 1}) || scheduler.AbortCount() != 1 {
		t.Fatalf("unexpected abort result: %+v count=%d", next, scheduler.AbortCount())
	}
	task, ok = scheduler.Next()
	if !ok || task.Version != next || task.Kind != TaskExecute {
		t.Fatalf("expected re-execution task: %+v ok=%v", task, ok)
	}
}

func TestDependencyRegistryResumesWaiters(t *testing.T) {
	registry := NewDependencyRegistry()
	scheduler := NewScheduler(2)
	waiter := Version{Txn: 1, Incarnation: 0}
	dependency := Version{Txn: 0, Incarnation: 0}
	registry.Register(waiter, dependency)
	scheduler.Wait(waiter)
	if scheduler.Status(waiter.Txn) != StatusWaiting {
		t.Fatalf("expected waiting status")
	}
	for _, resumed := range registry.Resolve(dependency) {
		scheduler.Resume(resumed)
	}
	if scheduler.Status(waiter.Txn) != StatusPending {
		t.Fatalf("expected resumed pending status")
	}
}

func TestSchedulerValidationCommitFlow(t *testing.T) {
	scheduler := NewScheduler(1)
	task, _ := scheduler.Next()
	scheduler.ScheduleValidation(task.Version)
	validation, ok := scheduler.Next()
	if !ok || validation.Kind != TaskValidate {
		t.Fatalf("expected validation task: %+v ok=%v", validation, ok)
	}
	scheduler.Commit(validation.Version)
	if scheduler.Status(0) != StatusCommitted {
		t.Fatalf("expected committed status")
	}
}
