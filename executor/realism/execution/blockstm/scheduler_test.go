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

func TestSchedulerAbortAndWaitResumesOnlyAfterDependency(t *testing.T) {
	scheduler := NewScheduler(1)
	initial, ok := scheduler.Next()
	if !ok {
		t.Fatal("missing initial task")
	}
	next := scheduler.AbortAndWait(initial.Version)
	if scheduler.Status(0) != StatusWaiting {
		t.Fatalf("expected waiting status, got %s", scheduler.Status(0))
	}
	if task, ok := scheduler.Next(); ok {
		t.Fatalf("waiting incarnation must not be runnable: %+v", task)
	}
	scheduler.Resume(next)
	task, ok := scheduler.Next()
	if !ok || task.Kind != TaskExecute || task.Version != next {
		t.Fatalf("unexpected resumed task: %+v ok=%v", task, ok)
	}
}

func TestDependencyRegistryResumesTypedWaiters(t *testing.T) {
	registry := NewDependencyRegistry()
	dependency := Version{Txn: 0, Incarnation: 0}
	waiter := SchedulerTask{Kind: TaskExecute, Version: Version{Txn: 1, Incarnation: 1}}
	registry.RegisterTask(waiter, dependency)
	registry.RegisterTask(waiter, dependency)
	resolved := registry.ResolveTasks(dependency)
	if len(resolved) != 1 || resolved[0] != waiter {
		t.Fatalf("unexpected dependency resolution: %#v", resolved)
	}
	if again := registry.ResolveTasks(dependency); len(again) != 0 {
		t.Fatalf("resolved dependency was not cleared: %#v", again)
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

func TestSchedulerAlwaysChoosesLowestTransactionIndex(t *testing.T) {
	scheduler := NewSchedulerWithOrder(4, []TxnIndex{3, 1})
	expected := []Version{{Txn: 0}, {Txn: 1}, {Txn: 2}, {Txn: 3}}
	for _, want := range expected {
		task, ok := scheduler.Next()
		if !ok || task.Kind != TaskExecute || task.Version != want {
			t.Fatalf("expected %+v, got %+v ok=%v", want, task, ok)
		}
	}
}

func TestValidationSchedulerAlwaysChoosesLowestTransactionIndex(t *testing.T) {
	scheduler := NewValidationSchedulerWithOrder(3, []TxnIndex{2})
	expected := []Version{{Txn: 0}, {Txn: 1}, {Txn: 2}}
	for _, want := range expected {
		task, ok := scheduler.Next()
		if !ok || task.Kind != TaskValidate || task.Version != want {
			t.Fatalf("expected validation %+v, got %+v ok=%v", want, task, ok)
		}
	}
}

func TestLowerIndexValidationPreemptsHigherIndexExecution(t *testing.T) {
	scheduler := NewScheduler(3)
	first, ok := scheduler.Next()
	if !ok || first.Version.Txn != 0 {
		t.Fatalf("unexpected first execution: %+v", first)
	}
	scheduler.ScheduleValidation(first.Version)
	next, ok := scheduler.Next()
	if !ok || next.Kind != TaskValidate || next.Version.Txn != 0 {
		t.Fatalf("lower validation did not preempt higher execution: %+v", next)
	}
}

func TestSchedulerDeduplicatesIdenticalQueuedTasks(t *testing.T) {
	scheduler := NewScheduler(1)
	initial, ok := scheduler.Next()
	if !ok {
		t.Fatal("missing initial task")
	}
	scheduler.ScheduleExecution(initial.Version)
	scheduler.ScheduleExecution(initial.Version)
	if got := scheduler.QueueLen(); got != 1 {
		t.Fatalf("duplicate execution tasks were queued: %d", got)
	}
	_, _ = scheduler.Next()
	scheduler.ScheduleValidationGeneration(initial.Version, 3)
	scheduler.ScheduleValidationGeneration(initial.Version, 3)
	if got := scheduler.QueueLen(); got != 1 {
		t.Fatalf("duplicate validation tasks were queued: %d", got)
	}
}
