package blockstm

import "sync"

type TransactionStatus string

const (
	StatusPending    TransactionStatus = "pending"
	StatusExecuting  TransactionStatus = "executing"
	StatusValidating TransactionStatus = "validating"
	StatusWaiting    TransactionStatus = "waiting"
	StatusCommitted  TransactionStatus = "committed"
	StatusAborted    TransactionStatus = "aborted"
)

type SchedulerTaskKind string

const (
	TaskExecute  SchedulerTaskKind = "execute"
	TaskValidate SchedulerTaskKind = "validate"
)

type SchedulerTask struct {
	Kind       SchedulerTaskKind `json:"kind"`
	Version    Version           `json:"version"`
	Generation uint64            `json:"generation,omitempty"`
}

type Scheduler struct {
	mu          sync.Mutex
	statuses    map[TxnIndex]TransactionStatus
	incarnation map[TxnIndex]Incarnation
	queue       []SchedulerTask
	queued      map[SchedulerTask]bool
	aborts      int
}

func NewScheduler(txCount int) *Scheduler {
	return NewSchedulerWithOrder(txCount, defaultTxnOrder(txCount))
}

func NewSchedulerWithOrder(txCount int, order []TxnIndex) *Scheduler {
	s := &Scheduler{statuses: map[TxnIndex]TransactionStatus{}, incarnation: map[TxnIndex]Incarnation{}, queued: map[SchedulerTask]bool{}}
	seen := map[TxnIndex]bool{}
	for _, txn := range order {
		if int(txn) >= txCount || seen[txn] {
			continue
		}
		seen[txn] = true
		s.statuses[txn] = StatusPending
		s.enqueueLocked(SchedulerTask{Kind: TaskExecute, Version: Version{Txn: txn}})
	}
	for _, txn := range defaultTxnOrder(txCount) {
		if seen[txn] {
			continue
		}
		s.statuses[txn] = StatusPending
		s.enqueueLocked(SchedulerTask{Kind: TaskExecute, Version: Version{Txn: txn}})
	}
	return s
}

func NewValidationSchedulerWithOrder(txCount int, order []TxnIndex) *Scheduler {
	s := &Scheduler{statuses: map[TxnIndex]TransactionStatus{}, incarnation: map[TxnIndex]Incarnation{}, queued: map[SchedulerTask]bool{}}
	seen := map[TxnIndex]bool{}
	for _, txn := range order {
		if int(txn) >= txCount || seen[txn] {
			continue
		}
		seen[txn] = true
		s.statuses[txn] = StatusValidating
		s.enqueueLocked(SchedulerTask{Kind: TaskValidate, Version: Version{Txn: txn}})
	}
	for _, txn := range defaultTxnOrder(txCount) {
		if seen[txn] {
			continue
		}
		s.statuses[txn] = StatusValidating
		s.enqueueLocked(SchedulerTask{Kind: TaskValidate, Version: Version{Txn: txn}})
	}
	return s
}

func defaultTxnOrder(txCount int) []TxnIndex {
	order := make([]TxnIndex, 0, txCount)
	for index := 0; index < txCount; index++ {
		order = append(order, TxnIndex(index))
	}
	return order
}

// Next returns the lowest transaction-index task currently available. This is
// the ordering rule used by Block-STM's collaborative scheduler: execution and
// validation tasks share one ordered work set, and workers prefer lower
// transaction indexes without introducing a global execution/validation phase
// barrier.
func (s *Scheduler) Next() (SchedulerTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 {
		best := 0
		for index := 1; index < len(s.queue); index++ {
			if schedulerTaskLess(s.queue[index], s.queue[best]) {
				best = index
			}
		}
		task := s.queue[best]
		s.queue = append(s.queue[:best], s.queue[best+1:]...)
		delete(s.queued, task)
		if task.Version.Incarnation != s.incarnation[task.Version.Txn] {
			continue
		}
		if s.statuses[task.Version.Txn] == StatusWaiting {
			continue
		}
		if task.Kind == TaskExecute {
			s.statuses[task.Version.Txn] = StatusExecuting
		} else {
			s.statuses[task.Version.Txn] = StatusValidating
		}
		return task, true
	}
	return SchedulerTask{}, false
}

func schedulerTaskLess(left, right SchedulerTask) bool {
	if left.Version.Txn != right.Version.Txn {
		return left.Version.Txn < right.Version.Txn
	}
	if left.Version.Incarnation != right.Version.Incarnation {
		return left.Version.Incarnation > right.Version.Incarnation
	}
	if left.Kind != right.Kind {
		return left.Kind == TaskExecute
	}
	// A newer validation generation supersedes an older in-flight/queued
	// validation of the same incarnation.
	return left.Generation > right.Generation
}

func (s *Scheduler) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *Scheduler) ScheduleValidation(version Version) {
	s.ScheduleValidationGeneration(version, 0)
}

func (s *Scheduler) ScheduleValidationGeneration(version Version, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version.Incarnation != s.incarnation[version.Txn] {
		return
	}
	s.enqueueLocked(SchedulerTask{Kind: TaskValidate, Version: version, Generation: generation})
	s.statuses[version.Txn] = StatusValidating
}

func (s *Scheduler) ScheduleExecution(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version.Incarnation != s.incarnation[version.Txn] {
		return
	}
	s.enqueueLocked(SchedulerTask{Kind: TaskExecute, Version: version})
	s.statuses[version.Txn] = StatusPending
}

func (s *Scheduler) enqueueLocked(task SchedulerTask) {
	if s.queued == nil {
		s.queued = map[SchedulerTask]bool{}
	}
	if s.queued[task] {
		return
	}
	s.queued[task] = true
	s.queue = append(s.queue, task)
}

func (s *Scheduler) Commit(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version.Incarnation == s.incarnation[version.Txn] {
		s.statuses[version.Txn] = StatusCommitted
	}
}

func (s *Scheduler) Abort(version Version) Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abortLocked(version, false)
}

// AbortAndWait records an execution that stopped after reading an ESTIMATE.
// Block-STM does not immediately make the next incarnation runnable: it is
// resumed only after the blocking transaction finishes its next incarnation.
func (s *Scheduler) AbortAndWait(version Version) Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abortLocked(version, true)
}

func (s *Scheduler) abortLocked(version Version, wait bool) Version {
	current := s.incarnation[version.Txn]
	if version.Incarnation != current {
		return Version{Txn: version.Txn, Incarnation: current}
	}
	next := Version{Txn: version.Txn, Incarnation: current + 1}
	s.incarnation[version.Txn] = next.Incarnation
	s.aborts++
	if wait {
		s.statuses[version.Txn] = StatusWaiting
		return next
	}
	s.statuses[version.Txn] = StatusAborted
	s.enqueueLocked(SchedulerTask{Kind: TaskExecute, Version: next})
	return next
}

func (s *Scheduler) Wait(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version.Incarnation == s.incarnation[version.Txn] {
		s.statuses[version.Txn] = StatusWaiting
	}
}

func (s *Scheduler) Resume(version Version) {
	s.ResumeTask(SchedulerTask{Kind: TaskExecute, Version: version})
}

func (s *Scheduler) ResumeTask(task SchedulerTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task.Version.Incarnation != s.incarnation[task.Version.Txn] {
		return
	}
	if s.statuses[task.Version.Txn] != StatusWaiting {
		return
	}
	s.enqueueLocked(task)
	if task.Kind == TaskValidate {
		s.statuses[task.Version.Txn] = StatusValidating
	} else {
		s.statuses[task.Version.Txn] = StatusPending
	}
}

func (s *Scheduler) Status(txn TxnIndex) TransactionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statuses[txn]
}

func (s *Scheduler) AbortCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborts
}

type DependencyRegistry struct {
	mu      sync.Mutex
	waiters map[Version][]SchedulerTask
	seen    map[Version]map[SchedulerTask]bool
}

func NewDependencyRegistry() *DependencyRegistry {
	return &DependencyRegistry{waiters: map[Version][]SchedulerTask{}, seen: map[Version]map[SchedulerTask]bool{}}
}

func (r *DependencyRegistry) Register(waiter Version, dependency Version) {
	r.RegisterTask(SchedulerTask{Kind: TaskExecute, Version: waiter}, dependency)
}

func (r *DependencyRegistry) RegisterTask(waiter SchedulerTask, dependency Version) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen[dependency] == nil {
		r.seen[dependency] = map[SchedulerTask]bool{}
	}
	if r.seen[dependency][waiter] {
		return
	}
	r.seen[dependency][waiter] = true
	r.waiters[dependency] = append(r.waiters[dependency], waiter)
}

func (r *DependencyRegistry) Resolve(dependency Version) []Version {
	tasks := r.ResolveTasks(dependency)
	versions := make([]Version, 0, len(tasks))
	for _, task := range tasks {
		versions = append(versions, task.Version)
	}
	return versions
}

func (r *DependencyRegistry) ResolveTasks(dependency Version) []SchedulerTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters := append([]SchedulerTask(nil), r.waiters[dependency]...)
	delete(r.waiters, dependency)
	delete(r.seen, dependency)
	return waiters
}
