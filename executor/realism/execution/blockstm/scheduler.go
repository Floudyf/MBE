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
	Kind    SchedulerTaskKind `json:"kind"`
	Version Version           `json:"version"`
}

type Scheduler struct {
	mu          sync.Mutex
	statuses    map[TxnIndex]TransactionStatus
	incarnation map[TxnIndex]Incarnation
	queue       []SchedulerTask
	aborts      int
}

func NewScheduler(txCount int) *Scheduler {
	s := &Scheduler{statuses: map[TxnIndex]TransactionStatus{}, incarnation: map[TxnIndex]Incarnation{}}
	for _, txn := range defaultTxnOrder(txCount) {
		s.statuses[txn] = StatusPending
		s.queue = append(s.queue, SchedulerTask{Kind: TaskExecute, Version: Version{Txn: txn}})
	}
	return s
}

func NewSchedulerWithOrder(txCount int, order []TxnIndex) *Scheduler {
	s := &Scheduler{statuses: map[TxnIndex]TransactionStatus{}, incarnation: map[TxnIndex]Incarnation{}}
	seen := map[TxnIndex]bool{}
	for _, txn := range order {
		if int(txn) >= txCount || seen[txn] {
			continue
		}
		seen[txn] = true
		s.statuses[txn] = StatusPending
		s.queue = append(s.queue, SchedulerTask{Kind: TaskExecute, Version: Version{Txn: txn}})
	}
	for _, txn := range defaultTxnOrder(txCount) {
		if seen[txn] {
			continue
		}
		s.statuses[txn] = StatusPending
		s.queue = append(s.queue, SchedulerTask{Kind: TaskExecute, Version: Version{Txn: txn}})
	}
	return s
}

func NewValidationSchedulerWithOrder(txCount int, order []TxnIndex) *Scheduler {
	s := &Scheduler{statuses: map[TxnIndex]TransactionStatus{}, incarnation: map[TxnIndex]Incarnation{}}
	seen := map[TxnIndex]bool{}
	for _, txn := range order {
		if int(txn) >= txCount || seen[txn] {
			continue
		}
		seen[txn] = true
		s.statuses[txn] = StatusValidating
		s.queue = append(s.queue, SchedulerTask{Kind: TaskValidate, Version: Version{Txn: txn}})
	}
	for _, txn := range defaultTxnOrder(txCount) {
		if seen[txn] {
			continue
		}
		s.statuses[txn] = StatusValidating
		s.queue = append(s.queue, SchedulerTask{Kind: TaskValidate, Version: Version{Txn: txn}})
	}
	return s
}

func defaultTxnOrder(txCount int) []TxnIndex {
	order := make([]TxnIndex, 0, txCount)
	for index := 0; index < txCount; index++ {
		txn := TxnIndex(index)
		order = append(order, txn)
	}
	return order
}

func (s *Scheduler) Next() (SchedulerTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return SchedulerTask{}, false
	}
	task := s.queue[0]
	s.queue = s.queue[1:]
	if task.Kind == TaskExecute {
		s.statuses[task.Version.Txn] = StatusExecuting
	} else {
		s.statuses[task.Version.Txn] = StatusValidating
	}
	return task, true
}

func (s *Scheduler) ScheduleValidation(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = append(s.queue, SchedulerTask{Kind: TaskValidate, Version: version})
	s.statuses[version.Txn] = StatusValidating
}

func (s *Scheduler) Commit(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[version.Txn] = StatusCommitted
}

func (s *Scheduler) Abort(version Version) Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := Version{Txn: version.Txn, Incarnation: s.incarnation[version.Txn] + 1}
	s.incarnation[version.Txn] = next.Incarnation
	s.statuses[version.Txn] = StatusAborted
	s.aborts++
	s.queue = append(s.queue, SchedulerTask{Kind: TaskExecute, Version: next})
	return next
}

func (s *Scheduler) Wait(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[version.Txn] = StatusWaiting
}

func (s *Scheduler) Resume(version Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statuses[version.Txn] == StatusWaiting {
		s.queue = append(s.queue, SchedulerTask{Kind: TaskExecute, Version: version})
		s.statuses[version.Txn] = StatusPending
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
	waiters map[Version][]Version
}

func NewDependencyRegistry() *DependencyRegistry {
	return &DependencyRegistry{waiters: map[Version][]Version{}}
}

func (r *DependencyRegistry) Register(waiter Version, dependency Version) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waiters[dependency] = append(r.waiters[dependency], waiter)
}

func (r *DependencyRegistry) Resolve(dependency Version) []Version {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters := append([]Version(nil), r.waiters[dependency]...)
	delete(r.waiters, dependency)
	return waiters
}
