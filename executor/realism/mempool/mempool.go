package mempool

import (
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/tx"
)

type entry struct {
	tx         tx.SignedTransaction
	admittedAt time.Time
	seq        int64
}

type Mempool struct {
	mu       sync.Mutex
	nodeID   string
	shardID  string
	policy   Policy
	nonces   *account.NonceManager
	byID     map[string]entry
	order    []string
	nextSeq  int64
	reserved map[string]bool
}

func New(nodeID, shardID string, policy Policy, nonces *account.NonceManager) *Mempool {
	if policy.Capacity <= 0 {
		policy.Capacity = DefaultPolicy().Capacity
	}
	if policy.TTL <= 0 {
		policy.TTL = DefaultPolicy().TTL
	}
	if nonces == nil {
		nonces = account.NewNonceManager()
	}
	return &Mempool{
		nodeID:   nodeID,
		shardID:  shardID,
		policy:   policy,
		nonces:   nonces,
		byID:     map[string]entry{},
		reserved: map[string]bool{},
	}
}

func (m *Mempool) Admit(item tx.SignedTransaction) AdmissionResult {
	return m.AdmitAt(item, time.Now())
}

func (m *Mempool) AdmitAt(item tx.SignedTransaction, now time.Time) AdmissionResult {
	return m.admitAt(item, now, true)
}

// AdmitRelay validates and queues a transaction that was already admitted by
// its source shard. Relay delivery is not a second account-nonce stream: the
// target shard must not reject it because unrelated source-shard transactions
// advanced the sender nonce before this relay arrived.
func (m *Mempool) AdmitRelay(item tx.SignedTransaction) AdmissionResult {
	return m.admitAt(item, time.Now(), false)
}

func (m *Mempool) admitAt(item tx.SignedTransaction, now time.Time, enforceNonce bool) AdmissionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	nowMS := now.UnixMilli()
	if err := tx.Verify(item); err != nil {
		return rejected(item, m.nodeID, m.shardID, reasonOf(err), len(m.byID), nowMS)
	}
	if m.hasLocked(item.TxID) {
		return rejected(item, m.nodeID, m.shardID, ReasonDuplicateTx, len(m.byID), nowMS)
	}
	if len(m.byID) >= m.policy.Capacity {
		return rejected(item, m.nodeID, m.shardID, ReasonCapacity, len(m.byID), nowMS)
	}
	if enforceNonce {
		if err := m.nonces.Accept(item.Sender, item.Nonce); err != nil {
			return rejected(item, m.nodeID, m.shardID, reasonOf(err), len(m.byID), nowMS)
		}
	}
	m.nextSeq++
	m.byID[item.TxID] = entry{tx: item, admittedAt: now, seq: m.nextSeq}
	m.order = append(m.order, item.TxID)
	return AdmissionResult{
		Accepted:    true,
		TxID:        item.TxID,
		Sender:      item.Sender,
		Receiver:    item.Receiver,
		Nonce:       item.Nonce,
		Action:      ActionAccepted,
		NodeID:      m.nodeID,
		ShardID:     m.shardID,
		MempoolSize: len(m.byID),
		QueueWaitMS: 0,
		Timestamp:   nowMS,
	}
}

func (m *Mempool) Remove(txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeLocked(txID)
}

func (m *Mempool) removeLocked(txID string) bool {
	if !m.hasLocked(txID) {
		return false
	}
	delete(m.byID, txID)
	m.order = filterID(m.order, txID)
	return true
}

func (m *Mempool) PopReady(limit int) []tx.SignedTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		return nil
	}
	if limit > len(m.order) {
		limit = len(m.order)
	}
	out := make([]tx.SignedTransaction, 0, limit)
	ids := append([]string(nil), m.order[:limit]...)
	for _, id := range ids {
		if e, ok := m.byID[id]; ok {
			out = append(out, e.tx)
			m.removeLocked(id)
		}
	}
	return out
}

func (m *Mempool) ReserveReady(limit int) []tx.SignedTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		return nil
	}
	out := []tx.SignedTransaction{}
	for _, id := range m.order {
		if len(out) >= limit {
			break
		}
		if m.reserved[id] {
			continue
		}
		if entry, ok := m.byID[id]; ok {
			m.reserved[id] = true
			out = append(out, entry.tx)
		}
	}
	return out
}
func (m *Mempool) CommitReserved(items []tx.SignedTransaction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range items {
		delete(m.reserved, item.TxID)
		m.removeLocked(item.TxID)
	}
}
func (m *Mempool) ReleaseReserved(items []tx.SignedTransaction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range items {
		delete(m.reserved, item.TxID)
	}
}

func (m *Mempool) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byID)
}

func (m *Mempool) ReservedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reserved)
}

func (m *Mempool) Has(txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasLocked(txID)
}

func (m *Mempool) hasLocked(txID string) bool {
	_, ok := m.byID[txID]
	return ok
}

func (m *Mempool) Capacity() int {
	return m.policy.Capacity
}

func (m *Mempool) Expire(now time.Time) []AdmissionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	expired := []AdmissionResult{}
	if m.policy.TTL <= 0 {
		return expired
	}
	for _, id := range append([]string(nil), m.order...) {
		e, ok := m.byID[id]
		if !ok {
			continue
		}
		if now.Sub(e.admittedAt) > m.policy.TTL {
			m.removeLocked(id)
			expired = append(expired, AdmissionResult{
				Accepted:     false,
				TxID:         e.tx.TxID,
				Sender:       e.tx.Sender,
				Receiver:     e.tx.Receiver,
				Nonce:        e.tx.Nonce,
				Action:       ActionExpired,
				RejectReason: ReasonTTLExpired,
				NodeID:       m.nodeID,
				ShardID:      m.shardID,
				MempoolSize:  len(m.byID),
				QueueWaitMS:  now.Sub(e.admittedAt).Milliseconds(),
				Timestamp:    now.UnixMilli(),
			})
		}
	}
	return expired
}

func (m *Mempool) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]SnapshotItem, 0, len(m.order))
	now := time.Now()
	for _, id := range m.order {
		e, ok := m.byID[id]
		if !ok {
			continue
		}
		items = append(items, SnapshotItem{
			TxID:        e.tx.TxID,
			Sender:      e.tx.Sender,
			Receiver:    e.tx.Receiver,
			Nonce:       e.tx.Nonce,
			Value:       e.tx.Value,
			AdmittedMS:  e.admittedAt.UnixMilli(),
			QueueWaitMS: now.Sub(e.admittedAt).Milliseconds(),
		})
	}
	return Snapshot{NodeID: m.nodeID, ShardID: m.shardID, Size: len(items), Capacity: m.policy.Capacity, Items: items}
}

func (m *Mempool) IDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.order))
	for _, id := range m.order {
		if _, ok := m.byID[id]; ok {
			ids = append(ids, id)
		}
	}
	return ids
}

func (m *Mempool) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{NodeID: m.nodeID, ShardID: m.shardID, Size: len(m.byID), Capacity: m.policy.Capacity}
}

func reasonOf(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if idx := strings.Index(text, ":"); idx >= 0 {
		return text[:idx]
	}
	return text
}

func filterID(ids []string, remove string) []string {
	out := ids[:0]
	for _, id := range ids {
		if id != remove {
			out = append(out, id)
		}
	}
	return out
}
