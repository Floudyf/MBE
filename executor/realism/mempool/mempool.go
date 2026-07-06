package mempool

import (
	"strings"
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
	nodeID  string
	shardID string
	policy  Policy
	nonces  *account.NonceManager
	byID    map[string]entry
	order   []string
	nextSeq int64
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
		nodeID:  nodeID,
		shardID: shardID,
		policy:  policy,
		nonces:  nonces,
		byID:    map[string]entry{},
	}
}

func (m *Mempool) Admit(item tx.SignedTransaction) AdmissionResult {
	return m.AdmitAt(item, time.Now())
}

func (m *Mempool) AdmitAt(item tx.SignedTransaction, now time.Time) AdmissionResult {
	nowMS := now.UnixMilli()
	if err := tx.Verify(item); err != nil {
		return rejected(item, m.nodeID, m.shardID, reasonOf(err), len(m.byID), nowMS)
	}
	if m.Has(item.TxID) {
		return rejected(item, m.nodeID, m.shardID, ReasonDuplicateTx, len(m.byID), nowMS)
	}
	if len(m.byID) >= m.policy.Capacity {
		return rejected(item, m.nodeID, m.shardID, ReasonCapacity, len(m.byID), nowMS)
	}
	if err := m.nonces.Accept(item.Sender, item.Nonce); err != nil {
		return rejected(item, m.nodeID, m.shardID, reasonOf(err), len(m.byID), nowMS)
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
	if !m.Has(txID) {
		return false
	}
	delete(m.byID, txID)
	m.order = filterID(m.order, txID)
	return true
}

func (m *Mempool) PopReady(limit int) []tx.SignedTransaction {
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
			m.Remove(id)
		}
	}
	return out
}

func (m *Mempool) Len() int {
	return len(m.byID)
}

func (m *Mempool) Has(txID string) bool {
	_, ok := m.byID[txID]
	return ok
}

func (m *Mempool) Capacity() int {
	return m.policy.Capacity
}

func (m *Mempool) Expire(now time.Time) []AdmissionResult {
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
			m.Remove(id)
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

func (m *Mempool) Stats() Stats {
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
