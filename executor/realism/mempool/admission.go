package mempool

import "metaverse-chainlab/executor/realism/tx"

const (
	ActionAccepted = "accepted"
	ActionRejected = "rejected"
	ActionExpired  = "expired"

	ReasonDuplicateTx = "duplicate_tx"
	ReasonCapacity    = "capacity_exceeded"
	ReasonTTLExpired  = "ttl_expired"
)

type AdmissionResult struct {
	Accepted     bool   `json:"accepted"`
	TxID         string `json:"tx_id"`
	Sender       string `json:"sender"`
	Receiver     string `json:"receiver"`
	Nonce        uint64 `json:"nonce"`
	Action       string `json:"action"`
	RejectReason string `json:"reject_reason,omitempty"`
	NodeID       string `json:"node_id"`
	ShardID      string `json:"shard_id"`
	MempoolSize  int    `json:"mempool_size"`
	QueueWaitMS  int64  `json:"queue_wait_ms"`
	Timestamp    int64  `json:"timestamp"`
}

func rejected(t tx.SignedTransaction, nodeID, shardID, reason string, size int, now int64) AdmissionResult {
	return AdmissionResult{
		Accepted:     false,
		TxID:         t.TxID,
		Sender:       t.Sender,
		Receiver:     t.Receiver,
		Nonce:        t.Nonce,
		Action:       ActionRejected,
		RejectReason: reason,
		NodeID:       nodeID,
		ShardID:      shardID,
		MempoolSize:  size,
		Timestamp:    now,
	}
}
