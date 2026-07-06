package mempool

type Snapshot struct {
	NodeID   string         `json:"node_id"`
	ShardID  string         `json:"shard_id"`
	Size     int            `json:"size"`
	Capacity int            `json:"capacity"`
	Items    []SnapshotItem `json:"items"`
}

type SnapshotItem struct {
	TxID        string `json:"tx_id"`
	Sender      string `json:"sender"`
	Receiver    string `json:"receiver"`
	Nonce       uint64 `json:"nonce"`
	Value       int64  `json:"value"`
	AdmittedMS  int64  `json:"admitted_ms"`
	QueueWaitMS int64  `json:"queue_wait_ms"`
}

type Stats struct {
	NodeID   string `json:"node_id"`
	ShardID  string `json:"shard_id"`
	Size     int    `json:"size"`
	Capacity int    `json:"capacity"`
}
