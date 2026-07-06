package xshard

type Stage string

const (
	Detected         Stage = "Detected"
	SourceLock       Stage = "SourceLock"
	RelayCertificate Stage = "RelayCertificate"
	TargetVerify     Stage = "TargetVerify"
	TargetCommit     Stage = "TargetCommit"
	SourceFinalize   Stage = "SourceFinalize"
	Timeout          Stage = "Timeout"
	Refund           Stage = "Refund"
	Abort            Stage = "Abort"
)

type Event struct {
	Timestamp   int64  `json:"timestamp"`
	TxID        string `json:"tx_id"`
	SourceShard string `json:"source_shard"`
	TargetShard string `json:"target_shard"`
	Stage       Stage  `json:"stage"`
	Success     bool   `json:"success"`
	EventHash   string `json:"event_hash"`
	Error       string `json:"error,omitempty"`
}
