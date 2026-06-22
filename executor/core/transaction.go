package core

// Transaction is the V0 single-chain trace record shared with Python writers.
type Transaction struct {
	TxID           string         `json:"tx_id"`
	TxType         string         `json:"tx_type"`
	Timestamp      float64        `json:"timestamp"`
	ChainID        string         `json:"chain_id"`
	Contract       string         `json:"contract"`
	Function       string         `json:"function"`
	Args           map[string]any `json:"args"`
	ReadSet        []string       `json:"read_set"`
	WriteSet       []string       `json:"write_set"`
	AccessList     []string       `json:"access_list"`
	Commutative    bool           `json:"commutative"`
	UpdateType     string         `json:"update_type"`
	Status         string         `json:"status"`
	ChainLatencyMS float64        `json:"chain_latency_ms"`
}
