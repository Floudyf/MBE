package p2p

import (
	"strconv"
	"sync"

	"metaverse-chainlab/executor/realism/metrics"
)

type NetworkLogEntry struct {
	Timestamp   int64
	NodeID      string
	PeerID      string
	Direction   string
	MessageType string
	MessageID   string
	Height      uint64
	View        uint64
	Sequence    uint64
	Bytes       int
	Success     bool
	Error       string
	LatencyMS   int64
}

type NetworkLog struct {
	mu      sync.Mutex
	entries []NetworkLogEntry
}

func (l *NetworkLog) Add(entry NetworkLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *NetworkLog) Entries() []NetworkLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]NetworkLogEntry(nil), l.entries...)
}

func (l *NetworkLog) WriteCSV(path string) error {
	rows := [][]string{}
	for _, e := range l.Entries() {
		rows = append(rows, []string{
			strconv.FormatInt(e.Timestamp, 10),
			e.NodeID,
			e.PeerID,
			e.Direction,
			e.MessageType,
			e.MessageID,
			strconv.FormatUint(e.Height, 10),
			strconv.FormatUint(e.View, 10),
			strconv.FormatUint(e.Sequence, 10),
			strconv.Itoa(e.Bytes),
			strconv.FormatBool(e.Success),
			e.Error,
			strconv.FormatInt(e.LatencyMS, 10),
		})
	}
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "peer_id", "direction", "message_type", "message_id", "height", "view", "sequence", "bytes", "success", "error", "latency_ms"}, rows)
}
