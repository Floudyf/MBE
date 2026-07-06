package pbft

import (
	"strconv"
	"sync"

	"metaverse-chainlab/executor/realism/metrics"
)

type MessageLogEntry struct {
	Timestamp   int64
	NodeID      string
	MessageType string
	FromNode    string
	BlockHash   string
	Height      uint64
	View        uint64
	Sequence    uint64
	Accepted    bool
	Error       string
}

type QuorumLogEntry struct {
	Timestamp  int64
	NodeID     string
	ShardID    string
	QuorumType string
	BlockHash  string
	Height     uint64
	View       uint64
	Votes      int
	Required   int
	Reached    bool
}

type ViewChangeLogEntry struct {
	Timestamp                 int64
	NodeID                    string
	OldView                   uint64
	NewView                   uint64
	LeaderID                  string
	Votes                     int
	Required                  int
	BasicViewChange           bool
	ProductionViewChangeProof bool
	Checkpoint                bool
	StableLog                 bool
}

type Logs struct {
	mu          sync.Mutex
	Messages    []MessageLogEntry
	Quorums     []QuorumLogEntry
	ViewChanges []ViewChangeLogEntry
}

func (l *Logs) AddMessage(e MessageLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Messages = append(l.Messages, e)
}

func (l *Logs) AddQuorum(e QuorumLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Quorums = append(l.Quorums, e)
}

func (l *Logs) AddViewChange(e ViewChangeLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ViewChanges = append(l.ViewChanges, e)
}

func (l *Logs) MessageCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.Messages)
}

func (l *Logs) WriteMessageCSV(path string) error {
	l.mu.Lock()
	rows := make([][]string, 0, len(l.Messages))
	for _, e := range l.Messages {
		rows = append(rows, []string{strconv.FormatInt(e.Timestamp, 10), e.NodeID, e.MessageType, e.FromNode, e.BlockHash, strconv.FormatUint(e.Height, 10), strconv.FormatUint(e.View, 10), strconv.FormatUint(e.Sequence, 10), strconv.FormatBool(e.Accepted), e.Error})
	}
	l.mu.Unlock()
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "message_type", "from_node", "block_hash", "height", "view", "sequence", "accepted", "error"}, rows)
}

func (l *Logs) WriteQuorumCSV(path string) error {
	l.mu.Lock()
	rows := make([][]string, 0, len(l.Quorums))
	for _, e := range l.Quorums {
		rows = append(rows, []string{strconv.FormatInt(e.Timestamp, 10), e.NodeID, e.ShardID, e.QuorumType, e.BlockHash, strconv.FormatUint(e.Height, 10), strconv.FormatUint(e.View, 10), strconv.Itoa(e.Votes), strconv.Itoa(e.Required), strconv.FormatBool(e.Reached)})
	}
	l.mu.Unlock()
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "shard_id", "quorum_type", "block_hash", "height", "view", "votes", "required", "reached"}, rows)
}

func (l *Logs) WriteViewChangeCSV(path string) error {
	l.mu.Lock()
	rows := make([][]string, 0, len(l.ViewChanges))
	for _, e := range l.ViewChanges {
		rows = append(rows, []string{strconv.FormatInt(e.Timestamp, 10), e.NodeID, strconv.FormatUint(e.OldView, 10), strconv.FormatUint(e.NewView, 10), e.LeaderID, strconv.Itoa(e.Votes), strconv.Itoa(e.Required), strconv.FormatBool(e.BasicViewChange), strconv.FormatBool(e.ProductionViewChangeProof), strconv.FormatBool(e.Checkpoint), strconv.FormatBool(e.StableLog)})
	}
	l.mu.Unlock()
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "old_view", "new_view", "leader_id", "votes", "required", "basic_view_change", "production_view_change_proof", "checkpoint", "stable_log"}, rows)
}
