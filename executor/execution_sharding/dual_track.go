package execution_sharding

type Track string

const (
	Fast         Track = "fast"
	Conservative Track = "conservative"
)

type DualTrackConfig struct {
	Enabled                                                    bool
	FastTrackMaxAccessSize                                     int
	ConservativeOnConflictHint, ConservativeOnMissingAccessSet bool
	SchedulerPolicy                                            string
}
type DualTrackTransaction struct {
	ID             string
	AccessKeys     []string
	ConflictHint   string
	ExecutionShard int
}
type TrackDecision struct {
	TxID           string
	Track          Track
	ExecutionShard int
	Reason         string
}
type SchedulerMetrics struct{ FastExecuted, ConservativeExecuted, BlockedOrDeferred, IdleCount int }

func ClassifyDualTrack(tx DualTrackTransaction, c DualTrackConfig) TrackDecision {
	if !c.Enabled {
		return TrackDecision{tx.ID, Conservative, tx.ExecutionShard, "disabled"}
	}
	if len(tx.AccessKeys) == 0 && c.ConservativeOnMissingAccessSet {
		return TrackDecision{tx.ID, Conservative, tx.ExecutionShard, "missing_access_set"}
	}
	if len(tx.AccessKeys) > c.FastTrackMaxAccessSize {
		return TrackDecision{tx.ID, Conservative, tx.ExecutionShard, "access_set_too_large"}
	}
	if c.ConservativeOnConflictHint && tx.ConflictHint != "" && tx.ConflictHint != "low" {
		return TrackDecision{tx.ID, Conservative, tx.ExecutionShard, "conflict_hint"}
	}
	return TrackDecision{tx.ID, Fast, tx.ExecutionShard, "safe_local_access"}
}
func Schedule(decisions []TrackDecision, shards int) ([]TrackDecision, SchedulerMetrics) {
	if shards <= 0 {
		shards = 1
	}
	fast := make([][]TrackDecision, shards)
	slow := make([][]TrackDecision, shards)
	for _, d := range decisions {
		s := d.ExecutionShard % shards
		if d.Track == Fast {
			fast[s] = append(fast[s], d)
		} else {
			slow[s] = append(slow[s], d)
		}
	}
	out := []TrackDecision{}
	m := SchedulerMetrics{}
	for {
		progress := false
		for s := 0; s < shards; s++ {
			if len(fast[s]) > 0 {
				out = append(out, fast[s][0])
				fast[s] = fast[s][1:]
				m.FastExecuted++
				progress = true
			} else if len(slow[s]) > 0 {
				out = append(out, slow[s][0])
				slow[s] = slow[s][1:]
				m.ConservativeExecuted++
				progress = true
			} else {
				m.IdleCount++
			}
		}
		if !progress {
			return out, m
		}
	}
}
