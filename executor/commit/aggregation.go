// Package commit models V1.7 replay-only hot update aggregation.
package commit

import "sort"

type Config struct {
	Enabled                                           bool
	MinHotCount, MaxGroupSize                         int
	RequireFastTrack, ConservativeOnConstraintFailure bool
	Policy                                            string
}
type Update struct {
	ID, PrimaryKey    string
	Fast, Commutative bool
	Delta             *float64
	ConflictHint      string
}
type Metrics struct{ CandidateTxCount, AggregatedTxCount, AggregatedCommitCount, ConservativeCommitCount, SavedCommitCount, GroupCount, HotKeyCount, ConstraintFailureCount, MissingDeltaCount, NonCommutativeCount int }
type Result struct {
	Aggregates map[string]float64
	Metrics    Metrics
}

func Aggregate(updates []Update, c Config) Result {
	if c.MinHotCount <= 0 {
		c.MinHotCount = 2
	}
	if c.MaxGroupSize <= 0 {
		c.MaxGroupSize = 64
	}
	r := Result{Aggregates: map[string]float64{}}
	eligible := map[string][]Update{}
	for _, u := range updates {
		if !c.Enabled || c.RequireFastTrack && !u.Fast || u.PrimaryKey == "" || u.ConflictHint != "" {
			r.Metrics.ConservativeCommitCount++
			continue
		}
		if !u.Commutative {
			r.Metrics.NonCommutativeCount++
			r.Metrics.ConservativeCommitCount++
			continue
		}
		if u.Delta == nil {
			r.Metrics.MissingDeltaCount++
			r.Metrics.ConservativeCommitCount++
			continue
		}
		r.Metrics.CandidateTxCount++
		eligible[u.PrimaryKey] = append(eligible[u.PrimaryKey], u)
	}
	keys := make([]string, 0, len(eligible))
	for k := range eligible {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		group := eligible[k]
		if len(group) < c.MinHotCount {
			r.Metrics.ConservativeCommitCount += len(group)
			continue
		}
		sum := 0.0
		for _, u := range group {
			sum += *u.Delta
		}
		if sum < 0 && c.ConservativeOnConstraintFailure {
			r.Metrics.ConstraintFailureCount++
			r.Metrics.ConservativeCommitCount += len(group)
			continue
		}
		r.Aggregates[k] = sum
		r.Metrics.GroupCount++
		r.Metrics.HotKeyCount++
		r.Metrics.AggregatedTxCount += len(group)
		r.Metrics.AggregatedCommitCount++
		r.Metrics.SavedCommitCount += len(group) - 1
	}
	return r
}
