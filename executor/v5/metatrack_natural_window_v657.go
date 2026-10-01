package v5

import (
	"strings"
	"time"
)

// MBE_METATRACK_NATURAL_PRODUCER_WINDOW_V657
//
// The experimental MetaTrack profile removes the independent fixed
// micro_batch_size boundary. Routing itself remains the v6.5.1 incremental
// rank-balanced algorithm. Only the signed RouteBatch packaging boundary is
// changed: it is closed by the already-configured block producer interval or
// by the already-configured block capacity, whichever becomes effective first.
//
// No wall-clock timestamp is used as algorithm input. For fixed-rate replay we
// derive the ideal offered-load release time from transaction ordinal and the
// configured target_submission_tps. max_throughput has no artificial release
// delay, so capacity is its only natural packaging boundary. This keeps repeated
// runs deterministic while reusing chain configuration instead of introducing
// another MetaTrack threshold.
const metaTrackNaturalProducerWindowPolicyV657 = "producer_interval_or_block_capacity_v657"

func metaTrackNaturalProducerWindowLogicalReleaseMSV657(plan WorkloadPlan, record WorkloadRecord) int64 {
	if strings.TrimSpace(plan.ReplayMode) != "fixed_rate" || plan.TargetSubmissionTPS <= 0 {
		return 0
	}
	return int64(record.Index) * 1000 / int64(plan.TargetSubmissionTPS)
}

func metaTrackNaturalProducerWindowShouldCloseV657(plan WorkloadPlan, interval time.Duration, blockSize int, current []WorkloadRecord, next WorkloadRecord) (bool, string) {
	if len(current) == 0 {
		return false, ""
	}
	if blockSize > 0 && len(current) >= blockSize {
		return true, "block_capacity"
	}
	intervalMS := interval.Milliseconds()
	if intervalMS <= 0 || strings.TrimSpace(plan.ReplayMode) != "fixed_rate" || plan.TargetSubmissionTPS <= 0 {
		return false, ""
	}
	firstReleaseMS := metaTrackNaturalProducerWindowLogicalReleaseMSV657(plan, current[0])
	nextReleaseMS := metaTrackNaturalProducerWindowLogicalReleaseMSV657(plan, next)
	if nextReleaseMS/intervalMS != firstReleaseMS/intervalMS {
		return true, "producer_interval"
	}
	return false, ""
}

type metaTrackNaturalWindowStatsV657 struct {
	SchemaVersion            string  `json:"schema_version"`
	Policy                   string  `json:"policy"`
	FixedMicroBatchEnabled   bool    `json:"fixed_micro_batch_enabled"`
	WindowSource             string  `json:"routing_window_source"`
	BlockSize                int     `json:"block_size"`
	BlockIntervalMS          int64   `json:"block_interval_ms"`
	ReplayMode               string  `json:"replay_mode"`
	TargetSubmissionTPS      int     `json:"target_submission_tps,omitempty"`
	WindowCount              int     `json:"window_count"`
	TimeBoundaryCount        int     `json:"time_boundary_count"`
	CapacityBoundaryCount    int     `json:"capacity_boundary_count"`
	EndOfStreamBoundaryCount int     `json:"end_of_stream_boundary_count"`
	MinWindowSize            int     `json:"min_window_size"`
	MaxWindowSize            int     `json:"max_window_size"`
	TotalTransactions        int     `json:"total_transactions"`
	AverageWindowSize        float64 `json:"average_window_size"`
}

func newMetaTrackNaturalWindowStatsV657(plan WorkloadPlan, interval time.Duration, blockSize int) *metaTrackNaturalWindowStatsV657 {
	replayMode := strings.TrimSpace(plan.ReplayMode)
	if replayMode == "" {
		replayMode = "max_throughput"
	}
	return &metaTrackNaturalWindowStatsV657{
		SchemaVersion:          "mbe_metatrack_natural_window_v657",
		Policy:                 metaTrackNaturalProducerWindowPolicyV657,
		FixedMicroBatchEnabled: false,
		WindowSource:           "block_producer_interval_or_block_capacity",
		BlockSize:              blockSize,
		BlockIntervalMS:        interval.Milliseconds(),
		ReplayMode:             replayMode,
		TargetSubmissionTPS:    plan.TargetSubmissionTPS,
	}
}

func (s *metaTrackNaturalWindowStatsV657) NoteWindow(size int, reason string) {
	if s == nil || size <= 0 {
		return
	}
	s.WindowCount++
	s.TotalTransactions += size
	if s.MinWindowSize == 0 || size < s.MinWindowSize {
		s.MinWindowSize = size
	}
	if size > s.MaxWindowSize {
		s.MaxWindowSize = size
	}
	switch reason {
	case "producer_interval":
		s.TimeBoundaryCount++
	case "block_capacity":
		s.CapacityBoundaryCount++
	case "end_of_stream":
		s.EndOfStreamBoundaryCount++
	}
	if s.WindowCount > 0 {
		s.AverageWindowSize = float64(s.TotalTransactions) / float64(s.WindowCount)
	}
}
