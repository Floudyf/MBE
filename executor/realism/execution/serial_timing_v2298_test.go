package execution

import (
	"testing"
	"time"
)

// TestSerialV2299DurationTruth uses known durations instead of assuming
// that a short real transaction must cross a Windows clock tick.
func TestSerialV2299DurationTruth(t *testing.T) {
	cases := []struct {
		duration   time.Duration
		ns, us, ms int64
	}{
		{0, 0, 0, 0},
		{750 * time.Nanosecond, 750, 0, 0},
		{1999 * time.Nanosecond, 1999, 1, 0},
		{1750777 * time.Nanosecond, 1750777, 1750, 1},
	}
	for _, tc := range cases {
		ns, us, ms := serialTimingFromDurationV2299(tc.duration)
		if ns != tc.ns || us != tc.us || ms != tc.ms {
			t.Fatalf("duration %v: ns=%d us=%d ms=%d", tc.duration, ns, us, ms)
		}
	}
}

// TestSerialV2299ExportsNanosecondTimingTruth verifies both real Serial
// paths preserve conversion invariants without manufacturing positive time.
func TestSerialV2299ExportsNanosecondTimingTruth(t *testing.T) {
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "txallo-v2299-timing", 8, "alice", "bob", 1, "v5_safe"))
	for name, result := range map[string]Result{
		"committed":  NewSerialExecutor().ExecuteBlock(b, map[string]string{}),
		"delta-only": NewSerialExecutor().ExecuteBlockDeltas(b, map[string]string{}),
	} {
		if result.SuccessfulTxs != 8 || len(result.TxDeltas) != 8 {
			t.Fatalf("%s: business execution incomplete: success=%d deltas=%d", name, result.SuccessfulTxs, len(result.TxDeltas))
		}
		if result.TransactionExecutionNS < 0 || result.TransactionExecutionUS != result.TransactionExecutionNS/1000 || result.TransactionExecutionMS != result.TransactionExecutionNS/1000000 {
			t.Fatalf("%s: timing truncation inconsistency: ns=%d us=%d ms=%d", name, result.TransactionExecutionNS, result.TransactionExecutionUS, result.TransactionExecutionMS)
		}
	}
}
