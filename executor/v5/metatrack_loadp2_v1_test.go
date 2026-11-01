package v5

import (
	"fmt"
	"reflect"
	"testing"
)

// P2 must never change the result relative to the already verified P1
// implementation over many linked batches and different key universes.
func TestMTLoadP2V1MultiBatchParity(t *testing.T) {
	for _, universe := range []int{16, 128, 256, 1024, 2048, 4096} {
		t.Run(fmt.Sprint("keys_", universe), func(t *testing.T) {
			p := mtLoadIsoTestConfigV1(nil)
			for _, in := range mtLoadIsoFastInputsV1(16, universe) {
				if p.incrementalV650 == nil {
					p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
				}
				before := mtBoundCloneStateV1(p.incrementalV650)
				want, wantFull := mtLoadP1OldP0OracleV1(p, in)
				got, gotFull := mtLoadIsoSelectBoundCOWV1(p, in)
				if !mtLoadP0SameSelectionV1(want, got) || !mtLoadP0SameSelectionV1(wantFull, gotFull) {
					t.Fatalf("P2 changed chosen COW plan batch=%d keys=%d", in.BatchIndex, universe)
				}
				if !reflect.DeepEqual(before, p.incrementalV650) {
					t.Fatalf("P2 leaked COW state batch=%d", in.BatchIndex)
				}
				mtBoundCOWApplyV3(p.incrementalV650, got.after)
			}
		})
	}
}

// The original P1 pruning predicate must stay identical to frozen Bound P0.
func TestMTLoadP2V1MonotoneProbeParity(t *testing.T) {
	for _, universe := range []int{16, 128, 1024} {
		p := mtLoadIsoTestConfigV1(nil)
		for _, in := range mtLoadIsoFastInputsV1(10, universe) {
			if p.incrementalV650 == nil {
				p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
			}
			base := p.incrementalV650
			touched := mtBoundTouchedForBatchV2(in)
			full := mtBoundRunRollbackV3(p, base, in, "full", touched)
			before := mtBoundCloneStateV1(base)
			cache := make([]*mtLoadP1StaticV1, len(in.Records))
			for _, mode := range []string{"exact", "remote"} {
				want := mtBoundPruneImpossibleV1(p, base, in, mode, full.trial, touched)
				got := mtLoadP1PruneImpossibleV1(p, base, in, mode, full.trial, touched, cache)
				if want != got {
					t.Fatalf("monotone predicate changed batch=%d mode=%s", in.BatchIndex, mode)
				}
				if !reflect.DeepEqual(before, base) {
					t.Fatalf("probe leaked COW state batch=%d mode=%s", in.BatchIndex, mode)
				}
			}
			mtBoundCOWApplyV3(base, full.after)
		}
	}
}

// A P2 skip must not drop an eligible alternative: compare a skipped candidate
// against the ORIGINAL full candidate planner and its acceptance guards.
func TestMTLoadP2V1SkipSoundness(t *testing.T) {
	for _, universe := range []int{16, 128, 1024, 4096} {
		p := mtLoadIsoTestConfigV1(nil)
		for _, in := range mtLoadIsoFastInputsV1(16, universe) {
			if p.incrementalV650 == nil {
				p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
			}
			base := p.incrementalV650
			touched := mtBoundTouchedForBatchV2(in)
			full := mtBoundRunRollbackV3(p, base, in, "full", touched)
			before := mtBoundCloneStateV1(base)
			cache := make([]*mtLoadP1StaticV1, len(in.Records))
			best := full
			for _, mode := range []string{"exact", "remote"} {
				p1Reject := mtLoadP1PruneImpossibleV1(p, base, in, mode, full.trial, touched, cache)
				p2Reject := mtLoadP2PruneImpossibleV1(p, base, in, mode, full.trial, best.trial, touched, cache)
				if !reflect.DeepEqual(before, base) {
					t.Fatalf("P2 skip leaked COW state batch=%d mode=%s", in.BatchIndex, mode)
				}
				if p2Reject && !p1Reject {
					alt := mtBoundRunRollbackV3(p, base, in, mode, touched)
					safe := mtBoundBatchSafeV1(alt.trial, full.trial) && mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) <= 10
					moves, pairs := mtBoundDeltaChangesV2(alt.after, full.after)
					eligible := safe && moves <= 50 && pairs <= 100 && alt.trial.skew <= full.trial.skew
					if eligible && (best.trial.mode == "full" || mtBoundLessV1(alt.trial, best.trial)) {
						t.Fatalf("P2 wrongly rejected eligible candidate batch=%d mode=%s", in.BatchIndex, mode)
					}
				}
				if !p2Reject {
					alt := mtBoundRunRollbackV3(p, base, in, mode, touched)
					moves, pairs := mtBoundDeltaChangesV2(alt.after, full.after)
					if mtBoundBatchSafeV1(alt.trial, full.trial) &&
						mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) <= 10 && moves <= 50 && pairs <= 100 && alt.trial.skew <= full.trial.skew &&
						(best.trial.mode == "full" || mtBoundLessV1(alt.trial, best.trial)) {
						best = alt
					}
				}
			}
			mtBoundCOWApplyV3(base, best.after)
		}
	}
}

// Algebraic decision properties, including equal-placement and tie cases.
func TestMTLoadP2V1ProofBoundaries(t *testing.T) {
	cases := []struct {
		name                            string
		fullExact, fullRemote, fullPeak int
		candExact, candRemote, candPeak int
		switches                        int
		reject                          bool
	}{
		{"identical", 10, 20, 50, 10, 20, 50, 0, true},
		{"switched_no_gain", 10, 20, 50, 10, 20, 50, 2, true},
		{"strict_exact_gain", 10, 20, 50, 9, 20, 50, 1, false},
		{"strict_remote_gain", 10, 20, 50, 10, 19, 50, 1, false},
		{"strict_peak_gain", 10, 20, 50, 10, 20, 49, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if mtLoadP2NoStrictGainV1(mtLoadP2ProbeResultV1{
				complete: true, switches: tc.switches, remote: tc.candRemote, exact: tc.candExact, peak: tc.candPeak},
				mtBoundTrialV1{peak: tc.fullPeak, plan: BatchRoutingPlan{RemoteAccessEstimate: tc.fullRemote, IncrementalExactCrossShardStateEdgeCount: tc.fullExact}}) != tc.reject {
				t.Fatal("strict gain safety proof mismatch")
			}
		})
	}
}
