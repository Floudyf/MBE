package v5

// MT_LOADP0_V1: native parity oracle for pruning ONLY. The original
// nonpruning COW selection is reproduced below and never used in production.
import (
	"fmt"
	"reflect"
	"testing"
)

func mtLoadP0UnprunedOracleV1(p *metaTrackRouting, in BatchRoutingInput) (mtBoundSavedTrialV2, mtBoundSavedTrialV2) {
	base := p.incrementalV650
	touched := mtBoundTouchedForBatchV2(in)
	full := mtBoundRunRollbackV3(p, base, in, "full", touched)
	selected := full
	if len(in.Records) == 100 && len(in.ShardIDs) == 2 && in.ExpectedTransactionCount > 0 {
		for _, mode := range []string{"exact", "remote"} {
			alt := mtBoundRunRollbackV3(p, base, in, mode, touched)
			if !mtBoundBatchSafeV1(alt.trial, full.trial) {
				continue
			}
			if mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) > 10 {
				continue
			}
			moves, pairs := mtBoundDeltaChangesV2(alt.after, full.after)
			if moves > 50 || pairs > 100 || alt.trial.skew > full.trial.skew {
				continue
			}
			if selected.trial.mode == "full" || mtBoundLessV1(alt.trial, selected.trial) {
				selected = alt
			}
		}
	}
	return selected, full
}

func mtLoadP0SameSelectionV1(a, b mtBoundSavedTrialV2) bool {
	return a.trial.mode == b.trial.mode &&
		reflect.DeepEqual(a.trial.plan, b.trial.plan) &&
		reflect.DeepEqual(a.after, b.after)
}

// The check applies the candidate delta only to advance the test's committed
// routing history, not to the actual runtime. Both paths start each batch at
// the identical history and all previews must roll back without leakage.
func TestMTLoadP0V1MultiBatchCOWParity(t *testing.T) {
	for _, universe := range []int{16, 256, 4096} {
		t.Run(fmt.Sprintf("keys_%d", universe), func(t *testing.T) {
			p := mtLoadIsoTestConfigV1(nil)
			for _, in := range mtLoadIsoFastInputsV1(12, universe) {
				if p.incrementalV650 == nil {
					p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
				}
				original := mtBoundCloneStateV1(p.incrementalV650)
				oldSelected, oldFull := mtLoadP0UnprunedOracleV1(p, in)
				if !reflect.DeepEqual(original, p.incrementalV650) {
					t.Fatalf("unpruned preview leaked at batch %d", in.BatchIndex)
				}
				newSelected, newFull := mtLoadIsoSelectBoundCOWV1(p, in)
				if !reflect.DeepEqual(original, p.incrementalV650) {
					t.Fatalf("pruned preview leaked at batch %d", in.BatchIndex)
				}
				if !mtLoadP0SameSelectionV1(oldFull, newFull) || !mtLoadP0SameSelectionV1(oldSelected, newSelected) {
					t.Fatalf("pruning changed selection or history delta at batch %d universe %d", in.BatchIndex, universe)
				}
				// Commit the same delta for next-batch history in the test.
				mtBoundCOWApplyV3(p.incrementalV650, newSelected.after)
			}
		})
	}
}

func TestMTLoadP0V1PruneNeverRejectsAcceptedCandidate(t *testing.T) {
	for _, universe := range []int{16, 128, 1024} {
		p := mtLoadIsoTestConfigV1(nil)
		for _, in := range mtLoadIsoFastInputsV1(8, universe) {
			if p.incrementalV650 == nil {
				p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
			}
			base := p.incrementalV650
			touched := mtBoundTouchedForBatchV2(in)
			full := mtBoundRunRollbackV3(p, base, in, "full", touched)
			baseline := mtBoundCloneStateV1(base)
			for _, mode := range []string{"exact", "remote"} {
				reject := mtBoundPruneImpossibleV1(p, base, in, mode, full.trial, touched)
				if !reflect.DeepEqual(baseline, base) {
					t.Fatalf("prune leaked history %d %s", in.BatchIndex, mode)
				}
				alt := mtBoundRunRollbackV3(p, base, in, mode, touched)
				moves, pairs := mtBoundDeltaChangesV2(alt.after, full.after)
				accepted := mtBoundBatchSafeV1(alt.trial, full.trial) &&
					mtBoundPlanSwitchesV1(alt.trial.plan, full.trial.plan) <= 10 &&
					moves <= 50 && pairs <= 100 && alt.trial.skew <= full.trial.skew
				if reject && accepted {
					t.Fatalf("unsafe prune batch=%d mode=%s universe=%d", in.BatchIndex, mode, universe)
				}
				if !reflect.DeepEqual(baseline, base) {
					t.Fatalf("candidate leaked history %d %s", in.BatchIndex, mode)
				}
			}
			// Advance along original Full, isolating proof from placement-policy changes.
			mtBoundCOWApplyV3(base, full.after)
		}
	}
}

func TestMTLoadP0V1PlanAndShadowHistoryParity(t *testing.T) {
	for _, universe := range []int{16, 256, 2048} {
		legacy := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		optimized := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		for _, in := range mtLoadIsoFastInputsV1(10, universe) {
			want := mtLoadIsoLegacyReferenceV2(legacy, in)
			got := optimized.planLoadIsoV1(in)
			if !reflect.DeepEqual(want, got) ||
				!reflect.DeepEqual(legacy.incrementalV650, optimized.incrementalV650) ||
				!reflect.DeepEqual(legacy.loadIsoV1.shadow.incrementalV650, optimized.loadIsoV1.shadow.incrementalV650) {
				t.Fatalf("full plan/live/shadow mismatch batch=%d universe=%d", in.BatchIndex, universe)
			}
		}
	}
}
