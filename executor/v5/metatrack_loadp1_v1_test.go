package v5

import (
	"fmt"
	"reflect"
	"testing"
)

// Compare cached and uncached P0 reject decisions, including the side effects
// that COW must roll back, across changing histories and key cardinalities.
func TestMTLoadP1V1PruneAndHistoryParity(t *testing.T) {
	for _, universe := range []int{16, 128, 1024, 4096} {
		t.Run(fmt.Sprintf("keys_%d", universe), func(t *testing.T) {
			p := mtLoadIsoTestConfigV1(nil)
			for _, in := range mtLoadIsoFastInputsV1(12, universe) {
				if p.incrementalV650 == nil {
					p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
				}
				base := p.incrementalV650
				touched := mtBoundTouchedForBatchV2(in)
				full := mtBoundRunRollbackV3(p, base, in, "full", touched)
				before := mtBoundCloneStateV1(base)
				static := make([]*mtLoadP1StaticV1, len(in.Records))
				for _, mode := range []string{"exact", "remote"} {
					want := mtBoundPruneImpossibleV1(p, base, in, mode, full.trial, touched)
					if !reflect.DeepEqual(before, base) {
						t.Fatalf("old prune leaked batch=%d mode=%s", in.BatchIndex, mode)
					}
					got := mtLoadP1PruneImpossibleV1(p, base, in, mode, full.trial, touched, static)
					if got != want {
						t.Fatalf("P1 prune differs batch=%d mode=%s got=%t want=%t", in.BatchIndex, mode, got, want)
					}
					if !reflect.DeepEqual(before, base) {
						t.Fatalf("P1 prune leaked batch=%d mode=%s", in.BatchIndex, mode)
					}
				}
				mtBoundCOWApplyV3(base, full.after)
			}
		})
	}
}

// Full/selected COW plans and all captured map deltas must remain exactly the
// same as the P0 selector; this oracle bypasses only the P1 static cache.
func mtLoadP1OldP0OracleV1(p *metaTrackRouting, in BatchRoutingInput) (mtBoundSavedTrialV2, mtBoundSavedTrialV2) {
	base := p.incrementalV650
	touched := mtBoundTouchedForBatchV2(in)
	full := mtBoundRunRollbackV3(p, base, in, "full", touched)
	selected := full
	if len(in.Records) == 100 && len(in.ShardIDs) == 2 && in.ExpectedTransactionCount > 0 {
		for _, mode := range []string{"exact", "remote"} {
			if mtBoundPruneImpossibleV1(p, base, in, mode, full.trial, touched) {
				continue
			}
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

func TestMTLoadP1V1FullDecisionParity(t *testing.T) {
	for _, universe := range []int{16, 256, 2048} {
		p := mtLoadIsoTestConfigV1(nil)
		for _, in := range mtLoadIsoFastInputsV1(10, universe) {
			if p.incrementalV650 == nil {
				p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
			}
			before := mtBoundCloneStateV1(p.incrementalV650)
			oldSelected, oldFull := mtLoadP1OldP0OracleV1(p, in)
			gotSelected, gotFull := mtLoadIsoSelectBoundCOWV1(p, in)
			if !mtLoadP0SameSelectionV1(oldSelected, gotSelected) || !mtLoadP0SameSelectionV1(oldFull, gotFull) {
				t.Fatalf("COW decision or history changed batch=%d universe=%d", in.BatchIndex, universe)
			}
			if !reflect.DeepEqual(before, p.incrementalV650) {
				t.Fatalf("preview leaked batch=%d", in.BatchIndex)
			}
			mtBoundCOWApplyV3(p.incrementalV650, gotSelected.after)
		}
	}
}

// Native routing integration checks complete plan, Shadow and Live states.
func TestMTLoadP1V1PlanShadowParity(t *testing.T) {
	for _, universe := range []int{16, 256, 2048} {
		legacy := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		improved := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		for _, in := range mtLoadIsoFastInputsV1(10, universe) {
			want := mtLoadIsoLegacyReferenceV2(legacy, in)
			got := improved.planLoadIsoV1(in)
			if !reflect.DeepEqual(want, got) || !reflect.DeepEqual(legacy.incrementalV650, improved.incrementalV650) || !reflect.DeepEqual(legacy.loadIsoV1.shadow.incrementalV650, improved.loadIsoV1.shadow.incrementalV650) {
				t.Fatalf("plan/live/shadow mismatch batch=%d universe=%d", in.BatchIndex, universe)
			}
		}
	}
}
