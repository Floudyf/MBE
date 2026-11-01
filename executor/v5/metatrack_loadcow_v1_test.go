package v5

// MT_LOADCOW_V1: a test-only oracle comparison. The old deep-fork chooser
// is intentionally retained unchanged in metatrack_loadiso_v1.go.
import (
	"fmt"
	"reflect"
	"testing"
)

func TestMTLoadCowV1ForkParityAndNoLeakedPreview(t *testing.T) {
	for _, universe := range []int{16, 256, 4096} {
		t.Run(fmt.Sprintf("keys_%d", universe), func(t *testing.T) {
			inputs := mtLoadIsoFastInputsV1(7, universe)
			planner := mtLoadIsoTestConfigV1(nil)
			for _, in := range inputs[:5] {
				planner.planIncrementalExactContinuityV650(in)
			}
			baseline := mtBoundCloneStateV1(planner.incrementalV650)
			oldChosen, oldFull := mtLoadIsoSelectBoundAndFullV2(planner, inputs[5])
			chosen, full := mtLoadIsoSelectBoundCOWV1(planner, inputs[5])
			if !reflect.DeepEqual(baseline, planner.incrementalV650) {
				t.Fatal("candidate preview leaked into persistent history")
			}
			if chosen.trial.mode != oldChosen.mode || !reflect.DeepEqual(chosen.trial.plan, oldChosen.plan) ||
				!reflect.DeepEqual(full.trial.plan, oldFull.plan) {
				t.Fatalf("full/candidate plan mismatch for key universe %d", universe)
			}
			physical := mtBoundCloneStateV1(planner.incrementalV650)
			mtBoundCOWApplyV3(physical, chosen.after)
			if !reflect.DeepEqual(physical, oldChosen.state) {
				t.Fatal("selected COW history differs from frozen deep-fork oracle")
			}
			reference := mtBoundCloneStateV1(planner.incrementalV650)
			mtBoundCOWApplyV3(reference, full.after)
			if !reflect.DeepEqual(reference, oldFull.state) {
				t.Fatal("Full COW history differs from frozen deep-fork oracle")
			}
		})
	}
}

func TestMTLoadCowV1TenBatchParity(t *testing.T) {
	for _, universe := range []int{16, 256, 2048} {
		t.Run(fmt.Sprintf("keys_%d", universe), func(t *testing.T) {
			before := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
			after := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
			for _, in := range mtLoadIsoFastInputsV1(10, universe) {
				want := mtLoadIsoLegacyReferenceV2(before, in)
				got := after.planLoadIsoV1(in)
				if !reflect.DeepEqual(want, got) ||
					!reflect.DeepEqual(before.incrementalV650, after.incrementalV650) ||
					!reflect.DeepEqual(before.loadIsoV1.shadow.incrementalV650, after.loadIsoV1.shadow.incrementalV650) {
					t.Fatalf("batch=%d universe=%d live/Full plans or states diverged", in.BatchIndex, universe)
				}
			}
		})
	}
}

func TestMTLoadCowV1ForcedSeparatedHistoryParity(t *testing.T) {
	old := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	now := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	for i, in := range mtLoadIsoFastInputsV1(7, 64) {
		expected := mtLoadIsoLegacyReferenceV2(old, in)
		if i == 1 {
			now.loadIsoV1.shadow.incrementalV650 = mtBoundCloneStateV1(now.loadIsoV1.shadow.incrementalV650)
			now.loadIsoV1.aligned = false
		}
		actual := now.planLoadIsoV1(in)
		if !reflect.DeepEqual(expected, actual) ||
			!reflect.DeepEqual(old.incrementalV650, now.incrementalV650) ||
			!reflect.DeepEqual(old.loadIsoV1.shadow.incrementalV650, now.loadIsoV1.shadow.incrementalV650) {
			t.Fatalf("forced diverged path changed at batch %d", i)
		}
	}
	if now.loadIsoV1.donorCalls == 0 || now.loadIsoV1.replayCalls == 0 {
		t.Fatal("diverged path did not exercise donor and replay")
	}
}
