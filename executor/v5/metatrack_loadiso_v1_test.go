package v5

import (
	"fmt"
	"metaverse-chainlab/executor/realism/tx"
	"reflect"
	"testing"
)

func mtLoadIsoTestConfigV1(extra map[string]any) *metaTrackRouting {
	cfg := map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true}
	for k, v := range extra {
		cfg[k] = v
	}
	return &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", cfg)}
}

func TestMTLoadIsoV1DonorPrefixAndRealCapacity(t *testing.T) {
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	inputs := []WorkloadRecord{{Index: 0, LogicalID: "tx0"}, {Index: 1, LogicalID: "tx1"}, {Index: 2, LogicalID: "tx2"}, {Index: 3, LogicalID: "tx3"}}
	in := BatchRoutingInput{BatchIndex: 0, Records: inputs, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 4}
	p := mtLoadIsoTestConfigV1(nil)
	donor := mtLoadIsoTestConfigV1(nil)
	snapshots, fullPlan := mtLoadIsoDonorV1(donor, in)
	if len(snapshots) != 4 || len(fullPlan.TransactionPlacements) != 4 {
		t.Fatal("donor prefix not recorded")
	}
	if snapshots[0]["s0"] != 0 || snapshots[0]["s1"] != 0 {
		t.Fatal("future Full placements leaked to first record")
	}
	for i := range snapshots {
		if snapshots[i]["s0"]+snapshots[i]["s1"] != i {
			t.Fatalf("donor snapshot leaked at %d", i)
		}
	}
	// Shadow ranks s1 strictly lower, but the REAL capacity is only 2 on each shard.
	fake := make([]map[string]int, 4)
	for i := range fake {
		fake[i] = map[string]int{"s0": 10000, "s1": 0}
	}
	actual := p.planLoadIsoExactV1(in, fake)
	if len(actual.TransactionPlacements) != 4 {
		t.Fatal("missing physical placements")
	}
	for i, pl := range actual.TransactionPlacements {
		if i < 2 && pl.ExecutionShard != "s1" {
			t.Fatalf("shadow rank not used at index %d: %s", i, pl.ExecutionShard)
		}
		if i >= 2 && pl.ExecutionShard != "s0" {
			t.Fatalf("real capacity not respected at index %d: %s", i, pl.ExecutionShard)
		}
	}
	state := p.incrementalV650
	if state.ShardLoad["s0"] != 2 || state.ShardLoad["s1"] != 2 || state.Processed != 4 {
		t.Fatalf("real capacity failed: %+v", state.ShardLoad)
	}
}

func TestMTLoadIsoV1FullParityWithEqualDonor(t *testing.T) {
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	records := make([]WorkloadRecord, 100)
	for i := range records {
		records[i] = WorkloadRecord{Index: i, LogicalID: "parity", RoutingOrdinal: uint64(i + 1),
			StateKeys: []string{"balance:a", "balance:b"}, AccessList: []tx.AccessItem{{Key: "balance:a", Mode: tx.AccessReadWrite}, {Key: "balance:b", Mode: tx.AccessReadWrite}}}
	}
	in := BatchRoutingInput{BatchIndex: 0, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 200}
	shadow := mtLoadIsoTestConfigV1(nil)
	snapshots, _ := mtLoadIsoDonorV1(shadow, in)
	real := mtLoadIsoTestConfigV1(nil)
	want := real.planIncrementalExactContinuityV650(in)
	candidate := mtLoadIsoTestConfigV1(nil)
	got := candidate.planLoadIsoExactV1(in, snapshots)
	if got.IncrementalHistoryDigestAfter != want.IncrementalHistoryDigestAfter || mtShadowPlacementDriftV1(got, want) != 0 {
		t.Fatal("equal donor load must preserve exactly Full placements/history")
	}
}

func TestMTLoadIsoV1IndependentHistoryAndFrozenBaselines(t *testing.T) {
	full := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	bound := mtLoadIsoTestConfigV1(nil)
	probe := mtLoadIsoTestConfigV1(map[string]any{"shadow_full_probe_v1": true})
	candidate := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for b := 0; b < 4; b++ {
		records := make([]WorkloadRecord, 100)
		for i := range records {
			idx := b*100 + i
			keys := []string{"balance:a", []string{"balance:b", "balance:c", "nonce:d"}[(idx/3)%3]}
			records[i] = WorkloadRecord{Index: idx, LogicalID: "loadiso-parity", RoutingOrdinal: uint64(idx + 1), StateKeys: keys,
				AccessList:    []tx.AccessItem{{Key: keys[0], Mode: tx.AccessReadWrite}, {Key: keys[1], Mode: tx.AccessReadWrite}},
				StateVersions: []tx.StateVersionDependency{{Key: keys[0], ProducedVersion: uint64(idx + 1)}}}
		}
		in := BatchRoutingInput{BatchIndex: b, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 400}
		pure := full.PlanBatch(in)
		existing := bound.PlanBatch(in)
		measured := probe.PlanBatch(in)
		actual := candidate.PlanBatch(in)
		if existing.PlanDigest != measured.PlanDigest || existing.IncrementalHistoryDigestAfter != measured.IncrementalHistoryDigestAfter {
			t.Fatalf("existing Bound/Shadow live plan changed at batch=%d", b)
		}
		if candidate.loadIsoV1 == nil || candidate.loadIsoV1.shadow == nil {
			t.Fatal("shadow Full absent")
		}
		if candidate.loadIsoV1.shadow.incrementalV650.HistoryDigest != full.incrementalV650.HistoryDigest || pure.IncrementalHistoryDigestAfter == "" {
			t.Fatalf("Shadow Full differs from independently running Full at batch=%d", b)
		}
		if actual.IncrementalRoutingPolicy != mtLoadIsoPolicyV1 || actual.PlanDigest == "" || len(actual.TransactionPlacements) != 100 {
			t.Fatalf("new candidate not selected or not signed batch=%d", b)
		}
		physical := candidate.incrementalV650
		if physical.ShardLoad["s0"]+physical.ShardLoad["s1"] != (b+1)*100 || physical.ShardLoad["s0"] > 200 || physical.ShardLoad["s1"] > 200 {
			t.Fatalf("physical balance violation batch=%d: %+v", b, physical.ShardLoad)
		}
	}
}

// MT_LOADFAST_V1: frozen v2 reference, test-only. No future inputs or
// new routing policy are introduced by the production fast path.
func mtLoadIsoLegacyReferenceV2(p *metaTrackRouting, in BatchRoutingInput) BatchRoutingPlan {
	if p.loadIsoV1 == nil {
		p.loadIsoV1 = &mtLoadIsoStateV1{}
	}
	iso := p.loadIsoV1
	if iso.shadow == nil || (in.BatchIndex == 0 && iso.batches > 0) {
		iso.shadow = mtShadowFullFromBoundV1(p)
		delete(iso.shadow.basicPlugin.config, "load_iso_shadow_v1")
		iso.batches = 0
		p.incrementalV650 = nil
	}
	if p.incrementalV650 == nil || !p.incrementalV650.compatible(in) {
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(in)
	}
	snapshots, _ := mtLoadIsoDonorV1(iso.shadow, in)
	chosen := mtLoadIsoSelectBoundV1(p, in)
	var result BatchRoutingPlan
	if chosen.mode != "full" {
		p.incrementalV650 = chosen.state
		result = chosen.plan
		for i := range result.TransactionPlacements {
			result.TransactionPlacements[i].Reason += ":bounded_batch_" + chosen.mode
		}
	} else {
		result = p.planLoadIsoExactV1(in, snapshots)
	}
	result.IncrementalRoutingPolicy = mtLoadIsoPolicyV1
	result.TransactionPolicy = "micro10_intervention_else_full_shadow_load_rank_real_capacity_v1"
	if !in.DeferFinalSeal {
		result.PlanDigest = routingPlanDigest(result)
	}
	iso.batches++
	return result
}

func mtLoadIsoFastInputsV1(batches int, keyUniverse int) []BatchRoutingInput {
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	all := make([]BatchRoutingInput, batches)
	versions := map[string]uint64{}
	for b := 0; b < batches; b++ {
		records := make([]WorkloadRecord, 100)
		for i := 0; i < 100; i++ {
			idx := b*100 + i
			first := fmt.Sprintf("balance:%d", (idx*7+b*3)%keyUniverse)
			second := fmt.Sprintf("balance:%d", (idx*13+7)%keyUniverse)
			if first == second {
				second = fmt.Sprintf("balance:%d", (idx*13+8)%keyUniverse)
			}
			keys := []string{first, second}
			deps := make([]tx.StateVersionDependency, 0, len(keys))
			for _, k := range keys {
				deps = append(deps, tx.StateVersionDependency{Key: k, RequiredVersion: versions[k], ProducedVersion: uint64(idx + 1)})
				versions[k] = uint64(idx + 1)
			}
			records[i] = WorkloadRecord{
				Index: idx, LogicalID: fmt.Sprintf("case:%d", idx), RoutingOrdinal: uint64(idx + 1),
				StateKeys: keys, AccessList: []tx.AccessItem{
					{Key: first, Mode: tx.AccessReadWrite}, {Key: second, Mode: tx.AccessReadWrite}},
				StateVersions: deps,
			}
		}
		all[b] = BatchRoutingInput{BatchIndex: b, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: batches * 100}
	}
	return all
}

func TestMTLoadIsoFastV1FullHistoryReuseParity(t *testing.T) {
	for _, keyUniverse := range []int{16, 256} {
		old := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		fast := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
		reused := 0
		for _, in := range mtLoadIsoFastInputsV1(5, keyUniverse) {
			expected := mtLoadIsoLegacyReferenceV2(old, in)
			actual := fast.planLoadIsoV1(in)
			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("route plan changed: keys=%d batch=%d, old=%s new=%s", keyUniverse, in.BatchIndex, expected.PlanDigest, actual.PlanDigest)
			}
			if old.incrementalV650.HistoryDigest != fast.incrementalV650.HistoryDigest ||
				old.loadIsoV1.shadow.incrementalV650.HistoryDigest != fast.loadIsoV1.shadow.incrementalV650.HistoryDigest {
				t.Fatalf("live/shadow history mismatch at batch %d", in.BatchIndex)
			}
			if fast.loadIsoV1.aligned && fast.incrementalV650 != fast.loadIsoV1.shadow.incrementalV650 {
				t.Fatalf("shared prefix not actually shared batch %d", in.BatchIndex)
			}
			if fast.loadIsoV1.reuseFullBatches < reused {
				t.Fatal("reuse counter regressed")
			}
			reused = fast.loadIsoV1.reuseFullBatches
		}
	}
}

func TestMTLoadIsoFastV1SeparatedHistoryFallbackParity(t *testing.T) {
	old := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	fast := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for i := 0; i < 4; i++ {
		recs := make([]WorkloadRecord, 100)
		for k := range recs {
			recs[k] = WorkloadRecord{Index: i*100 + k, LogicalID: fmt.Sprintf("forcedfallback:%d", i*100+k), RoutingOrdinal: uint64(i*100 + k + 1)}
		}
		in := BatchRoutingInput{BatchIndex: i, Records: recs, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 400}
		expected := mtLoadIsoLegacyReferenceV2(old, in)
		if i == 1 {
			// Force distinct state objects without altering their contents:
			// the fallback remains identical to the original implementation.
			fast.loadIsoV1.shadow.incrementalV650 = mtBoundCloneStateV1(fast.loadIsoV1.shadow.incrementalV650)
			fast.loadIsoV1.aligned = false
		}
		actual := fast.planLoadIsoV1(in)
		if !reflect.DeepEqual(expected, actual) ||
			old.incrementalV650.HistoryDigest != fast.incrementalV650.HistoryDigest ||
			old.loadIsoV1.shadow.incrementalV650.HistoryDigest != fast.loadIsoV1.shadow.incrementalV650.HistoryDigest {
			t.Fatalf("diverged fallback differs from v2 at batch %d", i)
		}
	}
	if fast.loadIsoV1.donorCalls == 0 || fast.loadIsoV1.replayCalls == 0 {
		t.Fatalf("original fallback not exercised donor=%d replay=%d", fast.loadIsoV1.donorCalls, fast.loadIsoV1.replayCalls)
	}
}

func TestMTLoadIsoFastV1CertainNoIntervention(t *testing.T) {
	a := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	b := mtLoadIsoTestConfigV1(map[string]any{"load_iso_shadow_v1": true})
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for batch := 0; batch < 3; batch++ {
		recs := make([]WorkloadRecord, 100)
		for i := range recs {
			recs[i] = WorkloadRecord{Index: batch*100 + i, LogicalID: fmt.Sprintf("noaccess:%d", batch*100+i), RoutingOrdinal: uint64(batch*100 + i + 1)}
		}
		in := BatchRoutingInput{BatchIndex: batch, Records: recs, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 300}
		want := mtLoadIsoLegacyReferenceV2(a, in)
		got := b.planLoadIsoV1(in)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("no-intervention output mismatch batch=%d", batch)
		}
	}
	if b.loadIsoV1.reuseFullBatches != 3 || b.loadIsoV1.donorCalls != 0 || b.loadIsoV1.replayCalls != 0 {
		t.Fatalf("unnecessary replay for identical Full history: reuse=%d donor=%d replay=%d", b.loadIsoV1.reuseFullBatches, b.loadIsoV1.donorCalls, b.loadIsoV1.replayCalls)
	}
}
