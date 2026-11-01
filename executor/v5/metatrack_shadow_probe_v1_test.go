package v5

import (
	"metaverse-chainlab/executor/realism/tx"
	"testing"
)

func TestMTShadowProbeV1LiveParityAndIndependentFull(t *testing.T) {
	full := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	bound := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
	probe := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true, "shadow_full_probe_v1": true})}
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for batch := 0; batch < 3; batch++ {
		records := make([]WorkloadRecord, 100)
		for i := range records {
			idx := batch*100 + i
			records[i] = WorkloadRecord{Index: idx, LogicalID: "shadow-test", StateKeys: []string{"balance:a", "balance:b"}, AccessList: []tx.AccessItem{{Key: "balance:a", Mode: tx.AccessReadWrite}, {Key: "balance:b", Mode: tx.AccessReadWrite}}}
		}
		in := BatchRoutingInput{BatchIndex: batch, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 300}
		baselineFull := full.PlanBatch(in)
		baselineBound := bound.PlanBatch(in)
		measured := probe.PlanBatch(in)
		if baselineBound.PlanDigest != measured.PlanDigest || baselineBound.IncrementalHistoryDigestAfter != measured.IncrementalHistoryDigestAfter {
			t.Fatalf("Shadow probe modified live Bound: batch=%d", batch)
		}
		if probe.shadowProbeV1 == nil || probe.shadowProbeV1.full == nil {
			t.Fatal("independent shadow was not created")
		}
		q := probe.shadowProbeV1.full
		if q.incrementalV650 == probe.incrementalV650 {
			t.Fatal("shadow and live routing history alias")
		}
		if q.incrementalV650.HistoryDigest != full.incrementalV650.HistoryDigest || baselineFull.PlanDigest == "" {
			t.Fatalf("Shadow not equivalent to independent Full: batch=%d", batch)
		}
		if measured.IncrementalHistoryTransactionCountAfter != (batch+1)*100 {
			t.Fatal("incorrect processed count")
		}
	}
}
func TestMTShadowProbeV1NeverUsesShadowForActualDecision(t *testing.T) {
	p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true, "shadow_full_probe_v1": true})}
	q := mtShadowFullFromBoundV1(p)
	for _, key := range []string{"bounded_batch_routing_v1", "shadow_full_probe_v1", "bound_selector_v1", "experimental_no_global_capacity_v1"} {
		if _, ok := q.config[key]; ok {
			t.Fatalf("shadow inherits treatment %s", key)
		}
	}
	if p.config["shadow_full_probe_v1"] != true {
		t.Fatal("changed actual router config")
	}
}

// No source router change: cardinality must match the V1 full-map reference,
// even with repeated keys, new shards for an existing key, and new key-pairs.
func TestMTShadowProbeV2CardinalityAndLivePlanParity(t *testing.T) {
	bound := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
	probe := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true, "shadow_full_probe_v1": true})}
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for batch := 0; batch < 18; batch++ {
		records := make([]WorkloadRecord, 80)
		for i := range records {
			idx := batch*80 + i
			a := "balance:a"
			b := []string{"balance:b", "balance:c", "balance:d", "balance:e", "nonce:z"}[(idx/5)%5]
			if i%7 == 0 {
				a = b
			}
			records[i] = WorkloadRecord{Index: idx, RoutingOrdinal: uint64(idx + 1), LogicalID: "shadow-cardinality", StateKeys: []string{a, b}, AccessList: []tx.AccessItem{{Key: a, Mode: tx.AccessReadWrite}, {Key: b, Mode: tx.AccessReadWrite}}, StateVersions: []tx.StateVersionDependency{{Key: a, ProducedVersion: uint64(idx + 1)}}}
		}
		in := BatchRoutingInput{BatchIndex: batch, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 1440}
		wantPlan := bound.PlanBatch(in)
		gotPlan := probe.PlanBatch(in)
		if gotPlan.PlanDigest != wantPlan.PlanDigest || gotPlan.IncrementalHistoryDigestAfter != wantPlan.IncrementalHistoryDigestAfter {
			t.Fatalf("live plan changed by incremental count: batch=%d", batch)
		}
		s := probe.shadowProbeV1.full.incrementalV650
		got := len(s.ShardLoad) + len(s.ProducerByVersion) + len(s.HomeShardByKey) + len(s.KeyShardSupport) + probe.shadowProbeV1.keyShardSlots + len(s.PairShardSupport) + probe.shadowProbeV1.pairShardSlots
		want := mtShadowStateEntriesV1(s) // reference-only: NOT called on the production hot path
		if got != want {
			t.Fatalf("incremental cardinality mismatch batch=%d got=%d want=%d", batch, got, want)
		}
	}
}
