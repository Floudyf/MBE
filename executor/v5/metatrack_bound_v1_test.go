package v5

import (
	"fmt"
	"metaverse-chainlab/executor/realism/tx"
	"reflect"
	"testing"
)

func TestMTBoundCloneV1DoesNotAliasNestedHistory(t *testing.T) {
	s := &metaTrackIncrementalRoutingStateV650{ShardLoad: map[string]int{"s0": 1}, ProducerByVersion: map[metaTrackIncrementalVersionSlotV650]metaTrackIncrementalProducerV650{{Key: "k", Version: 1}: {Shard: "s0"}}, PairShardSupport: map[string]map[string]int{"a\x00b": {"s0": 2}}, KeyShardSupport: map[string]map[string]int{"k": {"s1": 3}}, HomeShardByKey: map[string]string{"k": "s0"}}
	c := mtBoundCloneStateV1(s)
	c.ShardLoad["s0"] = 999
	c.PairShardSupport["a\x00b"]["s0"] = 999
	c.KeyShardSupport["k"]["s1"] = 999
	c.HomeShardByKey["k"] = "s1"
	c.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: "k", Version: 1}] = metaTrackIncrementalProducerV650{Shard: "s1"}
	if s.ShardLoad["s0"] != 1 || s.PairShardSupport["a\x00b"]["s0"] != 2 || s.KeyShardSupport["k"]["s1"] != 3 || s.HomeShardByKey["k"] != "s0" || s.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: "k", Version: 1}].Shard != "s0" {
		t.Fatal("candidate fork polluted Full history")
	}
}
func TestMTBoundSelectorV1Priority(t *testing.T) {
	a := []metaTrackIncrementalCandidateV650{{Shard: "s0", ReadyRank: 1, ExactCross: 1, RemoteCost: 9}, {Shard: "s1", ReadyRank: 2, ExactCross: 0, RemoteCost: 0}}
	if got := mtBoundAlternativeRankV1(a, "exact"); got[0].Shard != "s0" {
		t.Fatalf("exact should favor ready rank: %+v", got)
	}
	if got := mtBoundAlternativeRankV1(a, "remote"); got[0].Shard != "s1" {
		t.Fatalf("remote should favor Home access: %+v", got)
	}
}
func TestMTBoundV1FullFreezeAndIndependentHistory(t *testing.T) {
	full := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	bound := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
	sharding := builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}
	for batch := 0; batch < 2; batch++ {
		records := make([]WorkloadRecord, 100)
		for i := range records {
			index := batch*100 + i
			records[i] = WorkloadRecord{Index: index, LogicalID: "test-tx", StateKeys: []string{"balance:a", "balance:b"}, AccessList: []tx.AccessItem{{Key: "balance:a", Mode: tx.AccessReadWrite}, {Key: "balance:b", Mode: tx.AccessReadWrite}}}
		}
		input := BatchRoutingInput{BatchIndex: batch, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: sharding, ExpectedTransactionCount: 200}
		original := full.PlanBatch(input)
		alternative := bound.PlanBatch(input)
		if len(original.TransactionPlacements) != 100 || len(alternative.TransactionPlacements) != 100 {
			t.Fatal("invalid batch partition")
		}
		if original.IncrementalRoutingPolicy == mtBoundMicro10V1 || alternative.IncrementalRoutingPolicy != mtBoundMicro10V1 {
			t.Fatal("Full treatment leaked to baseline or flag ignored")
		}
		for i := range alternative.TransactionPlacements {
			if alternative.TransactionPlacements[i].ExecutionShard == "" {
				t.Fatal("empty execution shard")
			}
		}
	}
	if full.incrementalV650 == bound.incrementalV650 {
		t.Fatal("shared actual routing history")
	}
}

// The old deep-copy implementation is retained as the independent reference
// oracle in tests only; it never runs for the installed micro10 hot path.
func mtBoundLegacyPlanForTest(p *metaTrackRouting, input BatchRoutingInput) BatchRoutingPlan {
	p.incrementalMu.Lock()
	defer p.incrementalMu.Unlock()
	reset := input.BatchIndex == 0 && p.incrementalV650 != nil && p.incrementalV650.Processed > 0
	if reset || p.incrementalV650 == nil || !p.incrementalV650.compatible(input) {
		p.incrementalV650 = newMetaTrackIncrementalRoutingStateV650(input)
	}
	base := p.incrementalV650
	full := mtBoundForkV1(p, base, input, "full")
	selected := full
	if len(input.Records) == 100 && len(input.ShardIDs) == 2 && input.ExpectedTransactionCount > 0 {
		for _, mode := range []string{"exact", "remote"} {
			alternative := mtBoundForkV1(p, base, input, mode)
			if !mtBoundBatchSafeV1(alternative, full) || mtBoundPlanSwitchesV1(alternative.plan, full.plan) > 10 {
				continue
			}
			moves, pairs := mtBoundChangesV1(alternative, full, input)
			if moves > 50 || pairs > 100 || mtBoundSkewV1(alternative.state) > mtBoundSkewV1(full.state) {
				continue
			}
			if selected.mode == "full" || mtBoundLessV1(alternative, selected) {
				selected = alternative
			}
		}
	}
	p.incrementalV650 = selected.state
	plan := selected.plan
	plan.IncrementalRoutingPolicy = mtBoundMicro10V1
	plan.TransactionPolicy = "bounded_batch_same_history_pareto_micro10_v1"
	if selected.mode != "full" {
		for i := range plan.TransactionPlacements {
			plan.TransactionPlacements[i].Reason += ":bounded_batch_" + selected.mode
		}
	}
	if !input.DeferFinalSeal {
		plan.PlanDigest = routingPlanDigest(plan)
	}
	return plan
}

func mtBoundTestInputV2(batch, count, keys, expected int) BatchRoutingInput {
	records := make([]WorkloadRecord, count)
	for i := 0; i < count; i++ {
		idx := batch*100 + i
		access := make([]tx.AccessItem, 0, keys)
		for k := 0; k < keys; k++ {
			name := fmt.Sprintf("test:key:%d", (idx*7+k*31+(idx/9)*3)%(keys*50+11))
			access = append(access, tx.AccessItem{Key: name, Mode: tx.AccessReadWrite})
		}
		// A stable per-object chain tests ProducerByVersion COW and exact inputs.
		objectKey := fmt.Sprintf("test:object:%d", idx%11)
		access = append(access, tx.AccessItem{Key: objectKey, Mode: tx.AccessReadWrite})
		required := uint64(0)
		if idx >= 11 {
			required = uint64(idx - 11 + 1)
		}
		version := []tx.StateVersionDependency{{Key: objectKey, RequiredVersion: required, ProducedVersion: uint64(idx + 1)}}
		records[i] = WorkloadRecord{Index: idx, LogicalID: fmt.Sprintf("b%d-t%d", batch, i), RoutingOrdinal: uint64(idx + 1), AccessList: access, StateVersions: version}
	}
	return BatchRoutingInput{BatchIndex: batch, Records: records, ShardIDs: []string{"s0", "s1"}, Sharding: builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)}, ExpectedTransactionCount: expected}
}

func TestMTBoundDeltaV2EquivalenceWithLegacy(t *testing.T) {
	for _, case_ := range []struct {
		name    string
		keys    int
		batches int
		tail    int
	}{
		{"FW-like", 2, 3, 19}, {"DCL-like", 3, 3, 8}, {"AW-like", 12, 2, 11}, {"Henesys-like", 1, 3, 27},
	} {
		t.Run(case_.name, func(t *testing.T) {
			legacy := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
			optimized := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
			for batch := 0; batch <= case_.batches; batch++ {
				count := 100
				if batch == case_.batches {
					count = case_.tail
				}
				input := mtBoundTestInputV2(batch, count, case_.keys, case_.batches*100+case_.tail)
				old := mtBoundLegacyPlanForTest(legacy, input)
				newPlan := optimized.planBoundV1(input)
				if !reflect.DeepEqual(old, newPlan) {
					t.Fatalf("batch %d plans differ: first old=%+v new=%+v, policy old=%s new=%s", batch, old.TransactionPlacements[0], newPlan.TransactionPlacements[0], old.TransactionPolicy, newPlan.TransactionPolicy)
				}
				if !reflect.DeepEqual(legacy.incrementalV650, optimized.incrementalV650) {
					t.Fatalf("batch %d persistent routing state differs; old=%s new=%s", batch, legacy.incrementalV650.HistoryDigest, optimized.incrementalV650.HistoryDigest)
				}
			}
		})
	}
}

func TestMTBoundDeltaV2RestoresAllMapSlots(t *testing.T) {
	input := mtBoundTestInputV2(0, 100, 7, 200)
	s := newMetaTrackIncrementalRoutingStateV650(input)
	touched := mtBoundTouchedForBatchV2(input)
	p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	_ = mtBoundRunRollbackV3(p, s, input, "full", touched)
	if !reflect.DeepEqual(s, newMetaTrackIncrementalRoutingStateV650(input)) {
		t.Fatal("trial leaked routing changes into shared state")
	}
}

func BenchmarkMTBoundPlannerV2(b *testing.B) {
	for _, keys := range []int{2, 10} {
		b.Run(fmt.Sprintf("keys%d", keys), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
				for batch := 0; batch < 12; batch++ {
					p.planBoundV1(mtBoundTestInputV2(batch, 100, keys, 1200))
				}
			}
		})
	}
}

func BenchmarkMTBoundLegacyPlannerForComparison(b *testing.B) {
	for _, keys := range []int{2, 10} {
		b.Run(fmt.Sprintf("keys%d", keys), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true})}
				for batch := 0; batch < 12; batch++ {
					mtBoundLegacyPlanForTest(p, mtBoundTestInputV2(batch, 100, keys, 1200))
				}
			}
		})
	}
}

// The benchmark constructs a large pre-existing routing history outside the
// timed region, then plans ONE 100-record batch. Old fork cost scales with all
// prior keys/pairs; COW cost scales with current-batch touched keys/pairs.
func BenchmarkMTBoundLargeHistory(b *testing.B) {
	for _, mode := range []string{"legacy", "cow"} {
		b.Run(mode, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				input := mtBoundTestInputV2(1, 100, 8, 20_000)
				s := newMetaTrackIncrementalRoutingStateV650(input)
				for j := 0; j < 20000; j++ {
					s.PairShardSupport[fmt.Sprintf("old-pair-%d", j)] = map[string]int{"s0": 5, "s1": 2}
					s.ProducerByVersion[metaTrackIncrementalVersionSlotV650{Key: fmt.Sprintf("old-producer-%d", j), Version: uint64(j + 1)}] = metaTrackIncrementalProducerV650{Shard: "s1", Ordinal: uint64(j + 1)}
				}
				p := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true, "bounded_batch_routing_v1": true}), incrementalV650: s}
				b.StartTimer()
				if mode == "legacy" {
					mtBoundLegacyPlanForTest(p, input)
				} else {
					p.planBoundV1(input)
				}
			}
		})
	}
}

func TestMTBoundDeltaV2ThreeShardFallback(t *testing.T) {
	legacy := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	cow := &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", map[string]any{"incremental_exact_continuity_routing_v65": true})}
	for batch := 0; batch < 2; batch++ {
		input := mtBoundTestInputV2(batch, 100, 3, 200)
		input.ShardIDs = []string{"s0", "s1", "s2"}
		if a, b := mtBoundLegacyPlanForTest(legacy, input), cow.planBoundV1(input); !reflect.DeepEqual(a, b) {
			t.Fatal("3-shard fallback differs")
		}
		if !reflect.DeepEqual(legacy.incrementalV650, cow.incrementalV650) {
			t.Fatal("3-shard history differs")
		}
	}
}
