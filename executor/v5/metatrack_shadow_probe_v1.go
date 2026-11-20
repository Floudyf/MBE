package v5

// MT_SHADOW_CARD_V1: measurement-only independent Full history.
// The live plan is generated exclusively by the existing Bound planner.
// Shadow placement and history are NEVER used to decide or sign live placement.
import (
	"log"
	"runtime"
	"sync"
	"time"
)

type mtShadowProbeStateV1 struct {
	mu            sync.Mutex
	full          *metaTrackRouting
	batches       int
	totalShadowNS int64
	maxShadowNS   int64
	// Incremental structural cardinality; no full-map scan in the hot path.
	keyShardSlots  int
	pairShardSlots int
	totalCountNS   int64
}

func mtShadowFullFromBoundV1(p *metaTrackRouting) *metaTrackRouting {
	config := make(map[string]any, len(p.config))
	for k, v := range p.config {
		config[k] = v
	}
	for _, key := range []string{
		"bounded_batch_routing_v1", "shadow_full_probe_v1", "bound_selector_v1",
		"online_state_ready_routing_v1", "online_feedback_coldstart_v2",
		"online_capacity_guard_v2",
	} {
		delete(config, key)
	}
	q := &metaTrackRouting{basicPlugin: p.basicPlugin}
	q.basicPlugin.config = config
	return q
}

func mtShadowStateEntriesV1(s *metaTrackIncrementalRoutingStateV650) int {
	if s == nil {
		return 0
	}
	n := len(s.ShardLoad) + len(s.ProducerByVersion) + len(s.HomeShardByKey)
	for _, shares := range s.KeyShardSupport {
		n += 1 + len(shares)
	}
	for _, shares := range s.PairShardSupport {
		n += 1 + len(shares)
	}
	return n // structural cardinality only, NOT bytes of heap memory
}

// mtShadowTouchedV2 records only key/pair entries touched by this RouteBatch.
// It does not copy or scan the accumulated historical maps.
type mtShadowTouchedV2 struct {
	keyOldSlots  map[string]int
	pairOldSlots map[string]int
}

func mtShadowBeforeBatchV2(full *metaTrackIncrementalRoutingStateV650, input BatchRoutingInput) mtShadowTouchedV2 {
	t := mtShadowTouchedV2{keyOldSlots: map[string]int{}, pairOldSlots: map[string]int{}}
	for _, rec := range input.Records {
		keys := metaTrackIncrementalKeysV650(rec) // same normalized, deduplicated keys as Full
		for _, key := range keys {
			if _, seen := t.keyOldSlots[key]; !seen {
				if full != nil {
					t.keyOldSlots[key] = len(full.KeyShardSupport[key])
				} else {
					t.keyOldSlots[key] = 0
				}
			}
		}
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				pair := keyPair(keys[i], keys[j])
				if _, seen := t.pairOldSlots[pair]; !seen {
					if full != nil {
						t.pairOldSlots[pair] = len(full.PairShardSupport[pair])
					} else {
						t.pairOldSlots[pair] = 0
					}
				}
			}
		}
	}
	return t
}

func mtShadowUpdateCountV2(probe *mtShadowProbeStateV1, full *metaTrackIncrementalRoutingStateV650, touched mtShadowTouchedV2) int {
	if full == nil {
		panic("MT_SHADOW_COUNT_STOP: nil Full state")
	}
	for key, old := range touched.keyOldSlots {
		current := len(full.KeyShardSupport[key])
		if current < old {
			panic("MT_SHADOW_COUNT_STOP: key support shrank")
		}
		probe.keyShardSlots += current - old
	}
	for pair, old := range touched.pairOldSlots {
		current := len(full.PairShardSupport[pair])
		if current < old {
			panic("MT_SHADOW_COUNT_STOP: pair support shrank")
		}
		probe.pairShardSlots += current - old
	}
	return len(full.ShardLoad) + len(full.ProducerByVersion) + len(full.HomeShardByKey) +
		len(full.KeyShardSupport) + probe.keyShardSlots +
		len(full.PairShardSupport) + probe.pairShardSlots
}

func mtShadowPlacementDriftV1(a, b BatchRoutingPlan) int {
	if len(a.TransactionPlacements) != len(b.TransactionPlacements) {
		return -1
	}
	changed := 0
	for i := range a.TransactionPlacements {
		x, y := a.TransactionPlacements[i], b.TransactionPlacements[i]
		if x.TxIndex != y.TxIndex || x.LogicalID != y.LogicalID {
			return -1
		}
		if x.ExecutionShard != y.ExecutionShard {
			changed++
		}
	}
	return changed
}

func (p *metaTrackRouting) planShadowProbeV1(input BatchRoutingInput) BatchRoutingPlan {
	// The diagnostic state lock is independent of the existing Full / Bound lock.
	p.shadowOnceV1.Do(func() { p.shadowProbeV1 = &mtShadowProbeStateV1{} })
	state := p.shadowProbeV1
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.full == nil || (input.BatchIndex == 0 && state.batches > 0) {
		state.full = mtShadowFullFromBoundV1(p)
		state.batches, state.totalShadowNS, state.maxShadowNS = 0, 0, 0
		state.keyShardSlots, state.pairShardSlots, state.totalCountNS = 0, 0, 0
	}

	probeStarted := time.Now()
	actualStart := time.Now()
	actual := p.planBoundV1(input) // live outcome: EXACTLY the existing Bound planner
	actualNS := time.Since(actualStart).Nanoseconds()

	// Only scan THIS batch's keys/pairs. O(batch coaccess), not O(all prior coaccess).
	countStart := time.Now()
	touched := mtShadowBeforeBatchV2(state.full.incrementalV650, input)
	preCountNS := time.Since(countStart).Nanoseconds()
	shadowStart := time.Now()
	shadow := state.full.planIncrementalExactContinuityV650(input) // independent shadow Full history
	shadowNS := time.Since(shadowStart).Nanoseconds()
	countStart = time.Now()
	entries := mtShadowUpdateCountV2(state, state.full.incrementalV650, touched)
	postCountNS := time.Since(countStart).Nanoseconds()
	countNS := preCountNS + postCountNS
	state.totalCountNS += countNS

	// Fail closed: this is a diagnostic method, never silently publish mismatched history.
	if state.full.incrementalV650 == nil || p.incrementalV650 == nil ||
		state.full.incrementalV650.Processed != p.incrementalV650.Processed ||
		state.full.incrementalV650.Capacity != p.incrementalV650.Capacity ||
		shadow.IncrementalHistoryTransactionCountAfter != actual.IncrementalHistoryTransactionCountAfter ||
		mtShadowPlacementDriftV1(actual, shadow) < 0 {
		panic("MT_SHADOW_PROBE_STOP: invalid shadow history or transaction order")
	}

	state.batches++
	state.totalShadowNS += shadowNS
	if shadowNS > state.maxShadowNS {
		state.maxShadowNS = shadowNS
	}
	// Preserve V1 metric keys for old parsers; add phase timings for honest overhead analysis.
	// probe_pre_log_us excludes log I/O. The outer client's routePlanUS includes it.
	drift := mtShadowPlacementDriftV1(actual, shadow)
	liveSkew := mtBoundSkewV1(p.incrementalV650)
	shadowSkew := mtBoundSkewV1(state.full.incrementalV650)
	probePreLogUS := time.Since(probeStarted).Microseconds()
	log.Printf("[MT_SHADOW_PROBE_V1] batch=%d tx=%d live_us=%d shadow_us=%d cumulative_shadow_us=%d max_shadow_us=%d drift_now=%d live_exact=%d shadow_exact=%d live_remote=%d shadow_remote=%d live_load_skew=%d shadow_load_skew=%d shadow_state_entries=%d shadow_producers=%d shadow_key_support=%d shadow_pair_support=%d shadow_home_cache=%d shadow_history=%s live_history=%s count_us=%d cumulative_count_us=%d probe_pre_log_us=%d count_method=incremental_v2",
		input.BatchIndex, len(input.Records), actualNS/1000, shadowNS/1000,
		state.totalShadowNS/1000, state.maxShadowNS/1000,
		drift,
		actual.IncrementalExactCrossShardStateEdgeCount, shadow.IncrementalExactCrossShardStateEdgeCount,
		actual.RemoteAccessEstimate, shadow.RemoteAccessEstimate,
		liveSkew, shadowSkew,
		entries,
		len(state.full.incrementalV650.ProducerByVersion),
		len(state.full.incrementalV650.KeyShardSupport),
		len(state.full.incrementalV650.PairShardSupport),
		len(state.full.incrementalV650.HomeShardByKey),
		state.full.incrementalV650.HistoryDigest, p.incrementalV650.HistoryDigest,
		countNS/1000, state.totalCountNS/1000, probePreLogUS)
	// Go process-wide heap, not Shadow-only memory. Sample at low frequency only.
	if state.batches == 1 || state.batches%100 == 0 {
		started := time.Now()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		log.Printf("[MT_SHADOW_HEAP_V2] batch=%d heap_alloc_bytes=%d heap_inuse_bytes=%d heap_sys_bytes=%d total_alloc_bytes=%d gc_count=%d sample_us=%d scope=go_process_not_shadow_only",
			input.BatchIndex, mem.HeapAlloc, mem.HeapInuse, mem.HeapSys, mem.TotalAlloc, mem.NumGC, time.Since(started).Microseconds())
	}
	return actual
}
