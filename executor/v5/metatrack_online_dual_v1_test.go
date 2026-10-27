package v5

import (
	"strings"
	"testing"
	"time"
)

func mtDualFreshV1(now int64, rtt0, rtt1 float64) *mtOnlineFeedbackV1 {
	return &mtOnlineFeedbackV1{Shards: map[string]mtOnlineShardFeedbackV1{
		"s0": {Shard: "s0", SampledMS: now, RemoteLatencyMS: rtt0},
		"s1": {Shard: "s1", SampledMS: now, RemoteLatencyMS: rtt1},
	}}
}
func TestMTOnlineDualColdstartFreshProxy(t *testing.T) {
	now := time.Now().UnixMilli()
	xs := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Admissible: true, ReadyRank: 1, RemoteCost: 1},
		{Shard: "s1", Admissible: true, ReadyRank: 2, RemoteCost: 0},
	}
	f := mtDualFreshV1(now, 0, 0)
	if _, ok := mtOnlineRankCandidatesV1(xs, f, now); ok {
		t.Fatal("legacy scorer should fallback")
	}
	ranked, ok, proxy := mtOnlineRankCandidatesColdstartV1(xs, f, now)
	if !ok || !proxy || len(ranked) != 2 {
		t.Fatalf("coldstart fallback invalid: %t %t", ok, proxy)
	}
	if f.Shards["s0"].RemoteLatencyMS != 0 {
		t.Fatal("sampler snapshot mutated")
	}
	f.Shards["s0"] = mtOnlineShardFeedbackV1{Shard: "s0", SampledMS: now - 5000}
	if _, ok, _ := mtOnlineRankCandidatesColdstartV1(xs, f, now); ok {
		t.Fatal("stale candidate snapshot accepted")
	}
}
func TestMTOnlineDualOriginalWhenRTTExists(t *testing.T) {
	now := time.Now().UnixMilli()
	xs := []metaTrackIncrementalCandidateV650{{Shard: "s0", ReadyRank: 1}, {Shard: "s1", ReadyRank: 2}}
	f := mtDualFreshV1(now, 5, 0)
	original, ok := mtOnlineRankCandidatesV1(xs, f, now)
	patched, valid, proxy := mtOnlineRankCandidatesColdstartV1(xs, f, now)
	if !ok || !valid || proxy {
		t.Fatal("real RTT not preserved")
	}
	for i := range original {
		if original[i] != patched[i] {
			t.Fatal("real RTT ranking changed")
		}
	}
}
func TestMTOnlineDualLoadGuard(t *testing.T) {
	choose := func(xs []metaTrackIncrementalCandidateV650, batch int, rtt float64) string {
		now := time.Now().UnixMilli()
		f := mtDualFreshV1(now, rtt, rtt)
		return mtOnlineBalanceChooseV2(xs, batch, f).Shard
	}
	// Original dual_v1 incorrectly chose s0 even though its completion
	// was worse and it introduced a new cross-shard exact predecessor.
	base := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Load: 100, ReadyRank: 2, ExactCross: 1, WorstRank: 2000, RankSum: 2000},
		{Shard: "s1", Load: 900, ReadyRank: 1, ExactCross: 0, WorstRank: 1000, RankSum: 1000},
	}
	if got := choose(base, 100, 5); got != "s1" {
		t.Fatalf("must preserve exact chain and ready rank: %s", got)
	}
	if base[0].WorstRank != 2000 {
		t.Fatal("candidate input mutated")
	}
	// Even with no signed-cross penalty, large predicted time regressions
	// must not be justified by cumulative historical load.
	slow := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Load: 100, ReadyRank: 1, WorstRank: 12000, RankSum: 0},
		{Shard: "s1", Load: 900, ReadyRank: 1, WorstRank: 1000, RankSum: 0},
	}
	if got := choose(slow, 100, 5); got != "s1" {
		t.Fatalf("too slow alternative selected: %s", got)
	}
	// Load difference smaller than one RouteBatch is NOT an excuse for
	// alternating choices. This was the exact previous v1 pathology.
	smallSkew := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Load: 499, ReadyRank: 1, WorstRank: 1500},
		{Shard: "s1", Load: 500, ReadyRank: 1, WorstRank: 1000},
	}
	if got := choose(smallSkew, 100, 5); got != "s1" {
		t.Fatalf("strict alternation regression: %s", got)
	}
	// If skew is material, locality and readiness are unchanged, and the
	// cost difference is within one RTT, shifting an assignment is allowed.
	admissible := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Load: 100, ReadyRank: 1, ExactCross: 0, WorstRank: 2000},
		{Shard: "s1", Load: 900, ReadyRank: 1, ExactCross: 0, WorstRank: 1000},
	}
	if got := choose(admissible, 100, 5); got != "s0" {
		t.Fatalf("safe capacity relief not applied: %s", got)
	}
	if got := choose(admissible, 100, 0); got != "s0" {
		t.Fatalf("proxy budget should preserve safe relief: %s", got)
	}
	// Missing alternative: preserve baseline.
	if got := choose(admissible[:1], 100, 5); got != "s0" {
		t.Fatal("single candidate changed")
	}
	if !strings.Contains(mtOnlineDualReasonV1("online_joint_ready", true, true), "proxy_no_remote_sample") {
		t.Fatal("proxy label missing")
	}
	if !strings.Contains(mtOnlineDualReasonV1("online_joint_ready", false, true), "load_balance_guard_v2") {
		t.Fatal("v2 evidence label missing")
	}
}

func TestMTOnlineCapEnvelopeV3(t *testing.T) {
	// 10K/2-shard/100-record RouteBatch: 1000-record allowed excess.
	if v := mtOnlineCapacityEnvelopeV3(5200, 10000, 5000, 100); v != 3600 {
		t.Fatalf("envelope at batch 52: %d", v)
	}
	if v := mtOnlineCapacityEnvelopeV3(8000, 10000, 5000, 100); v != 5000 {
		t.Fatalf("envelope at batch 80: %d", v)
	}
	if v := mtOnlineCapacityEnvelopeV3(1000, 10000, 5000, 100); v != 1500 {
		t.Fatalf("early slack: %d", v)
	}
	if v := mtOnlineCapacityEnvelopeV3(9999, 10000, 5000, 100); v != 5000 {
		t.Fatalf("late full: %d", v)
	}
}
func TestMTOnlineCapNoEarlyExhaustionV3(t *testing.T) {
	// Pathological DCL-like preference (every step favors s1). A finite
	// forecast corridor must prevent s1 from filling at RouteBatch 52.
	loads := map[string]int{"s0": 0, "s1": 0}
	firstFull := 0
	for step := 1; step <= 10000; step++ {
		xs := []metaTrackIncrementalCandidateV650{
			{Shard: "s1", Admissible: loads["s1"] < 5000, Load: loads["s1"], ExactCross: 0, ReadyRank: 0, WorstRank: 100},
			{Shard: "s0", Admissible: loads["s0"] < 5000, Load: loads["s0"], ExactCross: 1, ReadyRank: 1, WorstRank: 200},
		}
		ranked := xs[:0]
		for _, c := range xs {
			if c.Admissible {
				ranked = append(ranked, c)
			}
		}
		if len(ranked) == 0 {
			t.Fatalf("all shard capacities exhausted at %d", step)
		}
		selected, _ := mtOnlineCapacityChooseV3(ranked, ranked[0], step, 10000, 5000, 100)
		if !selected.Admissible {
			t.Fatalf("ineligible chosen at %d", step)
		}
		loads[selected.Shard]++
		if firstFull == 0 && (loads["s0"] == 5000 || loads["s1"] == 5000) {
			firstFull = step
		}
		if step == 1000 && loads["s1"] < 900 {
			t.Fatalf("strict alternation restored: %v", loads)
		}
		if step == 5200 && loads["s1"] > 3600 {
			t.Fatalf("preemptive envelope not enforced: %v", loads)
		}
	}
	if firstFull < 7900 {
		t.Fatalf("capacity exhausted too early: %d", firstFull)
	}
	if loads["s0"] != 5000 || loads["s1"] != 5000 {
		t.Fatalf("hard capacity invariant: %v", loads)
	}
}
func TestMTOnlineCapSameBatchExactV3(t *testing.T) {
	xs := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Admissible: true, Load: 100, ExactCross: 0, ReadyRank: 1, WorstRank: 400},
		{Shard: "s1", Admissible: true, Load: 900, ExactCross: 1, ReadyRank: 2, WorstRank: 100},
	}
	deps := []metaTrackResolvedExactDependencyV669{{Producer: metaTrackIncrementalProducerV650{Shard: "s0", Ordinal: 1460}}}
	got, changed := mtOnlineSameBatchExactChooseV3(xs, xs[1], deps, 1401, 1461, 1500, 10000, 5000, 100)
	if !changed || got.Shard != "s0" {
		t.Fatalf("same-batch exact locality not retained: %v %v", got.Shard, changed)
	}
	deps = append(deps, metaTrackResolvedExactDependencyV669{Producer: metaTrackIncrementalProducerV650{Shard: "s1", Ordinal: 1459}})
	got, changed = mtOnlineSameBatchExactChooseV3(xs, xs[1], deps, 1401, 1461, 1500, 10000, 5000, 100)
	if changed || got.Shard != "s1" {
		t.Fatal("ambiguous multi-shard predecessors must not force a shard")
	}
	deps = deps[:1]
	xs[0].Admissible = false
	got, changed = mtOnlineSameBatchExactChooseV3(xs, xs[1], deps, 1401, 1461, 1500, 10000, 5000, 100)
	if changed || got.Shard != "s1" {
		t.Fatal("hard-cap inadmissible shard selected")
	}
	if r := mtOnlineCapReasonV3("online", true, true); r != "online:capacity_envelope_v3:same_batch_exact_v3" {
		t.Fatalf("reason %q", r)
	}
}
