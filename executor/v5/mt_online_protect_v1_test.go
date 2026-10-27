package v5

import (
	"testing"
	"time"
)

func mtProtectCandidatesV1() []metaTrackIncrementalCandidateV650 {
	return []metaTrackIncrementalCandidateV650{
		{Shard: "s0", Admissible: true, ReadyRank: 2, ExactCross: 1, RemoteCost: 2, Load: 30},
		{Shard: "s1", Admissible: true, ReadyRank: 2, ExactCross: 1, RemoteCost: 2, Load: 20},
	}
}
func mtProtectFeedbackV1(now int64, base, alt mtOnlineShardFeedbackV1) *mtOnlineFeedbackV1 {
	base.Shard = "s0"
	base.SampledMS = now
	alt.Shard = "s1"
	alt.SampledMS = now
	return &mtOnlineFeedbackV1{SampledMS: now, Shards: map[string]mtOnlineShardFeedbackV1{"s0": base, "s1": alt}}
}
func TestMTOnlineProtectV1NoFeedbackEqualsFull(t *testing.T) {
	c := mtProtectCandidatesV1()
	got, why := mtOnlineProtectedChoiceV1(c, "s0", nil, nil, time.Now().UnixMilli())
	if got.Shard != "s0" || why != "protect_full_no_feedback" {
		t.Fatalf("bad Full fallback: %v %s", got.Shard, why)
	}
}
func TestMTOnlineProtectV1QueueDominanceAllowsSafeChange(t *testing.T) {
	now := time.Now().UnixMilli()
	c := mtProtectCandidatesV1()
	f := mtProtectFeedbackV1(now, mtOnlineShardFeedbackV1{SchedulerQueue: 9, RequestQueue: 3, PendingFetch: 2, PendingVersion: 1, MempoolDepth: 8}, mtOnlineShardFeedbackV1{SchedulerQueue: 2, RequestQueue: 0, PendingFetch: 0, PendingVersion: 0, MempoolDepth: 3})
	got, why := mtOnlineProtectedChoiceV1(c, "s0", nil, f, now)
	if got.Shard != "s1" || why != "protect_online_queue_dominance" {
		t.Fatalf("valid switch blocked: %+v %s", got, why)
	}
}
func TestMTOnlineProtectV1RejectsStructuralRegressions(t *testing.T) {
	now := time.Now().UnixMilli()
	f := mtProtectFeedbackV1(now, mtOnlineShardFeedbackV1{SchedulerQueue: 9, RequestQueue: 3, MempoolDepth: 5}, mtOnlineShardFeedbackV1{SchedulerQueue: 0, RequestQueue: 0, MempoolDepth: 0})
	scenarios := []struct {
		name   string
		change func([]metaTrackIncrementalCandidateV650)
		deps   []metaTrackResolvedExactDependencyV669
	}{
		{"cross", func(c []metaTrackIncrementalCandidateV650) { c[1].ExactCross++ }, nil},
		{"depth", func(c []metaTrackIncrementalCandidateV650) { c[1].ReadyRank++ }, nil},
		{"remote", func(c []metaTrackIncrementalCandidateV650) { c[1].RemoteCost++ }, nil},
		{"hard-cap", func(c []metaTrackIncrementalCandidateV650) { c[1].Admissible = false }, nil},
		{"local-exact", func(c []metaTrackIncrementalCandidateV650) {}, []metaTrackResolvedExactDependencyV669{{Producer: metaTrackIncrementalProducerV650{Shard: "s0"}}}},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			c := mtProtectCandidatesV1()
			scenario.change(c)
			got, _ := mtOnlineProtectedChoiceV1(c, "s0", scenario.deps, f, now)
			if got.Shard != "s0" {
				t.Fatalf("broke protected Full dependency: %+v", got)
			}
		})
	}
}
func TestMTOnlineProtectV1RejectsNonDominanceAndStale(t *testing.T) {
	now := time.Now().UnixMilli()
	c := mtProtectCandidatesV1()
	f := mtProtectFeedbackV1(now, mtOnlineShardFeedbackV1{SchedulerQueue: 8, RequestQueue: 0, MempoolDepth: 2}, mtOnlineShardFeedbackV1{SchedulerQueue: 0, RequestQueue: 1, MempoolDepth: 0})
	got, _ := mtOnlineProtectedChoiceV1(c, "s0", nil, f, now)
	if got.Shard != "s0" {
		t.Fatalf("tradeoff improperly weighted into a switch: %+v", got)
	}
	f.Shards["s1"] = mtOnlineShardFeedbackV1{Shard: "s1", SampledMS: now - 100000, SchedulerQueue: 0}
	got, _ = mtOnlineProtectedChoiceV1(c, "s0", nil, f, now)
	if got.Shard != "s0" {
		t.Fatalf("stale snapshot changed Full: %+v", got)
	}
}
func TestMTOnlineProtectV1NoRTTDependence(t *testing.T) {
	now := time.Now().UnixMilli()
	c := mtProtectCandidatesV1()
	f := mtProtectFeedbackV1(now, mtOnlineShardFeedbackV1{SchedulerQueue: 3, RemoteLatencyMS: 1}, mtOnlineShardFeedbackV1{SchedulerQueue: 0, RemoteLatencyMS: 200})
	a, _ := mtOnlineProtectedChoiceV1(c, "s0", nil, f, now)
	f.Shards["s0"] = mtOnlineShardFeedbackV1{Shard: "s0", SampledMS: now, SchedulerQueue: 3, RemoteLatencyMS: 200}
	f.Shards["s1"] = mtOnlineShardFeedbackV1{Shard: "s1", SampledMS: now, SchedulerQueue: 0, RemoteLatencyMS: 1}
	b, _ := mtOnlineProtectedChoiceV1(c, "s0", nil, f, now)
	if a.Shard != b.Shard {
		t.Fatalf("unverified RTT changed Pareto routing: %s vs %s", a.Shard, b.Shard)
	}
}
