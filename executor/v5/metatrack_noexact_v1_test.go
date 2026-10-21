package v5

import "testing"

// MBE_MT_NOEXACT_ABL_V1: An exact-only preference must not affect a NoExact
// decision. The Full five-objective ranking must remain independent.
func TestMetaTrackNoExactV1IgnoresExactRanks(t *testing.T) {
	candidates := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", ReadyRank: 0, ExactCross: 0, RemoteCost: 0, CoaccessLocality: 0, Load: 10, Admissible: true},
		{Shard: "s1", ReadyRank: 100, ExactCross: 100, RemoteCost: 10, CoaccessLocality: 10, Load: 0, Admissible: true},
	}
	full := metaTrackIncrementalRankCandidatesV651(candidates)
	no := metaTrackRankWithoutExactV1(candidates)
	// Full chooses s0: two exact preferences plus cheaper remote access.
	if !(full[0].RankSum < full[1].RankSum) {
		t.Fatalf("five-criterion Full must prefer s0 in fixture: %#v", full)
	}
	// NoExact chooses s1: coaccess and load beat remote access (2:1).
	if !(no[1].RankSum < no[0].RankSum) {
		t.Fatalf("three-criterion NoExact must prefer s1: %#v", no)
	}
	changed := append([]metaTrackIncrementalCandidateV650(nil), candidates...)
	changed[0].ReadyRank, changed[0].ExactCross = 999, 999
	changed[1].ReadyRank, changed[1].ExactCross = 0, 0
	after := metaTrackRankWithoutExactV1(changed)
	for i := range no {
		if no[i].WorstRank != after[i].WorstRank || no[i].RankSum != after[i].RankSum {
			t.Fatalf("NoExact ranking depended on exact signals for shard %s", no[i].Shard)
		}
	}
}

func TestMetaTrackNoExactV1UsesExactlyThreeRanks(t *testing.T) {
	c := []metaTrackIncrementalCandidateV650{
		{Shard: "s0", ReadyRank: 5, ExactCross: 5, RemoteCost: 0, CoaccessLocality: 2, Load: 2},
		{Shard: "s1", ReadyRank: 0, ExactCross: 0, RemoteCost: 1, CoaccessLocality: 1, Load: 1},
		{Shard: "s2", ReadyRank: 0, ExactCross: 0, RemoteCost: 2, CoaccessLocality: 0, Load: 0},
	}
	got := metaTrackRankWithoutExactV1(c)
	for i := range got {
		r := metaTrackIncrementalCriterionRankV651(got, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.RemoteCost < b.RemoteCost })
		co := metaTrackIncrementalCriterionRankV651(got, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.CoaccessLocality > b.CoaccessLocality })
		l := metaTrackIncrementalCriterionRankV651(got, i, func(a, b metaTrackIncrementalCandidateV650) bool { return a.Load < b.Load })
		if got[i].WorstRank != maxInt(r, maxInt(co, l)) || got[i].RankSum != r+co+l {
			t.Fatalf("extra criteria leaked into NoExact ranking: %+v", got[i])
		}
	}
}
