package v5

import (
	"sort"
)

// porygonECDescriptorForHeight turns the existing deterministic committee
// adapter into a paper-shaped EC lifecycle.  The selection primitive remains
// the MBE adapter (not exact VRF); the lifecycle itself is consensus-bound.
func porygonECDescriptorForHeight(nodes []NodePlan, height uint64, orderingDomain string, shardCount, committeeCount int) PorygonECDescriptor {
	if shardCount < 1 {
		shardCount = 1
	}
	if committeeCount < 1 {
		committeeCount = 3
	}
	selectionHeight := porygonPaperECSelectionHeight(height)
	members := porygonSortedValidators(nodes, selectionHeight, orderingDomain)
	shards := map[string][]string{}
	for shard := 0; shard < shardCount; shard++ {
		sid := porygonExecutionShardID(shard)
		shards[sid] = porygonExecutionShardMembersAtHeight(nodes, selectionHeight, orderingDomain, sid, shardCount)
	}
	for sid := range shards {
		sort.Strings(shards[sid])
	}
	descriptor := PorygonECDescriptor{
		ECID: porygonECName(selectionHeight, committeeCount), BirthRound: selectionHeight,
		WitnessRound: selectionHeight, OrderingRound: height, ExecutionRound: height + 1, ExpireRound: height + 1,
		Members: append([]string(nil), members...), ExecutionShards: shards,
	}
	descriptor.CommitteeDigest = stableJSONDigest(struct {
		ECID    string              `json:"ec_id"`
		Members []string            `json:"members"`
		Shards  map[string][]string `json:"shards"`
	}{descriptor.ECID, descriptor.Members, descriptor.ExecutionShards})
	return descriptor
}
