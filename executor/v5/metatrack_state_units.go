package v5

import (
	"fmt"

	"metaverse-chainlab/executor/realism/tx"
)

// logicalStateHome separates the manuscript's persistent logical state owner
// (phi: K -> S) from the physical execution shard that serves that state unit.
// Multiple logical state units may be co-located on one physical shard.
type logicalStateHome struct {
	StateUnitID  string
	ServingShard string
	UnitIndex    int
	UnitCount    int
}

// logicalStateUnitRouting is intentionally optional. Baseline routing plugins
// keep their existing state-home semantics; MetaTrack alone implements this
// interface to expose the paper's S/P role separation.
type logicalStateUnitRouting interface {
	LogicalStateHome(key string, sharding ShardingPlugin, shards []string) logicalStateHome
	StateStorageUnitCount(shards []string) int
}

func (p *metaTrackRouting) StateStorageUnitCount(shards []string) int {
	if p == nil {
		return len(shards)
	}
	configured := intValue(p.config["state_storage_unit_count"])
	if configured > 0 {
		return configured
	}
	return len(shards)
}

func (p *metaTrackRouting) LogicalStateHome(key string, sharding ShardingPlugin, shards []string) logicalStateHome {
	if len(shards) == 0 || key == "" {
		return logicalStateHome{}
	}
	count := p.StateStorageUnitCount(shards)
	if count <= 0 {
		count = len(shards)
	}

	// Compatibility mode is the default: with no explicit logical-unit count,
	// preserve the exact historical physical home selected by the configured
	// sharding plugin. The unit identity is still explicit for paper evidence.
	if intValue(p.config["state_storage_unit_count"]) <= 0 {
		serving := shardFor(sharding, []string{key}, shards)
		unitIndex := 0
		for index, shard := range shards {
			if shard == serving {
				unitIndex = index
				break
			}
		}
		return logicalStateHome{
			StateUnitID:  fmt.Sprintf("u%06d", unitIndex),
			ServingShard: serving,
			UnitIndex:    unitIndex,
			UnitCount:    count,
		}
	}

	// Explicit role separation: hash the state key into a logical storage unit,
	// then deterministically co-locate that unit on an execution/PBFT shard.
	// This models S independently from P without pretending that S physical
	// machines exist in the single-host prototype.
	unitIndex := stableKey([]string{key}) % count
	return logicalStateHome{
		StateUnitID:  fmt.Sprintf("u%06d", unitIndex),
		ServingShard: shards[unitIndex%len(shards)],
		UnitIndex:    unitIndex,
		UnitCount:    count,
	}
}

func logicalStateHomeForRouting(routing RoutingPlugin, sharding ShardingPlugin, key string, shards []string) logicalStateHome {
	if provider, ok := routing.(logicalStateUnitRouting); ok {
		return provider.LogicalStateHome(key, sharding, shards)
	}
	return logicalStateHome{ServingShard: shardFor(sharding, []string{key}, shards), UnitCount: len(shards)}
}

func logicalStateStorageUnitCountForRouting(routing RoutingPlugin, shards []string) int {
	if provider, ok := routing.(logicalStateUnitRouting); ok {
		return provider.StateStorageUnitCount(shards)
	}
	return len(shards)
}

func (r *NodeRuntime) logicalStateHomeForKey(key string, shards []string) logicalStateHome {
	if r == nil {
		return logicalStateHome{}
	}
	return logicalStateHomeForRouting(r.plugins.Routing, r.plugins.Sharding, key, shards)
}

func (r *NodeRuntime) stateHomeShardForKey(key string, shards []string) string {
	return r.logicalStateHomeForKey(key, shards).ServingShard
}

func metaTrackPredictedRemoteAccessCounts(routing *metaTrackRouting, sharding ShardingPlugin, shardIDs []string, accesses []tx.AccessItem, executionShard string) (int, int) {
	reads, writes := 0, 0
	for _, access := range accesses {
		if access.Key == "" {
			continue
		}
		home := routing.LogicalStateHome(access.Key, sharding, shardIDs)
		if home.ServingShard == "" || home.ServingShard == executionShard {
			continue
		}
		if isReadMode(access.Mode) {
			reads++
		}
		if isWriteMode(access.Mode) {
			writes++
		}
	}
	return reads, writes
}

// routingBatchSizeProvider lets a batch-routing method decouple its planning
// micro-batch from the proposer block-size ceiling. A configured value of zero
// intentionally inherits the existing block producer size for compatibility.
type routingBatchSizeProvider interface {
	RoutingBatchSize(defaultSize int) int
}

func (p *metaTrackRouting) RoutingBatchSize(defaultSize int) int {
	if p != nil {
		if configured := intValue(p.config["micro_batch_size"]); configured > 0 {
			return configured
		}
	}
	if defaultSize > 0 {
		return defaultSize
	}
	return 1
}
