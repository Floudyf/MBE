// Package state_sharding provides persistent state placement (phi) modules.
package state_sharding

import "hash/fnv"

// Locator maps a state key to its persistent state shard. It defines phi:
// state_key -> state_shard and never represents state migration.
type Locator interface {
	LocateState(key string) int
}

// HashStateSharding is the V0/V1.2 default persistent-state placement module.
type HashStateSharding struct {
	shardCount int
}

// NewHashStateSharding creates a hash locator with a safe default shard count.
func NewHashStateSharding(shardCount int) HashStateSharding {
	if shardCount <= 0 {
		shardCount = 4
	}
	return HashStateSharding{shardCount: shardCount}
}

// LocateState returns phi(key), the logical persistent location of key.
func (s HashStateSharding) LocateState(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(s.shardCount))
}
