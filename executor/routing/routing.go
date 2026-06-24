// Package routing builds batch-level execution-side routes.
package routing

import "hash/fnv"

// StateLocator supplies phi state locations to routing modules.
type StateLocator interface {
	LocateState(key string) int
}

// Transaction contains the state keys needed to construct a batch route.
type Transaction struct {
	ID         string
	AccessKeys []string
}

// RoutingResult contains M_t, a batch-level state-key to execution-shard map.
// It is execution-side routing only and does not alter persistent phi placement.
type RoutingResult struct {
	StateToExecution map[string]int
}

// Builder generates a default execution-side route for a streaming batch.
type Builder interface {
	BuildRouting(batch []Transaction, states StateLocator) RoutingResult
}

// HashRouting is the V0/V1.2 default M_t builder. It intentionally contains no
// co-access or MetaTrack policy.
type HashRouting struct {
	shardCount int
}

// NewHashRouting creates the default hash route builder.
func NewHashRouting(shardCount int) HashRouting {
	if shardCount <= 0 {
		shardCount = 4
	}
	return HashRouting{shardCount: shardCount}
}

// BuildRouting returns M_t(key). Hash routing is deterministic and preserves
// the old default behaviour by using the same FNV-1a key hash as phi.
func (r HashRouting) BuildRouting(batch []Transaction, _ StateLocator) RoutingResult {
	result := RoutingResult{StateToExecution: make(map[string]int)}
	for _, tx := range batch {
		for _, key := range tx.AccessKeys {
			if _, exists := result.StateToExecution[key]; exists {
				continue
			}
			hash := fnv.New32a()
			_, _ = hash.Write([]byte(key))
			result.StateToExecution[key] = int(hash.Sum32() % uint32(r.shardCount))
		}
	}
	return result
}
