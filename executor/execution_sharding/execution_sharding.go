// Package execution_sharding assigns transactions to single-chain execution shards.
package execution_sharding

import "hash/fnv"

// Transaction is the routing information needed for psi_t assignment.
type Transaction struct {
	ID         string
	AccessKeys []string
}

// Context exposes the batch-level routing map M_t to an execution sharder.
type Context struct {
	StateToExecution map[string]int
}

// Assigner maps a transaction to its actual execution shard. It defines psi_t:
// tx -> execution_shard.
type Assigner interface {
	Assign(tx Transaction, ctx Context) int
}

// HashExecutionSharding is the V0/V1.2 default execution assignment module.
type HashExecutionSharding struct {
	shardCount int
}

// NewHashExecutionSharding creates a hash assigner with a safe default.
func NewHashExecutionSharding(shardCount int) HashExecutionSharding {
	if shardCount <= 0 {
		shardCount = 4
	}
	return HashExecutionSharding{shardCount: shardCount}
}

// Assign returns psi_t. It prefers the batch map M_t for the first accessed key;
// transactions without state access are deterministically assigned by ID.
func (s HashExecutionSharding) Assign(tx Transaction, ctx Context) int {
	if len(tx.AccessKeys) > 0 {
		if shard, ok := ctx.StateToExecution[tx.AccessKeys[0]]; ok {
			return shard
		}
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(tx.ID))
	return int(hash.Sum32() % uint32(s.shardCount))
}
