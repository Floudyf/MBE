// Package routing builds deterministic batch-level execution-side routes.
package routing

import (
	"hash/fnv"
	"sort"
	"time"
)

type StateLocator interface{ LocateState(string) int }
type Transaction struct {
	ID         string
	AccessKeys []string
}
type Metrics struct {
	CrossShardTxCount, RemoteKeyCount, CoAccessGroupCount int
	CrossShardTxRatio, RoutingTimeMS                      float64
	RoutingPolicy                                         string
}

// RoutingResult contains M_t and psi_t. M_t changes only execution routing, never phi.
type RoutingResult struct {
	StateToExecution map[string]int
	TxToExecution    map[string]int
	Metrics          Metrics
}
type Builder interface {
	BuildRouting([]Transaction, StateLocator) RoutingResult
}

type HashRouting struct{ shardCount int }

func NewHashRouting(shards int) HashRouting {
	if shards <= 0 {
		shards = 4
	}
	return HashRouting{shards}
}
func shard(key string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}
func unique(keys []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, k := range keys {
		if k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
func assign(tx Transaction, m map[string]int, shards int) int {
	keys := unique(tx.AccessKeys)
	if len(keys) > 0 {
		return m[keys[0]]
	}
	return shard(tx.ID, shards)
}
func metrics(batch []Transaction, m map[string]int, psi map[string]int, policy string, groups int, started time.Time) Metrics {
	x := Metrics{RoutingPolicy: policy, CoAccessGroupCount: groups, RoutingTimeMS: float64(time.Since(started).Microseconds()) / 1000}
	for _, tx := range batch {
		remote := 0
		for _, k := range unique(tx.AccessKeys) {
			if m[k] != psi[tx.ID] {
				remote++
			}
		}
		x.RemoteKeyCount += remote
		if remote > 0 {
			x.CrossShardTxCount++
		}
	}
	if len(batch) > 0 {
		x.CrossShardTxRatio = float64(x.CrossShardTxCount) / float64(len(batch))
	}
	return x
}
func (r HashRouting) BuildRouting(batch []Transaction, _ StateLocator) RoutingResult {
	start := time.Now()
	m := map[string]int{}
	for _, tx := range batch {
		for _, k := range unique(tx.AccessKeys) {
			m[k] = shard(k, r.shardCount)
		}
	}
	psi := map[string]int{}
	for _, tx := range batch {
		psi[tx.ID] = assign(tx, m, r.shardCount)
	}
	return RoutingResult{m, psi, metrics(batch, m, psi, "hash", len(m), start)}
}

// CoAccessRouting greedily joins high-affinity keys then places each group on the least-loaded shard.
type CoAccessRouting struct {
	shardCount, minWeight, maxGroupSize int
	balanceWeight                       float64
}

func NewCoAccessRouting(shards, minWeight, maxGroupSize int, balanceWeight float64) CoAccessRouting {
	if shards <= 0 {
		shards = 4
	}
	if minWeight <= 0 {
		minWeight = 1
	}
	if maxGroupSize <= 0 {
		maxGroupSize = 64
	}
	if balanceWeight <= 0 {
		balanceWeight = 1
	}
	return CoAccessRouting{shards, minWeight, maxGroupSize, balanceWeight}
}
func (r CoAccessRouting) BuildRouting(batch []Transaction, _ StateLocator) RoutingResult {
	start := time.Now()
	keys := []string{}
	weights := map[string]int{}
	for _, tx := range batch {
		ks := unique(tx.AccessKeys)
		keys = append(keys, ks...)
		for i := range ks {
			for j := i + 1; j < len(ks); j++ {
				weights[ks[i]+"\x00"+ks[j]]++
			}
		}
	}
	keys = unique(keys)
	parent := map[string]string{}
	size := map[string]int{}
	for _, k := range keys {
		parent[k] = k
		size[k] = 1
	}
	var find func(string) string
	find = func(k string) string {
		if parent[k] != k {
			parent[k] = find(parent[k])
		}
		return parent[k]
	}
	pairs := make([]string, 0, len(weights))
	for p, w := range weights {
		if w >= r.minWeight {
			pairs = append(pairs, p)
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if weights[pairs[i]] == weights[pairs[j]] {
			return pairs[i] < pairs[j]
		}
		return weights[pairs[i]] > weights[pairs[j]]
	})
	for _, p := range pairs {
		v := splitPair(p)
		a, b := find(v[0]), find(v[1])
		if a != b && size[a]+size[b] <= r.maxGroupSize {
			if a > b {
				a, b = b, a
			}
			parent[b] = a
			size[a] += size[b]
		}
	}
	groups := map[string][]string{}
	for _, k := range keys {
		groups[find(k)] = append(groups[find(k)], k)
	}
	roots := make([]string, 0, len(groups))
	for root := range groups {
		sort.Strings(groups[root])
		roots = append(roots, root)
	}
	sort.Slice(roots, func(i, j int) bool {
		if len(groups[roots[i]]) == len(groups[roots[j]]) {
			return groups[roots[i]][0] < groups[roots[j]][0]
		}
		return len(groups[roots[i]]) > len(groups[roots[j]])
	})
	load := make([]int, r.shardCount)
	m := map[string]int{}
	for _, root := range roots {
		best := 0
		for s := 1; s < r.shardCount; s++ {
			if float64(load[s])*r.balanceWeight < float64(load[best])*r.balanceWeight {
				best = s
			}
		}
		for _, k := range groups[root] {
			m[k] = best
		}
		load[best] += len(groups[root])
	}
	psi := map[string]int{}
	for _, tx := range batch {
		psi[tx.ID] = assign(tx, m, r.shardCount)
	}
	return RoutingResult{m, psi, metrics(batch, m, psi, "co_access", len(groups), start)}
}
func splitPair(value string) [2]string {
	for i := range value {
		if value[i] == 0 {
			return [2]string{value[:i], value[i+1:]}
		}
	}
	return [2]string{}
}
