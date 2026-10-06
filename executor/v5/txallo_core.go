package v5

// MBE_TXALLO_PAPER_V20: ICDE 2023 Algorithm 1/2 paper-fidelity core.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
)

type txalloHistoryTx struct {
	Accounts []string
}

func txalloUniqueSortedStrings(items []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

type txalloEdgeKey struct{ A, B string }
type txalloGraph struct {
	Nodes map[string]bool
	Edges map[txalloEdgeKey]float64
}

func newTxAlloGraph() *txalloGraph {
	return &txalloGraph{Nodes: map[string]bool{}, Edges: map[txalloEdgeKey]float64{}}
}
func txalloPair(a, b string) txalloEdgeKey {
	if a <= b {
		return txalloEdgeKey{a, b}
	}
	return txalloEdgeKey{b, a}
}
func (g *txalloGraph) addTransaction(accounts []string) {
	uniq := txalloUniqueSortedStrings(accounts)
	if len(uniq) == 0 {
		return
	}
	for _, a := range uniq {
		g.Nodes[a] = true
	}
	if len(uniq) == 1 {
		g.Edges[txalloPair(uniq[0], uniq[0])] += 1
		return
	}
	pairs := len(uniq) * (len(uniq) - 1) / 2
	if pairs <= 0 {
		return
	}
	w := 1.0 / float64(pairs)
	for i := 0; i < len(uniq); i++ {
		for j := i + 1; j < len(uniq); j++ {
			g.Edges[txalloPair(uniq[i], uniq[j])] += w
		}
	}
}
func (g *txalloGraph) addAll(txs []txalloHistoryTx) {
	for _, t := range txs {
		g.addTransaction(t.Accounts)
	}
}
func (g *txalloGraph) totalWeight() float64 {
	t := 0.0
	for _, w := range g.Edges {
		t += w
	}
	return t
}
func (g *txalloGraph) neighbors(v string) map[string]float64 {
	out := map[string]float64{}
	for e, w := range g.Edges {
		if e.A == v && e.B != v {
			out[e.B] += w
		} else if e.B == v && e.A != v {
			out[e.A] += w
		}
	}
	return out
}
func (g *txalloGraph) selfWeight(v string) float64 {
	return g.Edges[txalloPair(v, v)]
}

type txalloCommunityStat struct {
	Workload float64
	Uncapped float64
}

type txalloObjective struct {
	Throughput, CrossShardRatio, WorkloadStdDev float64
	Workloads                                   map[string]float64
}

func txalloShardSet(shards []string) map[string]bool {
	out := make(map[string]bool, len(shards))
	for _, shard := range shards {
		if shard != "" {
			out[shard] = true
		}
	}
	return out
}

// txalloBuildCommunityStats implements the paper's per-community workload
// sigma_i and sufficient-capacity throughput \hat{Lambda}_i. For a partial
// mapping (used only while Algorithm 1/2 places new nodes), an unassigned node
// is treated as outside every surviving shard. It is never silently assigned to
// shard 0.
func txalloBuildCommunityStats(g *txalloGraph, mapping map[string]string, shards []string, eta float64) map[string]txalloCommunityStat {
	stats := map[string]txalloCommunityStat{}
	valid := txalloShardSet(shards)
	for _, shard := range shards {
		stats[shard] = txalloCommunityStat{}
	}
	for e, w := range g.Edges {
		sa, sb := mapping[e.A], mapping[e.B]
		if !valid[sa] {
			sa = ""
		}
		if !valid[sb] {
			sb = ""
		}
		if e.A == e.B {
			if sa != "" {
				row := stats[sa]
				row.Workload += w
				row.Uncapped += w
				stats[sa] = row
			}
			continue
		}
		switch {
		case sa != "" && sa == sb:
			row := stats[sa]
			row.Workload += w
			row.Uncapped += w
			stats[sa] = row
		case sa != "" && sb != "" && sa != sb:
			ra := stats[sa]
			ra.Workload += eta * w
			ra.Uncapped += w / 2
			stats[sa] = ra
			rb := stats[sb]
			rb.Workload += eta * w
			rb.Uncapped += w / 2
			stats[sb] = rb
		case sa != "":
			ra := stats[sa]
			ra.Workload += eta * w
			ra.Uncapped += w / 2
			stats[sa] = ra
		case sb != "":
			rb := stats[sb]
			rb.Workload += eta * w
			rb.Uncapped += w / 2
			stats[sb] = rb
		}
	}
	return stats
}

func txalloThroughputFromStat(stat txalloCommunityStat, lambda float64) float64 {
	if stat.Workload <= 0 {
		return 0
	}
	if stat.Workload <= lambda {
		return stat.Uncapped
	}
	return lambda / stat.Workload * stat.Uncapped
}

// txalloEvaluate is intentionally complete-mapping only. Algorithm 1/2 partial
// placement is evaluated by Eq. (6)/(8) helpers below. This fail-closed contract
// prevents the old behavior that silently treated every unassigned account as
// belonging to shard 0.
func txalloEvaluate(g *txalloGraph, mapping map[string]string, shards []string, eta, lambda float64) txalloObjective {
	valid := txalloShardSet(shards)
	for node := range g.Nodes {
		shard := mapping[node]
		if !valid[shard] {
			panic(fmt.Sprintf("TxAllo incomplete mapping: account %q has invalid shard %q", node, shard))
		}
	}
	stats := txalloBuildCommunityStats(g, mapping, shards, eta)
	workloads := map[string]float64{}
	throughput := 0.0
	mean := 0.0
	for _, shard := range shards {
		row := stats[shard]
		workloads[shard] = row.Workload
		throughput += txalloThroughputFromStat(row, lambda)
		mean += row.Workload
	}
	if len(shards) > 0 {
		mean /= float64(len(shards))
	}
	variance := 0.0
	for _, shard := range shards {
		d := workloads[shard] - mean
		variance += d * d
	}
	if len(shards) > 0 {
		variance /= float64(len(shards))
	}
	total, cross := 0.0, 0.0
	for e, w := range g.Edges {
		total += w
		if e.A != e.B && mapping[e.A] != mapping[e.B] {
			cross += w
		}
	}
	ratio := 0.0
	if total > 0 {
		ratio = cross / total
	}
	return txalloObjective{Throughput: throughput, CrossShardRatio: ratio, WorkloadStdDev: math.Sqrt(variance), Workloads: workloads}
}

func txalloNodeOrder(nodes map[string]bool) []string {
	out := make([]string, 0, len(nodes))
	for n := range nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool {
		hi := sha256.Sum256([]byte(out[i]))
		hj := sha256.Sum256([]byte(out[j]))
		a := hex.EncodeToString(hi[:])
		b := hex.EncodeToString(hj[:])
		if a != b {
			return a < b
		}
		return out[i] < out[j]
	})
	return out
}

func deterministicLouvainOneLevel(g *txalloGraph) map[string]int {
	nodes := txalloNodeOrder(g.Nodes)
	comm := map[string]int{}
	for i, n := range nodes {
		comm[n] = i
	}
	degree := map[string]float64{}
	m2 := 0.0
	for e, w := range g.Edges {
		if e.A == e.B {
			degree[e.A] += 2 * w
			m2 += 2 * w
		} else {
			degree[e.A] += w
			degree[e.B] += w
			m2 += 2 * w
		}
	}
	if m2 == 0 {
		return comm
	}
	for pass := 0; pass < 100; pass++ {
		moved := false
		tot := map[int]float64{}
		for n, c := range comm {
			tot[c] += degree[n]
		}
		for _, n := range nodes {
			old := comm[n]
			ki := degree[n]
			tot[old] -= ki
			byComm := map[int]float64{}
			for nb, w := range g.neighbors(n) {
				byComm[comm[nb]] += w
			}
			candidates := make([]int, 0, len(byComm))
			for c := range byComm {
				candidates = append(candidates, c)
			}
			sort.Ints(candidates)
			best := old
			bestGain := 0.0
			for _, c := range candidates {
				gain := byComm[c] - tot[c]*ki/m2
				if gain > bestGain+1e-12 || (math.Abs(gain-bestGain) <= 1e-12 && gain > 0 && c < best) {
					best = c
					bestGain = gain
				}
			}
			comm[n] = best
			tot[best] += ki
			if best != old {
				moved = true
			}
		}
		if !moved {
			break
		}
	}
	members := map[int][]string{}
	for n, c := range comm {
		members[c] = append(members[c], n)
	}
	type row struct {
		id  int
		min string
	}
	rows := []row{}
	for c, ms := range members {
		sort.Strings(ms)
		rows = append(rows, row{c, ms[0]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].min < rows[j].min })
	ren := map[int]int{}
	for i, r := range rows {
		ren[r.id] = i
	}
	for n, c := range comm {
		comm[n] = ren[c]
	}
	return comm
}

func txalloProjectCommunities(comm map[string]int, members map[string][]string) map[string]int {
	groups := map[int][]string{}
	for node, community := range comm {
		groups[community] = append(groups[community], members[node]...)
	}
	type row struct {
		id      int
		members []string
		min     string
	}
	rows := make([]row, 0, len(groups))
	for id, original := range groups {
		sort.Strings(original)
		rows = append(rows, row{id: id, members: original, min: original[0]})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].min < rows[j].min })
	out := map[string]int{}
	for normalized, item := range rows {
		for _, original := range item.members {
			out[original] = normalized
		}
	}
	return out
}

// deterministicLouvainInitialization is a complete deterministic multi-level
// Louvain initialization: local moving is followed by community aggregation and
// repeated on the coarsened graph until no further coarsening occurs. The
// original-account projection is retained across levels.
func deterministicLouvainInitialization(g *txalloGraph) (map[string]int, int) {
	if len(g.Nodes) == 0 {
		return map[string]int{}, 0
	}
	current := g
	members := map[string][]string{}
	for node := range g.Nodes {
		members[node] = []string{node}
	}
	for level := 1; level <= 64; level++ {
		comm := deterministicLouvainOneLevel(current)
		communityNodes := map[int][]string{}
		for node, c := range comm {
			communityNodes[c] = append(communityNodes[c], node)
		}
		if len(communityNodes) == len(current.Nodes) {
			return txalloProjectCommunities(comm, members), level
		}
		communityID := map[int]string{}
		nextMembers := map[string][]string{}
		for c, nodes := range communityNodes {
			original := []string{}
			for _, node := range nodes {
				original = append(original, members[node]...)
			}
			sort.Strings(original)
			id := fmt.Sprintf("louvain:%02d:%s", level, original[0])
			communityID[c] = id
			nextMembers[id] = original
		}
		next := newTxAlloGraph()
		for _, id := range communityID {
			next.Nodes[id] = true
		}
		for edge, weight := range current.Edges {
			a := communityID[comm[edge.A]]
			b := communityID[comm[edge.B]]
			next.Edges[txalloPair(a, b)] += weight
		}
		current = next
		members = nextMembers
	}
	comm := deterministicLouvainOneLevel(current)
	return txalloProjectCommunities(comm, members), 64
}

type txalloRunMetrics struct {
	GRunCount, A_RunCount, ChangedAccounts, Iterations int
	LouvainLevels                                      int
	Objective                                          txalloObjective
}

type txalloAllocator struct {
	Graph                *txalloGraph
	Mapping              map[string]string
	Shards               []string
	Eta, Lambda, Epsilon float64
	Stats                map[string]txalloCommunityStat
	Metrics              txalloRunMetrics
}

func newTxAlloAllocator(shards []string, eta, lambda, epsilon float64) *txalloAllocator {
	return &txalloAllocator{Graph: newTxAlloGraph(), Mapping: map[string]string{}, Shards: append([]string(nil), shards...), Eta: eta, Lambda: lambda, Epsilon: epsilon, Stats: map[string]txalloCommunityStat{}}
}

func (a *txalloAllocator) params() {
	tw := a.Graph.totalWeight()
	if a.Eta <= 1 {
		a.Eta = 2
	}
	if a.Lambda <= 0 && len(a.Shards) > 0 {
		a.Lambda = tw / float64(len(a.Shards))
		if a.Lambda <= 0 {
			a.Lambda = 1
		}
	}
	if a.Epsilon <= 0 {
		a.Epsilon = 1e-5 * tw
		if a.Epsilon <= 0 {
			a.Epsilon = 1e-9
		}
	}
}

func (a *txalloAllocator) rebuildStats() {
	a.Stats = txalloBuildCommunityStats(a.Graph, a.Mapping, a.Shards, a.Eta)
}

func (a *txalloAllocator) candidateShards(v string, includeAllWhenEmpty bool) []string {
	seen := map[string]bool{}
	for nb := range a.Graph.neighbors(v) {
		if s := a.Mapping[nb]; s != "" && s != a.Mapping[v] {
			seen[s] = true
		}
	}
	out := []string{}
	for _, s := range a.Shards {
		if seen[s] {
			out = append(out, s)
		}
	}
	if len(out) == 0 && includeAllWhenEmpty {
		out = append(out, a.Shards...)
	}
	return out
}

func copyMapping(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (a *txalloAllocator) incident(v, shard string) (self, totalOther, toShard float64) {
	self = a.Graph.selfWeight(v)
	for nb, weight := range a.Graph.neighbors(v) {
		totalOther += weight
		if a.Mapping[nb] == shard {
			toShard += weight
		}
	}
	return self, totalOther, toShard
}

// Eq. (6): throughput gain when an unassigned/small-community node joins q.
func (a *txalloAllocator) joinResult(v, q string) (txalloCommunityStat, float64) {
	before := a.Stats[q]
	self, totalOther, toQ := a.incident(v, q)
	after := before
	after.Workload += self + a.Eta*(totalOther-toQ) + (1-a.Eta)*toQ
	after.Uncapped += self + totalOther/2
	gain := txalloThroughputFromStat(after, a.Lambda) - txalloThroughputFromStat(before, a.Lambda)
	return after, gain
}

func (a *txalloAllocator) leaveResult(v, p string) (txalloCommunityStat, float64) {
	before := a.Stats[p]
	self, totalOther, toP := a.incident(v, p)
	outsideP := totalOther - toP
	after := before
	after.Workload -= self + a.Eta*outsideP - (a.Eta-1)*toP
	after.Uncapped -= self + totalOther/2
	if after.Workload < 0 && after.Workload > -1e-9 {
		after.Workload = 0
	}
	if after.Uncapped < 0 && after.Uncapped > -1e-9 {
		after.Uncapped = 0
	}
	gain := txalloThroughputFromStat(after, a.Lambda) - txalloThroughputFromStat(before, a.Lambda)
	return after, gain
}

func (a *txalloAllocator) bestJoin(v string, cands []string) (string, float64) {
	best := ""
	bestGain := math.Inf(-1)
	for _, shard := range cands {
		_, gain := a.joinResult(v, shard)
		if gain > bestGain+1e-12 || (math.Abs(gain-bestGain) <= 1e-12 && (best == "" || shard < best)) {
			best, bestGain = shard, gain
		}
	}
	return best, bestGain
}

// Eq. (8): only p and q change when v moves p -> q.
func (a *txalloAllocator) bestMove(v string, cands []string) (string, float64) {
	old := a.Mapping[v]
	best := ""
	bestGain := 0.0
	for _, shard := range cands {
		if shard == old || shard == "" {
			continue
		}
		_, leaveGain := a.leaveResult(v, old)
		_, joinGain := a.joinResult(v, shard)
		gain := leaveGain + joinGain
		if gain > bestGain+1e-12 || (math.Abs(gain-bestGain) <= 1e-12 && gain > 0 && (best == "" || shard < best)) {
			best, bestGain = shard, gain
		}
	}
	return best, bestGain
}

func (a *txalloAllocator) applyJoin(v, shard string) {
	after, _ := a.joinResult(v, shard)
	a.Mapping[v] = shard
	a.Stats[shard] = after
}

func (a *txalloAllocator) applyMove(v, target string) {
	source := a.Mapping[v]
	leaveAfter, _ := a.leaveResult(v, source)
	joinAfter, _ := a.joinResult(v, target)
	a.Mapping[v] = target
	a.Stats[source] = leaveAfter
	a.Stats[target] = joinAfter
}

func (a *txalloAllocator) optimize(nodes []string) {
	for pass := 0; pass < 100; pass++ {
		delta := 0.0
		for _, v := range nodes {
			if a.Mapping[v] == "" {
				continue
			}
			cands := a.candidateShards(v, false)
			best, gain := a.bestMove(v, cands)
			if best != "" && gain > 0 {
				a.applyMove(v, best)
				delta += gain
				a.Metrics.ChangedAccounts++
			}
		}
		a.Metrics.Iterations++
		if delta < a.Epsilon {
			break
		}
	}
}

func (a *txalloAllocator) RunG(history []txalloHistoryTx) {
	a.Graph = newTxAlloGraph()
	a.Graph.addAll(history)
	a.params()
	community, levels := deterministicLouvainInitialization(a.Graph)
	a.Metrics.LouvainLevels = levels
	groups := map[int][]string{}
	for n, c := range community {
		groups[c] = append(groups[c], n)
	}
	type gr struct {
		id    int
		nodes []string
		work  float64
		min   string
	}
	rows := []gr{}
	for c, ns := range groups {
		sort.Strings(ns)
		temp := map[string]string{}
		for n := range a.Graph.Nodes {
			temp[n] = "other"
		}
		for _, n := range ns {
			temp[n] = "in"
		}
		stats := txalloBuildCommunityStats(a.Graph, temp, []string{"in", "other"}, a.Eta)
		rows = append(rows, gr{c, ns, stats["in"].Workload, ns[0]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if math.Abs(rows[i].work-rows[j].work) > 1e-12 {
			return rows[i].work > rows[j].work
		}
		return rows[i].min < rows[j].min
	})
	a.Mapping = map[string]string{}
	limit := len(a.Shards)
	if len(rows) < limit {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		for _, n := range rows[i].nodes {
			a.Mapping[n] = a.Shards[i]
		}
	}
	a.rebuildStats()

	small := map[string]bool{}
	for i := limit; i < len(rows); i++ {
		for _, n := range rows[i].nodes {
			small[n] = true
		}
	}
	for _, v := range txalloNodeOrder(small) {
		cands := a.candidateShards(v, true)
		best, _ := a.bestJoin(v, cands)
		if best == "" && len(a.Shards) > 0 {
			best = a.Shards[0]
		}
		if best != "" {
			a.applyJoin(v, best)
		}
	}
	for _, v := range txalloNodeOrder(a.Graph.Nodes) {
		if a.Mapping[v] != "" {
			continue
		}
		best, _ := a.bestJoin(v, a.Shards)
		if best == "" && len(a.Shards) > 0 {
			best = a.Shards[0]
		}
		if best != "" {
			a.applyJoin(v, best)
		}
	}
	a.optimize(txalloNodeOrder(a.Graph.Nodes))
	a.Metrics.GRunCount++
	a.Metrics.Objective = txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda)
}

func (a *txalloAllocator) RunA(newTxs []txalloHistoryTx) {
	known := map[string]bool{}
	for n := range a.Graph.Nodes {
		known[n] = true
	}
	a.Graph.addAll(newTxs)
	a.params()
	// New edges change sigma/hat even before any node moves.
	a.rebuildStats()
	affectedMap := map[string]bool{}
	for _, item := range newTxs {
		for _, v := range txalloUniqueSortedStrings(item.Accounts) {
			affectedMap[v] = true
		}
	}
	affected := txalloNodeOrder(affectedMap)
	for _, v := range affected {
		if known[v] && a.Mapping[v] != "" {
			continue
		}
		cands := a.candidateShards(v, true)
		best, _ := a.bestJoin(v, cands)
		if best == "" && len(a.Shards) > 0 {
			best = a.Shards[0]
		}
		if best != "" {
			a.applyJoin(v, best)
		}
	}
	a.optimize(affected)
	a.Metrics.A_RunCount++
	a.Metrics.Objective = txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda)
}
