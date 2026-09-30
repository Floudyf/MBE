package v5

import (
	"crypto/sha256"
	"encoding/hex"
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

type txalloObjective struct {
	Throughput, CrossShardRatio, WorkloadStdDev float64
	Workloads                                   map[string]float64
}

func txalloEvaluate(g *txalloGraph, mapping map[string]string, shards []string, eta, lambda float64) txalloObjective {
	workloads := map[string]float64{}
	hat := map[string]float64{}
	for _, s := range shards {
		workloads[s] = 0
		hat[s] = 0
	}
	total := 0.0
	cross := 0.0
	for e, w := range g.Edges {
		total += w
		sa := mapping[e.A]
		sb := mapping[e.B]
		if sa == "" && len(shards) > 0 {
			sa = shards[0]
		}
		if sb == "" {
			sb = sa
		}
		if e.A == e.B || sa == sb {
			workloads[sa] += w
			hat[sa] += w
		} else {
			cross += w
			workloads[sa] += eta * w
			workloads[sb] += eta * w
			hat[sa] += w / 2
			hat[sb] += w / 2
		}
	}
	throughput := 0.0
	mean := 0.0
	for _, s := range shards {
		sigma := workloads[s]
		h := hat[s]
		if sigma <= lambda || sigma == 0 {
			throughput += h
		} else {
			throughput += (lambda / sigma) * h
		}
		mean += sigma
	}
	if len(shards) > 0 {
		mean /= float64(len(shards))
	}
	variance := 0.0
	for _, s := range shards {
		d := workloads[s] - mean
		variance += d * d
	}
	if len(shards) > 0 {
		variance /= float64(len(shards))
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

// deterministicLouvainInitialization implements the deterministic local-moving
// phase of Louvain used by TxAllo as its initialization. Account hash order is
// fixed, as required by the paper for deterministic outputs.
func deterministicLouvainInitialization(g *txalloGraph) map[string]int {
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
	// normalize community ids deterministically by smallest member.
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

type txalloRunMetrics struct {
	GRunCount, A_RunCount, ChangedAccounts, Iterations int
	Objective                                          txalloObjective
}

type txalloAllocator struct {
	Graph                *txalloGraph
	Mapping              map[string]string
	Shards               []string
	Eta, Lambda, Epsilon float64
	Metrics              txalloRunMetrics
}

func newTxAlloAllocator(shards []string, eta, lambda, epsilon float64) *txalloAllocator {
	return &txalloAllocator{Graph: newTxAlloGraph(), Mapping: map[string]string{}, Shards: append([]string(nil), shards...), Eta: eta, Lambda: lambda, Epsilon: epsilon}
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
func (a *txalloAllocator) bestMove(v string, cands []string) (string, float64) {
	base := txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda).Throughput
	best := ""
	gain := 0.0
	old := a.Mapping[v]
	for _, s := range cands {
		if s == old {
			continue
		}
		a.Mapping[v] = s
		g := txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda).Throughput - base
		a.Mapping[v] = old
		if g > gain+1e-12 || (math.Abs(g-gain) <= 1e-12 && g > 0 && (best == "" || s < best)) {
			gain = g
			best = s
		}
	}
	return best, gain
}
func (a *txalloAllocator) optimize(nodes []string) {
	for pass := 0; pass < 100; pass++ {
		delta := 0.0
		for _, v := range nodes {
			c := a.candidateShards(v, false)
			best, g := a.bestMove(v, c)
			if best != "" && g > 0 {
				a.Mapping[v] = best
				delta += g
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
	community := deterministicLouvainInitialization(a.Graph)
	groups := map[int][]string{}
	for n, c := range community {
		groups[c] = append(groups[c], n)
	}
	// Rank Louvain communities by paper workload sigma; tie by smallest account.
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
		obj := txalloEvaluate(a.Graph, temp, []string{"in", "other"}, a.Eta, math.MaxFloat64)
		rows = append(rows, gr{c, ns, obj.Workloads["in"], ns[0]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].work != rows[j].work {
			return rows[i].work > rows[j].work
		}
		return rows[i].min < rows[j].min
	})
	a.Mapping = map[string]string{}
	limit := len(a.Shards)
	if len(rows) < limit {
		limit = len(rows)
	}
	large := map[int]string{}
	for i := 0; i < limit; i++ {
		large[rows[i].id] = a.Shards[i]
		for _, n := range rows[i].nodes {
			a.Mapping[n] = a.Shards[i]
		}
	}
	// Nodes from small communities: Algorithm 1 lines 2-9, joining gain; direct objective evaluation is mathematically equivalent to Eq. (6)/(8) but slower.
	for i := limit; i < len(rows); i++ {
		for _, v := range rows[i].nodes {
			cands := a.candidateShards(v, true)
			best := ""
			bestObj := -1.0
			for _, s := range cands {
				a.Mapping[v] = s
				o := txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda).Throughput
				if o > bestObj+1e-12 || (math.Abs(o-bestObj) <= 1e-12 && (best == "" || s < best)) {
					bestObj = o
					best = s
				}
			}
			a.Mapping[v] = best
		}
	}
	// Any nodes omitted because Louvain produced fewer communities/new isolated nodes.
	for _, v := range txalloNodeOrder(a.Graph.Nodes) {
		if a.Mapping[v] == "" {
			best, _ := a.bestMove(v, a.Shards)
			if best == "" {
				best = a.Shards[0]
			}
			a.Mapping[v] = best
		}
	}
	a.optimize(txalloNodeOrder(a.Graph.Nodes))
	a.Metrics.GRunCount++
	a.Metrics.Objective = txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda)
}

func (a *txalloAllocator) RunA(newTxs []txalloHistoryTx) {
	affectedMap := map[string]bool{}
	known := map[string]bool{}
	for n := range a.Graph.Nodes {
		known[n] = true
	}
	a.Graph.addAll(newTxs)
	a.params()
	for _, t := range newTxs {
		for _, v := range txalloUniqueSortedStrings(t.Accounts) {
			affectedMap[v] = true
		}
	}
	affected := txalloNodeOrder(affectedMap)
	for _, v := range affected {
		if known[v] && a.Mapping[v] != "" {
			continue
		}
		cands := a.candidateShards(v, true)
		best := ""
		bestObj := -1.0
		for _, s := range cands {
			a.Mapping[v] = s
			o := txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda).Throughput
			if o > bestObj+1e-12 || (math.Abs(o-bestObj) <= 1e-12 && (best == "" || s < best)) {
				bestObj = o
				best = s
			}
		}
		if best == "" && len(a.Shards) > 0 {
			best = a.Shards[0]
		}
		a.Mapping[v] = best
	}
	a.optimize(affected)
	a.Metrics.A_RunCount++
	a.Metrics.Objective = txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda)
}
