package v5

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestTxAlloV20EvaluateFailsClosedOnIncompleteMapping(t *testing.T) {
	g := newTxAlloGraph()
	g.addTransaction([]string{"a", "b"})
	defer func() {
		if recover() == nil {
			t.Fatal("incomplete mapping must fail closed instead of defaulting to shard0")
		}
	}()
	_ = txalloEvaluate(g, map[string]string{"a": "s0"}, []string{"s0", "s1"}, 2, 1)
}

func TestTxAlloV20Eq6JoinGainMatchesCommunityRebuild(t *testing.T) {
	g := newTxAlloGraph()
	g.addAll([]txalloHistoryTx{{[]string{"a", "b"}}, {[]string{"a", "v"}}, {[]string{"v", "c"}}, {[]string{"v"}}})
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 2, 1e-9)
	a.Graph = g
	a.Mapping = map[string]string{"a": "s0", "b": "s0", "c": "s1"}
	a.rebuildStats()
	before := a.Stats["s0"]
	_, gain := a.joinResult("v", "s0")
	m := copyMapping(a.Mapping)
	m["v"] = "s0"
	after := txalloBuildCommunityStats(g, m, a.Shards, a.Eta)["s0"]
	want := txalloThroughputFromStat(after, a.Lambda) - txalloThroughputFromStat(before, a.Lambda)
	if math.Abs(gain-want) > 1e-9 {
		t.Fatalf("Eq6 join gain mismatch got=%f want=%f", gain, want)
	}
}

func TestTxAlloV20Eq8MoveGainMatchesFullObjectiveDelta(t *testing.T) {
	g := newTxAlloGraph()
	g.addAll([]txalloHistoryTx{{[]string{"a", "b"}}, {[]string{"a", "c"}}, {[]string{"c", "d"}}, {[]string{"a"}}})
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 2, 1e-9)
	a.Graph = g
	a.Mapping = map[string]string{"a": "s0", "b": "s0", "c": "s1", "d": "s1"}
	a.rebuildStats()
	before := txalloEvaluate(g, a.Mapping, a.Shards, a.Eta, a.Lambda).Throughput
	_, leave := a.leaveResult("a", "s0")
	_, join := a.joinResult("a", "s1")
	m := copyMapping(a.Mapping)
	m["a"] = "s1"
	after := txalloEvaluate(g, m, a.Shards, a.Eta, a.Lambda).Throughput
	if math.Abs((leave+join)-(after-before)) > 1e-9 {
		t.Fatalf("Eq8 move gain mismatch local=%f full=%f", leave+join, after-before)
	}
}

func TestTxAlloV20LocalGainRandomizedOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20261002))
	shards := []string{"s0", "s1", "s2"}
	nodes := []string{"a", "b", "c", "d", "e", "f"}
	for trial := 0; trial < 100; trial++ {
		g := newTxAlloGraph()
		for i := 0; i < 40; i++ {
			a := nodes[rng.Intn(len(nodes))]
			b := nodes[rng.Intn(len(nodes))]
			if rng.Intn(5) == 0 {
				g.addTransaction([]string{a})
			} else {
				g.addTransaction([]string{a, b})
			}
		}
		mapping := map[string]string{}
		for _, node := range nodes {
			if g.Nodes[node] {
				mapping[node] = shards[rng.Intn(len(shards))]
			}
		}
		a := newTxAlloAllocator(shards, 2.0, 1.0+float64(rng.Intn(10)), 1e-9)
		a.Graph = g
		a.Mapping = mapping
		a.rebuildStats()
		base := txalloEvaluate(g, mapping, shards, a.Eta, a.Lambda).Throughput
		for node := range g.Nodes {
			source := mapping[node]
			for _, target := range shards {
				if target == source {
					continue
				}
				_, leave := a.leaveResult(node, source)
				_, join := a.joinResult(node, target)
				moved := copyMapping(mapping)
				moved[node] = target
				want := txalloEvaluate(g, moved, shards, a.Eta, a.Lambda).Throughput - base
				if math.Abs((leave+join)-want) > 1e-8 {
					t.Fatalf("trial=%d node=%s %s->%s local=%g full=%g", trial, node, source, target, leave+join, want)
				}
			}
		}
	}
}

func TestTxAlloV20RunGProducesCompleteDeterministicMapping(t *testing.T) {
	history := []txalloHistoryTx{}
	for i := 0; i < 20; i++ {
		history = append(history,
			txalloHistoryTx{[]string{"a", "b"}},
			txalloHistoryTx{[]string{"c", "d"}},
			txalloHistoryTx{[]string{"e", "f"}},
			txalloHistoryTx{[]string{"g", "h"}},
		)
	}
	history = append(history,
		txalloHistoryTx{[]string{"b", "c"}},
		txalloHistoryTx{[]string{"d", "e"}},
		txalloHistoryTx{[]string{"f", "g"}},
	)
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	b := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	a.RunG(history)
	b.RunG(history)
	if len(a.Mapping) != len(a.Graph.Nodes) {
		t.Fatalf("mapping incomplete nodes=%d mapping=%d", len(a.Graph.Nodes), len(a.Mapping))
	}
	if !reflect.DeepEqual(a.Mapping, b.Mapping) {
		t.Fatalf("mapping nondeterministic a=%v b=%v", a.Mapping, b.Mapping)
	}
	if a.Metrics.LouvainLevels <= 1 {
		t.Fatalf("fixture must exercise community aggregation beyond the first local-moving level: %d", a.Metrics.LouvainLevels)
	}
}

func TestTxAlloV20RunAAllocatesOnlyFromUpdatedGraphWithoutBreakingCompleteness(t *testing.T) {
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	a.RunG([]txalloHistoryTx{{[]string{"a", "b"}}, {[]string{"c", "d"}}})
	a.RunA([]txalloHistoryTx{{[]string{"e", "a"}}, {[]string{"e", "a"}}})
	if a.Mapping["e"] == "" {
		t.Fatal("A-TxAllo did not allocate newly committed account")
	}
	if len(a.Mapping) != len(a.Graph.Nodes) {
		t.Fatalf("A-TxAllo mapping incomplete nodes=%d mapping=%d", len(a.Graph.Nodes), len(a.Mapping))
	}
}
