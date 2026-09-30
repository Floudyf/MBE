package v5

import (
	"reflect"
	"testing"
)

func TestTxAlloGraphMultiAccountWeights(t *testing.T) {
	g := newTxAlloGraph()
	g.addTransaction([]string{"a", "b", "c"})
	if len(g.Edges) != 3 {
		t.Fatalf("edges=%v", g.Edges)
	}
	for _, w := range g.Edges {
		if w < 0.333333 || w > 0.333334 {
			t.Fatalf("weight=%f", w)
		}
	}
	if x := g.totalWeight(); x < 0.999999 || x > 1.000001 {
		t.Fatalf("total=%f", x)
	}
}
func TestTxAlloObjectivePenalizesCrossShard(t *testing.T) {
	g := newTxAlloGraph()
	g.addTransaction([]string{"a", "b"})
	same := txalloEvaluate(g, map[string]string{"a": "s0", "b": "s0"}, []string{"s0", "s1"}, 2, 1)
	cross := txalloEvaluate(g, map[string]string{"a": "s0", "b": "s1"}, []string{"s0", "s1"}, 2, 1)
	if same.CrossShardRatio != 0 || cross.CrossShardRatio != 1 {
		t.Fatalf("ratio same=%v cross=%v", same, cross)
	}
	if same.Throughput <= cross.Throughput {
		t.Fatalf("throughput same=%v cross=%v", same.Throughput, cross.Throughput)
	}
}
func TestTxAlloDeterministicGAndA(t *testing.T) {
	history := []txalloHistoryTx{{[]string{"a", "b"}}, {[]string{"a", "b"}}, {[]string{"c", "d"}}, {[]string{"c", "d"}}, {[]string{"b", "c"}}}
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	b := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	a.RunG(history)
	b.RunG(history)
	if !reflect.DeepEqual(a.Mapping, b.Mapping) {
		t.Fatalf("G nondeterministic %v %v", a.Mapping, b.Mapping)
	}
	delta := []txalloHistoryTx{{[]string{"e", "a"}}, {[]string{"e", "a"}}}
	a.RunA(delta)
	b.RunA(delta)
	if !reflect.DeepEqual(a.Mapping, b.Mapping) {
		t.Fatalf("A nondeterministic %v %v", a.Mapping, b.Mapping)
	}
	if a.Mapping["e"] == "" {
		t.Fatal("new node not allocated")
	}
}
func TestTxAlloLocalityEmerges(t *testing.T) {
	history := []txalloHistoryTx{}
	for i := 0; i < 10; i++ {
		history = append(history, txalloHistoryTx{[]string{"a", "b"}}, txalloHistoryTx{[]string{"c", "d"}})
	}
	history = append(history, txalloHistoryTx{[]string{"b", "c"}})
	a := newTxAlloAllocator([]string{"s0", "s1"}, 2, 0, 0)
	a.RunG(history)
	if a.Mapping["a"] != a.Mapping["b"] || a.Mapping["c"] != a.Mapping["d"] {
		t.Fatalf("locality mapping=%v", a.Mapping)
	}
}
