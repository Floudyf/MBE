package v5

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// These reference helpers reproduce the previously shipped full-edge-scan
// semantics independently of the new derived adjacency cache.
func txalloLegacyNeighborsV1(g *txalloGraph, v string) map[string]float64 {
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
func txalloLegacyIncidentV1(a *txalloAllocator, v, shard string) (self, other, to float64) {
	self = a.Graph.selfWeight(v)
	for n, w := range txalloLegacyNeighborsV1(a.Graph, v) {
		other += w
		if a.Mapping[n] == shard {
			to += w
		}
	}
	return
}
func txalloLegacyJoinV1(a *txalloAllocator, v, q string) (txalloCommunityStat, float64) {
	before := a.Stats[q]
	self, other, to := txalloLegacyIncidentV1(a, v, q)
	after := before
	after.Workload += self + a.Eta*(other-to) + (1-a.Eta)*to
	after.Uncapped += self + other/2
	return after, txalloThroughputFromStat(after, a.Lambda) - txalloThroughputFromStat(before, a.Lambda)
}
func txalloLegacyLeaveV1(a *txalloAllocator, v, p string) (txalloCommunityStat, float64) {
	before := a.Stats[p]
	self, other, to := txalloLegacyIncidentV1(a, v, p)
	after := before
	after.Workload -= self + a.Eta*(other-to) - (a.Eta-1)*to
	after.Uncapped -= self + other/2
	if after.Workload < 0 && after.Workload > -1e-9 {
		after.Workload = 0
	}
	if after.Uncapped < 0 && after.Uncapped > -1e-9 {
		after.Uncapped = 0
	}
	return after, txalloThroughputFromStat(after, a.Lambda) - txalloThroughputFromStat(before, a.Lambda)
}
func txalloLegacyBestJoinV1(a *txalloAllocator, v string, cands []string) string {
	best := ""
	bestGain := math.Inf(-1)
	for _, shard := range cands {
		_, gain := txalloLegacyJoinV1(a, v, shard)
		if gain > bestGain+1e-12 || (math.Abs(gain-bestGain) <= 1e-12 && (best == "" || shard < best)) {
			best, bestGain = shard, gain
		}
	}
	return best
}
func txalloLegacyBestMoveV1(a *txalloAllocator, v string, cands []string) (string, float64) {
	old := a.Mapping[v]
	best := ""
	bestGain := 0.0
	for _, shard := range cands {
		if shard == old || shard == "" {
			continue
		}
		_, leaveGain := txalloLegacyLeaveV1(a, v, old)
		_, joinGain := txalloLegacyJoinV1(a, v, shard)
		gain := leaveGain + joinGain
		if gain > bestGain+1e-12 || (math.Abs(gain-bestGain) <= 1e-12 && gain > 0 && (best == "" || shard < best)) {
			best, bestGain = shard, gain
		}
	}
	return best, bestGain
}
func txalloLegacyRunAV1(a *txalloAllocator, txs []txalloHistoryTx) {
	known := map[string]bool{}
	for n := range a.Graph.Nodes {
		known[n] = true
	}
	a.Graph.addAll(txs)
	a.params()
	a.rebuildStats()
	affectedMap := map[string]bool{}
	for _, tx := range txs {
		for _, v := range txalloUniqueSortedStrings(tx.Accounts) {
			affectedMap[v] = true
		}
	}
	affected := txalloNodeOrder(affectedMap)
	for _, v := range affected {
		if known[v] && a.Mapping[v] != "" {
			continue
		}
		cands := a.candidateShards(v, true)
		best := txalloLegacyBestJoinV1(a, v, cands)
		if best == "" && len(a.Shards) > 0 {
			best = a.Shards[0]
		}
		if best != "" {
			after, _ := txalloLegacyJoinV1(a, v, best)
			a.Mapping[v] = best
			a.Stats[best] = after
		}
	}
	for pass := 0; pass < 100; pass++ {
		delta := 0.0
		for _, v := range affected {
			if a.Mapping[v] == "" {
				continue
			}
			cands := a.candidateShards(v, false)
			target, gain := txalloLegacyBestMoveV1(a, v, cands)
			if target != "" && gain > 0 {
				source := a.Mapping[v]
				leaveAfter, _ := txalloLegacyLeaveV1(a, v, source)
				joinAfter, _ := txalloLegacyJoinV1(a, v, target)
				a.Mapping[v] = target
				a.Stats[source] = leaveAfter
				a.Stats[target] = joinAfter
				delta += gain
				a.Metrics.ChangedAccounts++
			}
		}
		a.Metrics.Iterations++
		if delta < a.Epsilon {
			break
		}
	}
	a.Metrics.A_RunCount++
	a.Metrics.Objective = txalloEvaluate(a.Graph, a.Mapping, a.Shards, a.Eta, a.Lambda)
}

func TestTxAlloPerfV1GraphNeighborsExactlyPreserved(t *testing.T) {
	g := newTxAlloGraph()
	for i := 0; i < 180; i++ {
		g.addTransaction([]string{"a", "b"})
		g.addTransaction([]string{"b", "c"})
		g.addTransaction([]string{"b", "b"})
		if i%5 == 0 {
			g.addTransaction([]string{"a", "c"})
		}
		for _, v := range []string{"a", "b", "c", "missing"} {
			if !reflect.DeepEqual(g.neighbors(v), txalloLegacyNeighborsV1(g, v)) {
				t.Fatalf("neighbor regression at step=%d account=%s", i, v)
			}
		}
	}
	// Mutating the returned map cannot contaminate later ledger decisions.
	neighbors := g.neighbors("a")
	neighbors["b"] = -999
	if g.neighbors("a")["b"] < 0 {
		t.Fatal("adjacency index was leaked/mutated")
	}
}

func TestTxAlloPerfV1RunAComparedWithOriginalFullScan(t *testing.T) {
	history := []txalloHistoryTx{}
	for i := 0; i < 60; i++ {
		a := "acct-" + string(rune('a'+i%12))
		b := "acct-" + string(rune('a'+(i*7+3)%12))
		history = append(history, txalloHistoryTx{Accounts: []string{a, b}})
	}
	optimized := newTxAlloAllocator([]string{"s0", "s1", "s2"}, 2, 0, 0)
	optimized.RunG(history)
	legacy := txalloCloneAllocator(optimized)
	for epoch := 0; epoch < 35; epoch++ {
		txs := []txalloHistoryTx{}
		for j := 0; j < 12; j++ {
			i := epoch*12 + j
			a := "acct-" + string(rune('a'+i%12))
			b := "acct-" + string(rune('a'+(i*7+3)%12))
			txs = append(txs, txalloHistoryTx{Accounts: []string{a, b}})
		}
		optimized.RunA(txs)
		txalloLegacyRunAV1(legacy, txs)
		if !reflect.DeepEqual(optimized.Mapping, legacy.Mapping) {
			t.Fatalf("epoch=%d mapping diverged", epoch)
		}
		if !reflect.DeepEqual(optimized.Graph.Edges, legacy.Graph.Edges) {
			t.Fatalf("epoch=%d graph diverged", epoch)
		}
		o, l := optimized.Metrics.Objective, legacy.Metrics.Objective
		if math.Abs(o.Throughput-l.Throughput) > 1e-7 || math.Abs(o.CrossShardRatio-l.CrossShardRatio) > 1e-10 {
			t.Fatalf("epoch=%d objective diverged: %v vs %v", epoch, o, l)
		}
	}
}

func txalloHistoryFixtureV1(epoch uint64) txalloMappingSnapshotV22 {
	mapping := map[string]string{"a": "s0", "b": "s1"}
	aliases := map[string]string{"sender_a": "a"}
	return txalloMappingSnapshotV22{
		SchemaVersion: txalloDynamicMappingSchema, Epoch: epoch,
		Mapping: mapping, Aliases: aliases, MappingDigest: stableJSONDigest(mapping),
		AliasesDigest: stableJSONDigest(aliases), StateDigest: txalloStateDigest(mapping, aliases),
	}
}
func TestTxAlloPerfV1HistoryAppendAndIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "epochs.jsonl")
	defer delete(txalloHistoryCursorsV1, filepath.Clean(path))
	for epoch := uint64(1); epoch <= 36; epoch++ {
		snap := txalloHistoryFixtureV1(epoch)
		if err := txalloAppendMappingEpochHistoryV229(path, snap, int64(epoch), "A-TxAllo"); err != nil {
			t.Fatal(err)
		}
	}
	if err := txalloAppendMappingEpochHistoryV229(path, txalloHistoryFixtureV1(36), 36, "A-TxAllo"); err != nil {
		t.Fatal("idempotence", err)
	}
	if err := txalloAppendMappingEpochHistoryV229(path, txalloHistoryFixtureV1(1), 1, "A-TxAllo"); err == nil {
		t.Fatal("regressed epoch accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte("\n")) != 36 {
		t.Fatal("history record count changed")
	}
	// A legitimate process restart must reread the complete historical file.
	delete(txalloHistoryCursorsV1, filepath.Clean(path))
	if err := txalloAppendMappingEpochHistoryV229(path, txalloHistoryFixtureV1(37), 37, "A-TxAllo"); err != nil {
		t.Fatal("cold restart", err)
	}
	// In-place corruption with a changed mtime is not silently accepted.
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(data, []byte("mbe_txallo_mapping_snapshot_v2"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	if bytes.Equal(changed, data) {
		t.Fatal("corruption fixture missing")
	}
	if err := os.WriteFile(path, changed, 0644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(5 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if err := txalloAppendMappingEpochHistoryV229(path, txalloHistoryFixtureV1(38), 38, "A-TxAllo"); err == nil {
		t.Fatal("corrupt old history accepted")
	}
}
