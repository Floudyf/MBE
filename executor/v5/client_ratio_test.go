package v5

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/p2p"
)

func TestSubmitWorkloadUsesConfiguredCrossShardRatioAcrossStream(t *testing.T) {
	for _, ratio := range []float64{0, 0.1, 0.25, 0.5, 1.0} {
		t.Run(strconv.FormatFloat(ratio, 'f', 2, 64), func(t *testing.T) {
			listeners := make([]net.Listener, 2)
			for i := range listeners {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				listeners[i] = listener
				go func(ln net.Listener) {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						go func() {
							defer conn.Close()
							reader := bufio.NewReader(conn)
							for {
								if _, _, err := p2p.DecodeReader(reader); err != nil {
									return
								}
							}
						}()
					}
				}(listener)
			}
			defer func() {
				for _, listener := range listeners {
					_ = listener.Close()
				}
			}()
			profile := map[string]PluginConfig{}
			for _, category := range Categories {
				profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
			}
			plan := Plan{NodeConfigs: []NodePlan{
				{NodeID: "n0", ShardID: "s0", Leader: true, ListenAddr: listeners[0].Addr().String(), PluginProfile: profile},
				{NodeID: "n1", ShardID: "s1", Leader: true, ListenAddr: listeners[1].Addr().String(), PluginProfile: profile},
			}, WorkloadPlan: WorkloadPlan{PluginID: "deterministic_signed_synthetic", TxCount: 1000, Seed: 17, CrossShardRatio: ratio}}
			out := t.TempDir()
			if err := SubmitWorkload(context.Background(), plan, out); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(filepath.Join(out, "routing_decision_log.csv"))
			if err != nil {
				t.Fatal(err)
			}
			rows, err := csv.NewReader(file).ReadAll()
			_ = file.Close()
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			firstCross := -1
			lastCross := -1
			for index, row := range rows[1:] {
				if len(row) > 5 && row[5] == "true" {
					count++
					if firstCross < 0 {
						firstCross = index
					}
					lastCross = index
				}
			}
			expected := int(ratio*1000 + 0.5)
			if count < expected-1 || count > expected+1 {
				t.Fatalf("ratio %.2f generated %d cross-shard transactions, expected %d", ratio, count, expected)
			}
			if ratio > 0 && ratio < 1 && lastCross-firstCross < 500 {
				t.Fatalf("cross-shard transactions concentrated in a narrow stream segment: first=%d last=%d", firstCross, lastCross)
			}
			var summary map[string]any
			data, err := os.ReadFile(filepath.Join(out, "client_submission_complete.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			if int(summary["requested_cross_shard_count"].(float64)) != expected || int(summary["generated_cross_shard_count"].(float64)) != count {
				t.Fatalf("client summary does not reflect ratio: %#v", summary)
			}
		})
	}
}

func TestCrossShardSelectorIsDeterministicAndSeeded(t *testing.T) {
	for _, seed := range []int{17, 23} {
		first := make([]bool, 1000)
		second := make([]bool, 1000)
		for index := range first {
			first[index] = crossShardAt(index, 1000, 0.25, seed)
			second[index] = crossShardAt(index, 1000, 0.25, seed)
		}
		for index := range first {
			if first[index] != second[index] {
				t.Fatalf("seed %d was not deterministic", seed)
			}
		}
	}
	if equal := func() bool {
		for index := 0; index < 1000; index++ {
			if crossShardAt(index, 1000, 0.25, 17) != crossShardAt(index, 1000, 0.25, 23) {
				return false
			}
		}
		return true
	}(); equal {
		t.Fatal("different seeds produced identical cross-shard positions")
	}
}

func TestRequestedCrossShardCountUsesHalfUpRounding(t *testing.T) {
	cases := []struct {
		total int
		ratio float64
		want  int
	}{
		{1, 0.5, 1}, {3, 0.5, 2}, {5, 0.1, 1}, {5, 0, 0}, {5, 1, 5},
	}
	for _, item := range cases {
		if got := requestedCrossShardCount(item.total, item.ratio); got != item.want {
			t.Fatalf("requested count (%d, %.2f)=%d, want %d", item.total, item.ratio, got, item.want)
		}
	}
}

func TestSubmitWorkloadRejectsCrossShardRatioOnSingleShard(t *testing.T) {
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	plan := Plan{NodeConfigs: []NodePlan{{NodeID: "n0", ShardID: "s0", Leader: true, PluginProfile: profile}}, WorkloadPlan: WorkloadPlan{TxCount: 1, CrossShardRatio: 0.5}}
	if err := SubmitWorkload(context.Background(), plan, t.TempDir()); err == nil {
		t.Fatal("single shard cross-shard workload was not rejected")
	}
}

func TestSubmitDatasetWorkloadUsesBuyerHomeShardAsIngress(t *testing.T) {
	listeners := make([]net.Listener, 2)
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = listener
		go func(ln net.Listener) {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					reader := bufio.NewReader(conn)
					for {
						if _, _, err := p2p.DecodeReader(reader); err != nil {
							return
						}
					}
				}()
			}
		}(listener)
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	profile := map[string]PluginConfig{}
	for _, category := range Categories {
		profile[category] = PluginConfig{PluginID: firstPlugin(category), Config: map[string]any{}}
	}
	profile["workload"] = PluginConfig{PluginID: "canonical_trace_replay", Config: map[string]any{}}
	out := t.TempDir()
	records := []map[string]any{}
	for index := 0; index < 32; index++ {
		buyer := "0x" + strings.Repeat("0", 40-len(strconv.Itoa(index+1))) + strconv.Itoa(index+1)
		contract := "0x" + strings.Repeat("0", 40-len(strconv.Itoa(index+101))) + strconv.Itoa(index+101)
		records = append(records, canonicalRecord(index, buyer, contract))
	}
	relative, hash := writeCanonicalFixture(t, filepath.Join(out, ".cache", "workloads"), records)
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, ListenAddr: listeners[0].Addr().String(), PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Leader: true, ListenAddr: listeners[1].Addr().String(), PluginProfile: profile},
	}, WorkloadPlan: canonicalPlan(relative, hash, len(records))}
	if err := SubmitWorkload(context.Background(), plan, out); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(out, "client_submission_log.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(records)+1 {
		t.Fatalf("unexpected submission rows: %v", rows)
	}
	crossRows := 0
	for _, row := range rows[1:] {
		if row[6] != "true" {
			continue
		}
		crossRows++
		if row[7] == "" || row[8] == "" || row[7] == row[8] {
			t.Fatalf("dataset cross-shard submission did not use distinct source/target shards: %v", row)
		}
	}
	if crossRows == 0 {
		t.Fatal("fixture did not produce any dataset cross-shard submissions")
	}
}
