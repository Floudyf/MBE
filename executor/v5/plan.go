package v5

import (
	"encoding/json"
	"fmt"
	"os"
)

type PluginConfig struct {
	PluginID string         `json:"plugin_id"`
	Config   map[string]any `json:"config"`
}
type NodePlan struct {
	NodeID        string                  `json:"node_id"`
	ShardID       string                  `json:"shard_id"`
	Role          string                  `json:"role"`
	Leader        bool                    `json:"leader"`
	ListenAddr    string                  `json:"listen_addr"`
	DataDir       string                  `json:"data_dir"`
	Validators    []string                `json:"validators"`
	PluginProfile map[string]PluginConfig `json:"plugin_profile"`
}
type WorkloadPlan struct {
	PluginID        string  `json:"plugin_id"`
	TxCount         int     `json:"tx_count"`
	Seed            int     `json:"seed"`
	CrossShardRatio float64 `json:"cross_shard_ratio"`
	TimeoutEvery    int     `json:"timeout_every"`
}
type Plan struct {
	PlanID           string         `json:"plan_id"`
	PlanDigest       string         `json:"plan_digest"`
	RuntimeStage     string         `json:"runtime_stage"`
	RuntimeTruth     string         `json:"runtime_truth"`
	ExecutionBackend string         `json:"execution_backend"`
	DurationMS       int            `json:"duration_ms"`
	NodeConfigs      []NodePlan     `json:"node_configs"`
	WorkloadPlan     WorkloadPlan   `json:"workload_plan"`
	FaultPlan        map[string]any `json:"fault_plan"`
	NoFallback       bool           `json:"no_fallback"`
}

func LoadPlan(path string) (Plan, error) {
	var p Plan
	raw, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, err
	}
	if p.ExecutionBackend != "real_cluster" {
		return p, fmt.Errorf("V5 plan backend must be real_cluster")
	}
	if !p.NoFallback {
		return p, fmt.Errorf("V5 plan must prohibit fallback")
	}
	return p, nil
}
func SaveJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
