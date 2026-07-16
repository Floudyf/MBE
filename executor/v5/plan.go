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
	PluginID                 string   `json:"plugin_id"`
	SourceType               string   `json:"source_type"`
	DatasetID                string   `json:"dataset_id"`
	VariantID                string   `json:"variant_id"`
	VariantMode              string   `json:"variant_mode"`
	MaterializedID           string   `json:"materialized_id"`
	CanonicalRelativePath    string   `json:"canonical_relative_path"`
	MaterializedRelativePath string   `json:"materialized_relative_path"`
	SourceSHA256             string   `json:"source_sha256"`
	CanonicalSHA256          string   `json:"canonical_sha256"`
	MaterializedSHA256       string   `json:"materialized_sha256"`
	BaseWindowSHA256         string   `json:"base_window_sha256"`
	BaseWindowHash           string   `json:"base_window_hash"`
	TruthLabel               string   `json:"truth_label"`
	SelectionMode            string   `json:"selection_mode"`
	ReplayMode               string   `json:"replay_mode"`
	SkewAxis                 string   `json:"skew_axis"`
	TargetAlpha              *float64 `json:"target_alpha"`
	GeneratorVersion         string   `json:"generator_version"`
	IdentityMappingVersion   string   `json:"identity_mapping_version"`
	NoFallback               bool     `json:"no_fallback"`
	TxCount                  int      `json:"tx_count"`
	RequestedTxCount         int      `json:"requested_tx_count"`
	ActualTxCount            int      `json:"actual_tx_count"`
	Seed                     int      `json:"seed"`
	CrossShardRatio          float64  `json:"cross_shard_ratio"`
	TimeoutEvery             int      `json:"timeout_every"`
	RequestedCrossShardRatio float64  `json:"requested_cross_shard_ratio"`
	RequestedCrossShardCount int      `json:"requested_cross_shard_count"`
	ExpectedCrossShardCount  int      `json:"expected_cross_shard_count"`
	ExpectedCrossShardRatio  float64  `json:"expected_cross_shard_ratio"`
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
