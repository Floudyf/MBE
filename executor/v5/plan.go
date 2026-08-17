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
	PluginID                 string         `json:"plugin_id"`
	SourceType               string         `json:"source_type"`
	DatasetID                string         `json:"dataset_id"`
	VariantID                string         `json:"variant_id"`
	VariantMode              string         `json:"variant_mode"`
	MaterializedID           string         `json:"materialized_id"`
	CanonicalRelativePath    string         `json:"canonical_relative_path"`
	MaterializedRelativePath string         `json:"materialized_relative_path"`
	SourceSHA256             string         `json:"source_sha256"`
	SourceFileSHA256         string         `json:"source_file_sha256"`
	CanonicalSHA256          string         `json:"canonical_sha256"`
	MaterializedSHA256       string         `json:"materialized_sha256"`
	BaseWindowSHA256         string         `json:"base_window_sha256"`
	BaseWindowHash           string         `json:"base_window_hash"`
	TruthLabel               string         `json:"truth_label"`
	SelectionMode            string         `json:"selection_mode"`
	VariantParameters        map[string]any `json:"variant_parameters"`
	AuditMetadata            map[string]any `json:"audit_metadata,omitempty"`
	ReplayMode               string         `json:"replay_mode"`
	TargetSubmissionTPS      int            `json:"target_submission_tps,omitempty"`
	SkewAxis                 string         `json:"skew_axis"`
	TargetAlpha              *float64       `json:"target_alpha"`
	GeneratorVersion         string         `json:"generator_version"`
	IdentityMappingVersion   string         `json:"identity_mapping_version"`
	NoFallback               bool           `json:"no_fallback"`
	TxCount                  int            `json:"tx_count"`
	RequestedTxCount         int            `json:"requested_tx_count"`
	ActualTxCount            int            `json:"actual_tx_count"`
	Seed                     int            `json:"seed"`
	CrossShardRatio          float64        `json:"cross_shard_ratio"`
	TimeoutEvery             int            `json:"timeout_every"`
	RequestedCrossShardRatio float64        `json:"requested_cross_shard_ratio"`
	RequestedCrossShardCount int            `json:"requested_cross_shard_count"`
	ExpectedCrossShardCount  int            `json:"expected_cross_shard_count"`
	ExpectedCrossShardRatio  float64        `json:"expected_cross_shard_ratio"`
}

// ArtifactContractEntry is evidence metadata owned by the Python compiler.
// The supervisor does not interpret it, but it must survive plan load/save so
// the persisted child plan remains the authoritative v2 formal contract.
type ArtifactContractEntry struct {
	PathPattern                  string   `json:"path_pattern"`
	Scope                        string   `json:"scope"`
	PerNode                      bool     `json:"per_node"`
	NodeIDs                      []string `json:"node_ids"`
	MinCount                     int      `json:"min_count"`
	RequiredForFormalEligibility bool     `json:"required_for_formal_eligibility"`
}

type Plan struct {
	PlanID                  string                  `json:"plan_id"`
	PlanDigest              string                  `json:"plan_digest"`
	RuntimeStage            string                  `json:"runtime_stage"`
	RuntimeTruth            string                  `json:"runtime_truth"`
	ExecutionBackend        string                  `json:"execution_backend"`
	DurationMS              int                     `json:"duration_ms"`
	PBFTIdentityScheme      string                  `json:"pbft_identity_scheme,omitempty"`
	PBFTPublicKeys          map[string]string       `json:"pbft_public_keys,omitempty"`
	NodeConfigs             []NodePlan              `json:"node_configs"`
	WorkloadPlan            WorkloadPlan            `json:"workload_plan"`
	FaultPlan               map[string]any          `json:"fault_plan"`
	NoFallback              bool                    `json:"no_fallback"`
	ArtifactContractVersion int                     `json:"artifact_contract_version"`
	ArtifactContract        []ArtifactContractEntry `json:"artifact_contract"`
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
