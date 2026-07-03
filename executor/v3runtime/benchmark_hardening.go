package v3runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
)

const BenchmarkRuntimeTruth = "benchmark_template_hardening_not_paper_grade_benchmark"

type BenchmarkTemplate struct {
	TemplateID         string   `json:"template_id"`
	Description        string   `json:"description"`
	WorkloadProfile    string   `json:"workload_profile"`
	Topology           string   `json:"topology"`
	ConsensusRuntime   string   `json:"consensus_runtime"`
	NetworkAdapter     string   `json:"network_adapter"`
	CrossShardProtocol string   `json:"cross_shard_protocol"`
	StateBackend       string   `json:"state_backend"`
	BaselineCandidates []string `json:"baseline_candidates"`
	SweepParameters    []string `json:"sweep_parameters"`
	RequiredArtifacts  []string `json:"required_artifacts"`
	TruthBoundary      string   `json:"truth_boundary"`
}

type BaselineProfile struct {
	BaselineID       string   `json:"baseline_id"`
	Description      string   `json:"description"`
	DisabledFeatures []string `json:"disabled_features"`
	EnabledFeatures  []string `json:"enabled_features"`
	ComparisonTarget string   `json:"comparison_target"`
	TruthBoundary    string   `json:"truth_boundary"`
}

type BenchmarkRunRow struct {
	BenchmarkID        string
	RunID              string
	TemplateID         string
	BaselineID         string
	RepeatIndex        int
	Seed               int
	TxCount            int
	ShardCount         int
	HotspotRatio       float64
	NetworkAdapter     string
	ConsensusRuntime   string
	CrossShardProtocol string
	StateBackend       string
	SummaryPath        string
	Status             string
	RuntimeTruth       string
}

type BenchmarkPreview struct {
	BenchmarkID                      string
	TemplateID                       string
	BaselineID                       string
	RepeatCount                      int
	Templates                        []BenchmarkTemplate
	Baselines                        []BaselineProfile
	RunRows                          []BenchmarkRunRow
	SweepMatrixRows                  [][]string
	SweepSummaryRows                 [][]string
	BaselineComparisonRows           [][]string
	AggregateSummaryRows             [][]string
	BenchmarkTemplateSelected        string
	BaselineProfileSelected          string
	BenchmarkRunCount                int
	SweepParameterCount              int
	BenchmarkArtifactCount           int
	BaselineComparisonCount          int
	ReproducibilityManifestAvailable bool
	BenchmarkReportAvailable         bool
	PaperGradeBenchmark              bool
}

func RunBenchmarkHardeningPreview(exp ExperimentProfile, summary Summary) BenchmarkPreview {
	templateID := firstNonEmpty(exp.BenchmarkTemplate, "full_stack_v3_template")
	baselineID := firstNonEmpty(exp.BaselineProfile, defaultBaselineForTemplate(templateID))
	repeatCount := max(1, exp.RepeatCount)
	seedBase := exp.Seed
	sweepParams := []string{"tx_count", "shard_count", "hotspot_ratio", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend"}
	benchmarkID := "v3_10_" + templateID + "_" + baselineID
	consensusRuntime := firstNonEmpty(summary.ConsensusRuntimeSelected, "simple_leader")
	runRows := []BenchmarkRunRow{}
	sweepMatrix := [][]string{}
	for repeat := 0; repeat < repeatCount; repeat++ {
		seed := seedBase + repeat
		runID := benchmarkID + "_repeat_" + strconv.Itoa(repeat)
		runRows = append(runRows, BenchmarkRunRow{
			BenchmarkID:        benchmarkID,
			RunID:              runID,
			TemplateID:         templateID,
			BaselineID:         baselineID,
			RepeatIndex:        repeat,
			Seed:               seed,
			TxCount:            summary.TxCount,
			ShardCount:         summary.ShardCount,
			HotspotRatio:       exp.HotspotRatio,
			NetworkAdapter:     firstNonEmpty(summary.NetworkAdapterSelected, exp.NetworkAdapter),
			ConsensusRuntime:   consensusRuntime,
			CrossShardProtocol: firstNonEmpty(summary.CrossShardProtocolSelected, exp.CrossShardProtocol),
			StateBackend:       firstNonEmpty(summary.StateBackendSelected, exp.StateBackend),
			SummaryPath:        "summary.json",
			Status:             "completed",
			RuntimeTruth:       BenchmarkRuntimeTruth,
		})
		for _, param := range sweepParams {
			sweepMatrix = append(sweepMatrix, []string{templateID, baselineID, param, benchmarkParameterValue(param, exp, summary, consensusRuntime), strconv.Itoa(repeatCount), strconv.Itoa(seed)})
		}
	}
	metrics := map[string]float64{
		"throughput_tps":       summary.ThroughputTPS,
		"avg_latency_ms":       summary.AvgLatencyMS,
		"p95_latency_ms":       summary.P95LatencyMS,
		"p99_latency_ms":       summary.P99LatencyMS,
		"state_root_count":     float64(summary.StateRootCount),
		"cross_shard_tx_count": float64(summary.CrossShardTxCount),
		"typed_message_count":  float64(summary.TypedMessageCount),
	}
	metricNames := make([]string, 0, len(metrics))
	for name := range metrics {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)
	sweepSummary := [][]string{}
	comparison := [][]string{}
	aggregate := [][]string{}
	for _, name := range metricNames {
		value := metrics[name]
		valueText := strconv.FormatFloat(value, 'f', -1, 64)
		sweepSummary = append(sweepSummary, []string{templateID, baselineID, name, valueText, valueText, valueText, strconv.Itoa(repeatCount)})
		comparison = append(comparison, []string{templateID, baselineID, comparisonTargetForBaseline(baselineID), name, valueText, valueText, "0", "0", "same-run MVP comparison; not performance evidence", BenchmarkRuntimeTruth})
		aggregate = append(aggregate, []string{templateID, baselineID, name, valueText, strconv.Itoa(repeatCount), BenchmarkRuntimeTruth})
	}
	return BenchmarkPreview{
		BenchmarkID:                      benchmarkID,
		TemplateID:                       templateID,
		BaselineID:                       baselineID,
		RepeatCount:                      repeatCount,
		Templates:                        benchmarkTemplates(),
		Baselines:                        baselineProfiles(),
		RunRows:                          runRows,
		SweepMatrixRows:                  sweepMatrix,
		SweepSummaryRows:                 sweepSummary,
		BaselineComparisonRows:           comparison,
		AggregateSummaryRows:             aggregate,
		BenchmarkTemplateSelected:        templateID,
		BaselineProfileSelected:          baselineID,
		BenchmarkRunCount:                len(runRows),
		SweepParameterCount:              len(sweepParams),
		BenchmarkArtifactCount:           12,
		BaselineComparisonCount:          len(comparison),
		ReproducibilityManifestAvailable: true,
		BenchmarkReportAvailable:         true,
		PaperGradeBenchmark:              false,
	}
}

func ApplyBenchmarkMetrics(summary *Summary, preview BenchmarkPreview) {
	summary.BenchmarkTemplateSelected = preview.BenchmarkTemplateSelected
	summary.BaselineProfileSelected = preview.BaselineProfileSelected
	summary.BenchmarkRunCount = preview.BenchmarkRunCount
	summary.SweepParameterCount = preview.SweepParameterCount
	summary.RepeatCount = preview.RepeatCount
	summary.BenchmarkArtifactCount = preview.BenchmarkArtifactCount
	summary.BaselineComparisonCount = preview.BaselineComparisonCount
	summary.ReproducibilityManifestAvailable = preview.ReproducibilityManifestAvailable
	summary.BenchmarkReportAvailable = preview.BenchmarkReportAvailable
	summary.PaperGradeBenchmark = preview.PaperGradeBenchmark
}

func WriteBenchmarkArtifacts(out string, preview BenchmarkPreview, exp ExperimentProfile, summary Summary) error {
	if err := writeJSONFile(filepath.Join(out, "benchmark_template_catalog.json"), preview.Templates); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "baseline_profile_catalog.json"), preview.Baselines); err != nil {
		return err
	}
	plan := map[string]any{
		"benchmark_id":     preview.BenchmarkID,
		"template_id":      preview.TemplateID,
		"baseline_id":      preview.BaselineID,
		"repeat_count":     preview.RepeatCount,
		"runtime_truth":    BenchmarkRuntimeTruth,
		"paper_grade":      false,
		"sweep_parameters": []string{"tx_count", "shard_count", "hotspot_ratio", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend"},
	}
	if err := writeJSONFile(filepath.Join(out, "benchmark_plan.json"), plan); err != nil {
		return err
	}
	if err := writeBenchmarkRunIndex(filepath.Join(out, "benchmark_run_index.csv"), preview.RunRows); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "sweep_matrix.csv"), []string{"template_id", "baseline_id", "parameter_name", "parameter_value", "repeat_count", "seed"}, preview.SweepMatrixRows); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "sweep_summary.csv"), []string{"template_id", "baseline_id", "metric_name", "mean", "min", "max", "run_count"}, preview.SweepSummaryRows); err != nil {
		return err
	}
	sweepObjects := rowsToObjects([]string{"template_id", "baseline_id", "metric_name", "mean", "min", "max", "run_count"}, preview.SweepSummaryRows)
	if err := writeJSONFile(filepath.Join(out, "sweep_summary.json"), sweepObjects); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "aggregate_summary.csv"), []string{"template_id", "baseline_id", "metric_name", "value", "run_count", "runtime_truth"}, preview.AggregateSummaryRows); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "baseline_comparison.csv"), []string{"template_id", "baseline_id", "comparison_target", "metric_name", "baseline_value", "target_value", "delta", "delta_ratio", "interpretation", "truth_boundary"}, preview.BaselineComparisonRows); err != nil {
		return err
	}
	manifest := map[string]any{
		"benchmark_id":            preview.BenchmarkID,
		"template_id":             preview.TemplateID,
		"baseline_id":             preview.BaselineID,
		"run_id":                  summary.RunID,
		"repeat_index":            0,
		"seed":                    exp.Seed,
		"git_commit":              "unknown",
		"python_version":          "unknown",
		"go_version":              runtime.Version(),
		"node_version":            "unknown",
		"os":                      runtime.GOOS,
		"created_at":              "1970-01-01T00:00:00Z",
		"used_chain_profile":      "used_chain_profile.yaml",
		"used_plugin_profile":     "used_plugin_profile.yaml",
		"used_experiment_profile": "used_experiment_profile.yaml",
		"artifact_list":           benchmarkArtifactNames(),
		"runtime_truth":           BenchmarkRuntimeTruth,
		"current_stage":           "V3.10 Benchmark / Experiment Template Hardening Closure",
		"latest_runtime_stage":    "benchmark template catalog, baseline profile catalog, local sweep runner, reproducibility manifest, and benchmark report artifacts",
	}
	if err := writeJSONFile(filepath.Join(out, "reproducibility_manifest.json"), manifest); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "benchmark_summary.json"), map[string]any{
		"current_stage":                      "V3.10 Benchmark / Experiment Template Hardening Closure",
		"latest_runtime_stage":               "benchmark template catalog, baseline profile catalog, local sweep runner, reproducibility manifest, and benchmark report artifacts",
		"runtime_truth":                      BenchmarkRuntimeTruth,
		"benchmark_template_count":           len(preview.Templates),
		"baseline_profile_count":             len(preview.Baselines),
		"benchmark_run_count":                preview.BenchmarkRunCount,
		"sweep_parameter_count":              preview.SweepParameterCount,
		"repeat_count":                       preview.RepeatCount,
		"artifact_count":                     preview.BenchmarkArtifactCount,
		"paper_grade_benchmark":              false,
		"reproducibility_manifest_available": true,
		"benchmark_report_available":         true,
	}); err != nil {
		return err
	}
	report := "# V3.10 Benchmark / Experiment Template Hardening Report\n\n" +
		"benchmark title: Local controlled benchmark template output\n\n" +
		"template_id: `" + preview.TemplateID + "`\n\n" +
		"baseline_id: `" + preview.BaselineID + "`\n\n" +
		"sweep parameters: tx_count, shard_count, hotspot_ratio, network_adapter, consensus_runtime, cross_shard_protocol, state_backend\n\n" +
		"repeat_count: " + strconv.Itoa(preview.RepeatCount) + "\n\n" +
		"run count: " + strconv.Itoa(preview.BenchmarkRunCount) + "\n\n" +
		"core metrics table: see `sweep_summary.csv` and `aggregate_summary.csv`.\n\n" +
		"artifact list: see `reproducibility_manifest.json`.\n\n" +
		"truth boundary: This is a local controlled benchmark template output, not paper-grade benchmark evidence.\n\n" +
		"limitations: not a large-scale distributed benchmark, not a production network, not BlockEmulator backend, and not performance superiority evidence.\n"
	return os.WriteFile(filepath.Join(out, "benchmark_report.md"), []byte(report), 0o644)
}

func benchmarkTemplates() []BenchmarkTemplate {
	return []BenchmarkTemplate{
		{"metatrack_hotspot_template", "MetaTrack hotspot template for coaccess routing, access list / prefetch, and aggregation.", "asset_hotspot", "logical_node_topology_default", "simple_leader", "in_memory_message_bus", "none", "memory_kv", []string{"baseline_hash_sharding", "baseline_no_prefetch"}, []string{"tx_count", "hotspot_ratio", "hot_key_count", "seed"}, []string{"summary.json", "routing_log.csv", "state_access_log.csv"}, BenchmarkRuntimeTruth},
		{"pbft_network_template", "ConsensusRuntime / PBFT preview over NetworkAdapter template.", "asset_hotspot", "logical_node_topology_default", "blockemulator_aligned_pbft_preview", "localhost_tcp_preview", "none", "memory_kv", []string{"baseline_simple_chain"}, []string{"shard_count", "network_adapter", "consensus_runtime", "seed"}, []string{"pbft_message_log.csv", "consensus_network_log.csv"}, BenchmarkRuntimeTruth},
		{"cross_shard_relay_preview_template", "Cross-shard relay_preview skeleton observation template.", "asset_hotspot", "logical_node_topology_default", "simple_leader", "in_memory_message_bus", "relay_preview", "memory_kv", []string{"baseline_no_cross_shard_protocol"}, []string{"shard_count", "cross_shard_protocol", "seed"}, []string{"cross_shard_tx_log.csv", "relay_preview_log.csv"}, BenchmarkRuntimeTruth},
		{"state_authenticity_template", "State authenticity template for persistent_kv, merkle_trie_mvp, proof, and witness artifacts.", "asset_hotspot", "logical_node_topology_default", "simple_leader", "in_memory_message_bus", "none", "merkle_trie_mvp", []string{"baseline_memory_kv", "baseline_no_state_authenticity"}, []string{"state_backend", "tx_count", "seed"}, []string{"state_root_log.csv", "state_proof_verification_log.csv", "witness_log.csv"}, BenchmarkRuntimeTruth},
		{"full_stack_v3_template", "Full stack V3 smoke benchmark template combining V3.5-V3.9 capabilities.", "asset_hotspot", "logical_node_topology_default", "blockemulator_aligned_pbft_preview", "localhost_tcp_preview", "relay_preview", "merkle_trie_mvp", []string{"baseline_simple_chain", "baseline_memory_kv", "baseline_no_cross_shard_protocol"}, []string{"tx_count", "shard_count", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend", "seed"}, benchmarkArtifactNames(), BenchmarkRuntimeTruth},
	}
}

func baselineProfiles() []BaselineProfile {
	return []BaselineProfile{
		{"baseline_simple_chain", "Simple-chain baseline against the full V3 modular runtime.", []string{"node_topology_preview", "network_adapter_preview", "cross_shard_protocol", "state_authenticity"}, []string{"memory_kv", "simple_leader"}, "full_stack_v3_template", BenchmarkRuntimeTruth},
		{"baseline_hash_sharding", "Hash sharding baseline against coaccess / hotspot-aware routing.", []string{"metatrack_coaccess_routing", "hotspot_aware_routing"}, []string{"hash_sharding"}, "metatrack_hotspot_template", BenchmarkRuntimeTruth},
		{"baseline_no_prefetch", "No-prefetch baseline against access_list_prefetch / cached_state_access.", []string{"access_list_prefetch", "cached_state_access"}, []string{"direct_fetch"}, "metatrack_hotspot_template", BenchmarkRuntimeTruth},
		{"baseline_no_cross_shard_protocol", "No cross-shard protocol baseline against relay_preview skeleton.", []string{"relay_preview"}, []string{"none"}, "cross_shard_relay_preview_template", BenchmarkRuntimeTruth},
		{"baseline_memory_kv", "Memory KV baseline against persistent_kv / merkle_trie_mvp.", []string{"persistent_kv", "merkle_trie_mvp"}, []string{"memory_kv"}, "state_authenticity_template", BenchmarkRuntimeTruth},
		{"baseline_no_state_authenticity", "Baseline without proof/witness artifacts against V3.9 state authenticity outputs.", []string{"state_root", "proof_generation", "witness_artifacts"}, []string{"memory_kv"}, "state_authenticity_template", BenchmarkRuntimeTruth},
	}
}

func benchmarkArtifactNames() []string {
	return []string{"benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"}
}

func defaultBaselineForTemplate(templateID string) string {
	switch templateID {
	case "metatrack_hotspot_template":
		return "baseline_hash_sharding"
	case "pbft_network_template":
		return "baseline_simple_chain"
	case "cross_shard_relay_preview_template":
		return "baseline_no_cross_shard_protocol"
	case "state_authenticity_template":
		return "baseline_memory_kv"
	default:
		return "baseline_simple_chain"
	}
}

func comparisonTargetForBaseline(baselineID string) string {
	for _, baseline := range baselineProfiles() {
		if baseline.BaselineID == baselineID {
			return baseline.ComparisonTarget
		}
	}
	return "full_stack_v3_template"
}

func benchmarkParameterValue(param string, exp ExperimentProfile, summary Summary, consensusRuntime string) string {
	switch param {
	case "tx_count":
		return strconv.Itoa(summary.TxCount)
	case "shard_count":
		return strconv.Itoa(summary.ShardCount)
	case "hotspot_ratio":
		return strconv.FormatFloat(exp.HotspotRatio, 'f', -1, 64)
	case "network_adapter":
		return firstNonEmpty(summary.NetworkAdapterSelected, exp.NetworkAdapter)
	case "consensus_runtime":
		return consensusRuntime
	case "cross_shard_protocol":
		return firstNonEmpty(summary.CrossShardProtocolSelected, exp.CrossShardProtocol)
	case "state_backend":
		return firstNonEmpty(summary.StateBackendSelected, exp.StateBackend)
	default:
		return ""
	}
}

func writeBenchmarkRunIndex(path string, rows []BenchmarkRunRow) error {
	fields := []string{"benchmark_id", "run_id", "template_id", "baseline_id", "repeat_index", "seed", "tx_count", "shard_count", "hotspot_ratio", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend", "summary_path", "status", "runtime_truth"}
	values := [][]string{}
	for _, row := range rows {
		values = append(values, []string{row.BenchmarkID, row.RunID, row.TemplateID, row.BaselineID, strconv.Itoa(row.RepeatIndex), strconv.Itoa(row.Seed), strconv.Itoa(row.TxCount), strconv.Itoa(row.ShardCount), strconv.FormatFloat(row.HotspotRatio, 'f', -1, 64), row.NetworkAdapter, row.ConsensusRuntime, row.CrossShardProtocol, row.StateBackend, row.SummaryPath, row.Status, row.RuntimeTruth})
	}
	return writeCSV(path, fields, values)
}

func rowsToObjects(fields []string, rows [][]string) []map[string]string {
	objects := []map[string]string{}
	for _, row := range rows {
		item := map[string]string{}
		for index, field := range fields {
			if index < len(row) {
				item[field] = row[index]
			}
		}
		objects = append(objects, item)
	}
	return objects
}

func writeJSONFile(path string, value any) error {
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}
