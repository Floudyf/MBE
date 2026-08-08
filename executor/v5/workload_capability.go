package v5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

const CompatibilityBlockedPrefix = "V5_COMPATIBILITY_BLOCKED:"

type WorkloadCapabilityReport struct {
	Status                    string  `json:"status"`
	BlockExecutorID           string  `json:"block_executor_id"`
	SourceType                string  `json:"source_type"`
	TopologyShardCount        int     `json:"topology_shard_count"`
	InspectedTransactionCount int     `json:"inspected_transaction_count"`
	ActualCrossShardCount     int     `json:"actual_cross_shard_count"`
	ActualCrossShardRatio     float64 `json:"actual_cross_shard_ratio"`
	SupportsMultiShard        bool    `json:"supports_multi_shard_deployment"`
	SupportsCrossShardTx      bool    `json:"supports_cross_shard_transaction"`
	Blocker                   string  `json:"blocker,omitempty"`
}

type WorkloadCapabilityError struct {
	Report WorkloadCapabilityReport
}

func (e *WorkloadCapabilityError) Error() string {
	return CompatibilityBlockedPrefix + " " + e.Report.Blocker
}

// PreflightWorkloadCapabilities reads the exact compiled workload with the
// same Go iterator, identity mapping and sharding plugin used by the client.
// It runs before node processes are created, so an unsupported workload cannot
// become a misleading completed-invalid experiment.
func PreflightWorkloadCapabilities(ctx context.Context, plan Plan, dataDir string) (WorkloadCapabilityReport, error) {
	report := WorkloadCapabilityReport{Status: "passed", SourceType: plan.WorkloadPlan.SourceType, SupportsMultiShard: true, SupportsCrossShardTx: true}
	if len(plan.NodeConfigs) == 0 {
		return report, fmt.Errorf("workload capability preflight requires node configs")
	}
	plugins, err := InstantiatePlugins(plan.NodeConfigs[0].PluginProfile)
	if err != nil {
		return report, err
	}
	report.BlockExecutorID = plugins.BlockExecutor.ID()
	shards := uniquePlanShardCount(plan.NodeConfigs)
	report.TopologyShardCount = shards
	iterator, err := plugins.Workload.NewIterator(plan.WorkloadPlan, shards, dataDir, plugins.Sharding)
	if err != nil {
		return report, err
	}
	defer iterator.Close()
	for {
		record, nextErr := iterator.Next(ctx)
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				break
			}
			return report, nextErr
		}
		report.InspectedTransactionCount++
		if record.CrossShard {
			report.ActualCrossShardCount++
		}
	}
	if report.InspectedTransactionCount > 0 {
		report.ActualCrossShardRatio = float64(report.ActualCrossShardCount) / float64(report.InspectedTransactionCount)
	}
	if strings.EqualFold(report.BlockExecutorID, groundhogBlockExecutorID) {
		report.SupportsCrossShardTx = false
		if report.ActualCrossShardCount > 0 {
			report.Status = "blocked_incompatible_workload"
			report.Blocker = fmt.Sprintf("Groundhog baseline received %d cross-shard transactions (ratio=%.6f); the reproduced typed-commutative snapshot executor currently supports multi-shard deployment with shard-local transactions only", report.ActualCrossShardCount, report.ActualCrossShardRatio)
			return report, &WorkloadCapabilityError{Report: report}
		}
	}
	return report, nil
}

func uniquePlanShardCount(nodes []NodePlan) int {
	seen := map[string]bool{}
	for _, node := range nodes {
		if node.ShardID != "" {
			seen[node.ShardID] = true
		}
	}
	if len(seen) == 0 {
		return 1
	}
	return len(seen)
}
