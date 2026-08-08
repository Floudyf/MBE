package v5

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestGroundhogWorkloadPreflightBlocksExactCrossShardTransactionsBeforeLaunch(t *testing.T) {
	plan := groundhogCapabilityTestPlan(0.4)
	report, err := PreflightWorkloadCapabilities(context.Background(), plan, t.TempDir())
	var compatibilityErr *WorkloadCapabilityError
	if !errors.As(err, &compatibilityErr) {
		t.Fatalf("expected Groundhog workload capability error, got report=%#v err=%v", report, err)
	}
	if report.Status != "blocked_incompatible_workload" || report.ActualCrossShardCount != 4 {
		t.Fatalf("unexpected exact preflight report: %#v", report)
	}
	if report.SupportsCrossShardTx || !report.SupportsMultiShard {
		t.Fatalf("Groundhog capability boundary is wrong: %#v", report)
	}
	if !strings.HasPrefix(err.Error(), CompatibilityBlockedPrefix) {
		t.Fatalf("compatibility error must remain machine-detectable: %v", err)
	}
}

func TestGroundhogWorkloadPreflightAllowsMultiShardDeploymentForShardLocalTransactions(t *testing.T) {
	plan := groundhogCapabilityTestPlan(0)
	report, err := PreflightWorkloadCapabilities(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "passed" || report.TopologyShardCount != 2 || report.ActualCrossShardCount != 0 {
		t.Fatalf("unexpected shard-local Groundhog preflight report: %#v", report)
	}
	if !report.SupportsMultiShard || report.SupportsCrossShardTx {
		t.Fatalf("Groundhog should support multi-shard deployment but not cross-shard transactions: %#v", report)
	}
}

func groundhogCapabilityTestPlan(crossShardRatio float64) Plan {
	profile := groundhogTestProfile()
	return Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		WorkloadPlan: WorkloadPlan{
			PluginID:        "deterministic_signed_synthetic",
			SourceType:      "synthetic",
			TxCount:         10,
			Seed:            7,
			CrossShardRatio: crossShardRatio,
			NoFallback:      true,
		},
		NodeConfigs: []NodePlan{
			{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0"}, PluginProfile: profile},
			{NodeID: "n1", ShardID: "s1", Leader: true, Validators: []string{"n1"}, PluginProfile: profile},
		},
	}
}
