package v3runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommitteeEpochOneEpochWritesNoopReconfiguration(t *testing.T) {
	nodeRuntime := BuildLogicalNodeArtifacts(DefaultNodeTopologyConfig(), nil, nil)
	preview := RunCommitteeEpochRuntime(nodeRuntime, ExperimentProfile{EnableCommitteeEpoch: true, EpochCount: 1})
	if !preview.Enabled || preview.EpochCount != 1 || preview.ReconfigurationEventCount != 0 {
		t.Fatalf("epoch_count=1 should generate no-op reconfiguration: %+v", preview)
	}
	if len(preview.CommitteeRows) == 0 || preview.CommitteeEpochTruth != CommitteeEpochTruth {
		t.Fatalf("missing committee assignment/truth: %+v", preview)
	}
	dir := t.TempDir()
	if err := WriteCommitteeEpochArtifacts(dir, preview); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shard_assignment_log.csv", "committee_assignment_log.csv", "committee_summary.json", "epoch_log.csv", "reconfiguration_plan.json", "reshard_plan_log.csv", "reconfiguration_summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
}

func TestCommitteeEpochMultipleEpochsIsDeterministicAndReconfigures(t *testing.T) {
	cfg := DefaultNodeTopologyConfig()
	cfg.ShardCount = 3
	nodeRuntime := BuildLogicalNodeArtifacts(cfg, nil, nil)
	first := RunCommitteeEpochRuntime(nodeRuntime, ExperimentProfile{EnableCommitteeEpoch: true, EpochCount: 3})
	second := RunCommitteeEpochRuntime(nodeRuntime, ExperimentProfile{EnableCommitteeEpoch: true, EpochCount: 3})
	if first.ReconfigurationEventCount != 2 || len(first.ReshardRows) != 3 {
		t.Fatalf("unexpected reconfiguration plan: %+v", first)
	}
	if len(first.CommitteeRows) != len(second.CommitteeRows) || first.CommitteeRows[0][3] != second.CommitteeRows[0][3] {
		t.Fatalf("committee assignment should be deterministic: %+v vs %+v", first.CommitteeRows, second.CommitteeRows)
	}
}
