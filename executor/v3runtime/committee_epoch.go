package v3runtime

import (
	"path/filepath"
	"strconv"
)

const CommitteeEpochTruth = "committee_epoch_mvp_not_secure_reconfiguration"

type CommitteeEpochRuntime struct {
	Enabled                   bool
	EpochCount                int
	ShardCount                int
	CommitteeCount            int
	ValidatorCount            int
	ExecutorCount             int
	StorageCount              int
	SupervisorCount           int
	ReconfigurationEventCount int
	CommitteeEpochTruth       string
	ShardRows                 [][]string
	CommitteeRows             [][]string
	EpochRows                 [][]string
	ReshardRows               [][]string
	ReconfigurationPlan       []map[string]any
}

func RunCommitteeEpochRuntime(nodeRuntime NodeRuntimeArtifacts, exp ExperimentProfile) CommitteeEpochRuntime {
	enabled := exp.EnableCommitteeEpoch
	epochCount := exp.EpochCount
	if epochCount <= 0 {
		epochCount = 1
	}
	if epochCount > 5 {
		epochCount = 5
	}
	result := CommitteeEpochRuntime{
		Enabled:             enabled,
		EpochCount:          epochCount,
		ShardCount:          max(1, nodeRuntime.Config.ShardCount),
		CommitteeCount:      max(1, nodeRuntime.Config.ShardCount),
		ValidatorCount:      nodeRuntime.CountRole("validator"),
		ExecutorCount:       nodeRuntime.CountRole("executor"),
		StorageCount:        nodeRuntime.CountRole("storage"),
		SupervisorCount:     nodeRuntime.CountRole("supervisor"),
		CommitteeEpochTruth: CommitteeEpochTruth,
	}
	if !enabled {
		return result
	}
	for epochID := 0; epochID < epochCount; epochID++ {
		transition := "genesis_noop"
		if epochID > 0 {
			transition = "round_robin_light_reconfiguration"
			result.ReconfigurationEventCount++
		}
		result.EpochRows = append(result.EpochRows, []string{strconv.Itoa(epochID), strconv.Itoa(epochID * 1000), strconv.Itoa((epochID+1)*1000 - 1), strconv.Itoa(result.ShardCount), strconv.Itoa(result.CommitteeCount), transition, "active", "deterministic epoch MVP"})
		for _, node := range nodeRuntime.Nodes {
			assignedShard := node.ShardID
			if epochID > 0 && node.Role == "validator" && result.ShardCount > 0 {
				assignedShard = (node.ShardID + epochID) % result.ShardCount
			}
			result.ShardRows = append(result.ShardRows, []string{strconv.Itoa(epochID), strconv.Itoa(assignedShard), node.NodeID, node.Role, "assigned", "deterministic local assignment"})
			if node.Role == "validator" {
				result.CommitteeRows = append(result.CommitteeRows, []string{strconv.Itoa(epochID), "committee-" + strconv.Itoa(assignedShard), strconv.Itoa(assignedShard), node.NodeID, "validator", "true", "deterministic committee MVP"})
			}
		}
		eventType := "noop"
		reason := "epoch_count=1 no-op reconfiguration"
		if epochID > 0 {
			eventType = "validator_round_robin"
			reason = "deterministic light reconfiguration plan; not secure random resharding"
		}
		result.ReshardRows = append(result.ReshardRows, []string{strconv.Itoa(epochID), eventType, strconv.Itoa(result.ShardCount), reason, CommitteeEpochTruth})
		result.ReconfigurationPlan = append(result.ReconfigurationPlan, map[string]any{
			"epoch_id":    epochID,
			"event_type":  eventType,
			"shard_count": result.ShardCount,
			"reason":      reason,
			"truth":       CommitteeEpochTruth,
		})
	}
	return result
}

func WriteCommitteeEpochArtifacts(out string, runtime CommitteeEpochRuntime) error {
	if !runtime.Enabled {
		return nil
	}
	if err := writeCSV(filepath.Join(out, "shard_assignment_log.csv"), []string{"epoch_id", "shard_id", "node_id", "role", "assignment_status", "reason"}, runtime.ShardRows); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "committee_assignment_log.csv"), []string{"epoch_id", "committee_id", "shard_id", "validator_node_id", "committee_role", "active", "reason"}, runtime.CommitteeRows); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "committee_summary.json"), map[string]any{
		"epoch_count":           runtime.EpochCount,
		"shard_count":           runtime.ShardCount,
		"committee_count":       runtime.CommitteeCount,
		"validator_count":       runtime.ValidatorCount,
		"executor_count":        runtime.ExecutorCount,
		"storage_count":         runtime.StorageCount,
		"supervisor_count":      runtime.SupervisorCount,
		"committee_truth":       runtime.CommitteeEpochTruth,
		"committee_epoch_truth": runtime.CommitteeEpochTruth,
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "epoch_log.csv"), []string{"epoch_id", "start_height", "end_height", "shard_count", "committee_count", "transition_type", "status", "reason"}, runtime.EpochRows); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "reconfiguration_plan.json"), runtime.ReconfigurationPlan); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "reshard_plan_log.csv"), []string{"epoch_id", "event_type", "shard_count", "reason", "truth"}, runtime.ReshardRows); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(out, "reconfiguration_summary.json"), map[string]any{
		"epoch_count":                    runtime.EpochCount,
		"reconfiguration_event_count":    runtime.ReconfigurationEventCount,
		"committee_epoch_truth":          runtime.CommitteeEpochTruth,
		"secure_random_reconfiguration":  false,
		"production_committee_lifecycle": false,
	})
}
