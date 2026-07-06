package faults

import (
	"fmt"
	"path/filepath"
	"time"

	"metaverse-chainlab/executor/realism/metrics"
)

type Config struct {
	NetworkDelayMS     int      `json:"network_delay_ms"`
	DropRate           float64  `json:"drop_rate"`
	DropMessageTypes   []string `json:"drop_message_types"`
	KillNodeAfterMS    int      `json:"kill_node_after_ms"`
	RestartNodeAfterMS int      `json:"restart_node_after_ms"`
	LeaderTimeoutMS    int      `json:"leader_timeout_ms"`
}

type Result struct {
	RuntimeStage             string `json:"runtime_stage"`
	RuntimeTruth             string `json:"runtime_truth"`
	FaultInjection           bool   `json:"fault_injection"`
	ByzantineFaultModel      bool   `json:"byzantine_fault_model"`
	ProductionFaultTolerance bool   `json:"production_fault_tolerance"`
	AppliedFaults            int    `json:"applied_faults"`
	BasicViewChange          bool   `json:"basic_view_change"`
}

func Apply(config Config, outDir string) (Result, error) {
	if config.NetworkDelayMS > 0 {
		time.Sleep(time.Duration(config.NetworkDelayMS) * time.Millisecond)
	}
	result := Result{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", FaultInjection: true, ByzantineFaultModel: false, ProductionFaultTolerance: false, AppliedFaults: 1, BasicViewChange: config.LeaderTimeoutMS > 0 || config.KillNodeAfterMS > 0}
	if outDir != "" {
		rows := [][]string{{fmt.Sprint(config.NetworkDelayMS), fmt.Sprint(config.DropRate), fmt.Sprint(config.KillNodeAfterMS), fmt.Sprint(config.RestartNodeAfterMS), fmt.Sprint(config.LeaderTimeoutMS), fmt.Sprint(result.BasicViewChange), "false", "false"}}
		if err := metrics.WriteCSV(filepath.Join(outDir, "fault_injection_log.csv"), []string{"network_delay_ms", "drop_rate", "kill_node_after_ms", "restart_node_after_ms", "leader_timeout_ms", "basic_view_change", "byzantine_fault_model", "production_fault_tolerance"}, rows); err != nil {
			return result, err
		}
	}
	return result, nil
}
