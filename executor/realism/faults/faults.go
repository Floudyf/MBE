package faults

import (
	"crypto/sha256"
	"encoding/binary"
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

type Policy struct {
	Enabled            bool     `json:"enabled"`
	Seed               int64    `json:"seed"`
	DelayMS            int      `json:"delay_ms"`
	DropRate           float64  `json:"drop_rate"`
	DropMessageTypes   []string `json:"drop_message_types"`
	TargetPeerIDs      []string `json:"target_peer_ids"`
	KillNodeAfterMS    int      `json:"kill_node_after_ms"`
	RestartNodeAfterMS int      `json:"restart_node_after_ms"`
}

type Decision struct {
	Delay      time.Duration
	Drop       bool
	Reason     string
	FaultEvent bool
}

func (p Policy) Decide(direction, peerID, messageType, messageID string) Decision {
	if !p.Enabled {
		return Decision{}
	}
	if len(p.TargetPeerIDs) > 0 && !contains(p.TargetPeerIDs, peerID) {
		return Decision{}
	}
	decision := Decision{}
	if p.DelayMS > 0 {
		decision.Delay = time.Duration(p.DelayMS) * time.Millisecond
		decision.FaultEvent = true
		decision.Reason = "network_delay"
	}
	if contains(p.DropMessageTypes, messageType) {
		decision.Drop = true
		decision.FaultEvent = true
		decision.Reason = "message_type_drop"
		return decision
	}
	if p.DropRate > 0 && deterministicDrop(p.Seed, direction, peerID, messageType, messageID, p.DropRate) {
		decision.Drop = true
		decision.FaultEvent = true
		decision.Reason = "drop_rate"
	}
	return decision
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

func contains(items []string, needle string) bool {
	for _, item := range items {
		if item == needle {
			return true
		}
	}
	return false
}

func deterministicDrop(seed int64, direction, peerID, messageType, messageID string, rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s:%s:%s", seed, direction, peerID, messageType, messageID)))
	value := binary.BigEndian.Uint64(sum[:8])
	return float64(value%1_000_000)/1_000_000.0 < rate
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
