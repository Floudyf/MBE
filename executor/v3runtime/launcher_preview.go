package v3runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type LauncherPreview struct {
	Addresses []NodeAddressEntry
}

type NodeAddressEntry struct {
	NodeID               string `json:"node_id"`
	ShardID              int    `json:"shard_id"`
	NodeIndex            int    `json:"node_index"`
	Role                 string `json:"role"`
	LogicalAddress       string `json:"logical_address"`
	PreviewHost          string `json:"preview_host"`
	PreviewPort          int    `json:"preview_port"`
	ProcessName          string `json:"process_name"`
	LaunchCommandWindows string `json:"launch_command_windows"`
	LaunchCommandLinux   string `json:"launch_command_linux"`
	RuntimeMode          string `json:"runtime_mode"`
	NetworkMode          string `json:"network_mode"`
	NetworkAdapter       string `json:"network_adapter"`
	Status               string `json:"status"`
}

func BuildLauncherPreview(nodeRuntime NodeRuntimeArtifacts) LauncherPreview {
	addresses := make([]NodeAddressEntry, 0, len(nodeRuntime.Nodes))
	for i, node := range nodeRuntime.Nodes {
		port := 9100 + i
		processName := "mbe_" + node.NodeID
		windowsCommand := fmt.Sprintf("start cmd /k go run ./cmd/replay --mode node-preview --node-id %s --shard-id %d --role %s --topology-file topology.json --output-dir . --preview-only", node.NodeID, node.ShardID, node.Role)
		linuxCommand := fmt.Sprintf("go run ./cmd/replay --mode node-preview --node-id %s --shard-id %d --role %s --topology-file topology.json --output-dir . --preview-only &", node.NodeID, node.ShardID, node.Role)
		addresses = append(addresses, NodeAddressEntry{
			NodeID:               node.NodeID,
			ShardID:              node.ShardID,
			NodeIndex:            node.NodeIndex,
			Role:                 node.Role,
			LogicalAddress:       node.LogicalAddress,
			PreviewHost:          "127.0.0.1",
			PreviewPort:          port,
			ProcessName:          processName,
			LaunchCommandWindows: windowsCommand,
			LaunchCommandLinux:   linuxCommand,
			RuntimeMode:          node.RuntimeMode,
			NetworkMode:          node.NetworkMode,
			NetworkAdapter:       node.NetworkMode,
			Status:               "preview_only",
		})
	}
	return LauncherPreview{Addresses: addresses}
}

func writeLauncherPreviewArtifacts(out string, nodeRuntime NodeRuntimeArtifacts, launcher LauncherPreview) error {
	if err := writeNodeAddressTableCSV(filepath.Join(out, "node_address_table.csv"), launcher.Addresses); err != nil {
		return err
	}
	if err := writeTopologyJSON(filepath.Join(out, "topology.json"), nodeRuntime, launcher); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "launch_nodes_windows.bat"), []byte(windowsLauncherScript(launcher)), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "launch_nodes_linux.sh"), []byte(linuxLauncherScript(launcher)), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "launcher_readme.md"), []byte(launcherReadme()), 0o644)
}

func writeNodeAddressTableCSV(path string, addresses []NodeAddressEntry) error {
	fields := []string{"node_id", "shard_id", "node_index", "role", "logical_address", "preview_host", "preview_port", "process_name", "launch_command_windows", "launch_command_linux", "runtime_mode", "network_mode", "network_adapter", "status"}
	rows := [][]string{}
	for _, address := range addresses {
		rows = append(rows, []string{
			address.NodeID,
			strconv.Itoa(address.ShardID),
			strconv.Itoa(address.NodeIndex),
			address.Role,
			address.LogicalAddress,
			address.PreviewHost,
			strconv.Itoa(address.PreviewPort),
			address.ProcessName,
			address.LaunchCommandWindows,
			address.LaunchCommandLinux,
			address.RuntimeMode,
			address.NetworkMode,
			address.NetworkAdapter,
			address.Status,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeTopologyJSON(path string, nodeRuntime NodeRuntimeArtifacts, launcher LauncherPreview) error {
	payload := map[string]any{
		"stage":         "V3.6.1",
		"runtime_truth": "localhost_tcp_typed_message_preview_not_real_pbft",
		"topology": map[string]any{
			"shard_count":             nodeRuntime.Config.ShardCount,
			"validators_per_shard":    nodeRuntime.Config.ValidatorsPerShard,
			"executors_per_shard":     nodeRuntime.Config.ExecutorsPerShard,
			"storage_nodes_per_shard": nodeRuntime.Config.StorageNodesPerShard,
			"supervisor_enabled":      nodeRuntime.Config.SupervisorEnabled,
			"node_runtime_mode":       nodeRuntime.Config.NodeRuntimeMode,
			"network_mode":            nodeRuntime.Config.NetworkMode,
			"network_adapter":         nodeRuntime.Config.NetworkAdapter,
		},
		"derived": map[string]any{
			"logical_node_count":    len(nodeRuntime.Nodes),
			"validator_node_count":  nodeRuntime.CountRole("validator"),
			"executor_node_count":   nodeRuntime.CountRole("executor"),
			"storage_node_count":    nodeRuntime.CountRole("storage"),
			"supervisor_node_count": nodeRuntime.CountRole("supervisor"),
			"node_address_count":    len(launcher.Addresses),
		},
		"nodes": launcher.Addresses,
		"truth": map[string]bool{
			"tcp_preview_only":               true,
			"not_production_network":         true,
			"not_real_pbft":                  true,
			"not_blockemulator_backend":      true,
			"not_real_multi_process_runtime": true,
			"typed_message_preview_only":     true,
		},
	}
	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytes, 0o644)
}

func windowsLauncherScript(launcher LauncherPreview) string {
	lines := []string{
		"@echo off",
		"REM V3.6.1 local node process and NetworkAdapter preview only. Not real PBFT.",
		"REM Generated from logical node topology; commands start preview entry points only.",
	}
	for _, address := range launcher.Addresses {
		lines = append(lines, address.LaunchCommandWindows)
	}
	return joinLines(lines)
}

func linuxLauncherScript(launcher LauncherPreview) string {
	lines := []string{
		"#!/usr/bin/env bash",
		"set -euo pipefail",
		"# V3.6.1 local node process and NetworkAdapter preview only. Not real PBFT.",
		"# Generated from logical node topology; commands start preview entry points only.",
	}
	for _, address := range launcher.Addresses {
		lines = append(lines, address.LaunchCommandLinux)
	}
	lines = append(lines, "wait")
	return joinLines(lines)
}

func launcherReadme() string {
	return "# V3.6.1 NetworkAdapter Typed Message Preview\n\nThis launcher package is generated from logical node topology.\nThe commands target the local node process preview entry point: `go run ./cmd/replay --mode node-preview --node-id <node_id> --role <role> --topology-file topology.json --preview-only`.\nRun that command from the `executor/` module directory, or adjust the topology path to the generated artifact directory.\nThe scripts are preview artifacts only.\nThey may exercise localhost TCP typed message preview when `network_adapter=localhost_tcp_preview`.\nThey do not implement real PBFT/HotStuff/Raft.\nThey are not a production network.\nThey are not BlockEmulator backend.\nThe next stage is V3.6.2 Consensus-light over NetworkAdapter + V3.6 Closure.\n"
}

func joinLines(lines []string) string {
	text := ""
	for _, line := range lines {
		text += line + "\n"
	}
	return text
}
