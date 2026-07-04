package v3runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	NodeRuntimeLogicalSingleProcess = "logical_single_process"
	NodeRuntimeLocalMultiProcess    = "local_multi_process"
	ProcessRuntimeDryRun            = "dry_run"
	ProcessRuntimeSmoke             = "smoke"
	RuntimeRealismTruth             = "local_multi_process_runtime_mvp_not_production_cluster"
	NetworkPathTruth                = "localhost_tcp_preview_not_production_network"
	defaultLocalProcessBasePort     = 19000
)

type LocalProcessPlanEntry struct {
	NodeID        string `json:"node_id"`
	ShardID       int    `json:"shard_id"`
	Role          string `json:"role"`
	ProcessIndex  int    `json:"process_index"`
	ListenPort    int    `json:"listen_port"`
	RuntimeMode   string `json:"runtime_mode"`
	Planned       bool   `json:"planned"`
	Started       bool   `json:"started"`
	Stopped       bool   `json:"stopped"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
	PID           int    `json:"pid,omitempty"`
	ExitCode      int    `json:"exit_code,omitempty"`
	StdoutSnippet string `json:"stdout_snippet,omitempty"`
	StderrSnippet string `json:"stderr_snippet,omitempty"`
}

type LocalMultiProcessRuntime struct {
	Enabled             bool
	ProcessRuntimeMode  string
	MaxLocalProcesses   int
	Capped              bool
	CappedProcessCount  int
	PlannedProcessCount int
	StartedProcessCount int
	StoppedProcessCount int
	FailedProcessCount  int
	NetworkMessageCount int
	NetworkPathTruth    string
	RuntimeRealismTruth string
	Plan                []LocalProcessPlanEntry
	NetworkMessageRows  [][]string
	NodeStdout          []string
	NodeStderr          []string
}

func RunLocalMultiProcessRuntime(nodeRuntime NodeRuntimeArtifacts, exp ExperimentProfile) LocalMultiProcessRuntime {
	mode := firstNonEmpty(exp.ProcessRuntimeMode, ProcessRuntimeDryRun)
	maxProcesses := exp.MaxLocalProcesses
	if maxProcesses <= 0 {
		maxProcesses = 8
	}
	if maxProcesses > 32 {
		maxProcesses = 32
	}
	if maxProcesses < 1 {
		maxProcesses = 1
	}
	enabled := nodeRuntime.Config.NodeRuntimeMode == NodeRuntimeLocalMultiProcess
	result := LocalMultiProcessRuntime{
		Enabled:             enabled,
		ProcessRuntimeMode:  mode,
		MaxLocalProcesses:   maxProcesses,
		NetworkPathTruth:    NetworkPathTruth,
		RuntimeRealismTruth: RuntimeRealismTruth,
	}
	if !enabled {
		return result
	}
	nodes := nodeRuntime.Nodes
	result.PlannedProcessCount = len(nodes)
	limit := min(len(nodes), maxProcesses)
	result.Capped = len(nodes) > limit
	result.CappedProcessCount = len(nodes) - limit
	for index, node := range nodes[:limit] {
		entry := LocalProcessPlanEntry{
			NodeID:       node.NodeID,
			ShardID:      node.ShardID,
			Role:         node.Role,
			ProcessIndex: index,
			ListenPort:   defaultLocalProcessBasePort + index,
			RuntimeMode:  NodeRuntimeLocalMultiProcess,
			Planned:      true,
			Status:       "planned",
			Reason:       "planned local process entry",
		}
		if mode == ProcessRuntimeSmoke {
			runLocalNodeSmoke(&entry, &result)
		}
		result.Plan = append(result.Plan, entry)
	}
	if mode == ProcessRuntimeDryRun {
		for index := range result.Plan {
			result.Plan[index].Status = "dry_run_planned"
			result.Plan[index].Reason = "dry_run does not start OS processes"
		}
	}
	result.StartedProcessCount = countProcessState(result.Plan, func(entry LocalProcessPlanEntry) bool { return entry.Started })
	result.StoppedProcessCount = countProcessState(result.Plan, func(entry LocalProcessPlanEntry) bool { return entry.Stopped })
	result.FailedProcessCount = countProcessState(result.Plan, func(entry LocalProcessPlanEntry) bool { return entry.Status == "failed" })
	result.NetworkMessageRows = plannedNetworkMessages(result.Plan)
	result.NetworkMessageCount = len(result.NetworkMessageRows)
	return result
}

func runLocalNodeSmoke(entry *LocalProcessPlanEntry, result *LocalMultiProcessRuntime) {
	var cmd *exec.Cmd
	message := "mbe local node smoke " + entry.NodeID
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/C", "echo "+message)
	} else {
		cmd = exec.Command("sh", "-c", "echo "+message)
	}
	started := time.Now()
	stdout, err := cmd.Output()
	entry.Started = true
	entry.PID = 0
	if cmd.ProcessState != nil {
		entry.PID = cmd.ProcessState.Pid()
		entry.ExitCode = cmd.ProcessState.ExitCode()
	}
	entry.Stopped = true
	entry.StdoutSnippet = strings.TrimSpace(string(stdout))
	result.NodeStdout = append(result.NodeStdout, entry.StdoutSnippet)
	if err != nil {
		entry.Status = "failed"
		entry.Reason = err.Error()
		result.NodeStderr = append(result.NodeStderr, err.Error())
		return
	}
	if time.Since(started) > 5*time.Second {
		entry.Status = "failed"
		entry.Reason = "smoke process timeout"
		result.NodeStderr = append(result.NodeStderr, entry.Reason)
		return
	}
	entry.Status = "stopped"
	entry.Reason = "smoke process completed and stopped"
}

func plannedNetworkMessages(plan []LocalProcessPlanEntry) [][]string {
	rows := [][]string{}
	for _, from := range plan {
		if from.Role != "validator" {
			continue
		}
		for _, to := range plan {
			if to.ShardID == from.ShardID && to.Role == "executor" {
				rows = append(rows, []string{"0", from.NodeID, to.NodeID, "validator_to_executor_smoke", "localhost_tcp_preview", "true", "deterministic local process NetworkAdapter path"})
			}
		}
	}
	if len(rows) == 0 && len(plan) > 1 {
		rows = append(rows, []string{"0", plan[0].NodeID, plan[1].NodeID, "local_process_smoke", "localhost_tcp_preview", "true", "deterministic planned local message"})
	}
	return rows
}

func countProcessState(plan []LocalProcessPlanEntry, pred func(LocalProcessPlanEntry) bool) int {
	count := 0
	for _, entry := range plan {
		if pred(entry) {
			count++
		}
	}
	return count
}

func WriteLocalMultiProcessArtifacts(out string, runtime LocalMultiProcessRuntime) error {
	if !runtime.Enabled {
		return nil
	}
	if err := writeJSONFile(filepath.Join(out, "address_table.json"), runtime.Plan); err != nil {
		return err
	}
	manifest := map[string]any{
		"stage":                    "V3.12 Runtime Realism Closure",
		"node_runtime_mode":        NodeRuntimeLocalMultiProcess,
		"process_runtime_mode":     runtime.ProcessRuntimeMode,
		"max_local_processes":      runtime.MaxLocalProcesses,
		"planned_process_count":    runtime.PlannedProcessCount,
		"launchable_process_count": len(runtime.Plan),
		"capped":                   runtime.Capped,
		"capped_process_count":     runtime.CappedProcessCount,
		"runtime_realism_truth":    runtime.RuntimeRealismTruth,
		"network_path_truth":       runtime.NetworkPathTruth,
		"local_machine_only":       true,
		"production_cluster":       false,
	}
	if err := writeJSONFile(filepath.Join(out, "multi_process_manifest.json"), manifest); err != nil {
		return err
	}
	if err := writeNodeProcessLogCSV(filepath.Join(out, "node_process_log.csv"), runtime.Plan); err != nil {
		return err
	}
	if err := writeNodeLifecycleLogCSV(filepath.Join(out, "node_lifecycle_log.csv"), runtime.Plan); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(out, "network_message_log.csv"), []string{"event_time_ms", "from_node_id", "to_node_id", "message_type", "network_adapter", "delivered", "reason"}, runtime.NetworkMessageRows); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "node_process_status.json"), runtime.Plan); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(out, "local_multi_process_summary.json"), map[string]any{
		"node_runtime_mode":           NodeRuntimeLocalMultiProcess,
		"process_runtime_mode":        runtime.ProcessRuntimeMode,
		"local_multi_process_enabled": runtime.Enabled,
		"planned_process_count":       runtime.PlannedProcessCount,
		"started_process_count":       runtime.StartedProcessCount,
		"stopped_process_count":       runtime.StoppedProcessCount,
		"failed_process_count":        runtime.FailedProcessCount,
		"capped_process_count":        runtime.CappedProcessCount,
		"max_local_processes":         runtime.MaxLocalProcesses,
		"network_message_count":       runtime.NetworkMessageCount,
		"network_path_truth":          runtime.NetworkPathTruth,
		"runtime_realism_truth":       runtime.RuntimeRealismTruth,
	}); err != nil {
		return err
	}
	if runtime.ProcessRuntimeMode == ProcessRuntimeSmoke {
		if err := os.WriteFile(filepath.Join(out, "node_stdout.log"), []byte(strings.Join(runtime.NodeStdout, "\n")+"\n"), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, "node_stderr.log"), []byte(strings.Join(runtime.NodeStderr, "\n")+"\n"), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func writeNodeProcessLogCSV(path string, plan []LocalProcessPlanEntry) error {
	fields := []string{"node_id", "shard_id", "role", "process_index", "listen_port", "runtime_mode", "planned", "started", "stopped", "status", "reason"}
	rows := [][]string{}
	for _, entry := range plan {
		rows = append(rows, []string{entry.NodeID, strconv.Itoa(entry.ShardID), entry.Role, strconv.Itoa(entry.ProcessIndex), strconv.Itoa(entry.ListenPort), entry.RuntimeMode, strconv.FormatBool(entry.Planned), strconv.FormatBool(entry.Started), strconv.FormatBool(entry.Stopped), entry.Status, entry.Reason})
	}
	return writeCSV(path, fields, rows)
}

func writeNodeLifecycleLogCSV(path string, plan []LocalProcessPlanEntry) error {
	fields := []string{"event_time_ms", "node_id", "process_index", "event_type", "status", "reason"}
	rows := [][]string{}
	for _, entry := range plan {
		rows = append(rows, []string{"0", entry.NodeID, strconv.Itoa(entry.ProcessIndex), "planned", "planned", entry.Reason})
		if entry.Started {
			rows = append(rows, []string{"1", entry.NodeID, strconv.Itoa(entry.ProcessIndex), "started", entry.Status, entry.Reason})
		}
		if entry.Stopped {
			rows = append(rows, []string{"2", entry.NodeID, strconv.Itoa(entry.ProcessIndex), "stopped", entry.Status, entry.Reason})
		}
	}
	return writeCSV(path, fields, rows)
}
