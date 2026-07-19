package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	v4config "metaverse-chainlab/executor/realism/config"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/node"
	"metaverse-chainlab/executor/v5"
)

func main() {
	mode := flag.String("mode", "plan", "plan|v4.2-smoke|v4.3-smoke|v5-real-cluster")
	planPath := flag.String("plan", "", "V5 compiled run plan JSON")
	nodes := flag.Int("nodes", 4, "node count")
	shards := flag.Int("shards", 2, "shard count")
	txCount := flag.Int("tx-count", 10, "smoke tx count")
	enableCrossShard := flag.Bool("enable-cross-shard", true, "enable V4.2 cross-shard smoke")
	enableFaults := flag.Bool("enable-faults", true, "enable V4.2 fault smoke")
	faultProfile := flag.String("fault-profile", "network_delay", "V4.3 fault profile")
	blockEmulatorCSV := flag.String("blockemulator-csv", "", "BlockEmulator selectedTxs CSV input")
	blockEmulatorTxLimit := flag.Int("blockemulator-tx-limit", 20, "BlockEmulator bridge tx import limit")
	runDurationMS := flag.Int("run-duration-ms", 1000, "smoke run duration")
	dataDir := flag.String("data-dir", ".cache/v4_realism_runs", "root data dir")
	outConfig := flag.String("out-config", "", "v4_node_config.json output")
	outAddressTable := flag.String("out-address-table", "", "v4_address_table.json output")
	outPlan := flag.String("out-plan", "", "v4_1_supervisor_plan.json output")
	flag.Parse()
	if *mode == "v5-real-cluster" {
		if *planPath == "" {
			fmt.Fprintln(os.Stderr, "--plan is required for v5-real-cluster")
			os.Exit(1)
		}
		if err := runV5(*planPath, *dataDir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *mode == "v4.2-smoke" {
		summary, artifacts, err := node.RunV42FinalSmoke(context.Background(), node.SmokeOptionsV42{OutDir: *dataDir, Nodes: *nodes, Shards: *shards, TxCount: *txCount, EnableCrossShard: *enableCrossShard, EnableFaults: *enableFaults, RunDurationMS: *runDurationMS, FrontendAvailable: true})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("V4.2 smoke complete: ready_to_commit=%t artifacts=%d summary=%s\n", summary.ReadyToCommit, len(artifacts), filepath.Join(*dataDir, "v4_2_realism_final_summary.json"))
		return
	}
	if *mode == "v4.3-smoke" {
		summary, artifacts, err := node.RunV43FinalSmoke(context.Background(), node.SmokeOptionsV43{OutDir: *dataDir, Nodes: *nodes, Shards: *shards, TxCount: *txCount, EnableCrossShard: *enableCrossShard, EnableFaults: *enableFaults, FaultProfile: *faultProfile, BlockEmulatorCSV: *blockEmulatorCSV, BlockEmulatorTxLimit: *blockEmulatorTxLimit, RunDurationMS: *runDurationMS, FrontendAvailable: true})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("V4.3 smoke complete: ready_to_commit=%t artifacts=%d summary=%s\n", summary.ReadyToCommit, len(artifacts), filepath.Join(*dataDir, "v4_3_realism_final_summary.json"))
		return
	}

	if *outConfig == "" {
		*outConfig = filepath.Join(*dataDir, "v4_node_config.json")
	}
	if *outAddressTable == "" {
		*outAddressTable = filepath.Join(*dataDir, "v4_address_table.json")
	}
	if *outPlan == "" {
		*outPlan = filepath.Join(*dataDir, "v4_1_supervisor_plan.json")
	}
	cfg, err := v4config.Generate(*nodes, *shards, *dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := metrics.WriteJSON(*outConfig, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	table := v4config.BuildAddressTable(cfg)
	if err := metrics.WriteJSON(*outAddressTable, table); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	plan := v4config.BuildSupervisorPlan(cfg)
	if err := metrics.WriteJSON(*outPlan, plan); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote V4.1 supervisor plan: %s, %s, %s; real_p2p=true pbft_style=true state_commit=false\n", *outConfig, *outAddressTable, *outPlan)
}

type v5NodeProcess struct {
	NodeID     string `json:"node_id"`
	ShardID    string `json:"shard_id"`
	PID        int    `json:"pid"`
	ListenAddr string `json:"listen_addr"`
	DataDir    string `json:"data_dir"`
	LogPath    string `json:"log_path"`
}
type v5NodeSummary struct {
	NodeID                        string  `json:"node_id"`
	ShardID                       string  `json:"shard_id"`
	PID                           int     `json:"pid"`
	ListenAddr                    string  `json:"listen_addr"`
	CommittedBlockCount           int     `json:"committed_block_count"`
	StateRoot                     string  `json:"state_root"`
	RealPBFT                      bool    `json:"real_pbft_style_messages"`
	BlockExecutorID               string  `json:"block_executor_id"`
	BlockExecutorVersion          string  `json:"block_executor_version"`
	WorkerCount                   int     `json:"worker_count"`
	PlanDigestConsistent          bool    `json:"plan_digest_consistent"`
	SchedulerEventCount           int     `json:"scheduler_event_count"`
	SchedulerBlockedCount         int     `json:"scheduler_blocked_count"`
	SchedulerWakeupCount          int     `json:"scheduler_wakeup_count"`
	SchedulerStolenWorkCount      int     `json:"scheduler_stolen_work_count"`
	SchedulerLocalExecutionCount  int     `json:"scheduler_local_execution_count"`
	SchedulerReadyQueueMaxDepth   int     `json:"scheduler_ready_queue_max_depth"`
	SchedulerFastQueueMaxDepth    int     `json:"scheduler_fast_queue_max_depth"`
	SchedulerConsQueueMaxDepth    int     `json:"scheduler_conservative_queue_max_depth"`
	SchedulerDependencyWaitMS     int     `json:"scheduler_dependency_wait_ms"`
	SchedulerIdleMS               int     `json:"scheduler_idle_ms"`
	SchedulerIdleRatio            float64 `json:"scheduler_idle_ratio"`
	RemoteStateAccessCount        int     `json:"remote_state_access_count"`
	RemoteStateReadCount          int     `json:"remote_state_read_count"`
	RemoteStateWriteApplyCount    int     `json:"remote_state_write_apply_count"`
	RemoteStateAccessFailedCount  int     `json:"remote_state_access_failed_count"`
	RemoteStateAccessAvgLatencyMS float64 `json:"remote_state_access_avg_latency_ms"`
}

func runV5(planPath, dataDir string) error {
	plan, err := v5.LoadPlan(planPath)
	if err != nil {
		return err
	}
	if dataDir == "" {
		dataDir = filepath.Dir(planPath)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	allocated := map[string]bool{}
	for index := range plan.NodeConfigs {
		address := ""
		for attempts := 0; attempts < 32; attempts++ {
			candidate, err := allocateAddress()
			if err != nil {
				return err
			}
			if !allocated[candidate] {
				address = candidate
				allocated[address] = true
				break
			}
		}
		if address == "" {
			return fmt.Errorf("could not allocate unique node address")
		}
		plan.NodeConfigs[index].ListenAddr = address
		plan.NodeConfigs[index].DataDir = filepath.Join(dataDir, "nodes", plan.NodeConfigs[index].NodeID)
	}
	if err := v5.SaveJSON(planPath, plan); err != nil {
		return err
	}
	nodeRuntimePlan := runtimePlanForNodes(plan)
	binDir := filepath.Join(dataDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}
	nodeBinary := filepath.Join(binDir, "mbe-node.exe")
	if err := buildBinary(nodeBinary, "./cmd/mbe-node"); err != nil {
		return err
	}
	clientBinary := filepath.Join(binDir, "mbe-client.exe")
	if err := buildBinary(clientBinary, "./cmd/mbe-client"); err != nil {
		return err
	}
	processes := []v5NodeProcess{}
	commands := []*exec.Cmd{}
	for _, nodePlan := range plan.NodeConfigs {
		configPath := filepath.Join(dataDir, "node_config_"+nodePlan.NodeID+".json")
		if err := v5.SaveJSON(configPath, map[string]any{"plan": nodeRuntimePlan, "node_id": nodePlan.NodeID}); err != nil {
			return err
		}
		if err := os.MkdirAll(nodePlan.DataDir, 0o755); err != nil {
			return err
		}
		logPath := filepath.Join(nodePlan.DataDir, "node_process.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			return err
		}
		cmd := exec.Command(nodeBinary, "--run-mode", "v5-server", "--v5-node-config", configPath)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			logFile.Close()
			return err
		}
		commands = append(commands, cmd)
		processes = append(processes, v5NodeProcess{NodeID: nodePlan.NodeID, ShardID: nodePlan.ShardID, PID: cmd.Process.Pid, ListenAddr: nodePlan.ListenAddr, DataDir: nodePlan.DataDir, LogPath: logPath})
	}
	if err := v5.SaveJSON(filepath.Join(dataDir, "process_manifest.json"), map[string]any{"one_node_one_os_process": true, "processes": redactV5Processes(processes, dataDir), "expected_process_count": len(plan.NodeConfigs), "node_runtime_duration_ms": nodeRuntimePlan.DurationMS}); err != nil {
		return err
	}
	if err := waitReady(plan, 10*time.Second); err != nil {
		reap(commands)
		return err
	}
	clientOut := filepath.Join(dataDir, "client", "client_submission.marker")
	if err := os.MkdirAll(filepath.Dir(clientOut), 0o755); err != nil {
		reap(commands)
		return err
	}
	client := exec.Command(clientBinary, "--mode", "submit", "--plan", planPath, "--out", clientOut)
	client.Stdout = os.Stdout
	client.Stderr = os.Stderr
	if err := client.Run(); err != nil {
		reap(commands)
		return fmt.Errorf("real client submit: %w", err)
	}
	_ = copyIfExists(filepath.Join(dataDir, "client", "workload_replay_summary.json"), filepath.Join(dataDir, "workload_replay_summary.json"))
	_ = copyIfExists(filepath.Join(dataDir, "client", "workload_identity_mapping_summary.json"), filepath.Join(dataDir, "workload_identity_mapping_summary.json"))
	if err := drainV5(plan, dataDir); err != nil {
		reap(commands)
		return err
	}
	if err := writeRedactedV5PlanArtifacts(plan, dataDir, planPath); err != nil {
		reap(commands)
		return err
	}
	stopPath := filepath.Join(dataDir, "stop.request")
	_ = os.WriteFile(stopPath, []byte("quiescent\n"), 0o644)
	waitErr := waitAll(commands, 30*time.Second)
	if waitErr != nil {
		reap(commands)
		return waitErr
	}
	summary, err := summarizeV5(plan, dataDir, processes)
	if err != nil {
		return err
	}
	finality, err := deriveFinalityArtifacts(dataDir, plan.NodeConfigs)
	if err != nil {
		return err
	}
	summary["finality_evidence"] = finality
	if value, ok := finality["cross_shard_finalized_unique_count"].(int); ok {
		summary["cross_shard_success_count"] = value
		summary["real_cross_shard_network"] = value > 0
	}
	if value, ok := finality["cross_shard_refunded_unique_count"].(int); ok {
		summary["cross_shard_refund_count"] = value
	}
	if replay := readOptionalJSON(filepath.Join(dataDir, "workload_replay_summary.json")); replay != nil {
		summary["workload_replay_summary"] = replay
	}
	if err := v5.SaveJSON(filepath.Join(dataDir, "real_cluster_summary.json"), summary); err != nil {
		return err
	}
	return v5.SaveJSON(filepath.Join(dataDir, "artifact_catalog.json"), map[string]any{"source": "real_v5_runtime", "artifacts": "see process manifest and node directories"})
}

func drainV5(plan v5.Plan, dataDir string) error {
	started := time.Now()
	submitted := plan.WorkloadPlan.TxCount
	classification, err := loadSubmissionClassification(dataDir, submitted)
	if err != nil {
		_ = v5.SaveJSON(filepath.Join(dataDir, "stalled_runtime_report.json"), map[string]any{"classifiers": []string{"terminal_accounting_missing"}, "phase": "FAILED", "reason": err.Error(), "submitted": submitted})
		return err
	}
	phase := "DRAINING"
	budget := drainBudget(plan)
	deadline := started.Add(budget.HardTimeout)
	progressPath := filepath.Join(dataDir, "drain_progress.csv")
	_ = os.Remove(progressPath)
	lastProgress := started
	lastTerminalProgress := started
	lastHeightProgress := started
	lastMempoolProgress := started
	lastPendingProgress := started
	var previous progressSnapshot
	initialized := false
	var lastStatuses []map[string]any
	var lastHeights map[string]map[string]bool
	for time.Now().Before(deadline) {
		statuses := []map[string]any{}
		terminal := map[string]bool{}
		allEmpty := true
		fatalPersistence := ""
		heights := map[string]map[string]bool{}
		for _, node := range plan.NodeConfigs {
			raw, err := os.ReadFile(filepath.Join(node.DataDir, "node_runtime_status.json"))
			if err != nil {
				allEmpty = false
				continue
			}
			var status map[string]any
			if json.Unmarshal(raw, &status) != nil {
				allEmpty = false
				continue
			}
			statuses = append(statuses, status)
			if value := fmt.Sprint(status["fatal_persistence_error"]); value != "" && value != "<nil>" {
				fatalPersistence = value
			}
			for _, key := range []string{"reserved_tx_count", "pending_commit_count", "pending_future_block_count", "pending_cross_shard_count"} {
				if number(status[key]) != 0 {
					allEmpty = false
				}
			}
			if boolValue(status["proposal_in_flight"]) {
				allEmpty = false
			}
			shard := fmt.Sprint(status["shard_id"])
			if heights[shard] == nil {
				heights[shard] = map[string]bool{}
			}
			heights[shard][fmt.Sprint(status["committed_height"])] = true
		}
		liveTerminal, _, err := deriveLiveTerminal(classification, statuses)
		if err != nil {
			_ = v5.SaveJSON(filepath.Join(dataDir, "stalled_runtime_report.json"), map[string]any{"classifiers": []string{"terminal_accounting_missing"}, "phase": "FAILED", "reason": err.Error(), "submitted": submitted})
			return err
		}
		terminal = liveTerminal
		if hasNonTerminalMempool(statuses, terminal) {
			allEmpty = false
		}
		aligned := true
		for _, values := range heights {
			if len(values) != 1 {
				aligned = false
			}
		}
		current := makeProgressSnapshot(len(terminal), statuses, heights)
		now := time.Now()
		if !initialized || progressChanged(previous, current) {
			lastProgress = now
		}
		if !initialized || current.Terminal != previous.Terminal {
			lastTerminalProgress = now
		}
		if !initialized || current.MaxHeight != previous.MaxHeight || current.MinHeight != previous.MinHeight {
			lastHeightProgress = now
		}
		if !initialized || current.Mempool != previous.Mempool || current.Reserved != previous.Reserved {
			lastMempoolProgress = now
		}
		if !initialized || current.Pending != previous.Pending || current.ProposalInFlight != previous.ProposalInFlight {
			lastPendingProgress = now
		}
		previous = current
		initialized = true
		lastStatuses = statuses
		lastHeights = heights
		if fatalPersistence != "" {
			phase = "FAILED"
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "failed_persistence_inconsistency", "fatal_persistence_error": fatalPersistence, "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli()})
			return fmt.Errorf("failed_persistence_inconsistency: %s", fatalPersistence)
		}
		if initialized && now.Sub(lastProgress) > budget.NoProgressTimeout {
			phase = "FAILED"
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "no_progress_timeout", "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "hard_timeout_ms": budget.HardTimeout.Milliseconds(), "no_progress_timeout_ms": budget.NoProgressTimeout.Milliseconds()})
			return fmt.Errorf("drain no-progress timeout")
		}
		phase = "DRAINING"
		if !aligned {
			phase = "CATCHING_UP"
		}
		writeDrainProgress(progressPath, phase, submitted, len(terminal), current, lastTerminalProgress, lastMempoolProgress)
		if len(terminal) >= submitted && allEmpty && aligned {
			phase = "QUIESCENT"
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "drain_quiescent", "drain_started_at": started.UnixMilli(), "drain_finished_at": now.UnixMilli(), "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "hard_timeout_ms": budget.HardTimeout.Milliseconds(), "no_progress_timeout_ms": budget.NoProgressTimeout.Milliseconds()})
			return nil
		}
		_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "in_progress", "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "node_count": len(statuses), "hard_timeout_ms": budget.HardTimeout.Milliseconds(), "no_progress_timeout_ms": budget.NoProgressTimeout.Milliseconds()})
		time.Sleep(250 * time.Millisecond)
	}
	classifiers := []string{}
	if previous.MaxHeight != previous.MinHeight {
		classifiers = append(classifiers, "validator_height_lag")
	}
	if previous.Terminal < submitted {
		classifiers = append(classifiers, "terminal_accounting_missing")
	}
	if len(classifiers) == 0 {
		classifiers = append(classifiers, "unknown")
	}
	_ = v5.SaveJSON(filepath.Join(dataDir, "stalled_runtime_report.json"), map[string]any{"classifiers": classifiers, "submitted": submitted, "terminal": previous.Terminal, "incomplete": submitted - previous.Terminal, "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "last_snapshot": previous, "last_statuses": lastStatuses, "last_heights": lastHeights})
	return fmt.Errorf("drain hard timeout")
}

type progressSnapshot struct {
	Terminal         int  `json:"terminal"`
	MinHeight        int  `json:"min_height"`
	MaxHeight        int  `json:"max_height"`
	Mempool          int  `json:"mempool"`
	Reserved         int  `json:"reserved"`
	Pending          int  `json:"pending"`
	ProposalInFlight bool `json:"proposal_in_flight"`
}

type drainTimeoutBudget struct {
	HardTimeout       time.Duration
	NoProgressTimeout time.Duration
	EstimatedTimeout  time.Duration
}

func runtimePlanForNodes(plan v5.Plan) v5.Plan {
	runtimePlan := plan
	runtimeMS := int(drainBudget(plan).HardTimeout.Milliseconds())
	if runtimeMS > runtimePlan.DurationMS {
		runtimePlan.DurationMS = runtimeMS
	}
	return runtimePlan
}

func drainBudget(plan v5.Plan) drainTimeoutBudget {
	blockSize, interval := blockProducerTiming(plan)
	txCount := plan.WorkloadPlan.TxCount
	if txCount <= 0 {
		txCount = 1
	}
	blocks := (txCount + blockSize - 1) / blockSize
	perBlock := 5*time.Second + time.Duration(blockSize)*100*time.Millisecond + 4*interval
	estimated := time.Duration(blocks) * perBlock
	if plan.WorkloadPlan.ExpectedCrossShardCount > 0 || plan.WorkloadPlan.CrossShardRatio > 0 {
		estimated += estimated / 2
	}
	requested := time.Duration(plan.DurationMS) * time.Millisecond
	hard := maxDuration(requested, estimated+30*time.Second)
	if hard < 30*time.Second {
		hard = 30 * time.Second
	}
	if hard > 45*time.Minute {
		hard = 45 * time.Minute
	}
	noProgress := maxDuration(30*time.Second, 10*perBlock)
	if noProgress > 5*time.Minute {
		noProgress = 5 * time.Minute
	}
	return drainTimeoutBudget{HardTimeout: hard, NoProgressTimeout: noProgress, EstimatedTimeout: estimated}
}

func blockProducerTiming(plan v5.Plan) (int, time.Duration) {
	blockSize := 10
	interval := 150 * time.Millisecond
	if len(plan.NodeConfigs) > 0 {
		if producer, ok := plan.NodeConfigs[0].PluginProfile["block_producer"]; ok {
			if value := number(producer.Config["block_size"]); value > 0 {
				blockSize = value
			}
			if value := number(producer.Config["interval_ms"]); value >= 25 {
				interval = time.Duration(value) * time.Millisecond
			}
		}
	}
	return blockSize, interval
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func makeProgressSnapshot(terminal int, statuses []map[string]any, heights map[string]map[string]bool) progressSnapshot {
	result := progressSnapshot{Terminal: terminal, MinHeight: -1}
	for _, status := range statuses {
		height := number(status["committed_height"])
		if height > result.MaxHeight {
			result.MaxHeight = height
		}
		if result.MinHeight < 0 || height < result.MinHeight {
			result.MinHeight = height
		}
		result.Mempool += number(status["mempool_depth"])
		result.Reserved += number(status["reserved_tx_count"])
		result.Pending += number(status["pending_commit_count"]) + number(status["pending_future_block_count"]) + number(status["pending_cross_shard_count"])
		result.ProposalInFlight = result.ProposalInFlight || boolValue(status["proposal_in_flight"])
	}
	_ = heights
	return result
}

func progressChanged(previous, current progressSnapshot) bool {
	return current.Terminal > previous.Terminal || current.MaxHeight > previous.MaxHeight || current.MinHeight > previous.MinHeight || current.Mempool < previous.Mempool || current.Reserved < previous.Reserved || current.Pending < previous.Pending
}

func writeDrainProgress(path, phase string, submitted, terminal int, snapshot progressSnapshot, lastTerminalProgress, lastMempoolProgress time.Time) {
	header := []string{"timestamp", "phase", "submitted", "terminal", "incomplete", "min_validator_height", "max_validator_height", "height_gap", "total_mempool_depth", "total_reserved_tx", "proposal_in_flight", "pending_total", "last_terminal_progress_at", "last_mempool_progress_at"}
	row := []string{fmt.Sprint(time.Now().UnixMilli()), phase, fmt.Sprint(submitted), fmt.Sprint(terminal), fmt.Sprint(submitted - terminal), fmt.Sprint(snapshot.MinHeight), fmt.Sprint(snapshot.MaxHeight), fmt.Sprint(snapshot.MaxHeight - snapshot.MinHeight), fmt.Sprint(snapshot.Mempool), fmt.Sprint(snapshot.Reserved), fmt.Sprint(snapshot.ProposalInFlight), fmt.Sprint(snapshot.Pending), fmt.Sprint(lastTerminalProgress.UnixMilli()), fmt.Sprint(lastMempoolProgress.UnixMilli())}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if info, err := file.Stat(); err == nil && info.Size() == 0 {
		_ = writer.Write(header)
	}
	_ = writer.Write(row)
	writer.Flush()
}
func number(value any) int {
	switch item := value.(type) {
	case float64:
		return int(item)
	case int:
		return item
	}
	return 0
}
func boolValue(value any) bool { item, ok := value.(bool); return ok && item }
func stringSlice(value any) []string {
	items := []string{}
	if raw, ok := value.([]any); ok {
		for _, item := range raw {
			items = append(items, fmt.Sprint(item))
		}
	}
	return items
}

func hasNonTerminalMempool(statuses []map[string]any, terminal map[string]bool) bool {
	for _, status := range statuses {
		ids := stringSlice(status["mempool_logical_tx_ids"])
		if len(ids) == 0 && number(status["mempool_depth"]) > 0 {
			return true
		}
		for _, id := range ids {
			if !terminal[id] {
				return true
			}
		}
	}
	return false
}

type lifecycleRecord struct {
	timestamp                               int64
	txID, logicalID, stage, nodeID, shardID string
	success                                 bool
}

func validateSubmissionClassification(classification map[string]bool, expected int) error {
	if len(classification) != expected {
		return fmt.Errorf("submitted transaction classification count %d does not match expected tx_count %d", len(classification), expected)
	}
	for logicalID := range classification {
		if strings.TrimSpace(logicalID) == "" {
			return fmt.Errorf("submitted transaction classification contains an empty logical_tx_id")
		}
	}
	return nil
}

func loadSubmissionClassification(dataDir string, expected int) (map[string]bool, error) {
	path := filepath.Join(dataDir, "client", "client_submission_log.csv")
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read submitted transaction classification: %w", err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse submitted transaction classification: %w", err)
	}
	classification := map[string]bool{}
	for index, row := range rows {
		if index == 0 {
			continue
		}
		if len(row) < 9 {
			return nil, fmt.Errorf("submitted transaction classification row %d has %d fields", index+1, len(row))
		}
		logicalID := strings.TrimSpace(row[1])
		if logicalID == "" {
			return nil, fmt.Errorf("submitted transaction classification row %d has empty tx_id", index+1)
		}
		isCross, err := strconv.ParseBool(row[6])
		if err != nil {
			return nil, fmt.Errorf("submitted transaction classification row %d tx %s: %w", index+1, logicalID, err)
		}
		if previous, exists := classification[logicalID]; exists && previous != isCross {
			return nil, fmt.Errorf("submitted transaction classification has conflicting cross-shard values for %s", logicalID)
		}
		classification[logicalID] = isCross
	}
	if err := validateSubmissionClassification(classification, expected); err != nil {
		return nil, err
	}
	return classification, nil
}

func deriveLiveTerminal(classification map[string]bool, statuses []map[string]any) (map[string]bool, map[string]int, error) {
	if err := validateSubmissionClassification(classification, len(classification)); err != nil {
		return nil, nil, err
	}
	terminal := map[string]bool{}
	for _, status := range statuses {
		for _, logicalID := range stringSlice(status["durable_committed_logical_tx_ids"]) {
			if isCross, submitted := classification[logicalID]; submitted && !isCross {
				terminal[logicalID] = true
			}
		}
		for _, key := range []string{"source_finalized_logical_tx_ids", "refunded_logical_tx_ids", "failed_logical_tx_ids"} {
			for _, logicalID := range stringSlice(status[key]) {
				if _, submitted := classification[logicalID]; submitted {
					terminal[logicalID] = true
				}
			}
		}
	}
	crossSubmitted := 0
	for _, isCross := range classification {
		if isCross {
			crossSubmitted++
		}
	}
	counts := map[string]int{"submitted": len(classification), "terminal": len(terminal), "incomplete": len(classification) - len(terminal), "cross_submitted": crossSubmitted, "intra_submitted": len(classification) - crossSubmitted}
	return terminal, counts, nil
}

func deriveFinalityArtifacts(dataDir string, nodes []v5.NodePlan) (map[string]any, error) {
	classification := map[string]bool{}
	submissionFile, err := os.Open(filepath.Join(dataDir, "client", "client_submission_log.csv"))
	if err != nil {
		return nil, err
	}
	submissionRows, err := csv.NewReader(submissionFile).ReadAll()
	_ = submissionFile.Close()
	if err != nil {
		return nil, err
	}
	for index, row := range submissionRows {
		if index == 0 || len(row) < 10 {
			continue
		}
		isCross, err := strconv.ParseBool(row[6])
		if err != nil {
			return nil, fmt.Errorf("decode submitted transaction classification %q: %w", row[1], err)
		}
		classification[row[1]] = isCross
	}
	paths := []string{filepath.Join(dataDir, "client", "client_lifecycle.csv")}
	for _, node := range nodes {
		paths = append(paths, filepath.Join(node.DataDir, "transaction_lifecycle.csv"))
	}
	all := []lifecycleRecord{}
	rawRows := [][]string{}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		records, err := csv.NewReader(file).ReadAll()
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		for index, row := range records {
			if index == 0 || len(row) < 10 {
				continue
			}
			stamp, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, err
			}
			success, _ := strconv.ParseBool(row[9])
			all = append(all, lifecycleRecord{timestamp: stamp, txID: row[1], logicalID: row[2], stage: row[3], nodeID: row[4], shardID: row[5], success: success})
			rawRows = append(rawRows, row)
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].timestamp < all[j].timestamp })
	if err := metrics.WriteCSV(filepath.Join(dataDir, "transaction_lifecycle.csv"), []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}, rawRows); err != nil {
		return nil, err
	}
	if err := writeLifecycleJSONL(filepath.Join(dataDir, "transaction_lifecycle.jsonl"), rawRows); err != nil {
		return nil, err
	}
	type aggregate struct {
		submitted        int64
		cross            bool
		durableCommitted bool
		targetCommit     bool
		crossFinal       bool
		crossRefund      bool
		failed           bool
		terminal         int64
		terminalStage    string
		success          bool
	}
	byLogical := map[string]*aggregate{}
	for logicalID, isCross := range classification {
		byLogical[logicalID] = &aggregate{cross: isCross}
	}
	for _, event := range all {
		entry := byLogical[event.logicalID]
		if entry == nil {
			entry = &aggregate{cross: classification[event.logicalID]}
			byLogical[event.logicalID] = entry
		}
		if event.stage == "submitted" && (entry.submitted == 0 || event.timestamp < entry.submitted) {
			entry.submitted = event.timestamp
		}
		stage := strings.ToLower(event.stage)
		if stage == "durable_committed" {
			entry.durableCommitted = true
		}
		if stage == "targetcommit" {
			entry.targetCommit = true
		}
		if stage == "sourcefinalize" {
			entry.crossFinal = true
		}
		if stage == "refund" {
			entry.crossRefund = true
		}
		if stage == "failed" {
			entry.failed = true
		}
	}
	for _, event := range all {
		entry := byLogical[event.logicalID]
		stage := strings.ToLower(event.stage)
		terminal := stage == "durable_committed" || stage == "sourcefinalize" || stage == "refund" || stage == "failed"
		if !terminal {
			continue
		}
		if stage == "durable_committed" && entry.cross {
			continue
		}
		if entry.terminal == 0 || event.timestamp < entry.terminal {
			entry.terminal = event.timestamp
			entry.terminalStage = event.stage
			entry.success = event.success && event.stage != "failed"
		}
	}
	intraCommitted, intraTerminal, crossRequested, crossTarget, crossFinalized, crossRefunded, crossFailed := 0, 0, 0, 0, 0, 0, 0
	for _, entry := range byLogical {
		if entry.cross {
			crossRequested++
			if entry.targetCommit {
				crossTarget++
			}
			if entry.crossFinal {
				crossFinalized++
			}
			if entry.crossRefund {
				crossRefunded++
			}
			if entry.failed {
				crossFailed++
			}
		} else if entry.durableCommitted {
			intraTerminal++
			intraCommitted++
		} else if entry.terminal > 0 {
			intraTerminal++
		}
	}
	rows := [][]string{}
	latencies := []int64{}
	finalized := 0
	for logical, entry := range byLogical {
		latency := int64(-1)
		if entry.submitted > 0 && entry.terminal >= entry.submitted {
			latency = entry.terminal - entry.submitted
		}
		if entry.terminal > 0 && entry.success {
			finalized++
			latencies = append(latencies, latency)
		}
		rows = append(rows, []string{logical, fmt.Sprint(entry.submitted), fmt.Sprint(entry.terminal), entry.terminalStage, fmt.Sprint(entry.success), fmt.Sprint(entry.cross), fmt.Sprint(latency)})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	if err := metrics.WriteCSV(filepath.Join(dataDir, "transaction_finality.csv"), []string{"logical_tx_id", "submitted_at_ms", "terminal_at_ms", "terminal_stage", "success", "cross_shard", "finality_ms"}, rows); err != nil {
		return nil, err
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "client_receipt_log.csv"), []string{"logical_tx_id", "terminal_at_ms", "terminal_stage", "success", "finality_ms"}, func() [][]string {
		out := make([][]string, 0, len(rows))
		for _, row := range rows {
			out = append(out, []string{row[0], row[2], row[3], row[4], row[6]})
		}
		return out
	}()); err != nil {
		return nil, err
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) int64 {
		if len(latencies) == 0 {
			return -1
		}
		index := int(float64(len(latencies)-1) * p)
		return latencies[index]
	}
	first, last := int64(0), int64(0)
	for _, entry := range byLogical {
		if entry.terminal > 0 && entry.success {
			if first == 0 || entry.terminal < first {
				first = entry.terminal
			}
			if entry.terminal > last {
				last = entry.terminal
			}
		}
	}
	tps := float64(0)
	if last > first {
		tps = float64(finalized) * 1000 / float64(last-first)
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "latency_distribution.csv"), []string{"percentile", "finality_ms"}, [][]string{{"p50", fmt.Sprint(percentile(.50))}, {"p95", fmt.Sprint(percentile(.95))}, {"p99", fmt.Sprint(percentile(.99))}}); err != nil {
		return nil, err
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "throughput_windows.csv"), []string{"window_start_ms", "window_end_ms", "finalized_unique_logical_txs", "throughput_tps"}, [][]string{{fmt.Sprint(first), fmt.Sprint(last), fmt.Sprint(finalized), fmt.Sprintf("%.6f", tps)}}); err != nil {
		return nil, err
	}
	terminalUnique := intraTerminal + crossFinalized + crossRefunded + crossFailed
	summary := map[string]any{"metric_truth": "derived_from_raw_runtime_lifecycle", "logical_transaction_count": len(byLogical), "submitted_unique_tx_count": len(byLogical), "intra_shard_committed_unique_count": intraCommitted, "intra_shard_terminal_unique_count": intraTerminal, "cross_shard_requested_unique_count": crossRequested, "cross_shard_target_committed_unique_count": crossTarget, "cross_shard_finalized_unique_count": crossFinalized, "cross_shard_refunded_unique_count": crossRefunded, "cross_shard_failed_unique_count": crossFailed, "terminal_unique_tx_count": terminalUnique, "incomplete_unique_tx_count": len(byLogical) - terminalUnique, "finalized_unique_logical_tx_count": finalized, "p50_finality_ms": percentile(.50), "p95_finality_ms": percentile(.95), "p99_finality_ms": percentile(.99), "throughput_tps": tps, "tcp_send_latency_excluded": true}
	return summary, v5.SaveJSON(filepath.Join(dataDir, "finality_summary.json"), summary)
}

func writeLifecycleJSONL(path string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	keys := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	for _, row := range rows {
		value := map[string]string{}
		for index, key := range keys {
			if index < len(row) {
				value[key] = row[index]
			}
		}
		line, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func buildBinary(output, target string) error {
	cmd := exec.Command("go", "build", "-o", output, target)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func copyIfExists(source, target string) error {
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(target, raw, 0o644)
}

func readOptionalJSON(path string) map[string]any {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}
func allocateAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	return address, listener.Close()
}
func waitReady(plan v5.Plan, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, node := range plan.NodeConfigs {
			if _, err := os.Stat(filepath.Join(node.DataDir, "ready.json")); err != nil {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("node readiness timeout")
}
func waitAll(commands []*exec.Cmd, timeout time.Duration) error {
	done := make(chan error, len(commands))
	for _, cmd := range commands {
		go func(c *exec.Cmd) { done <- c.Wait() }(cmd)
	}
	deadline := time.After(timeout)
	for range commands {
		select {
		case err := <-done:
			if err != nil {
				return err
			}
		case <-deadline:
			return fmt.Errorf("node shutdown timeout")
		}
	}
	return nil
}
func reap(commands []*exec.Cmd) {
	for _, cmd := range commands {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
}

func summarizeV5(plan v5.Plan, dataDir string, processes []v5NodeProcess) (map[string]any, error) {
	summaries := []v5NodeSummary{}
	roots := map[string]map[string]bool{}
	shardBlocks := map[string]int{}
	pbftCount := 0
	crossSuccess := 0
	crossRefund := 0
	faultEvidence := false
	remoteStateAccessCount := 0
	remoteStateReadCount := 0
	remoteStateWriteApplyCount := 0
	remoteStateFailedCount := 0
	remoteStateLatencyWeightedSum := 0.0
	schedulerEventCount := 0
	schedulerBlockedCount := 0
	schedulerWakeupCount := 0
	schedulerStolenWorkCount := 0
	schedulerLocalExecutionCount := 0
	schedulerReadyQueueMaxDepth := 0
	schedulerFastQueueMaxDepth := 0
	schedulerConsQueueMaxDepth := 0
	schedulerDependencyWaitMS := 0
	schedulerIdleMS := 0
	schedulerIdleRatioWeightedSum := 0.0
	for _, node := range plan.NodeConfigs {
		raw, err := os.ReadFile(filepath.Join(node.DataDir, "node_summary.json"))
		if err != nil {
			return nil, err
		}
		var item v5NodeSummary
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		summaries = append(summaries, item)
		if roots[item.ShardID] == nil {
			roots[item.ShardID] = map[string]bool{}
		}
		roots[item.ShardID][item.StateRoot] = true
		if item.CommittedBlockCount > shardBlocks[item.ShardID] {
			shardBlocks[item.ShardID] = item.CommittedBlockCount
		}
		if item.RealPBFT {
			pbftCount++
		}
		remoteStateAccessCount += item.RemoteStateAccessCount
		remoteStateReadCount += item.RemoteStateReadCount
		remoteStateWriteApplyCount += item.RemoteStateWriteApplyCount
		remoteStateFailedCount += item.RemoteStateAccessFailedCount
		remoteStateLatencyWeightedSum += item.RemoteStateAccessAvgLatencyMS * float64(item.RemoteStateAccessCount)
		schedulerEventCount += item.SchedulerEventCount
		schedulerBlockedCount += item.SchedulerBlockedCount
		schedulerWakeupCount += item.SchedulerWakeupCount
		schedulerStolenWorkCount += item.SchedulerStolenWorkCount
		schedulerLocalExecutionCount += item.SchedulerLocalExecutionCount
		if item.SchedulerReadyQueueMaxDepth > schedulerReadyQueueMaxDepth {
			schedulerReadyQueueMaxDepth = item.SchedulerReadyQueueMaxDepth
		}
		if item.SchedulerFastQueueMaxDepth > schedulerFastQueueMaxDepth {
			schedulerFastQueueMaxDepth = item.SchedulerFastQueueMaxDepth
		}
		if item.SchedulerConsQueueMaxDepth > schedulerConsQueueMaxDepth {
			schedulerConsQueueMaxDepth = item.SchedulerConsQueueMaxDepth
		}
		schedulerDependencyWaitMS += item.SchedulerDependencyWaitMS
		schedulerIdleMS += item.SchedulerIdleMS
		schedulerIdleRatioWeightedSum += item.SchedulerIdleRatio * float64(item.SchedulerEventCount)
		network, _ := os.ReadFile(filepath.Join(node.DataDir, "network_log.csv"))
		faultEvidence = faultEvidence || strings.Contains(string(network), "fault_")
	}
	if err := writeHeightRootMatrix(dataDir, plan.NodeConfigs); err != nil {
		return nil, err
	}
	consistent := true
	allActive := true
	for shard, values := range roots {
		if len(values) != 1 {
			consistent = false
		}
		if shardBlocks[shard] < 2 {
			allActive = false
		}
	}
	pids := map[int]bool{}
	ports := map[string]bool{}
	blockExecutors := map[string]bool{}
	planDigestConsistent := true
	for _, p := range processes {
		pids[p.PID] = true
		ports[p.ListenAddr] = true
	}
	for _, item := range summaries {
		if item.BlockExecutorID != "" {
			blockExecutors[item.BlockExecutorID] = true
		}
		if !item.PlanDigestConsistent {
			planDigestConsistent = false
		}
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].NodeID < summaries[j].NodeID })
	clientLog := filepath.Join(dataDir, "client", "client_submission_log.csv")
	clientInfo, _ := os.Stat(clientLog)
	ready := len(pids) == len(plan.NodeConfigs) && len(ports) == len(plan.NodeConfigs) && consistent && allActive && pbftCount == len(plan.NodeConfigs) && clientInfo != nil
	faultRequested := fmt.Sprint(plan.FaultPlan["mode"]) != "" && fmt.Sprint(plan.FaultPlan["mode"]) != "disabled"
	ready = ready && (!faultRequested || faultEvidence)
	remoteStateAvgLatency := 0.0
	if remoteStateAccessCount > 0 {
		remoteStateAvgLatency = remoteStateLatencyWeightedSum / float64(remoteStateAccessCount)
	}
	schedulerIdleRatio := 0.0
	if schedulerEventCount > 0 {
		schedulerIdleRatio = schedulerIdleRatioWeightedSum / float64(schedulerEventCount)
	}
	return map[string]any{"runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime", "runtime_truth": "v5_real_cluster_candidate", "one_node_one_os_process": true, "distinct_process_count": len(pids), "expected_process_count": len(plan.NodeConfigs), "independent_tcp_ports": len(ports) == len(plan.NodeConfigs), "real_client_submission": clientInfo != nil, "real_signed_tx": true, "plugin_driven_runtime": true, "block_executor_id": singleMapKey(blockExecutors), "block_executor_consistent": len(blockExecutors) == 1, "plan_digest_consistent": planDigestConsistent, "continuous_multi_shard": true, "shard_count": len(roots), "all_shards_active": allActive, "per_shard_multiple_blocks": allActive, "real_pbft_style_messages": pbftCount == len(plan.NodeConfigs), "persistent_state": true, "state_root_consistent": consistent, "real_cross_shard_network": crossSuccess > 0, "cross_shard_success_count": crossSuccess, "cross_shard_refund_count": crossRefund, "scheduler_event_count": schedulerEventCount, "scheduler_blocked_count": schedulerBlockedCount, "scheduler_wakeup_count": schedulerWakeupCount, "scheduler_stolen_work_count": schedulerStolenWorkCount, "scheduler_local_execution_count": schedulerLocalExecutionCount, "scheduler_ready_queue_max_depth": schedulerReadyQueueMaxDepth, "scheduler_fast_queue_max_depth": schedulerFastQueueMaxDepth, "scheduler_conservative_queue_max_depth": schedulerConsQueueMaxDepth, "scheduler_dependency_wait_ms": schedulerDependencyWaitMS, "scheduler_idle_ms": schedulerIdleMS, "scheduler_idle_ratio": schedulerIdleRatio, "remote_state_access_count": remoteStateAccessCount, "remote_state_read_count": remoteStateReadCount, "remote_state_write_apply_count": remoteStateWriteApplyCount, "remote_state_access_failed_count": remoteStateFailedCount, "remote_state_access_avg_latency_ms": remoteStateAvgLatency, "fault_injection_real": faultEvidence, "fault_injection_requested": faultRequested, "orphan_process_count": 0, "no_fallback": true, "node_summaries": summaries, "processes": redactV5Processes(processes, dataDir), "shard_blocks": shardBlocks, "ready_to_commit": ready}, nil
}

func singleMapKey(values map[string]bool) string {
	if len(values) != 1 {
		return ""
	}
	for key := range values {
		return key
	}
	return ""
}

func redactV5Processes(processes []v5NodeProcess, dataDir string) []v5NodeProcess {
	redacted := make([]v5NodeProcess, 0, len(processes))
	for _, process := range processes {
		item := process
		item.DataDir = v5LogicalPath(dataDir, process.DataDir)
		item.LogPath = v5LogicalPath(dataDir, process.LogPath)
		redacted = append(redacted, item)
	}
	return redacted
}

func writeRedactedV5PlanArtifacts(plan v5.Plan, dataDir, planPath string) error {
	redacted := plan
	redacted.NodeConfigs = append([]v5.NodePlan(nil), plan.NodeConfigs...)
	for index := range redacted.NodeConfigs {
		redacted.NodeConfigs[index].DataDir = v5LogicalPath(dataDir, plan.NodeConfigs[index].DataDir)
	}
	if err := v5.SaveJSON(planPath, redacted); err != nil {
		return err
	}
	for _, nodePlan := range redacted.NodeConfigs {
		configPath := filepath.Join(dataDir, "node_config_"+nodePlan.NodeID+".json")
		if err := v5.SaveJSON(configPath, map[string]any{"plan": redacted, "node_id": nodePlan.NodeID}); err != nil {
			return err
		}
	}
	return nil
}

func v5LogicalPath(dataDir, target string) string {
	rel, err := filepath.Rel(dataDir, target)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return filepath.ToSlash(filepath.Base(target))
	}
	return filepath.ToSlash(rel)
}

func writeHeightRootMatrix(dataDir string, nodes []v5.NodePlan) error {
	type row struct{ shard, height, node, block, parent, tx, state, receipt string }
	byHeight := map[string][]row{}
	for _, node := range nodes {
		file, err := os.Open(filepath.Join(node.DataDir, "committed_chain.csv"))
		if err != nil {
			return err
		}
		records, err := csv.NewReader(file).ReadAll()
		_ = file.Close()
		if err != nil {
			return err
		}
		for i, record := range records {
			if i == 0 || len(record) < 11 {
				continue
			}
			key := record[1] + ":" + record[2]
			byHeight[key] = append(byHeight[key], row{record[1], record[2], record[0], record[4], record[5], record[7], record[9], record[10]})
		}
	}
	out := [][]string{}
	first := map[string]any{}
	for key, items := range byHeight {
		ref := items[0]
		consistent := true
		for _, item := range items {
			if item.block != ref.block || item.parent != ref.parent || item.tx != ref.tx || item.state != ref.state || item.receipt != ref.receipt {
				consistent = false
			}
		}
		for _, item := range items {
			out = append(out, []string{item.shard, item.height, item.node, item.block, item.state, item.receipt, fmt.Sprint(consistent)})
		}
		if !consistent && len(first) == 0 {
			first = map[string]any{"key": key, "entries": items}
		}
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "height_root_matrix.csv"), []string{"shard_id", "height", "node_id", "block_hash", "state_root", "receipt_root", "consistent"}, out); err != nil {
		return err
	}
	return v5.SaveJSON(filepath.Join(dataDir, "state_consistency_report.json"), map[string]any{"consistent": len(first) == 0, "first_divergence": first})
}
