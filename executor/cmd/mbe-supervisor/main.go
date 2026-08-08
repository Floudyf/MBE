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
	NodeID                           string  `json:"node_id"`
	ShardID                          string  `json:"shard_id"`
	PID                              int     `json:"pid"`
	ListenAddr                       string  `json:"listen_addr"`
	CommittedBlockCount              int     `json:"committed_block_count"`
	StateRoot                        string  `json:"state_root"`
	BusinessStateDigest              string  `json:"business_state_digest"`
	StateReadyWaitCount              int64   `json:"state_ready_wait_count"`
	StateReadyResumeCount            int64   `json:"state_ready_resume_count"`
	StatePrefetchWaitMS              int64   `json:"state_prefetch_wait_ms"`
	RemoteStateFetchCount            int64   `json:"remote_state_fetch_count"`
	RemoteStateFetchCompletedCount   int64   `json:"remote_state_fetch_completed_count"`
	StateReadySchedulerMode          string  `json:"state_ready_scheduler_mode"`
	VersionedStateReadyWaveCount     int64   `json:"versioned_state_ready_wave_count"`
	VersionedStateReadyWaitCount     int64   `json:"versioned_state_ready_wait_observation_count"`
	VersionedStateReadyResolvedCount int64   `json:"versioned_state_ready_resolved_token_count"`
	VersionedStateProbeCount         int64   `json:"versioned_state_probe_count"`
	VersionedStateProbeLatencyMS     int64   `json:"versioned_state_probe_latency_ms"`
	VersionedStateReadyMaxWaveWidth  int64   `json:"versioned_state_ready_max_wave_width"`
	VersionedStateReadySchedulerMode string  `json:"versioned_state_ready_scheduler_mode"`
	RealPBFT                         bool    `json:"real_pbft_style_messages"`
	BlockExecutorID                  string  `json:"block_executor_id"`
	BlockExecutorVersion             string  `json:"block_executor_version"`
	WorkerCount                      int     `json:"worker_count"`
	PlanDigestConsistent             bool    `json:"plan_digest_consistent"`
	FastTrackCount                   int     `json:"fast_track_count"`
	ConservativeTrackCount           int     `json:"conservative_track_count"`
	AggregationGroupCount            int     `json:"aggregation_group_count"`
	SchedulerEventCount              int     `json:"scheduler_event_count"`
	SchedulerBlockedCount            int     `json:"scheduler_blocked_count"`
	SchedulerWakeupCount             int     `json:"scheduler_wakeup_count"`
	SchedulerStolenWorkCount         int     `json:"scheduler_stolen_work_count"`
	SchedulerLocalExecutionCount     int     `json:"scheduler_local_execution_count"`
	SchedulerReadyQueueMaxDepth      int     `json:"scheduler_ready_queue_max_depth"`
	SchedulerFastQueueMaxDepth       int     `json:"scheduler_fast_queue_max_depth"`
	SchedulerConsQueueMaxDepth       int     `json:"scheduler_conservative_queue_max_depth"`
	SchedulerDependencyWaitMS        int     `json:"scheduler_dependency_wait_ms"`
	SchedulerIdleMS                  int     `json:"scheduler_idle_ms"`
	SchedulerIdleRatio               float64 `json:"scheduler_idle_ratio"`
	RemoteStateAccessCount           int     `json:"remote_state_access_count"`
	RemoteStateReadCount             int     `json:"remote_state_read_count"`
	RemoteStateWriteApplyCount       int     `json:"remote_state_write_apply_count"`
	RemoteOperationUnknownCount      int     `json:"remote_operation_unknown_kind_count"`
	PhysicalRemoteOperationCount     int     `json:"physical_remote_operation_count"`
	PhysicalRemoteFetchCount         int     `json:"physical_remote_fetch_count"`
	PhysicalRemoteWritebackCount     int     `json:"physical_remote_writeback_count"`
	PhysicalRemoteFailedCount        int     `json:"physical_remote_failed_count"`
	RemoteStateAccessFailedCount     int     `json:"remote_state_access_failed_count"`
	RemoteStateAccessAvgLatencyMS    float64 `json:"remote_state_access_avg_latency_ms"`
	LogicalUpdateCount               int     `json:"logical_update_count"`
	PhysicalUpdateCount              int     `json:"physical_update_count"`
	ExecutedLogicalTxCount           int     `json:"executed_logical_transaction_count"`
	ExecutedTxInstanceCount          int     `json:"executed_transaction_instance_count"`
	PreAggregationPhysicalOps        int     `json:"pre_aggregation_physical_op_count"`
	PostAggregationPhysicalOps       int     `json:"post_aggregation_physical_op_count"`
	AggregatedKeyCount               int     `json:"aggregated_key_count"`
	AggregatedLogicalDeltaCount      int     `json:"aggregated_logical_delta_count"`
	PhysicalOpsSavedCount            int     `json:"physical_ops_saved_count"`
	AggregationReductionRatio        float64 `json:"aggregation_reduction_ratio"`
	ConfiguredBlockSize              int     `json:"configured_block_size"`
	ConfiguredBlockIntervalMS        int     `json:"configured_block_interval_ms"`
	ActualCommittedBlockCount        int     `json:"actual_committed_block_count"`
	ActualAverageTxPerBlock          float64 `json:"actual_average_tx_per_block"`
	ActualMinTxPerBlock              int     `json:"actual_min_tx_per_block"`
	ActualMaxTxPerBlock              int     `json:"actual_max_tx_per_block"`
	ActualBlockIntervalMeanMS        float64 `json:"actual_block_interval_mean_ms"`
	ActualBlockIntervalP95MS         int64   `json:"actual_block_interval_p95_ms"`
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
	preflight, preflightErr := v5.PreflightWorkloadCapabilities(context.Background(), plan, dataDir)
	if err := v5.SaveJSON(filepath.Join(dataDir, "workload_capability_preflight.json"), preflight); err != nil {
		return err
	}
	if preflightErr != nil {
		return preflightErr
	}
	plan.WorkloadPlan.ExpectedCrossShardCount = preflight.ActualCrossShardCount
	plan.WorkloadPlan.ExpectedCrossShardRatio = preflight.ActualCrossShardRatio
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
	shutdownTimeout := shutdownBudget(plan)
	shutdownStarted := time.Now()
	waitErr := waitAll(commands, shutdownTimeout)
	shutdownElapsed := time.Since(shutdownStarted)
	_ = v5.SaveJSON(filepath.Join(dataDir, "shutdown_status.json"), map[string]any{"requested_at": shutdownStarted.UnixMilli(), "finished_at": time.Now().UnixMilli(), "timeout_ms": shutdownTimeout.Milliseconds(), "elapsed_ms": shutdownElapsed.Milliseconds(), "process_count": len(commands), "success": waitErr == nil, "error": errorString(waitErr)})
	if waitErr != nil {
		reap(commands)
		return waitErr
	}
	summary, err := summarizeV5(plan, dataDir, processes)
	if err != nil {
		return err
	}
	finality, err := deriveFinalityArtifacts(dataDir, plan.NodeConfigs, planUsesStatelessDirectExecution(plan))
	if err != nil {
		return err
	}
	summary["finality_evidence"] = finality
	if planUsesStatelessDirectExecution(plan) {
		summary["cross_shard_execution_mode"] = "stateless_direct_execution"
		summary["legacy_cross_shard_protocol"] = false
	} else {
		summary["cross_shard_execution_mode"] = "legacy_lock_relay_finalize"
		summary["legacy_cross_shard_protocol"] = true
	}
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

func planUsesStatelessDirectExecution(plan v5.Plan) bool {
	if len(plan.NodeConfigs) == 0 {
		return false
	}
	routing, ok := plan.NodeConfigs[0].PluginProfile["routing"]
	if !ok {
		return false
	}
	switch routing.PluginID {
	case "metatrack_coaccess_routing", "stateless_hash_routing":
		return true
	default:
		return false
	}
}

func drainV5(plan v5.Plan, dataDir string) error {
	started := time.Now()
	statelessDirect := planUsesStatelessDirectExecution(plan)
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
		fatalExecution := ""
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
			if value := fmt.Sprint(status["fatal_execution_error"]); value != "" && value != "<nil>" {
				fatalExecution = value
			}
			for _, key := range []string{"reserved_tx_count", "pending_commit_count", "pending_future_block_count", "pending_cross_shard_count", "pending_state_delta_count", "pending_state_delta_key_count", "ready_state_delta_count"} {
				if number(status[key]) != 0 {
					allEmpty = false
				}
			}
			shard := fmt.Sprint(status["shard_id"])
			if heights[shard] == nil {
				heights[shard] = map[string]bool{}
			}
			heights[shard][fmt.Sprint(status["committed_height"])] = true
		}
		liveTerminal, _, err := deriveLiveTerminal(classification, statuses, statelessDirect)
		if err != nil {
			_ = v5.SaveJSON(filepath.Join(dataDir, "stalled_runtime_report.json"), map[string]any{"classifiers": []string{"terminal_accounting_missing"}, "phase": "FAILED", "reason": err.Error(), "submitted": submitted})
			return err
		}
		terminal = liveTerminal
		if hasNonTerminalMempool(statuses, terminal) {
			allEmpty = false
		}
		if hasPendingProposalWork(statuses, terminal) {
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
			reason := "failed_persistence_inconsistency: " + fatalPersistence
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "failed_persistence_inconsistency", "fatal_persistence_error": fatalPersistence, "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli()})
			writeStalledRuntimeReport(dataDir, []string{"fatal_persistence_error"}, reason, submitted, len(terminal), current, statuses, heights, lastProgress, lastTerminalProgress, lastHeightProgress, lastMempoolProgress, lastPendingProgress)
			return fmt.Errorf("%s", reason)
		}
		if fatalExecution != "" {
			phase = "FAILED"
			reason := "failed_deterministic_execution: " + fatalExecution
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "failed_deterministic_execution", "fatal_execution_error": fatalExecution, "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli()})
			writeStalledRuntimeReport(dataDir, []string{"fatal_execution_error"}, reason, submitted, len(terminal), current, statuses, heights, lastProgress, lastTerminalProgress, lastHeightProgress, lastMempoolProgress, lastPendingProgress)
			return fmt.Errorf("%s", reason)
		}
		if initialized && now.Sub(lastProgress) > budget.NoProgressTimeout {
			phase = "FAILED"
			reason := "drain no-progress timeout"
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "no_progress_timeout", "drain_started_at": started.UnixMilli(), "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "hard_timeout_ms": budget.HardTimeout.Milliseconds(), "no_progress_timeout_ms": budget.NoProgressTimeout.Milliseconds()})
			writeStalledRuntimeReport(dataDir, []string{"no_progress_timeout"}, reason, submitted, len(terminal), current, statuses, heights, lastProgress, lastTerminalProgress, lastHeightProgress, lastMempoolProgress, lastPendingProgress)
			return fmt.Errorf("%s", reason)
		}
		phase = "DRAINING"
		if !aligned {
			phase = "CATCHING_UP"
		}
		writeDrainProgress(progressPath, phase, submitted, len(terminal), current, lastTerminalProgress, lastMempoolProgress)
		if len(terminal) >= submitted && allEmpty && aligned {
			phase = "QUIESCENT"
			finishedAt := quiescentCompletionTime(started, now, statuses)
			_ = v5.SaveJSON(filepath.Join(dataDir, "drain_status.json"), map[string]any{"submitted": submitted, "terminal": len(terminal), "incomplete": submitted - len(terminal), "phase": phase, "completion_reason": "drain_quiescent", "drain_started_at": started.UnixMilli(), "drain_finished_at": finishedAt.UnixMilli(), "drain_observed_quiescent_at": now.UnixMilli(), "drain_finish_source": "max_node_last_progress_at_bounded", "last_progress_at": lastProgress.UnixMilli(), "last_terminal_progress_at": lastTerminalProgress.UnixMilli(), "last_height_progress_at": lastHeightProgress.UnixMilli(), "last_mempool_progress_at": lastMempoolProgress.UnixMilli(), "last_pending_progress_at": lastPendingProgress.UnixMilli(), "hard_timeout_ms": budget.HardTimeout.Milliseconds(), "no_progress_timeout_ms": budget.NoProgressTimeout.Milliseconds()})
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
	writeStalledRuntimeReport(dataDir, classifiers, "drain hard timeout", submitted, previous.Terminal, previous, lastStatuses, lastHeights, lastProgress, lastTerminalProgress, lastHeightProgress, lastMempoolProgress, lastPendingProgress)
	return fmt.Errorf("drain hard timeout")
}

func writeStalledRuntimeReport(dataDir string, classifiers []string, reason string, submitted, terminal int, snapshot progressSnapshot, statuses []map[string]any, heights map[string]map[string]bool, lastProgress, lastTerminalProgress, lastHeightProgress, lastMempoolProgress, lastPendingProgress time.Time) {
	_ = v5.SaveJSON(filepath.Join(dataDir, "stalled_runtime_report.json"), map[string]any{
		"classifiers":               classifiers,
		"phase":                     "FAILED",
		"reason":                    reason,
		"submitted":                 submitted,
		"terminal":                  terminal,
		"incomplete":                submitted - terminal,
		"last_progress_at":          lastProgress.UnixMilli(),
		"last_terminal_progress_at": lastTerminalProgress.UnixMilli(),
		"last_height_progress_at":   lastHeightProgress.UnixMilli(),
		"last_mempool_progress_at":  lastMempoolProgress.UnixMilli(),
		"last_pending_progress_at":  lastPendingProgress.UnixMilli(),
		"last_snapshot":             snapshot,
		"last_statuses":             statuses,
		"last_heights":              heights,
	})
}

type progressSnapshot struct {
	Terminal                       int   `json:"terminal"`
	MinHeight                      int   `json:"min_height"`
	MaxHeight                      int   `json:"max_height"`
	Mempool                        int   `json:"mempool"`
	Reserved                       int   `json:"reserved"`
	Pending                        int   `json:"pending"`
	ProposalInFlight               bool  `json:"proposal_in_flight"`
	BlockExecutionHeight           int   `json:"block_execution_height"`
	BlockExecutionProgressAtMS     int64 `json:"block_execution_progress_at_ms"`
	BlockExecutionTaskCount        int   `json:"block_execution_task_count"`
	BlockValidationTaskCount       int   `json:"block_validation_task_count"`
	BlockExecutionValidatedCount   int   `json:"block_execution_validated_count"`
	BlockExecutionAbortCount       int   `json:"block_execution_abort_count"`
	BlockExecutionReexecutionCount int   `json:"block_execution_reexecution_count"`
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
	if hard > 90*time.Minute {
		hard = 90 * time.Minute
	}
	noProgress := maxDuration(30*time.Second, 10*perBlock)
	if noProgress > 5*time.Minute {
		noProgress = 5 * time.Minute
	}
	return drainTimeoutBudget{HardTimeout: hard, NoProgressTimeout: noProgress, EstimatedTimeout: estimated}
}

func blockProducerTiming(plan v5.Plan) (int, time.Duration) {
	blockSize := 100
	interval := 75 * time.Millisecond
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
		result.Pending += number(status["pending_commit_count"]) + number(status["pending_future_block_count"]) + number(status["pending_cross_shard_count"]) + number(status["pending_state_delta_count"]) + number(status["pending_state_delta_key_count"]) + number(status["ready_state_delta_count"])
		result.ProposalInFlight = result.ProposalInFlight || boolValue(status["proposal_in_flight"])
		if value := number(status["block_execution_height"]); value > result.BlockExecutionHeight {
			result.BlockExecutionHeight = value
		}
		if value := int64(number(status["block_execution_progress_at_ms"])); value > result.BlockExecutionProgressAtMS {
			result.BlockExecutionProgressAtMS = value
		}
		result.BlockExecutionTaskCount += number(status["block_execution_task_count"])
		result.BlockValidationTaskCount += number(status["block_validation_task_count"])
		result.BlockExecutionValidatedCount += number(status["block_execution_validated_count"])
		result.BlockExecutionAbortCount += number(status["block_execution_abort_count"])
		result.BlockExecutionReexecutionCount += number(status["block_execution_reexecution_count"])
	}
	_ = heights
	return result
}

func progressChanged(previous, current progressSnapshot) bool {
	return current.Terminal > previous.Terminal ||
		current.MaxHeight > previous.MaxHeight ||
		current.MinHeight > previous.MinHeight ||
		current.Mempool < previous.Mempool ||
		current.Reserved < previous.Reserved ||
		current.Pending < previous.Pending ||
		current.BlockExecutionHeight > previous.BlockExecutionHeight ||
		(current.BlockExecutionHeight == previous.BlockExecutionHeight && current.BlockExecutionTaskCount > previous.BlockExecutionTaskCount) ||
		(current.BlockExecutionHeight == previous.BlockExecutionHeight && current.BlockValidationTaskCount > previous.BlockValidationTaskCount) ||
		(current.BlockExecutionHeight == previous.BlockExecutionHeight && current.BlockExecutionValidatedCount > previous.BlockExecutionValidatedCount) ||
		(current.BlockExecutionHeight == previous.BlockExecutionHeight && current.BlockExecutionAbortCount > previous.BlockExecutionAbortCount) ||
		(current.BlockExecutionHeight == previous.BlockExecutionHeight && current.BlockExecutionReexecutionCount > previous.BlockExecutionReexecutionCount)
}

func writeDrainProgress(path, phase string, submitted, terminal int, snapshot progressSnapshot, lastTerminalProgress, lastMempoolProgress time.Time) {
	header := []string{"timestamp", "phase", "submitted", "terminal", "incomplete", "min_validator_height", "max_validator_height", "height_gap", "total_mempool_depth", "total_reserved_tx", "proposal_in_flight", "pending_total", "block_execution_height", "block_execution_progress_at_ms", "block_execution_task_count", "block_validation_task_count", "block_execution_validated_count", "block_execution_abort_count", "block_execution_reexecution_count", "last_terminal_progress_at", "last_mempool_progress_at"}
	row := []string{fmt.Sprint(time.Now().UnixMilli()), phase, fmt.Sprint(submitted), fmt.Sprint(terminal), fmt.Sprint(submitted - terminal), fmt.Sprint(snapshot.MinHeight), fmt.Sprint(snapshot.MaxHeight), fmt.Sprint(snapshot.MaxHeight - snapshot.MinHeight), fmt.Sprint(snapshot.Mempool), fmt.Sprint(snapshot.Reserved), fmt.Sprint(snapshot.ProposalInFlight), fmt.Sprint(snapshot.Pending), fmt.Sprint(snapshot.BlockExecutionHeight), fmt.Sprint(snapshot.BlockExecutionProgressAtMS), fmt.Sprint(snapshot.BlockExecutionTaskCount), fmt.Sprint(snapshot.BlockValidationTaskCount), fmt.Sprint(snapshot.BlockExecutionValidatedCount), fmt.Sprint(snapshot.BlockExecutionAbortCount), fmt.Sprint(snapshot.BlockExecutionReexecutionCount), fmt.Sprint(lastTerminalProgress.UnixMilli()), fmt.Sprint(lastMempoolProgress.UnixMilli())}
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

func hasPendingProposalWork(statuses []map[string]any, terminal map[string]bool) bool {
	for _, status := range statuses {
		if !boolValue(status["proposal_in_flight"]) {
			continue
		}
		detailsAvailable, ok := status["proposal_work_details_available"].(bool)
		if !ok || !detailsAvailable {
			// Older or incomplete node status remains conservative: an opaque
			// in-flight proposal may still contain transaction or system work.
			return true
		}
		if number(status["proposal_system_state_delta_count"]) > 0 {
			return true
		}
		for _, logicalID := range stringSlice(status["proposal_logical_tx_ids"]) {
			if !terminal[logicalID] {
				return true
			}
		}
	}
	return false
}

func quiescentCompletionTime(started, observed time.Time, statuses []map[string]any) time.Time {
	latestProgressMS := int64(0)
	for _, status := range statuses {
		if value := timestampValue(status["last_progress_at"]); value > latestProgressMS {
			latestProgressMS = value
		}
	}
	if latestProgressMS <= 0 {
		return observed
	}
	finished := time.UnixMilli(latestProgressMS)
	if finished.Before(started) {
		finished = started
	}
	if finished.After(observed) {
		finished = observed
	}
	return finished
}

func timestampValue(value any) int64 {
	switch item := value.(type) {
	case float64:
		return int64(item)
	case float32:
		return int64(item)
	case int64:
		return item
	case int:
		return int64(item)
	case json.Number:
		parsed, _ := item.Int64()
		return parsed
	}
	return 0
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

func csvHeaderIndex(header []string, name string) int {
	for index, value := range header {
		if strings.EqualFold(strings.TrimSpace(value), name) {
			return index
		}
	}
	return -1
}

func loadClientLogicalIDMapping(dataDir string) (map[string]string, error) {
	path := filepath.Join(dataDir, "client", "client_lifecycle.csv")
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("read client lifecycle identity mapping: %w", err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse client lifecycle identity mapping: %w", err)
	}
	if len(rows) == 0 {
		return map[string]string{}, nil
	}
	txIndex := csvHeaderIndex(rows[0], "tx_id")
	logicalIndex := csvHeaderIndex(rows[0], "logical_tx_id")
	if txIndex < 0 || logicalIndex < 0 {
		return map[string]string{}, nil
	}
	mapping := map[string]string{}
	for rowIndex, row := range rows[1:] {
		if txIndex >= len(row) || logicalIndex >= len(row) {
			return nil, fmt.Errorf("client lifecycle identity row %d has %d fields", rowIndex+2, len(row))
		}
		txID := strings.TrimSpace(row[txIndex])
		logicalID := strings.TrimSpace(row[logicalIndex])
		if txID == "" || logicalID == "" {
			continue
		}
		if previous, exists := mapping[txID]; exists && previous != logicalID {
			return nil, fmt.Errorf("client lifecycle maps physical tx %s to conflicting logical ids %s and %s", txID, previous, logicalID)
		}
		mapping[txID] = logicalID
	}
	return mapping, nil
}

func parseSubmissionClassification(dataDir string, rows [][]string, expected int) (map[string]bool, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("submitted transaction classification is empty")
	}
	txIndex := csvHeaderIndex(rows[0], "tx_id")
	logicalIndex := csvHeaderIndex(rows[0], "logical_tx_id")
	crossIndex := csvHeaderIndex(rows[0], "is_cross_shard")
	if txIndex < 0 {
		return nil, fmt.Errorf("submitted transaction classification is missing tx_id")
	}
	if crossIndex < 0 {
		return nil, fmt.Errorf("submitted transaction classification is missing is_cross_shard")
	}
	logicalByPhysical := map[string]string{}
	if logicalIndex < 0 {
		mapping, err := loadClientLogicalIDMapping(dataDir)
		if err != nil {
			return nil, err
		}
		logicalByPhysical = mapping
	}
	classification := map[string]bool{}
	for rowIndex, row := range rows[1:] {
		if txIndex >= len(row) || crossIndex >= len(row) || (logicalIndex >= 0 && logicalIndex >= len(row)) {
			return nil, fmt.Errorf("submitted transaction classification row %d has %d fields", rowIndex+2, len(row))
		}
		physicalID := strings.TrimSpace(row[txIndex])
		if physicalID == "" {
			return nil, fmt.Errorf("submitted transaction classification row %d has empty tx_id", rowIndex+2)
		}
		logicalID := ""
		if logicalIndex >= 0 {
			logicalID = strings.TrimSpace(row[logicalIndex])
		}
		if logicalID == "" {
			logicalID = strings.TrimSpace(logicalByPhysical[physicalID])
		}
		if logicalID == "" {
			// Legacy runs used the physical id as the logical id.
			logicalID = physicalID
		}
		isCross, err := strconv.ParseBool(strings.TrimSpace(row[crossIndex]))
		if err != nil {
			return nil, fmt.Errorf("submitted transaction classification row %d logical tx %s: %w", rowIndex+2, logicalID, err)
		}
		if previous, exists := classification[logicalID]; exists && previous != isCross {
			return nil, fmt.Errorf("submitted transaction classification has conflicting cross-shard values for %s", logicalID)
		}
		classification[logicalID] = isCross
	}
	// expected < 0 means the caller is reading a legacy artifact that did not
	// persist an authoritative submitted count. The live drain path and current
	// artifacts always pass a non-negative count and keep the strict check.
	if expected >= 0 {
		if err := validateSubmissionClassification(classification, expected); err != nil {
			return nil, err
		}
	}
	return classification, nil
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
	return parseSubmissionClassification(dataDir, rows, expected)
}

func deriveLiveTerminal(classification map[string]bool, statuses []map[string]any, statelessDirect bool) (map[string]bool, map[string]int, error) {
	if err := validateSubmissionClassification(classification, len(classification)); err != nil {
		return nil, nil, err
	}
	terminal := map[string]bool{}
	for _, status := range statuses {
		for _, logicalID := range stringSlice(status["durable_committed_logical_tx_ids"]) {
			if isCross, submitted := classification[logicalID]; submitted && (!isCross || statelessDirect) {
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

func deriveFinalityArtifacts(dataDir string, nodes []v5.NodePlan, statelessDirect bool) (map[string]any, error) {
	drain, err := readDrainStatus(dataDir)
	if err != nil {
		return nil, err
	}
	expectedSubmitted := drain.Submitted
	if expectedSubmitted <= 0 {
		// drain_status.json files produced before logical-identity accounting did
		// not contain submitted. In that compatibility case the submission CSV
		// remains authoritative; -1 disables only this redundant count check.
		expectedSubmitted = -1
	}
	classification, err := loadSubmissionClassification(dataDir, expectedSubmitted)
	if err != nil {
		return nil, err
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
		if stage == "durable_committed" && entry.cross && !statelessDirect {
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
			if entry.crossFinal || (statelessDirect && entry.durableCommitted) {
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
			if entry.submitted > 0 && (first == 0 || entry.submitted < first) {
				first = entry.submitted
			}
			if entry.terminal > last {
				last = entry.terminal
			}
		}
	}
	timing := computeFinalityTiming(finalized, first, last, drain.FinishedAt, countSystemDeltaDrainBlocks(dataDir, nodes))
	if err := metrics.WriteCSV(filepath.Join(dataDir, "latency_distribution.csv"), []string{"percentile", "finality_ms"}, [][]string{{"p50", fmt.Sprint(percentile(.50))}, {"p95", fmt.Sprint(percentile(.95))}, {"p99", fmt.Sprint(percentile(.99))}}); err != nil {
		return nil, err
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "throughput_windows.csv"), []string{"window_name", "window_start_ms", "window_end_ms", "finalized_unique_logical_txs", "duration_ms", "throughput_tps"}, [][]string{
		{"logical_finality", fmt.Sprint(timing.LogicalWindowStartMS), fmt.Sprint(timing.LogicalWindowEndMS), fmt.Sprint(finalized), fmt.Sprint(timing.LogicalFinalityDurationMS), fmt.Sprintf("%.6f", timing.LogicalFinalityTPS)},
		{"end_to_end_completion", fmt.Sprint(timing.CompletionWindowStartMS), fmt.Sprint(timing.CompletionWindowEndMS), fmt.Sprint(finalized), fmt.Sprint(timing.CompletionDurationMS), fmt.Sprintf("%.6f", timing.EndToEndTPS)},
	}); err != nil {
		return nil, err
	}
	terminalUnique := intraTerminal + crossFinalized + crossRefunded + crossFailed
	summary := map[string]any{"metric_truth": "derived_from_raw_runtime_lifecycle_and_drain_completion", "cross_shard_execution_mode": map[bool]string{true: "stateless_direct_execution", false: "legacy_lock_relay_finalize"}[statelessDirect], "logical_transaction_count": len(byLogical), "submitted_unique_tx_count": len(byLogical), "intra_shard_committed_unique_count": intraCommitted, "intra_shard_terminal_unique_count": intraTerminal, "cross_shard_requested_unique_count": crossRequested, "cross_shard_target_committed_unique_count": crossTarget, "cross_shard_finalized_unique_count": crossFinalized, "cross_shard_refunded_unique_count": crossRefunded, "cross_shard_failed_unique_count": crossFailed, "terminal_unique_tx_count": terminalUnique, "incomplete_unique_tx_count": len(byLogical) - terminalUnique, "finalized_unique_logical_tx_count": finalized, "p50_finality_ms": percentile(.50), "p95_finality_ms": percentile(.95), "p99_finality_ms": percentile(.99), "throughput_tps": timing.EndToEndTPS, "logical_window_start_ms": timing.LogicalWindowStartMS, "logical_window_end_ms": timing.LogicalWindowEndMS, "logical_finality_duration_ms": timing.LogicalFinalityDurationMS, "logical_finality_tps": timing.LogicalFinalityTPS, "drain_started_at_ms": timing.DrainStartedAtMS, "drain_finished_at_ms": timing.DrainFinishedAtMS, "drain_duration_ms": timing.DrainDurationMS, "system_delta_drain_block_count": timing.SystemDeltaDrainBlockCount, "completion_window_start_ms": timing.CompletionWindowStartMS, "completion_window_end_ms": timing.CompletionWindowEndMS, "completion_duration_ms": timing.CompletionDurationMS, "end_to_end_tps": timing.EndToEndTPS, "tail_completion_overhead_ms": timing.TailCompletionOverheadMS, "tcp_send_latency_excluded": true}
	return summary, v5.SaveJSON(filepath.Join(dataDir, "finality_summary.json"), summary)
}

type drainStatusArtifact struct {
	Submitted        int    `json:"submitted"`
	CompletionReason string `json:"completion_reason"`
	FinishedAt       int64  `json:"drain_finished_at"`
}

type finalityTiming struct {
	LogicalWindowStartMS       int64
	LogicalWindowEndMS         int64
	LogicalFinalityDurationMS  int64
	LogicalFinalityTPS         float64
	DrainStartedAtMS           int64
	DrainFinishedAtMS          int64
	DrainDurationMS            int64
	SystemDeltaDrainBlockCount int
	CompletionWindowStartMS    int64
	CompletionWindowEndMS      int64
	CompletionDurationMS       int64
	EndToEndTPS                float64
	TailCompletionOverheadMS   int64
}

func readDrainStatus(dataDir string) (drainStatusArtifact, error) {
	raw, err := os.ReadFile(filepath.Join(dataDir, "drain_status.json"))
	if err != nil {
		return drainStatusArtifact{}, fmt.Errorf("read drain_status.json for completion timing: %w", err)
	}
	var status drainStatusArtifact
	if err := json.Unmarshal(raw, &status); err != nil {
		return drainStatusArtifact{}, fmt.Errorf("decode drain_status.json: %w", err)
	}
	if status.CompletionReason != "drain_quiescent" {
		return drainStatusArtifact{}, fmt.Errorf("drain completion is not quiescent: %s", status.CompletionReason)
	}
	if status.FinishedAt <= 0 {
		return drainStatusArtifact{}, fmt.Errorf("drain_status.json missing drain_finished_at")
	}
	return status, nil
}

func computeFinalityTiming(finalized int, logicalStart, logicalEnd, drainFinished int64, drainBlocks int) finalityTiming {
	if drainFinished < logicalEnd {
		drainFinished = logicalEnd
	}
	timing := finalityTiming{
		LogicalWindowStartMS:       logicalStart,
		LogicalWindowEndMS:         logicalEnd,
		DrainStartedAtMS:           logicalEnd,
		DrainFinishedAtMS:          drainFinished,
		SystemDeltaDrainBlockCount: drainBlocks,
		CompletionWindowStartMS:    logicalStart,
		CompletionWindowEndMS:      drainFinished,
		TailCompletionOverheadMS:   drainFinished - logicalEnd,
	}
	timing.LogicalFinalityDurationMS = positiveDuration(logicalStart, logicalEnd)
	timing.DrainDurationMS = positiveDuration(logicalEnd, drainFinished)
	timing.CompletionDurationMS = positiveDuration(logicalStart, drainFinished)
	timing.LogicalFinalityTPS = throughput(finalized, timing.LogicalFinalityDurationMS)
	timing.EndToEndTPS = throughput(finalized, timing.CompletionDurationMS)
	return timing
}

func positiveDuration(start, end int64) int64 {
	if end > start {
		return end - start
	}
	return 0
}

func throughput(count int, durationMS int64) float64 {
	if durationMS <= 0 {
		return 0
	}
	return float64(count) * 1000 / float64(durationMS)
}

func countSystemDeltaDrainBlocks(dataDir string, nodes []v5.NodePlan) int {
	count := 0
	paths := []string{}
	for _, node := range nodes {
		paths = append(paths, filepath.Join(node.DataDir, "block_execution_summary.json"))
	}
	if len(paths) == 0 {
		entries, err := os.ReadDir(dataDir)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() && strings.HasPrefix(entry.Name(), "node_") {
					paths = append(paths, filepath.Join(dataDir, entry.Name(), "block_execution_summary.json"))
				}
			}
		}
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		reader := csv.NewReader(file)
		rows, err := reader.ReadAll()
		_ = file.Close()
		if err != nil || len(rows) == 0 {
			continue
		}
		systemIndex, txIndex := -1, -1
		for index, name := range rows[0] {
			if name == "system_delta_count" {
				systemIndex = index
			}
			if name == "tx_count" {
				txIndex = index
			}
		}
		if systemIndex < 0 || txIndex < 0 {
			continue
		}
		for _, row := range rows[1:] {
			if systemIndex < len(row) && txIndex < len(row) && csvInt(row[systemIndex]) > 0 && csvInt(row[txIndex]) == 0 {
				count++
			}
		}
	}
	return count
}

func csvInt(value string) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return parsed
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

func shutdownBudget(plan v5.Plan) time.Duration {
	timeout := 30 * time.Second
	txBudget := time.Duration(plan.WorkloadPlan.TxCount/50) * time.Second
	nodeBudget := time.Duration(len(plan.NodeConfigs)) * 5 * time.Second
	intervalBudget := shutdownBlockInterval(plan) * 4
	timeout += txBudget + nodeBudget + intervalBudget
	if timeout < 30*time.Second {
		return 30 * time.Second
	}
	if timeout > 10*time.Minute {
		return 10 * time.Minute
	}
	return timeout
}

func shutdownBlockInterval(plan v5.Plan) time.Duration {
	for _, node := range plan.NodeConfigs {
		if plugin, ok := node.PluginProfile["block_producer"]; ok {
			if raw, ok := plugin.Config["interval_ms"]; ok {
				switch value := raw.(type) {
				case int:
					if value > 0 {
						return time.Duration(value) * time.Millisecond
					}
				case float64:
					if value > 0 {
						return time.Duration(value) * time.Millisecond
					}
				}
			}
		}
	}
	return 75 * time.Millisecond
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
	remoteOperationUnknownCount := 0
	physicalRemoteOperationCount := 0
	physicalRemoteFetchCount := 0
	physicalRemoteWritebackCount := 0
	physicalRemoteFailedCount := 0
	remoteStateLatencyWeightedSum := 0.0
	logicalUpdateCount := 0
	physicalUpdateCount := 0
	physicalReplicaExecutedLogicalTxCount := 0
	executedLogicalTxByShard := map[string]int{}
	executedTxInstanceCount := 0
	preAggregationPhysicalOps := 0
	postAggregationPhysicalOps := 0
	aggregatedKeyCount := 0
	aggregatedLogicalDeltaCount := 0
	physicalOpsSavedCount := 0
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
	stateReadyWaitByShard := map[string]int64{}
	stateReadyResumeByShard := map[string]int64{}
	stateReadyWaitMSByShard := map[string]int64{}
	stateReadyFetchByShard := map[string]int64{}
	stateReadyFetchCompletedByShard := map[string]int64{}
	stateReadyModes := map[string]bool{}
	versionedWaveByShard := map[string]int64{}
	versionedWaitByShard := map[string]int64{}
	versionedResolvedByShard := map[string]int64{}
	versionedProbeByShard := map[string]int64{}
	versionedProbeLatencyByShard := map[string]int64{}
	versionedMaxWaveWidth := int64(0)
	versionedModes := map[string]bool{}
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
		remoteOperationUnknownCount += item.RemoteOperationUnknownCount
		physicalRemoteOperationCount += item.PhysicalRemoteOperationCount
		physicalRemoteFetchCount += item.PhysicalRemoteFetchCount
		physicalRemoteWritebackCount += item.PhysicalRemoteWritebackCount
		physicalRemoteFailedCount += item.PhysicalRemoteFailedCount
		remoteStateLatencyWeightedSum += item.RemoteStateAccessAvgLatencyMS * float64(item.RemoteStateAccessCount)
		logicalUpdateCount += item.LogicalUpdateCount
		physicalUpdateCount += item.PhysicalUpdateCount
		physicalReplicaExecutedLogicalTxCount += item.ExecutedLogicalTxCount
		if item.ExecutedLogicalTxCount > executedLogicalTxByShard[item.ShardID] {
			executedLogicalTxByShard[item.ShardID] = item.ExecutedLogicalTxCount
		}
		executedTxInstanceCount += item.ExecutedTxInstanceCount
		preAggregationPhysicalOps += item.PreAggregationPhysicalOps
		postAggregationPhysicalOps += item.PostAggregationPhysicalOps
		aggregatedKeyCount += item.AggregatedKeyCount
		aggregatedLogicalDeltaCount += item.AggregatedLogicalDeltaCount
		physicalOpsSavedCount += item.PhysicalOpsSavedCount
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
		if item.StateReadyWaitCount > stateReadyWaitByShard[item.ShardID] {
			stateReadyWaitByShard[item.ShardID] = item.StateReadyWaitCount
		}
		if item.StateReadyResumeCount > stateReadyResumeByShard[item.ShardID] {
			stateReadyResumeByShard[item.ShardID] = item.StateReadyResumeCount
		}
		if item.StatePrefetchWaitMS > stateReadyWaitMSByShard[item.ShardID] {
			stateReadyWaitMSByShard[item.ShardID] = item.StatePrefetchWaitMS
		}
		if item.RemoteStateFetchCount > stateReadyFetchByShard[item.ShardID] {
			stateReadyFetchByShard[item.ShardID] = item.RemoteStateFetchCount
		}
		if item.RemoteStateFetchCompletedCount > stateReadyFetchCompletedByShard[item.ShardID] {
			stateReadyFetchCompletedByShard[item.ShardID] = item.RemoteStateFetchCompletedCount
		}
		if item.StateReadySchedulerMode != "" {
			stateReadyModes[item.StateReadySchedulerMode] = true
		}
		if item.VersionedStateReadyWaveCount > versionedWaveByShard[item.ShardID] {
			versionedWaveByShard[item.ShardID] = item.VersionedStateReadyWaveCount
		}
		if item.VersionedStateReadyWaitCount > versionedWaitByShard[item.ShardID] {
			versionedWaitByShard[item.ShardID] = item.VersionedStateReadyWaitCount
		}
		if item.VersionedStateReadyResolvedCount > versionedResolvedByShard[item.ShardID] {
			versionedResolvedByShard[item.ShardID] = item.VersionedStateReadyResolvedCount
		}
		if item.VersionedStateProbeCount > versionedProbeByShard[item.ShardID] {
			versionedProbeByShard[item.ShardID] = item.VersionedStateProbeCount
		}
		if item.VersionedStateProbeLatencyMS > versionedProbeLatencyByShard[item.ShardID] {
			versionedProbeLatencyByShard[item.ShardID] = item.VersionedStateProbeLatencyMS
		}
		if item.VersionedStateReadyMaxWaveWidth > versionedMaxWaveWidth {
			versionedMaxWaveWidth = item.VersionedStateReadyMaxWaveWidth
		}
		if item.VersionedStateReadySchedulerMode != "" {
			versionedModes[item.VersionedStateReadySchedulerMode] = true
		}
		network, _ := os.ReadFile(filepath.Join(node.DataDir, "network_log.csv"))
		faultEvidence = faultEvidence || strings.Contains(string(network), "fault_")
	}
	matrixStateConsistent, matrixReceiptConsistent, err := writeHeightRootMatrix(dataDir, plan.NodeConfigs)
	if err != nil {
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
	consistent = consistent && matrixStateConsistent
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
	remoteStateAggregate, err := writeRemoteStateAggregate(dataDir, plan.NodeConfigs, logicalTxCount(plan))
	if err != nil {
		return nil, err
	}
	blockProductionAggregate, err := writeBlockProductionAggregate(dataDir, plan.NodeConfigs)
	if err != nil {
		return nil, err
	}
	mechanismMetrics, err := writeMechanismAggregates(dataDir, summaries, plan.NodeConfigs, remoteStateAggregate)
	if err != nil {
		return nil, err
	}
	schedulerIdleRatio := 0.0
	if schedulerEventCount > 0 {
		schedulerIdleRatio = schedulerIdleRatioWeightedSum / float64(schedulerEventCount)
	}
	replicaDeduplicatedExecutedLogicalTxCount := 0
	for _, count := range executedLogicalTxByShard {
		replicaDeduplicatedExecutedLogicalTxCount += count
	}
	sumInt64 := func(values map[string]int64) int64 {
		total := int64(0)
		for _, value := range values {
			total += value
		}
		return total
	}
	stateReadyMode := singleMapKey(stateReadyModes)
	versionedStateReadyMode := singleMapKey(versionedModes)
	return map[string]any{
		"runtime_stage":                                       "v5_1_real_plugin_driven_multi_process_multishard_runtime",
		"runtime_truth":                                       "v5_real_cluster_candidate",
		"one_node_one_os_process":                             true,
		"distinct_process_count":                              len(pids),
		"expected_process_count":                              len(plan.NodeConfigs),
		"independent_tcp_ports":                               len(ports) == len(plan.NodeConfigs),
		"real_client_submission":                              clientInfo != nil,
		"real_signed_tx":                                      true,
		"plugin_driven_runtime":                               true,
		"block_executor_id":                                   singleMapKey(blockExecutors),
		"block_executor_consistent":                           len(blockExecutors) == 1,
		"plan_digest_consistent":                              planDigestConsistent,
		"continuous_multi_shard":                              true,
		"shard_count":                                         len(roots),
		"all_shards_active":                                   allActive,
		"per_shard_multiple_blocks":                           allActive,
		"real_pbft_style_messages":                            pbftCount == len(plan.NodeConfigs),
		"persistent_state":                                    true,
		"state_root_consistent":                               consistent,
		"receipt_root_consistent":                             matrixReceiptConsistent,
		"real_cross_shard_network":                            crossSuccess > 0,
		"cross_shard_success_count":                           crossSuccess,
		"cross_shard_refund_count":                            crossRefund,
		"configured_block_size":                               blockProductionAggregate["configured_block_size"],
		"configured_block_interval_ms":                        blockProductionAggregate["configured_block_interval_ms"],
		"actual_committed_block_count":                        blockProductionAggregate["actual_committed_block_count"],
		"actual_average_tx_per_block":                         blockProductionAggregate["actual_average_tx_per_block"],
		"actual_min_tx_per_block":                             blockProductionAggregate["actual_min_tx_per_block"],
		"actual_max_tx_per_block":                             blockProductionAggregate["actual_max_tx_per_block"],
		"actual_block_interval_mean_ms":                       blockProductionAggregate["actual_block_interval_mean_ms"],
		"actual_block_interval_p95_ms":                        blockProductionAggregate["actual_block_interval_p95_ms"],
		"block_production_summary":                            blockProductionAggregate,
		"logical_update_count":                                logicalUpdateCount,
		"physical_update_count":                               physicalUpdateCount,
		"logical_update_count_deprecated":                     true,
		"physical_update_count_deprecated":                    true,
		"executed_logical_transaction_count":                  replicaDeduplicatedExecutedLogicalTxCount,
		"physical_replica_executed_logical_transaction_count": physicalReplicaExecutedLogicalTxCount,
		"executed_logical_transaction_count_truth_scope":      "replica_deduplicated_by_shard",
		"executed_transaction_instance_count":                 executedTxInstanceCount,
		"pre_aggregation_physical_op_count":                   preAggregationPhysicalOps,
		"post_aggregation_physical_op_count":                  postAggregationPhysicalOps,
		"aggregated_key_count":                                aggregatedKeyCount,
		"aggregated_logical_delta_count":                      aggregatedLogicalDeltaCount,
		"physical_ops_saved_count":                            physicalOpsSavedCount,
		"aggregation_reduction_ratio":                         ratio(physicalOpsSavedCount, preAggregationPhysicalOps),
		"scheduler_event_count":                               schedulerEventCount,
		"scheduler_blocked_count":                             schedulerBlockedCount,
		"scheduler_wakeup_count":                              schedulerWakeupCount,
		"scheduler_stolen_work_count":                         schedulerStolenWorkCount,
		"scheduler_local_execution_count":                     schedulerLocalExecutionCount,
		"scheduler_ready_queue_max_depth":                     schedulerReadyQueueMaxDepth,
		"scheduler_fast_queue_max_depth":                      schedulerFastQueueMaxDepth,
		"scheduler_conservative_queue_max_depth":              schedulerConsQueueMaxDepth,
		"scheduler_dependency_wait_ms":                        schedulerDependencyWaitMS,
		"scheduler_idle_ms":                                   schedulerIdleMS,
		"scheduler_idle_ratio":                                schedulerIdleRatio,
		"state_ready_wait_count":                              sumInt64(stateReadyWaitByShard),
		"state_ready_resume_count":                            sumInt64(stateReadyResumeByShard),
		"state_prefetch_wait_ms":                              sumInt64(stateReadyWaitMSByShard),
		"remote_state_fetch_count":                            sumInt64(stateReadyFetchByShard),
		"remote_state_fetch_completed_count":                  sumInt64(stateReadyFetchCompletedByShard),
		"state_ready_scheduler_mode":                          stateReadyMode,
		"versioned_state_ready_wave_count":                    sumInt64(versionedWaveByShard),
		"versioned_state_ready_wait_observation_count":        sumInt64(versionedWaitByShard),
		"versioned_state_ready_resolved_token_count":          sumInt64(versionedResolvedByShard),
		"versioned_state_probe_count":                         sumInt64(versionedProbeByShard),
		"versioned_state_probe_latency_ms":                    sumInt64(versionedProbeLatencyByShard),
		"versioned_state_ready_max_wave_width":                versionedMaxWaveWidth,
		"versioned_state_ready_scheduler_mode":                versionedStateReadyMode,
		"remote_state_access_count":                           remoteStateAccessCount,
		"remote_state_read_count":                             remoteStateReadCount,
		"remote_state_write_apply_count":                      remoteStateWriteApplyCount,
		"remote_operation_unknown_kind_count":                 remoteOperationUnknownCount,
		"physical_remote_operation_count":                     physicalRemoteOperationCount,
		"physical_remote_fetch_count":                         physicalRemoteFetchCount,
		"physical_remote_writeback_count":                     physicalRemoteWritebackCount,
		"physical_remote_failed_count":                        physicalRemoteFailedCount,
		"replica_deduplicated_remote_operation_count":         remoteStateAggregate["replica_deduplicated_remote_operation_count"],
		"replica_deduplicated_remote_fetch_count":             remoteStateAggregate["replica_deduplicated_remote_fetch_count"],
		"replica_deduplicated_remote_writeback_count":         remoteStateAggregate["replica_deduplicated_remote_writeback_count"],
		"remote_fetches_per_logical_tx":                       remoteStateAggregate["remote_fetches_per_logical_tx"],
		"remote_writebacks_per_logical_tx":                    remoteStateAggregate["remote_writebacks_per_logical_tx"],
		"remote_operations_per_logical_tx":                    remoteStateAggregate["remote_operations_per_logical_tx"],
		"replica_amplification_factor":                        remoteStateAggregate["replica_amplification_factor"],
		"remote_fetch_replica_amplification_factor":           remoteStateAggregate["remote_fetch_replica_amplification_factor"],
		"remote_writeback_replica_amplification_factor":       remoteStateAggregate["remote_writeback_replica_amplification_factor"],
		"mechanism_metrics":                                   mechanismMetrics,
		"remote_state_access_failed_count":                    remoteStateFailedCount,
		"remote_state_access_avg_latency_ms":                  remoteStateAvgLatency,
		"fault_injection_real":                                faultEvidence,
		"fault_injection_requested":                           faultRequested,
		"orphan_process_count":                                0,
		"no_fallback":                                         true,
		"node_summaries":                                      summaries,
		"processes":                                           redactV5Processes(processes, dataDir),
		"shard_blocks":                                        shardBlocks,
		"ready_to_commit":                                     ready,
	}, nil
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

func logicalTxCount(plan v5.Plan) int {
	if plan.WorkloadPlan.ActualTxCount > 0 {
		return plan.WorkloadPlan.ActualTxCount
	}
	if plan.WorkloadPlan.TxCount > 0 {
		return plan.WorkloadPlan.TxCount
	}
	return plan.WorkloadPlan.RequestedTxCount
}

func writeRemoteStateAggregate(dataDir string, nodes []v5.NodePlan, logicalTxCount int) (map[string]any, error) {
	type op struct {
		key            string
		normalizedKind string
		nodeID         string
		executionShard string
		homeShard      string
		stateKey       string
		blockHash      string
		sourceBlock    string
		sourceHeight   string
		deltaID        string
		txID           string
		latencyMS      string
	}
	physicalTotal := 0
	physicalFetch := 0
	physicalWriteback := 0
	physicalFailed := 0
	unknown := 0
	dedup := map[string]op{}
	physicalRows := [][]string{}
	physicalHeader := []string{"timestamp", "node_id", "execution_shard", "height", "block_hash", "tx_id", "state_key", "qualified_home_key", "home_shard", "response_execution_shard", "access_kind", "normalized_kind", "latency_ms", "witness_digest", "home_state_root", "success", "error", "delta_id", "source_height", "source_block_hash", "update_semantics"}
	for _, node := range nodes {
		path := filepath.Join(node.DataDir, "remote_state_access.csv")
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		closeErr := file.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(records) == 0 {
			continue
		}
		header := map[string]int{}
		for index, name := range records[0] {
			header[name] = index
		}
		for _, row := range records[1:] {
			kind := v5.NormalizeRemoteOperationKind(csvValue(row, header, "access_kind", ""))
			physicalRows = append(physicalRows, []string{
				csvValue(row, header, "timestamp", ""),
				csvValue(row, header, "node_id", node.NodeID),
				csvValue(row, header, "execution_shard", ""),
				csvValue(row, header, "height", ""),
				csvValue(row, header, "block_hash", ""),
				csvValue(row, header, "tx_id", ""),
				csvValue(row, header, "state_key", ""),
				csvValue(row, header, "qualified_home_key", ""),
				csvValue(row, header, "home_shard", ""),
				csvValue(row, header, "response_execution_shard", ""),
				csvValue(row, header, "access_kind", ""),
				kind,
				csvValue(row, header, "latency_ms", ""),
				csvValue(row, header, "witness_digest", ""),
				csvValue(row, header, "home_state_root", ""),
				csvValue(row, header, "success", ""),
				csvValue(row, header, "error", ""),
				csvValue(row, header, "delta_id", ""),
				csvValue(row, header, "source_height", ""),
				csvValue(row, header, "source_block_hash", ""),
				csvValue(row, header, "update_semantics", ""),
			})
			if !csvBool(row, header, "success") {
				physicalFailed++
				continue
			}
			switch kind {
			case "fetch":
				physicalFetch++
			case "writeback":
				physicalWriteback++
			default:
				unknown++
			}
			physicalTotal++
			key := remoteStateDedupKey(row, header, kind)
			if _, ok := dedup[key]; !ok {
				dedup[key] = op{
					key:            key,
					normalizedKind: kind,
					nodeID:         csvValue(row, header, "node_id", ""),
					executionShard: csvValue(row, header, "execution_shard", ""),
					homeShard:      csvValue(row, header, "home_shard", ""),
					stateKey:       csvValue(row, header, "state_key", ""),
					blockHash:      csvValue(row, header, "block_hash", ""),
					sourceBlock:    csvValue(row, header, "source_block_hash", ""),
					sourceHeight:   csvValue(row, header, "source_height", ""),
					deltaID:        csvValue(row, header, "delta_id", ""),
					txID:           csvValue(row, header, "tx_id", ""),
					latencyMS:      csvValue(row, header, "latency_ms", ""),
				}
			}
		}
	}
	dedupFetch := 0
	dedupWriteback := 0
	dedupUnknown := 0
	rows := make([][]string, 0, len(dedup))
	keys := make([]string, 0, len(dedup))
	for key := range dedup {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		item := dedup[key]
		switch item.normalizedKind {
		case "fetch":
			dedupFetch++
		case "writeback":
			dedupWriteback++
		default:
			dedupUnknown++
		}
		rows = append(rows, []string{item.normalizedKind, item.blockHash, item.sourceBlock, item.sourceHeight, item.executionShard, item.homeShard, item.stateKey, item.deltaID, item.txID, item.nodeID, item.latencyMS, item.key})
	}
	aggregateDir := filepath.Join(dataDir, "aggregate")
	if err := os.MkdirAll(aggregateDir, 0o755); err != nil {
		return nil, err
	}
	if err := metrics.WriteCSV(filepath.Join(dataDir, "physical_remote_state_operations.csv"), physicalHeader, physicalRows); err != nil {
		return nil, err
	}
	if err := metrics.WriteCSV(filepath.Join(aggregateDir, "replica_deduplicated_remote_operations.csv"), []string{"normalized_kind", "block_hash", "source_block_hash", "source_height", "execution_shard", "home_shard", "state_key", "delta_id", "tx_id", "example_node_id", "example_latency_ms", "dedup_key"}, rows); err != nil {
		return nil, err
	}
	dedupTotal := len(dedup)
	summary := map[string]any{
		"schema_version":                                 "mbe_remote_state_metrics_v2",
		"truth_scope":                                    "node_physical_and_replica_deduplicated",
		"physical_remote_operation_count":                physicalTotal,
		"physical_remote_fetch_count":                    physicalFetch,
		"physical_remote_writeback_count":                physicalWriteback,
		"physical_remote_failed_count":                   physicalFailed,
		"remote_operation_unknown_kind_count":            unknown,
		"replica_deduplicated_remote_operation_count":    dedupTotal,
		"replica_deduplicated_remote_fetch_count":        dedupFetch,
		"replica_deduplicated_remote_writeback_count":    dedupWriteback,
		"replica_deduplicated_remote_unknown_kind_count": dedupUnknown,
		"remote_fetches_per_logical_tx":                  ratio(dedupFetch, logicalTxCount),
		"remote_writebacks_per_logical_tx":               ratio(dedupWriteback, logicalTxCount),
		"remote_operations_per_logical_tx":               ratio(dedupTotal, logicalTxCount),
		"replica_amplification_factor":                   ratio(physicalTotal, dedupTotal),
		"remote_fetch_replica_amplification_factor":      ratio(physicalFetch, dedupFetch),
		"remote_writeback_replica_amplification_factor":  ratio(physicalWriteback, dedupWriteback),
		"logical_tx_count":                               logicalTxCount,
		"physical_remote_operation_artifact":             "physical_remote_state_operations.csv",
		"replica_deduplicated_operation_artifact":        "aggregate/replica_deduplicated_remote_operations.csv",
	}
	return summary, v5.SaveJSON(filepath.Join(aggregateDir, "remote_state_metrics_summary.json"), summary)
}

func writeBlockProductionAggregate(dataDir string, nodes []v5.NodePlan) (map[string]any, error) {
	type blockRow struct {
		shardID    string
		height     string
		blockHash  string
		txCount    int
		finishedAt int64
	}
	dedup := map[string]blockRow{}
	configuredBlockSize := 100
	configuredIntervalMS := 75
	for index, node := range nodes {
		if index == 0 {
			if producer, ok := node.PluginProfile["block_producer"]; ok {
				if value := number(producer.Config["block_size"]); value > 0 {
					configuredBlockSize = value
				}
				if value := number(producer.Config["interval_ms"]); value > 0 {
					configuredIntervalMS = value
				}
			}
		}
		file, err := os.Open(filepath.Join(node.DataDir, "committed_chain.csv"))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		records, err := csv.NewReader(file).ReadAll()
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		for rowIndex, row := range records {
			if rowIndex == 0 || len(row) < 13 {
				continue
			}
			txCount := 0
			_, _ = fmt.Sscan(row[6], &txCount)
			var finishedAt int64
			_, _ = fmt.Sscan(row[12], &finishedAt)
			key := strings.Join([]string{row[1], row[2], row[4]}, "|")
			dedup[key] = blockRow{shardID: row[1], height: row[2], blockHash: row[4], txCount: txCount, finishedAt: finishedAt}
		}
	}
	rows := make([]blockRow, 0, len(dedup))
	for _, item := range dedup {
		rows = append(rows, item)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].shardID != rows[j].shardID {
			return rows[i].shardID < rows[j].shardID
		}
		if rows[i].height != rows[j].height {
			return rows[i].height < rows[j].height
		}
		return rows[i].blockHash < rows[j].blockHash
	})
	txSum := 0
	minTx := 0
	maxTx := 0
	byShard := map[string][]blockRow{}
	for index, item := range rows {
		txSum += item.txCount
		if index == 0 || item.txCount < minTx {
			minTx = item.txCount
		}
		if index == 0 || item.txCount > maxTx {
			maxTx = item.txCount
		}
		byShard[item.shardID] = append(byShard[item.shardID], item)
	}
	intervals := []int64{}
	for _, shardRows := range byShard {
		sort.Slice(shardRows, func(i, j int) bool { return shardRows[i].finishedAt < shardRows[j].finishedAt })
		for index := 1; index < len(shardRows); index++ {
			if shardRows[index].finishedAt > 0 && shardRows[index-1].finishedAt > 0 {
				if delta := shardRows[index].finishedAt - shardRows[index-1].finishedAt; delta >= 0 {
					intervals = append(intervals, delta)
				}
			}
		}
	}
	intervalSum := int64(0)
	for _, value := range intervals {
		intervalSum += value
	}
	averageTx := 0.0
	if len(rows) > 0 {
		averageTx = float64(txSum) / float64(len(rows))
	}
	intervalMean := 0.0
	if len(intervals) > 0 {
		intervalMean = float64(intervalSum) / float64(len(intervals))
	}
	summary := map[string]any{
		"schema_version":                "mbe_block_production_summary_v1",
		"truth_scope":                   "replica_deduplicated_committed_chain",
		"configured_block_size":         configuredBlockSize,
		"configured_block_interval_ms":  configuredIntervalMS,
		"actual_committed_block_count":  len(rows),
		"actual_average_tx_per_block":   averageTx,
		"actual_min_tx_per_block":       minTx,
		"actual_max_tx_per_block":       maxTx,
		"actual_block_interval_mean_ms": intervalMean,
		"actual_block_interval_p95_ms":  int64Percentile(intervals, 0.95),
		"replica_deduplication_key":     "shard_id|height|block_hash",
		"source_artifact":               "nodes/*/committed_chain.csv",
	}
	aggregateDir := filepath.Join(dataDir, "aggregate")
	if err := os.MkdirAll(aggregateDir, 0o755); err != nil {
		return nil, err
	}
	return summary, v5.SaveJSON(filepath.Join(aggregateDir, "block_production_summary.json"), summary)
}

func writeMechanismAggregates(dataDir string, summaries []v5NodeSummary, nodes []v5.NodePlan, remoteState map[string]any) (map[string]any, error) {
	aggregateDir := filepath.Join(dataDir, "aggregate")
	if err := os.MkdirAll(aggregateDir, 0o755); err != nil {
		return nil, err
	}
	metatrackApplicable := false
	metatrack := map[string]any{
		"schema_version": "mbe_metatrack_aggregate_summary_v1",
		"status":         "not_applicable",
	}
	physicalFast := 0
	physicalConservative := 0
	fastByShard := map[string]int{}
	conservativeByShard := map[string]int{}
	aggregationGroups := 0
	schedulerEvents := 0
	blocked := 0
	wakeup := 0
	preOps := 0
	postOps := 0
	aggregatedKeys := 0
	aggregatedLogicalDeltas := 0
	for _, item := range summaries {
		if item.FastTrackCount > 0 || item.ConservativeTrackCount > 0 || item.AggregationGroupCount > 0 || item.RemoteStateAccessCount > 0 {
			metatrackApplicable = true
		}
		physicalFast += item.FastTrackCount
		physicalConservative += item.ConservativeTrackCount
		if item.FastTrackCount > fastByShard[item.ShardID] {
			fastByShard[item.ShardID] = item.FastTrackCount
		}
		if item.ConservativeTrackCount > conservativeByShard[item.ShardID] {
			conservativeByShard[item.ShardID] = item.ConservativeTrackCount
		}
		aggregationGroups += item.AggregationGroupCount
		schedulerEvents += item.SchedulerEventCount
		blocked += item.SchedulerBlockedCount
		wakeup += item.SchedulerWakeupCount
		preOps += item.PreAggregationPhysicalOps
		postOps += item.PostAggregationPhysicalOps
		aggregatedKeys += item.AggregatedKeyCount
		aggregatedLogicalDeltas += item.AggregatedLogicalDeltaCount
	}
	if metatrackApplicable {
		fast := 0
		conservative := 0
		for _, count := range fastByShard {
			fast += count
		}
		for _, count := range conservativeByShard {
			conservative += count
		}
		totalTracks := fast + conservative
		metatrack = map[string]any{
			"schema_version":                                     "mbe_metatrack_aggregate_summary_v1",
			"status":                                             "available",
			"fast_track_logical_tx_count":                        fast,
			"conservative_track_logical_tx_count":                conservative,
			"logical_track_count_truth_scope":                    "replica_deduplicated_by_shard",
			"physical_replica_fast_track_instance_count":         physicalFast,
			"physical_replica_conservative_track_instance_count": physicalConservative,
			"fast_track_ratio":                                   ratio(fast, totalTracks),
			"conservative_track_ratio":                           ratio(conservative, totalTracks),
			"runtime_scheduler_event_count":                      schedulerEvents,
			"blocked_scheduler_event_count":                      blocked,
			"wakeup_scheduler_event_count":                       wakeup,
			"blocked_logical_tx_count":                           blocked,
			"blocked_logical_tx_count_deprecated":                true,
			"wakeup_logical_tx_count":                            wakeup,
			"wakeup_logical_tx_count_deprecated":                 true,
			"aggregation_group_count":                            aggregationGroups,
			"aggregated_key_count":                               aggregatedKeys,
			"aggregated_logical_delta_count":                     aggregatedLogicalDeltas,
			"pre_aggregation_physical_op_count":                  preOps,
			"post_aggregation_physical_op_count":                 postOps,
			"physical_ops_saved_count":                           maxInt(preOps-postOps, 0),
			"aggregation_reduction_ratio":                        ratio(maxInt(preOps-postOps, 0), preOps),
			"physical_remote_fetch_count":                        remoteState["physical_remote_fetch_count"],
			"physical_remote_writeback_count":                    remoteState["physical_remote_writeback_count"],
			"replica_deduplicated_remote_fetch_count":            remoteState["replica_deduplicated_remote_fetch_count"],
			"replica_deduplicated_remote_writeback_count":        remoteState["replica_deduplicated_remote_writeback_count"],
		}
	}
	blockSTM, err := aggregateBlockSTM(nodes)
	if err != nil {
		return nil, err
	}
	if err := v5.SaveJSON(filepath.Join(aggregateDir, "metatrack_aggregate_summary.json"), metatrack); err != nil {
		return nil, err
	}
	if err := v5.SaveJSON(filepath.Join(aggregateDir, "block_stm_aggregate_summary.json"), blockSTM); err != nil {
		return nil, err
	}
	mechanism := map[string]any{
		"schema_version": "mbe_mechanism_metrics_summary_v1",
		"metatrack":      metatrack,
		"block_stm":      blockSTM,
		"remote_state":   remoteState,
	}
	return mechanism, v5.SaveJSON(filepath.Join(aggregateDir, "mechanism_metrics_summary.json"), mechanism)
}

func aggregateBlockSTM(nodes []v5.NodePlan) (map[string]any, error) {
	type nodeMetrics struct {
		nodeID                    string
		shardID                   string
		workerCount               int
		maximumParallelWidth      int
		maximumConcurrentExec     int
		maximumIncarnation        int
		abortCount                int
		dependencyAbortCount      int
		validationAbortCount      int
		reexecutionCount          int
		validationFailure         int
		validatedSpeculative      int
		dependencyWaitCount       int
		dependencyResumeCount     int
		estimatePublishCount      int
		estimateMarkCount         int
		estimateReadCount         int
		businessExecutionCount    int
		committedTransactionCount int
		serialFallbackCount       int
		serialEquivalent          bool
	}
	values := []nodeMetrics{}
	for _, node := range nodes {
		raw := map[string]any{}
		path := filepath.Join(node.DataDir, "block_stm_summary.json")
		if err := readJSONMap(path, &raw); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		metricsMap, _ := raw["block_stm_metrics"].(map[string]any)
		if metricsMap == nil {
			continue
		}
		estimatePublish := intFromAny(metricsMap["estimate_count"])
		if estimatePublish == 0 {
			estimatePublish = intFromAny(metricsMap["estimate_mark_count"])
		}
		values = append(values, nodeMetrics{
			nodeID:                    node.NodeID,
			shardID:                   node.ShardID,
			workerCount:               intFromAny(metricsMap["worker_count"]),
			maximumParallelWidth:      intFromAny(metricsMap["maximum_parallel_width"]),
			maximumConcurrentExec:     intFromAny(metricsMap["maximum_concurrent_executions"]),
			maximumIncarnation:        intFromAny(metricsMap["maximum_incarnation"]),
			abortCount:                intFromAny(metricsMap["abort_count"]),
			dependencyAbortCount:      intFromAny(metricsMap["dependency_abort_count"]),
			validationAbortCount:      intFromAny(metricsMap["validation_abort_count"]),
			reexecutionCount:          intFromAny(metricsMap["reexecution_count"]),
			validationFailure:         intFromAny(metricsMap["validation_failure_count"]),
			validatedSpeculative:      intFromAny(metricsMap["validated_speculative_result_count"]),
			dependencyWaitCount:       intFromAny(metricsMap["dependency_wait_count"]),
			dependencyResumeCount:     intFromAny(metricsMap["dependency_resume_count"]),
			estimatePublishCount:      estimatePublish,
			estimateMarkCount:         intFromAny(metricsMap["estimate_mark_count"]),
			estimateReadCount:         intFromAny(metricsMap["estimate_read_count"]),
			businessExecutionCount:    intFromAny(metricsMap["business_execution_invocation_count"]),
			committedTransactionCount: intFromAny(metricsMap["committed_transaction_count"]),
			serialFallbackCount:       intFromAny(metricsMap["serial_fallback_count"]),
			serialEquivalent:          boolFromAny(raw["serial_equivalent"]),
		})
	}
	if len(values) == 0 {
		return map[string]any{"schema_version": "mbe_block_stm_aggregate_summary_v2", "status": "not_applicable"}, nil
	}
	workerCounts := map[int]bool{}
	serialEquivalent := true
	abortSum := 0
	dependencyAbortSum := 0
	validationAbortSum := 0
	reexecutionSum := 0
	validationFailureSum := 0
	validatedSpeculativeSum := 0
	dependencyWaitSum := 0
	dependencyResumeSum := 0
	estimatePublishSum := 0
	estimateMarkSum := 0
	estimateReadSum := 0
	businessExecutionSum := 0
	committedPhysicalSum := 0
	serialFallbackSum := 0
	maxWidth := 0
	maxConcurrent := 0
	maxIncarnation := 0
	maxAbort := 0
	maxReexecution := 0
	committedByShard := map[string]int{}
	perValidator := make([]map[string]any, 0, len(values))
	for _, item := range values {
		workerCounts[item.workerCount] = true
		serialEquivalent = serialEquivalent && item.serialEquivalent
		abortSum += item.abortCount
		dependencyAbortSum += item.dependencyAbortCount
		validationAbortSum += item.validationAbortCount
		reexecutionSum += item.reexecutionCount
		validationFailureSum += item.validationFailure
		validatedSpeculativeSum += item.validatedSpeculative
		dependencyWaitSum += item.dependencyWaitCount
		dependencyResumeSum += item.dependencyResumeCount
		estimatePublishSum += item.estimatePublishCount
		estimateMarkSum += item.estimateMarkCount
		estimateReadSum += item.estimateReadCount
		businessExecutionSum += item.businessExecutionCount
		committedPhysicalSum += item.committedTransactionCount
		serialFallbackSum += item.serialFallbackCount
		maxWidth = maxInt(maxWidth, item.maximumParallelWidth)
		maxConcurrent = maxInt(maxConcurrent, item.maximumConcurrentExec)
		maxIncarnation = maxInt(maxIncarnation, item.maximumIncarnation)
		maxAbort = maxInt(maxAbort, item.abortCount)
		maxReexecution = maxInt(maxReexecution, item.reexecutionCount)
		if item.committedTransactionCount > committedByShard[item.shardID] {
			committedByShard[item.shardID] = item.committedTransactionCount
		}
		perValidator = append(perValidator, map[string]any{
			"node_id":                             item.nodeID,
			"shard_id":                            item.shardID,
			"abort_count":                         item.abortCount,
			"dependency_abort_count":              item.dependencyAbortCount,
			"validation_abort_count":              item.validationAbortCount,
			"reexecution_count":                   item.reexecutionCount,
			"validation_failure_count":            item.validationFailure,
			"dependency_wait_count":               item.dependencyWaitCount,
			"dependency_resume_count":             item.dependencyResumeCount,
			"estimate_publish_count":              item.estimatePublishCount,
			"estimate_mark_count":                 item.estimateMarkCount,
			"estimate_read_count":                 item.estimateReadCount,
			"maximum_incarnation":                 item.maximumIncarnation,
			"business_execution_invocation_count": item.businessExecutionCount,
			"committed_transaction_count":         item.committedTransactionCount,
			"serial_fallback_count":               item.serialFallbackCount,
		})
	}
	replicaDeduplicatedCommitted := 0
	for _, count := range committedByShard {
		replicaDeduplicatedCommitted += count
	}
	return map[string]any{
		"schema_version":                                     "mbe_block_stm_aggregate_summary_v2",
		"status":                                             "available",
		"metric_truth_scope":                                 "physical_replica_totals_with_explicit_per_validator_and_replica_deduplicated_counts",
		"worker_count":                                       singleIntKey(workerCounts),
		"worker_count_replica_consistent":                    len(workerCounts) == 1,
		"maximum_parallel_width":                             maxWidth,
		"maximum_concurrent_executions":                      maxConcurrent,
		"maximum_incarnation":                                maxIncarnation,
		"abort_count":                                        abortSum,
		"abort_count_truth_scope":                            "physical_replica_total",
		"dependency_abort_count":                             dependencyAbortSum,
		"validation_abort_count":                             validationAbortSum,
		"abort_decomposition_consistent":                     abortSum == dependencyAbortSum+validationAbortSum,
		"reexecution_count":                                  reexecutionSum,
		"reexecution_count_truth_scope":                      "physical_replica_total",
		"validation_failure_count":                           validationFailureSum,
		"validated_speculative_result_count":                 validatedSpeculativeSum,
		"dependency_wait_count":                              dependencyWaitSum,
		"dependency_resume_count":                            dependencyResumeSum,
		"estimate_publish_count":                             estimatePublishSum,
		"estimate_publish_source_field":                      "estimate_count",
		"estimate_mark_count":                                estimateMarkSum,
		"estimate_read_count":                                estimateReadSum,
		"business_execution_invocation_count":                businessExecutionSum,
		"committed_transaction_count_physical_replica_total": committedPhysicalSum,
		"replica_deduplicated_committed_transaction_count":   replicaDeduplicatedCommitted,
		"serial_fallback_count":                              serialFallbackSum,
		"serial_equivalent":                                  serialEquivalent,
		"physical_replica_count":                             len(values),
		"abort_count_per_validator_mean":                     ratio(abortSum, len(values)),
		"abort_count_per_validator_max":                      maxAbort,
		"dependency_abort_count_per_validator_mean":          ratio(dependencyAbortSum, len(values)),
		"validation_abort_count_per_validator_mean":          ratio(validationAbortSum, len(values)),
		"reexecution_count_per_validator_mean":               ratio(reexecutionSum, len(values)),
		"reexecution_count_per_validator_max":                maxReexecution,
		"validation_failure_count_per_validator_mean":        ratio(validationFailureSum, len(values)),
		"dependency_wait_count_per_validator_mean":           ratio(dependencyWaitSum, len(values)),
		"dependency_resume_count_per_validator_mean":         ratio(dependencyResumeSum, len(values)),
		"estimate_publish_count_per_validator_mean":          ratio(estimatePublishSum, len(values)),
		"business_execution_count_per_validator_mean":        ratio(businessExecutionSum, len(values)),
		"per_validator":                                      perValidator,
	}, nil
}

func readJSONMap(path string, out *map[string]any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func singleIntKey(values map[int]bool) int {
	if len(values) != 1 {
		return 0
	}
	for key := range values {
		return key
	}
	return 0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	default:
		return 0
	}
}

func boolFromAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		return normalized == "true" || normalized == "1" || normalized == "yes"
	default:
		return false
	}
}

func csvValue(row []string, header map[string]int, name, fallback string) string {
	index, ok := header[name]
	if !ok || index < 0 || index >= len(row) {
		return fallback
	}
	return row[index]
}

func csvBool(row []string, header map[string]int, name string) bool {
	value := strings.ToLower(strings.TrimSpace(csvValue(row, header, name, "")))
	return value == "true" || value == "1" || value == "yes"
}

func remoteStateDedupKey(row []string, header map[string]int, normalizedKind string) string {
	executionShard := csvValue(row, header, "execution_shard", "")
	homeShard := csvValue(row, header, "home_shard", "")
	stateKey := csvValue(row, header, "state_key", "")
	if normalizedKind == "writeback" {
		sourceBlock := csvValue(row, header, "source_block_hash", "")
		if sourceBlock == "" {
			sourceBlock = csvValue(row, header, "block_hash", "")
		}
		deltaID := csvValue(row, header, "delta_id", "")
		if deltaID == "" {
			deltaID = strings.Join([]string{csvValue(row, header, "tx_id", ""), csvValue(row, header, "update_semantics", ""), csvValue(row, header, "witness_digest", "")}, "|")
		}
		return strings.Join([]string{normalizedKind, sourceBlock, executionShard, homeShard, stateKey, deltaID}, "|")
	}
	return strings.Join([]string{normalizedKind, csvValue(row, header, "block_hash", ""), executionShard, homeShard, stateKey}, "|")
}

func ratio(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func int64Percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]int64(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	index := int(float64(len(copyValues)-1) * p)
	if index < 0 {
		index = 0
	}
	if index >= len(copyValues) {
		index = len(copyValues) - 1
	}
	return copyValues[index]
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

func writeHeightRootMatrix(dataDir string, nodes []v5.NodePlan) (bool, bool, error) {
	type row struct{ shard, height, node, block, parent, tx, state, receipt string }
	byHeight := map[string][]row{}
	for _, node := range nodes {
		file, err := os.Open(filepath.Join(node.DataDir, "committed_chain.csv"))
		if err != nil {
			return false, false, err
		}
		records, err := csv.NewReader(file).ReadAll()
		_ = file.Close()
		if err != nil {
			return false, false, err
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
	stateConsistent := true
	receiptConsistent := true
	for key, items := range byHeight {
		ref := items[0]
		consistent := true
		for _, item := range items {
			if item.state != ref.state {
				stateConsistent = false
			}
			if item.receipt != ref.receipt {
				receiptConsistent = false
			}
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
		return false, false, err
	}
	return stateConsistent, receiptConsistent, v5.SaveJSON(filepath.Join(dataDir, "state_consistency_report.json"), map[string]any{"consistent": len(first) == 0, "state_root_consistent": stateConsistent, "receipt_root_consistent": receiptConsistent, "first_divergence": first})
}
