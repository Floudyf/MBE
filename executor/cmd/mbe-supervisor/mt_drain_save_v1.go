package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"metaverse-chainlab/executor/v5"
)

// mtDrainSaveResult describes a post-failure artifact flush. It NEVER changes
// drain_status.json or the original error returned by runV5.
type mtDrainSaveResult struct {
	NodeID          string `json:"node_id"`
	Exited          bool   `json:"exited"`
	ExitError       string `json:"exit_error,omitempty"`
	ForcedKill      bool   `json:"forced_kill"`
	ProcessWaitDone bool   `json:"process_wait_done"`
	BlockSummary    bool   `json:"block_summary"`
	RuntimeMetrics  bool   `json:"runtime_metrics"`
	NetworkLog      bool   `json:"network_log"`
	RemoteStateLog  bool   `json:"remote_state_log"`
}

type mtDrainSaveWait struct {
	Index int
	Err   error
}

func mtDrainSaveHaveFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}

// MBE_MT_DRAIN_SAVE_V1: only the supervisor owns a failed V5 Drain. The
// supervisor's existing lease remains renewed while this function runs, so
// its nodes can finish exporting instead of being killed immediately. The
// bounded grace is the existing (at most 10-minute) shutdownBudget.
func mtDrainSaveAfterFailure(plan v5.Plan, dataDir string, commands []*exec.Cmd, drainErr error) {
	mtDrainSaveAfterFailureWithBudget(plan, dataDir, commands, drainErr, shutdownBudget(plan))
}

func mtDrainSaveAfterFailureWithBudget(plan v5.Plan, dataDir string, commands []*exec.Cmd, drainErr error, grace time.Duration) {
	if grace <= 0 {
		grace = time.Second
	}
	started := time.Now()
	stopFile := filepath.Join(dataDir, "stop.request")
	stopErr := os.WriteFile(stopFile, []byte("failed_drain_artifact_flush\n"), 0644)

	results := make([]mtDrainSaveResult, len(commands))
	for i := range commands {
		if i < len(plan.NodeConfigs) {
			results[i].NodeID = plan.NodeConfigs[i].NodeID
		} else {
			results[i].NodeID = fmt.Sprintf("node_index_%d", i)
		}
	}

	// Each exec.Cmd.Wait is called exactly once. In particular, do NOT call
	// reap(commands) after starting the Wait goroutines: simultaneous Waits
	// on the same Cmd are unsafe and destroy child-exit evidence.
	waits := make(chan mtDrainSaveWait, len(commands))
	for i, c := range commands {
		go func(index int, cmd *exec.Cmd) {
			waits <- mtDrainSaveWait{Index: index, Err: cmd.Wait()}
		}(i, c)
	}

	timer := time.NewTimer(grace)
	defer timer.Stop()
	completed := 0
	timedOut := stopErr != nil
	for completed < len(commands) && !timedOut {
		select {
		case done := <-waits:
			mtDrainRecordWait(results, done)
			completed++
		case <-timer.C:
			timedOut = true
		}
	}
	if timedOut || stopErr != nil {
		for i, c := range commands {
			if results[i].ProcessWaitDone || c.Process == nil {
				continue
			}
			results[i].ForcedKill = true
			_ = c.Process.Kill()
		}
		// After Kill, allow a short bounded reap window. No double Wait.
		last := time.NewTimer(10 * time.Second)
		for completed < len(commands) {
			select {
			case done := <-waits:
				mtDrainRecordWait(results, done)
				completed++
			case <-last.C:
				completed = len(commands)
			}
		}
		last.Stop()
	}

	for i, node := range plan.NodeConfigs {
		if i >= len(results) {
			break
		}
		dir := node.DataDir
		if dir == "" {
			dir = filepath.Join(dataDir, "nodes", node.NodeID)
		}
		results[i].BlockSummary = mtDrainSaveHaveFile(filepath.Join(dir, "block_execution_summary.json"))
		results[i].RuntimeMetrics = mtDrainSaveHaveFile(filepath.Join(dir, "runtime_metrics.json"))
		results[i].NetworkLog = mtDrainSaveHaveFile(filepath.Join(dir, "network_log.csv"))
		results[i].RemoteStateLog = mtDrainSaveHaveFile(filepath.Join(dir, "remote_state_access.csv"))
	}
	cause := "<unknown>"
	if drainErr != nil {
		cause = drainErr.Error()
	}
	stopError := ""
	if stopErr != nil {
		stopError = stopErr.Error()
	}
	payload := map[string]any{
		"schema_version":                         "mt_drain_save_v1",
		"original_drain_error":                   cause,
		"original_failure_preserved":             true,
		"stop_request_path":                      stopFile,
		"stop_request_error":                     stopError,
		"grace_limit_ms":                         grace.Milliseconds(),
		"flush_elapsed_ms":                       time.Since(started).Milliseconds(),
		"forced_shutdown":                        timedOut || stopErr != nil,
		"node_results":                           results,
		"post_failure_artifacts_only":            true,
		"counts_are_not_reclassified_as_success": true,
	}
	if err := v5.SaveJSON(filepath.Join(dataDir, "mt_drain_failure_flush_v1.json"), payload); err != nil {
		fmt.Fprintf(os.Stderr, "[MTDrainSave] failure to save cleanup status: %v\n", err)
	}
	fmt.Fprintf(os.Stderr, "[MTDrainSave] failed Drain preserved; node artifact flush took %s; original failure: %s\n", time.Since(started).Round(time.Millisecond), cause)
}

func mtDrainRecordWait(results []mtDrainSaveResult, done mtDrainSaveWait) {
	if done.Index < 0 || done.Index >= len(results) {
		return
	}
	r := &results[done.Index]
	r.ProcessWaitDone = true
	r.Exited = done.Err == nil
	if done.Err != nil {
		r.ExitError = done.Err.Error()
	}
}
