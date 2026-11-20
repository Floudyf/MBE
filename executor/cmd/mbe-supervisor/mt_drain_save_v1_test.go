package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"metaverse-chainlab/executor/v5"
)

func TestMTDrainSaveChildProcess(t *testing.T) {
	if os.Getenv("MT_DRAIN_TEST_CHILD") == "" {
		return
	}
	root := os.Getenv("MT_DRAIN_TEST_DIR")
	if os.Getenv("MT_DRAIN_TEST_CHILD") == "hang" {
		time.Sleep(5 * time.Second)
		os.Exit(0)
	}
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(filepath.Join(root, "stop.request")); err == nil {
			dir := filepath.Join(root, "nodes", "n0")
			if err := os.MkdirAll(dir, 0755); err != nil {
				os.Exit(11)
			}
			for _, name := range []string{"block_execution_summary.json", "runtime_metrics.json", "network_log.csv", "remote_state_access.csv"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("simulated\n"), 0644); err != nil {
					os.Exit(12)
				}
			}
			os.Exit(0)
		}
		time.Sleep(10 * time.Millisecond)
	}
	os.Exit(13)
}

func mtDrainSaveStartTestNode(t *testing.T, dir, mode string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestMTDrainSaveChildProcess$")
	command.Env = append(os.Environ(), "MT_DRAIN_TEST_CHILD="+mode, "MT_DRAIN_TEST_DIR="+dir)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	return command
}

func TestMTDrainSaveFlushOnHardTimeout(t *testing.T) {
	root := t.TempDir()
	cmd := mtDrainSaveStartTestNode(t, root, "flush")
	plan := v5.Plan{}
	plan.NodeConfigs = append(plan.NodeConfigs, v5.NodePlan{NodeID: "n0", DataDir: filepath.Join(root, "nodes", "n0")})
	mtDrainSaveAfterFailureWithBudget(plan, root, []*exec.Cmd{cmd}, errors.New("drain hard timeout"), 4*time.Second)
	var saved struct {
		OriginalDrainError       string              `json:"original_drain_error"`
		OriginalFailurePreserved bool                `json:"original_failure_preserved"`
		NodeResults              []mtDrainSaveResult `json:"node_results"`
	}
	raw, err := os.ReadFile(filepath.Join(root, "mt_drain_failure_flush_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.OriginalDrainError != "drain hard timeout" || !saved.OriginalFailurePreserved {
		t.Fatalf("failure status changed: %+v", saved)
	}
	if len(saved.NodeResults) != 1 || !saved.NodeResults[0].Exited || !saved.NodeResults[0].BlockSummary || !saved.NodeResults[0].RuntimeMetrics || !saved.NodeResults[0].NetworkLog || !saved.NodeResults[0].RemoteStateLog {
		t.Fatalf("node export missing: %+v", saved.NodeResults)
	}
	if _, err := os.Stat(filepath.Join(root, "stop.request")); err != nil {
		t.Fatal(err)
	}
}

func TestMTDrainSaveKillsUnresponsiveNode(t *testing.T) {
	root := t.TempDir()
	cmd := mtDrainSaveStartTestNode(t, root, "hang")
	mtDrainSaveAfterFailureWithBudget(v5.Plan{}, root, []*exec.Cmd{cmd}, errors.New("drain hard timeout"), 50*time.Millisecond)
	raw, err := os.ReadFile(filepath.Join(root, "mt_drain_failure_flush_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"forced_shutdown": true`) || !strings.Contains(string(raw), `"forced_kill": true`) {
		t.Fatalf("hung node not killed: %s", raw)
	}
}
