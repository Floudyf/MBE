package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/tx"
)

type RunResult struct {
	Summary          Summary
	Admissions       []mempool.AdmissionResult
	SummaryPath      string
	MempoolLogPath   string
	AdmissionLogPath string
}

func RunOnce(cfg Config) (RunResult, error) {
	if cfg.NodeID == "" {
		return RunResult{}, fmt.Errorf("node_id is required")
	}
	if cfg.ShardID == "" {
		return RunResult{}, fmt.Errorf("shard_id is required")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(".cache", "v4_realism_runs", cfg.NodeID)
	}
	if cfg.SummaryOut == "" {
		cfg.SummaryOut = filepath.Join(cfg.DataDir, "v4_0_real_node_summary.json")
	}
	if cfg.MempoolLogOut == "" {
		cfg.MempoolLogOut = filepath.Join(cfg.DataDir, "node_mempool_log.csv")
	}
	if cfg.AdmissionLogOut == "" {
		cfg.AdmissionLogOut = filepath.Join(cfg.DataDir, "node_admission_log.csv")
	}
	n := New(cfg)
	admissions := []mempool.AdmissionResult{}
	total := 0
	if cfg.InputJSONL != "" {
		f, err := os.Open(cfg.InputJSONL)
		if err != nil {
			return RunResult{}, fmt.Errorf("open input jsonl: %w", err)
		}
		defer f.Close()
		if err := tx.ReadJSONL(f, func(item tx.SignedTransaction) error {
			total++
			admissions = append(admissions, n.Mempool.Admit(item))
			return nil
		}); err != nil {
			return RunResult{}, err
		}
	}
	accepted := 0
	for _, result := range admissions {
		if result.Accepted {
			accepted++
		}
	}
	summary := NewSummary(cfg, total, accepted, total-accepted, n.Mempool.Len(), cfg.RealTraceImport)
	if err := writeAdmissionCSV(cfg.MempoolLogOut, admissions); err != nil {
		return RunResult{}, err
	}
	if err := writeAdmissionCSV(cfg.AdmissionLogOut, admissions); err != nil {
		return RunResult{}, err
	}
	if err := metrics.WriteJSON(cfg.SummaryOut, summary); err != nil {
		return RunResult{}, err
	}
	compatSummary := filepath.Join(filepath.Dir(cfg.SummaryOut), "v4_node_foundation_summary.json")
	if compatSummary != cfg.SummaryOut {
		if err := metrics.WriteJSON(compatSummary, summary); err != nil {
			return RunResult{}, err
		}
	}
	return RunResult{Summary: summary, Admissions: admissions, SummaryPath: cfg.SummaryOut, MempoolLogPath: cfg.MempoolLogOut, AdmissionLogPath: cfg.AdmissionLogOut}, nil
}

func RunServerSkeleton(cfg Config) (RunResult, error) {
	if cfg.RunMode == "" {
		cfg.RunMode = "server"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(".cache", "v4_realism_runs", cfg.NodeID)
	}
	if cfg.SummaryOut == "" {
		cfg.SummaryOut = filepath.Join(cfg.DataDir, "v4_0_real_node_summary.json")
	}
	summary := NewSummary(cfg, 0, 0, 0, 0, false)
	if err := metrics.WriteJSON(cfg.SummaryOut, summary); err != nil {
		return RunResult{}, err
	}
	return RunResult{Summary: summary, SummaryPath: cfg.SummaryOut}, nil
}

func writeAdmissionCSV(path string, rows []mempool.AdmissionResult) error {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			strconv.FormatInt(row.Timestamp, 10),
			row.NodeID,
			row.ShardID,
			row.TxID,
			row.Sender,
			row.Receiver,
			strconv.FormatUint(row.Nonce, 10),
			row.Action,
			strconv.FormatBool(row.Accepted),
			row.RejectReason,
			strconv.Itoa(row.MempoolSize),
			strconv.FormatInt(row.QueueWaitMS, 10),
		})
	}
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "shard_id", "tx_id", "sender", "receiver", "nonce", "action", "accepted", "reject_reason", "mempool_size", "queue_wait_ms"}, out)
}
