package trace

import (
	"fmt"
	"os"
	"path/filepath"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/tx"
)

type ImportOptions struct {
	InputCSV     string
	OutputJSONL  string
	LogCSV       string
	SummaryJSON  string
	Seed         string
	SourceFormat string
}

type ImportSummary struct {
	RuntimeStage  string `json:"runtime_stage"`
	RuntimeTruth  string `json:"runtime_truth"`
	SourceFormat  string `json:"source_format"`
	ImportedCount int    `json:"imported_count"`
	RejectedCount int    `json:"rejected_count"`
	RealP2P       bool   `json:"real_p2p"`
	RealPBFT      bool   `json:"real_pbft"`
}

func ImportCSV(opts ImportOptions) (ImportSummary, error) {
	if opts.SourceFormat == "" {
		opts.SourceFormat = "csv_basic"
	}
	if opts.Seed == "" {
		opts.Seed = "mbe-trace-import"
	}
	records, logs, rejected, err := readCSV(opts)
	if err != nil {
		return ImportSummary{}, err
	}
	if err := os.MkdirAll(filepath.Dir(opts.OutputJSONL), 0o755); err != nil {
		return ImportSummary{}, fmt.Errorf("create import output dir: %w", err)
	}
	f, err := os.Create(opts.OutputJSONL)
	if err != nil {
		return ImportSummary{}, fmt.Errorf("create signed tx jsonl: %w", err)
	}
	if err := tx.WriteJSONL(f, records); err != nil {
		f.Close()
		return ImportSummary{}, err
	}
	if err := f.Close(); err != nil {
		return ImportSummary{}, fmt.Errorf("close signed tx jsonl: %w", err)
	}
	if opts.LogCSV != "" {
		if err := metrics.WriteCSV(opts.LogCSV, []string{"row", "accepted", "reject_reason", "tx_id", "sender", "receiver", "nonce"}, logs); err != nil {
			return ImportSummary{}, err
		}
	}
	summary := ImportSummary{RuntimeStage: "v4_0_real_node_foundation", RuntimeTruth: "v4_real_node_foundation", SourceFormat: opts.SourceFormat, ImportedCount: len(records), RejectedCount: rejected, RealP2P: false, RealPBFT: false}
	if opts.SummaryJSON != "" {
		if err := metrics.WriteJSON(opts.SummaryJSON, summary); err != nil {
			return ImportSummary{}, err
		}
	}
	return summary, nil
}
