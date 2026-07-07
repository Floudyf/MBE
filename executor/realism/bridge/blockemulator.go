package bridge

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/tx"
)

type ComparisonSummary struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	TxCount                        int    `json:"tx_count"`
	NodeCount                      int    `json:"node_count"`
	ShardCount                     int    `json:"shard_count"`
	ConsensusMessageCount          int    `json:"consensus_message_count"`
	NetworkMessageCount            int    `json:"network_message_count"`
	CommittedBlocks                int    `json:"committed_blocks"`
	CommittedTxs                   int    `json:"committed_txs"`
	StateRootMismatchCount         int    `json:"state_root_mismatch_count"`
	CrossShardTxCount              int    `json:"cross_shard_tx_count"`
	RecoverySupported              bool   `json:"recovery_supported"`
	FaultInjectionSupported        bool   `json:"fault_injection_supported"`
	BlockEmulatorBridgeMVP         bool   `json:"blockemulator_bridge_mvp"`
	FullBlockEmulatorCompatibility bool   `json:"full_blockemulator_compatibility"`
}

type ImportOptions struct {
	Input            string
	OutDir           string
	Limit            int
	Seed             string
	SourceKind       string
	FieldMap         map[string]string
	RunV4AfterImport bool
}

type ImportSummary struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	Source                         string `json:"source"`
	SourceFormat                   string `json:"source_format"`
	ImportedTxCount                int    `json:"blockemulator_imported_tx_count"`
	RejectedCount                  int    `json:"rejected_count"`
	SignedTxVerifyPassCount        int    `json:"signed_tx_verify_pass_count"`
	BlockEmulatorTraceToSignedTx   bool   `json:"blockemulator_trace_to_signed_tx"`
	BlockEmulatorV4RunCompleted    bool   `json:"blockemulator_v4_run_completed"`
	BlockEmulatorBridgeUpgraded    bool   `json:"blockemulator_bridge_upgraded"`
	BlockEmulatorBridgeMVP         bool   `json:"blockemulator_bridge_mvp"`
	FullBlockEmulatorCompatibility bool   `json:"full_blockemulator_compatibility"`
	SignedTxJSONL                  string `json:"signed_tx_jsonl"`
}

func ImportTraceCSV(input, outDir string) (int, error) {
	f, err := os.Open(input)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return 0, err
	}
	count := 0
	if len(rows) > 0 {
		count = len(rows) - 1
	}
	if outDir != "" {
		if err := metrics.WriteCSV(filepath.Join(outDir, "blockemulator_trace_import_log.csv"), []string{"source", "imported_rows", "blockemulator_bridge_mvp", "full_blockemulator_compatibility"}, [][]string{{input, fmt.Sprint(count), "true", "false"}}); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func ImportSelectedTxsCSV(opts ImportOptions) (ImportSummary, []tx.SignedTransaction, error) {
	if opts.Seed == "" {
		opts.Seed = "blockemulator-bridge"
	}
	if opts.SourceKind == "" {
		opts.SourceKind = "blockemulator_selected_txs_csv"
	}
	f, err := os.Open(opts.Input)
	if err != nil {
		return ImportSummary{}, nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return ImportSummary{}, nil, err
	}
	index := headerIndex(header)
	senderField := mapped(index, opts.FieldMap, "sender", "from", "fromAddr", "account_from")
	receiverField := mapped(index, opts.FieldMap, "receiver", "to", "toAddr", "account_to")
	valueField := mapped(index, opts.FieldMap, "value", "amount")
	timestampField := mapped(index, opts.FieldMap, "timestamp", "time")
	if senderField < 0 || receiverField < 0 || valueField < 0 {
		return ImportSummary{}, nil, fmt.Errorf("malformed_blockemulator_csv: missing sender/receiver/value fields")
	}
	nonceBySender := map[string]uint64{}
	imported := []tx.SignedTransaction{}
	rejected := 0
	rows := [][]string{}
	line := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		line++
		if err != nil {
			return ImportSummary{}, nil, err
		}
		if opts.Limit > 0 && len(imported) >= opts.Limit {
			break
		}
		sourceSender := strings.TrimSpace(record[senderField])
		receiver := strings.TrimSpace(record[receiverField])
		value, err := strconv.ParseInt(strings.TrimSpace(record[valueField]), 10, 64)
		if err != nil || value <= 0 || sourceSender == "" || receiver == "" {
			rejected++
			rows = append(rows, []string{strconv.Itoa(line), sourceSender, receiver, "rejected", "invalid_fields"})
			continue
		}
		timestamp := int64(line)
		if timestampField >= 0 {
			if parsed, err := strconv.ParseInt(strings.TrimSpace(record[timestampField]), 10, 64); err == nil {
				timestamp = parsed
			}
		}
		nonce := nonceBySender[sourceSender]
		nonceBySender[sourceSender] = nonce + 1
		generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sourceSender, Receiver: receiver, StartNonce: nonce, Value: value, Seed: opts.Seed, SourceKind: opts.SourceKind, StartTimeMS: timestamp})
		if err != nil {
			rejected++
			rows = append(rows, []string{strconv.Itoa(line), sourceSender, receiver, "rejected", err.Error()})
			continue
		}
		if err := tx.Verify(generated[0]); err != nil {
			rejected++
			rows = append(rows, []string{strconv.Itoa(line), sourceSender, receiver, "rejected", err.Error()})
			continue
		}
		imported = append(imported, generated[0])
		rows = append(rows, []string{strconv.Itoa(line), sourceSender, receiver, "accepted", generated[0].TxID})
	}
	summary := ImportSummary{RuntimeStage: "v4_3_blockemulator_surpass_realism_closure", RuntimeTruth: "v4_blockemulator_surpass_realism_closure", Source: opts.Input, SourceFormat: "blockemulator_selected_txs_csv_subset", ImportedTxCount: len(imported), RejectedCount: rejected, SignedTxVerifyPassCount: len(imported), BlockEmulatorTraceToSignedTx: true, BlockEmulatorV4RunCompleted: opts.RunV4AfterImport, BlockEmulatorBridgeUpgraded: true, BlockEmulatorBridgeMVP: true, FullBlockEmulatorCompatibility: false}
	if opts.OutDir != "" {
		jsonlPath := filepath.Join(opts.OutDir, "blockemulator_signed_txs.jsonl")
		if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
			return summary, imported, err
		}
		out, err := os.Create(jsonlPath)
		if err != nil {
			return summary, imported, err
		}
		if err := tx.WriteJSONL(out, imported); err != nil {
			_ = out.Close()
			return summary, imported, err
		}
		if err := out.Close(); err != nil {
			return summary, imported, err
		}
		summary.SignedTxJSONL = jsonlPath
		if err := metrics.WriteCSV(filepath.Join(opts.OutDir, "blockemulator_mapping_log.csv"), []string{"line", "source_sender", "receiver", "status", "detail"}, rows); err != nil {
			return summary, imported, err
		}
		if err := metrics.WriteJSON(filepath.Join(opts.OutDir, "blockemulator_import_summary.json"), summary); err != nil {
			return summary, imported, err
		}
	}
	return summary, imported, nil
}

func WriteComparisonSummary(outDir string, summary ComparisonSummary) error {
	summary.RuntimeStage = "v4_2_state_cross_shard_recovery_frontend"
	summary.RuntimeTruth = "v4_real_state_cross_shard_recovery"
	summary.BlockEmulatorBridgeMVP = true
	summary.FullBlockEmulatorCompatibility = false
	return metrics.WriteJSON(filepath.Join(outDir, "blockemulator_comparison_summary.json"), summary)
}

func headerIndex(header []string) map[string]int {
	index := map[string]int{}
	for i, item := range header {
		index[strings.ToLower(strings.TrimSpace(item))] = i
	}
	return index
}

func mapped(index map[string]int, fieldMap map[string]string, canonical string, aliases ...string) int {
	if fieldMap != nil {
		if field, ok := fieldMap[canonical]; ok {
			if idx, ok := index[strings.ToLower(strings.TrimSpace(field))]; ok {
				return idx
			}
		}
	}
	names := append([]string{canonical}, aliases...)
	for _, name := range names {
		if idx, ok := index[strings.ToLower(name)]; ok {
			return idx
		}
	}
	return -1
}
