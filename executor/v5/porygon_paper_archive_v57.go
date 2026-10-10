package v5

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// v5.7 is an observer-only durable evidence archive.  The paper lifecycle,
// quorum, PBFT, Proposal U/T, and 4/6-round rules remain authoritative.
// Only terminal rows that are about to leave the bounded protocol cache are
// written.  A failed diagnostic write does NOT delete the source rows.
var porygonV57PaperHeader = []string{
	"tx_id", "paper_status", "origin_height", "cross_shard", "witness_round",
	"ordering_round", "pre_execution_round", "update_proposal_height",
	"update_execution_height", "commit_proposal_height", "commit_round", "recovery_mode",
}

func porygonV57LifecycleRow(item *PorygonTxRoundLifecycle) []string {
	return []string{
		item.TxID, string(item.Status), strconv.FormatUint(item.OriginHeight, 10),
		strconv.FormatBool(item.CrossShard), strconv.FormatUint(item.WitnessRound, 10),
		strconv.FormatUint(item.OrderingRound, 10), strconv.FormatUint(item.PreExecutionRound, 10),
		strconv.FormatUint(item.UpdateProposalHeight, 10),
		strconv.FormatUint(item.UpdateExecutionHeight, 10),
		strconv.FormatUint(item.CommitProposalHeight, 10),
		strconv.FormatUint(item.CommitRound, 10), item.RecoveryMode,
	}
}

func porygonV57Terminal(status porygonPaperTxStatus) bool {
	return status == porygonPaperTxCommitted || status == porygonPaperTxAbandoned || status == porygonPaperTxRolledBack
}

func porygonV57ArchiveDirectory(nodeDir string) string {
	return filepath.Join(nodeDir, "porygon_paper_audit_archive_v57")
}

func porygonV57ReadArchiveFile(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("porygon v5.7 audit archive decode %s: %w", path, err)
	}
	if len(rows) == 0 || !reflect.DeepEqual(rows[0], porygonV57PaperHeader) {
		return nil, fmt.Errorf("porygon v5.7 audit archive header mismatch: %s", path)
	}
	for _, row := range rows[1:] {
		if len(row) != len(porygonV57PaperHeader) || row[0] == "" || !porygonV57Terminal(porygonPaperTxStatus(row[1])) {
			return nil, fmt.Errorf("porygon v5.7 audit archive invalid terminal row: %s", path)
		}
	}
	return rows[1:], nil
}

func porygonV57PersistTerminalBatch(nodeDir string, durableHeight uint64, rows [][]string) error {
	if nodeDir == "" {
		return errors.New("porygon v5.7 archive requires node data directory")
	}
	dir := porygonV57ArchiveDirectory(nodeDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][0] < rows[j][0] })
	target := filepath.Join(dir, fmt.Sprintf("%020d.csv", durableHeight))
	if existing, err := porygonV57ReadArchiveFile(target); err == nil {
		if reflect.DeepEqual(existing, rows) {
			return nil
		} // repeated durable/prune call
		return fmt.Errorf("porygon v5.7 audit archive same-height identity mismatch: %s", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err // fail closed for corrupt evidence; never delete source rows
	}
	tmp, err := os.CreateTemp(dir, ".paper-v57-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	writer := csv.NewWriter(tmp)
	if err := writer.Write(porygonV57PaperHeader); err != nil {
		tmp.Close()
		return err
	}
	if err := writer.WriteAll(rows); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// A single atomic rename makes an interrupted archive batch invisible.
	// This diagnostic file never acts as a PBFT/finality authority.
	return os.Rename(name, target)
}

func (r *NodeRuntime) porygonV57ArchiveAndPruneTerminals(state *porygonPaperRuntimeState, cutoff, durableHeight uint64) {
	if state == nil {
		return
	}
	state.mu.Lock()
	candidates := make([][]string, 0)
	for _, item := range state.txs {
		if item != nil && item.OriginHeight < cutoff && porygonV57Terminal(item.Status) {
			candidates = append(candidates, porygonV57LifecycleRow(item))
		}
	}
	state.mu.Unlock()
	if len(candidates) == 0 {
		return
	}
	if err := porygonV57PersistTerminalBatch(r.node.DataDir, durableHeight, candidates); err != nil {
		r.addPorygonRuntimeMetric("porygon_v57_archive_write_error_count", 1)
		return // preserve the original authoritative lifecycle rows
	}
	state.mu.Lock()
	removed := int64(0)
	for _, row := range candidates {
		item := state.txs[row[0]]
		if item != nil && reflect.DeepEqual(porygonV57LifecycleRow(item), row) {
			delete(state.txs, row[0])
			removed++
		}
	}
	state.mu.Unlock()
	r.addPorygonRuntimeMetric("porygon_v57_archive_terminal_record_count", removed)
}

func (r *NodeRuntime) porygonV57ArchivedPaperRows() ([][]string, error) {
	if r.node.DataDir == "" {
		return nil, nil
	}
	dir := porygonV57ArchiveDirectory(r.node.DataDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([][]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		rows, err := porygonV57ReadArchiveFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
	}
	return result, nil
}

func porygonV57IdenticalPaperRows(left, right []string) bool {
	return reflect.DeepEqual(left, right)
}

// The runtime already authenticates O(h) via PBFT.  This accessor never
// guesses or advances an order height; the current E(h) height is only a
// fallback for unit/replay paths without an installed pipeline state.
// CALL BEFORE taking the paper state.mu to preserve pipeline->paper lock order.
func (r *NodeRuntime) porygonV57LifecycleOrderedHeight(executedHeight uint64) uint64 {
	value, ok := porygonPipelineRuntimes.Load(r)
	if !ok {
		return executedHeight
	}
	pipeline := value.(*porygonPipelineRuntime)
	pipeline.mu.Lock()
	ordered := pipeline.orderedHeight
	pipeline.mu.Unlock()
	if ordered < executedHeight {
		return executedHeight
	}
	return ordered
}
