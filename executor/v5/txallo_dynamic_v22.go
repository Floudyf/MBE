package v5

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// MBE_TXALLO_DYNAMIC_V222
// MBE lifecycle/control-plane adaptation around the frozen paper core.
// Algorithm 1/2, Louvain, Eq. (6)/(8), eta/lambda/epsilon and PBFT are unchanged.
const (
	txalloDynamicEpochFeedName       = "txallo_epoch_lifecycle.jsonl"
	txalloDynamicMappingSnapshotName = "txallo_mapping_snapshot.json"
	txalloDynamicMappingAckName      = "txallo_mapping_ack.json"
	txalloDynamicBlockSidecarName    = "txallo_dynamic_blocks.jsonl.gz"
	txalloDynamicMappingSchema       = "mbe_txallo_mapping_snapshot_v2"
	txalloDynamicMappingAckSchema    = "mbe_txallo_mapping_ack_v1"
	txalloDynamicEpochFeedSchema     = "mbe_txallo_epoch_lifecycle_v1"
	txalloDynamicBlockRowSchema      = "mbe_txallo_dynamic_block_v1"
)

type txalloDynamicAllocationRuntime interface {
	HistoricalAllocationBootstrapper
	txalloAccountMappingProvider
	TxAlloDynamicEnabled() bool
	TxAlloAEpochBlocks() int
	TxAlloGlobalEpochMultiple() int
	TxAlloMappingEpoch() uint64
	TxAlloMappingStateDigest() string
	TxAlloStatefulReplicaEnabled() bool
	ConfirmTxAlloReplicaConvergence(uint64, string, int) error
	ApplyCommittedTxAlloEpoch([]WorkloadRecord, uint64, bool) error
}

type txalloMappingRefresher interface {
	TxAlloDynamicEnabled() bool
	TxAlloMappingEpoch() uint64
	TxAlloMappingStateDigest() string
	RefreshTxAlloMapping() error
}

type txalloMappingSnapshotV22 struct {
	SchemaVersion string            `json:"schema_version"`
	Epoch         uint64            `json:"epoch"`
	Mapping       map[string]string `json:"mapping"`
	Aliases       map[string]string `json:"aliases"`
	MappingDigest string            `json:"mapping_digest"`
	AliasesDigest string            `json:"aliases_digest"`
	StateDigest   string            `json:"state_digest"`
}

type txalloMappingEpochRowV229 struct {
	SchemaVersion   string            `json:"schema_version"`
	Epoch           uint64            `json:"epoch"`
	Mapping         map[string]string `json:"mapping"`
	Aliases         map[string]string `json:"aliases"`
	MappingDigest   string            `json:"mapping_digest"`
	AliasesDigest   string            `json:"aliases_digest"`
	StateDigest     string            `json:"state_digest"`
	SourceEpoch     int64             `json:"source_epoch"`
	UpdateAlgorithm string            `json:"update_algorithm"`
	TimestampMS     int64             `json:"timestamp_ms"`
}

type txalloMappingAckV222 struct {
	SchemaVersion string `json:"schema_version"`
	NodeID        string `json:"node_id"`
	Epoch         uint64 `json:"epoch"`
	StateDigest   string `json:"state_digest"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	TimestampMS   int64  `json:"timestamp_ms"`
}

type txalloDynamicBlockRowV222 struct {
	SchemaVersion     string `json:"schema_version"`
	MaterializedIndex int    `json:"materialized_index"`
	RawSourceRowIndex int    `json:"raw_source_row_index"`
	TransactionID     string `json:"transaction_id"`
	BlockNum          int64  `json:"block_num"`
	EpochCoordinate   int64  `json:"epoch_coordinate,omitempty"` // MBE_MV_TXALLO_EVENT_CLOCK_V1: observed source event position, never chain block
	GlobalSequence    int64  `json:"global_sequence"`
	SenderID          string `json:"sender_id"`
	ReceiverID        string `json:"receiver_id"`
}

type txalloDynamicBlockIndexV222 struct {
	Blocks      []int64
	Senders     []string
	Receivers   []string
	AnchorBlock int64
	LastBlock   int64
}

type txalloEpochLifecycleRowV22 struct {
	SchemaVersion string `json:"schema_version"`
	LogicalTxID   string `json:"logical_tx_id"`
	Stage         string `json:"stage"`
	Success       bool   `json:"success"`
	BlockHeight   uint64 `json:"block_height,omitempty"`
}

func (p *txalloAccountSharding) TxAlloDynamicEnabled() bool {
	if p == nil {
		return false
	}
	if raw, exists := p.config["dynamic_a_txallo_runtime"]; exists {
		return boolFromAny(raw)
	}
	return false
}

func (p *txalloAccountSharding) TxAlloAEpochBlocks() int {
	if p == nil {
		return 15
	}
	blocks := intValue(p.config["a_epoch_blocks"])
	if blocks <= 0 {
		return 15
	}
	return blocks
}

func (p *txalloAccountSharding) TxAlloGlobalEpochMultiple() int {
	if p == nil {
		return 20
	}
	multiple := intValue(p.config["g_epoch_multiple"])
	if multiple <= 1 {
		return 20
	}
	return multiple
}

func (p *txalloAccountSharding) TxAlloMappingEpoch() uint64 {
	if p == nil {
		return 0
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mappingEpoch
}

func txalloStateDigest(mapping, aliases map[string]string) string {
	return stableJSONDigest(map[string]any{"mapping": mapping, "aliases": aliases})
}

func (p *txalloAccountSharding) TxAlloMappingStateDigest() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.allocator == nil {
		return ""
	}
	return txalloStateDigest(p.allocator.Mapping, p.aliases)
}

func txalloSourceBlockEpoch(blockNum, anchorBlock int64, epochBlocks int) (int64, error) {
	if blockNum <= 0 || anchorBlock <= 0 {
		return 0, fmt.Errorf("TxAllo dynamic A requires positive source block numbers")
	}
	if blockNum < anchorBlock {
		return 0, fmt.Errorf("TxAllo dynamic source block precedes evaluation anchor")
	}
	if epochBlocks <= 0 {
		return 0, fmt.Errorf("TxAllo dynamic A requires a positive block epoch")
	}
	return (blockNum - anchorBlock) / int64(epochBlocks), nil
}

func txalloLoadDynamicBlockIndexV222(dataDir string, plan WorkloadPlan) (*txalloDynamicBlockIndexV222, error) {
	rawMeta, ok := plan.AuditMetadata["txallo_dynamic_blocks"]
	if !ok {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar metadata missing")
	}
	meta, ok := rawMeta.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar metadata invalid")
	}
	rel := strings.TrimSpace(fmt.Sprint(meta["relative_path"]))
	expectedSHA := strings.ToLower(strings.TrimSpace(fmt.Sprint(meta["sha256"])))
	expectedCount := intValue(meta["selected_count"])
	anchorRaw := intValue(meta["evaluation_anchor_raw_row_index"])
	clockKind, _ := meta["epoch_clock_source"].(string)
	if clockKind != "" && clockKind != "mbe_event_order_index" {
		return nil, fmt.Errorf("unsupported TxAllo clock provenance: %s", clockKind)
	}
	eventClock := clockKind == "mbe_event_order_index"
	if rel == "" || len(expectedSHA) != 64 || expectedCount <= 0 || expectedCount != plan.ActualTxCount {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar metadata incomplete")
	}
	path, err := txalloRunHistoryPath(dataDir, rel)
	if err != nil {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar path: %w", err)
	}
	actualSHA, err := txalloFileSHA256(path)
	if err != nil {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar SHA-256: %w", err)
	}
	if !strings.EqualFold(actualSHA, expectedSHA) {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar SHA-256 mismatch")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), maxWorkloadRecordBytes)
	blocks := make([]int64, 0, expectedCount)
	senders := make([]string, 0, expectedCount)
	receivers := make([]string, 0, expectedCount)
	lastBlock := int64(-1)
	lastSeq := int64(-1)
	for sc.Scan() {
		var row txalloDynamicBlockRowV222
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("TxAllo dynamic block sidecar decode: %w", err)
		}
		index := len(blocks)
		schema := txalloDynamicBlockRowSchema
		if eventClock {
			schema = "mbe_txallo_event_clock_v1"
		}
		if row.SchemaVersion != schema || row.MaterializedIndex != index {
			return nil, fmt.Errorf("TxAllo dynamic block sidecar index/schema/clock mismatch")
		}
		if row.RawSourceRowIndex != anchorRaw+index {
			return nil, fmt.Errorf("TxAllo dynamic block sidecar raw-source alignment mismatch")
		}
		coordinate := row.BlockNum
		if eventClock {
			// No block number is invented. The explicitly typed event coordinate
			// drives identical 15-event A intervals across all methods.
			if row.BlockNum != 0 || row.EpochCoordinate != int64(row.RawSourceRowIndex)+1 || row.GlobalSequence != row.EpochCoordinate {
				return nil, fmt.Errorf("TxAllo event-order clock provenance/alignment invalid")
			}
			coordinate = row.EpochCoordinate
		} else if row.EpochCoordinate != 0 {
			return nil, fmt.Errorf("TxAllo observed source block sidecar cannot carry virtual clock coordinates")
		}
		if coordinate <= 0 || (lastBlock > 0 && coordinate < lastBlock) {
			return nil, fmt.Errorf("TxAllo dynamic source clock regressed or invalid")
		}
		if row.GlobalSequence <= lastSeq {
			return nil, fmt.Errorf("TxAllo dynamic block sidecar global sequence invalid")
		}
		if strings.TrimSpace(row.TransactionID) == "" || strings.TrimSpace(row.SenderID) == "" || strings.TrimSpace(row.ReceiverID) == "" {
			return nil, fmt.Errorf("TxAllo dynamic block sidecar identity incomplete")
		}
		lastBlock = coordinate
		lastSeq = row.GlobalSequence
		blocks = append(blocks, coordinate)
		senders = append(senders, strings.ToLower(strings.TrimSpace(row.SenderID)))
		receivers = append(receivers, strings.ToLower(strings.TrimSpace(row.ReceiverID)))
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(blocks) != expectedCount {
		return nil, fmt.Errorf("TxAllo dynamic block sidecar count=%d want=%d", len(blocks), expectedCount)
	}
	return &txalloDynamicBlockIndexV222{
		Blocks: blocks, Senders: senders, Receivers: receivers,
		AnchorBlock: blocks[0], LastBlock: blocks[len(blocks)-1],
	}, nil
}

func (index *txalloDynamicBlockIndexV222) BlockForRecord(record WorkloadRecord) (int64, error) {
	if index == nil || record.Index < 0 || record.Index >= len(index.Blocks) {
		return 0, fmt.Errorf("TxAllo dynamic block lookup out of range")
	}
	sender := strings.ToLower(strings.TrimSpace(record.SenderID))
	receiver := strings.ToLower(strings.TrimSpace(record.ReceiverID))
	if sender == "" || receiver == "" || sender != index.Senders[record.Index] || receiver != index.Receivers[record.Index] {
		return 0, fmt.Errorf("TxAllo dynamic block sidecar account alignment mismatch at materialized index %d", record.Index)
	}
	return index.Blocks[record.Index], nil
}

func txalloHistoryFromWorkloadRecords(records []WorkloadRecord) []txalloHistoryTx {
	out := make([]txalloHistoryTx, 0, len(records))
	for _, record := range records {
		accounts := txalloUniqueSortedStrings([]string{
			strings.ToLower(strings.TrimSpace(record.SenderID)),
			strings.ToLower(strings.TrimSpace(record.ReceiverID)),
		})
		if len(accounts) > 0 {
			out = append(out, txalloHistoryTx{Accounts: accounts})
		}
	}
	return out
}

func txalloCloneAllocator(input *txalloAllocator) *txalloAllocator {
	if input == nil {
		return nil
	}
	out := &txalloAllocator{
		Graph:   newTxAlloGraph(),
		Mapping: copyMapping(input.Mapping),
		Shards:  append([]string(nil), input.Shards...),
		Eta:     input.Eta,
		Lambda:  input.Lambda,
		Epsilon: input.Epsilon,
		Stats:   map[string]txalloCommunityStat{},
		Metrics: input.Metrics,
	}
	for node := range input.Graph.Nodes {
		out.Graph.Nodes[node] = true
	}
	for edge, weight := range input.Graph.Edges {
		out.Graph.Edges[edge] = weight
	}
	for shard, stat := range input.Stats {
		out.Stats[shard] = stat
	}
	if input.Metrics.Objective.Workloads != nil {
		out.Metrics.Objective.Workloads = map[string]float64{}
		for shard, value := range input.Metrics.Objective.Workloads {
			out.Metrics.Objective.Workloads[shard] = value
		}
	}
	return out
}

func txalloMovedExistingAccounts(before, after map[string]string) []string {
	moved := []string{}
	for account, oldShard := range before {
		if newShard := after[account]; newShard != "" && newShard != oldShard {
			moved = append(moved, account)
		}
	}
	sort.Strings(moved)
	return moved
}

// txalloStatefulMigrationAccounts also covers accounts that were not part of
// the previous learned mapping but have already executed in the just-closed
// epoch under their deterministic fallback placement. Once those transactions
// are committed, changing such an account from fallback shard -> learned shard
// is a real state-home change in MBE's local-state Stateful-TxAllo substrate.
// The paper core is untouched; this is an MBE correctness guard until a real
// state migration / globally replicated state substrate is installed.
func txalloStatefulMigrationAccounts(p *txalloAccountSharding, before, after map[string]string, records []WorkloadRecord) []string {
	required := map[string]bool{}
	for _, account := range txalloMovedExistingAccounts(before, after) {
		required[account] = true
	}
	if p != nil {
		for _, record := range records {
			for _, account := range txalloUniqueSortedStrings([]string{
				strings.ToLower(strings.TrimSpace(record.SenderID)),
				strings.ToLower(strings.TrimSpace(record.ReceiverID)),
			}) {
				newShard := after[account]
				if newShard == "" {
					continue
				}
				oldShard := before[account]
				if oldShard == "" {
					oldShard = p.fallbackShard(account, p.shards)
				}
				if oldShard != "" && oldShard != newShard {
					required[account] = true
				}
			}
		}
	}
	out := make([]string, 0, len(required))
	for account := range required {
		out = append(out, account)
	}
	sort.Strings(out)
	return out
}

func (p *txalloAccountSharding) txalloMappingSnapshotLocked() txalloMappingSnapshotV22 {
	mapping := copyMapping(p.allocator.Mapping)
	aliases := copyMapping(p.aliases)
	return txalloMappingSnapshotV22{
		SchemaVersion: txalloDynamicMappingSchema,
		Epoch:         p.mappingEpoch,
		Mapping:       mapping,
		Aliases:       aliases,
		MappingDigest: stableJSONDigest(mapping),
		AliasesDigest: stableJSONDigest(aliases),
		StateDigest:   txalloStateDigest(mapping, aliases),
	}
}

func txalloControlJSONBytes(value any) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func txalloWriteSyncedTemp(path string, raw []byte) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("TxAllo control-file path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	tmp := fmt.Sprintf("%s.tmp-%d-%d", path, os.Getpid(), time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if _, err := f.Write(raw); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return tmp, nil
}

func txalloWriteAtomicJSON(path string, value any) error {
	raw, err := txalloControlJSONBytes(value)
	if err != nil {
		return err
	}
	tmp, err := txalloWriteSyncedTemp(path, raw)
	if err != nil {
		return err
	}
	if err := txalloAtomicReplace(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// txalloPublishImmutableJSON is the protocol publication primitive for mapping
// snapshots and node ACKs. Epoch files are write-once. Re-publication is
// allowed only when the exact sealed bytes already exist; conflicting bytes are
// a protocol error and are never overwritten.
func txalloPublishImmutableJSON(path string, value any) error {
	raw, err := txalloControlJSONBytes(value)
	if err != nil {
		return err
	}
	if existing, err := txalloReadFileStable(path); err == nil {
		if bytes.Equal(existing, raw) {
			return nil
		}
		return fmt.Errorf("TxAllo immutable control file conflicts: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	tmp, err := txalloWriteSyncedTemp(path, raw)
	if err != nil {
		return err
	}
	if err := txalloAtomicPublishNew(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if errors.Is(err, os.ErrExist) {
			existing, readErr := txalloReadFileStable(path)
			if readErr == nil && bytes.Equal(existing, raw) {
				return nil
			}
			if readErr != nil {
				return readErr
			}
			return fmt.Errorf("TxAllo immutable control file conflicts after publish race: %s", path)
		}
		return err
	}
	return nil
}

func txalloMappingSnapshotEpochPath(base string, epoch uint64) string {
	return filepath.Join(filepath.Dir(base), fmt.Sprintf("txallo_mapping_snapshot_e%06d.json", epoch))
}

func txalloMappingAckEpochPath(dataDir string, epoch uint64) string {
	return filepath.Join(dataDir, fmt.Sprintf("txallo_mapping_ack_e%06d.json", epoch))
}

var txalloMappingEpochHistoryMu sync.Mutex

func txalloAppendMappingEpochHistoryV229(path string, snapshot txalloMappingSnapshotV22, sourceEpoch int64, algorithm string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("TxAllo mapping epoch history path is empty")
	}
	if err := txalloValidSnapshot(snapshot, uniqueStringsFromMappingV229(snapshot.Mapping)); err != nil {
		return err
	}
	txalloMappingEpochHistoryMu.Lock()
	defer txalloMappingEpochHistoryMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if raw, err := txalloReadFileStable(path); err == nil {
		maxEpoch := uint64(0)
		haveEpoch := false
		sameEpoch := false
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var row txalloMappingEpochRowV229
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				return fmt.Errorf("TxAllo mapping epoch history decode: %w", err)
			}
			if row.SchemaVersion != txalloDynamicMappingSchema || row.StateDigest == "" {
				return fmt.Errorf("TxAllo mapping epoch history contains invalid row")
			}
			if row.Epoch == snapshot.Epoch {
				if row.StateDigest != snapshot.StateDigest {
					return fmt.Errorf("TxAllo mapping epoch history conflicts at epoch %d", snapshot.Epoch)
				}
				sameEpoch = true
			}
			if !haveEpoch || row.Epoch > maxEpoch {
				maxEpoch = row.Epoch
				haveEpoch = true
			}
		}
		if haveEpoch && snapshot.Epoch < maxEpoch {
			return fmt.Errorf("TxAllo mapping epoch history regressed: got=%d latest=%d", snapshot.Epoch, maxEpoch)
		}
		if sameEpoch {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	row := txalloMappingEpochRowV229{SchemaVersion: snapshot.SchemaVersion, Epoch: snapshot.Epoch, Mapping: copyMapping(snapshot.Mapping), Aliases: copyMapping(snapshot.Aliases), MappingDigest: snapshot.MappingDigest, AliasesDigest: snapshot.AliasesDigest, StateDigest: snapshot.StateDigest, SourceEpoch: sourceEpoch, UpdateAlgorithm: algorithm, TimestampMS: time.Now().UnixMilli()}
	payload, err := json.Marshal(row)
	if err != nil {
		return err
	}
	f, err := txalloOpenAppendFileStable(path, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(payload, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// uniqueStringsFromMappingV229 returns the declared shard domain needed by the
// existing snapshot validator without importing any new topology mechanism.
func uniqueStringsFromMappingV229(mapping map[string]string) []string {
	seen := map[string]bool{}
	for _, shard := range mapping {
		if strings.TrimSpace(shard) != "" {
			seen[shard] = true
		}
	}
	out := make([]string, 0, len(seen))
	for shard := range seen {
		out = append(out, shard)
	}
	sort.Strings(out)
	return out
}

func (p *txalloAccountSharding) writeTxAlloMappingSnapshot() error {
	p.mu.RLock()
	if p.allocator == nil || strings.TrimSpace(p.mappingSnapshotPath) == "" {
		p.mu.RUnlock()
		return nil
	}
	path := p.mappingSnapshotPath
	snapshot := p.txalloMappingSnapshotLocked()
	p.mu.RUnlock()
	// Keep the historical latest alias for UI/export compatibility, but publish
	// the epoch-specific immutable file as the runtime protocol source of truth.
	if err := txalloWriteAtomicJSON(path, snapshot); err != nil {
		return err
	}
	historyPath := filepath.Join(filepath.Dir(path), txalloMappingEpochHistoryName)
	if err := txalloAppendMappingEpochHistoryV229(historyPath, snapshot, -1, "initial_G-TxAllo"); err != nil {
		return err
	}
	return txalloPublishImmutableJSON(txalloMappingSnapshotEpochPath(path, snapshot.Epoch), snapshot)
}

func (p *txalloAccountSharding) ConfirmTxAlloReplicaConvergence(sourceEpoch uint64, digest string, tokenCount int) error {
	if p == nil || !p.TxAlloStatefulReplicaEnabled() {
		return fmt.Errorf("TxAllo Stateful replica convergence confirmation requires paper replicated-state mode")
	}
	if strings.TrimSpace(digest) == "" || tokenCount < 0 {
		return fmt.Errorf("TxAllo Stateful replica convergence proof is invalid")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	expectedEpoch := uint64(intValue(p.evidence["closed_source_epoch_count"]))
	if sourceEpoch != expectedEpoch {
		return fmt.Errorf("TxAllo replica convergence epoch mismatch: got=%d want=%d", sourceEpoch, expectedEpoch)
	}
	p.replicaConvergenceSet = true
	p.replicaConvergedEpoch = sourceEpoch
	p.replicaConvergedDigest = digest
	p.replicaConvergedCount = tokenCount
	p.evidence["stateful_replica_convergence_confirmed"] = true
	p.evidence["stateful_replica_convergence_source_epoch"] = sourceEpoch
	p.evidence["stateful_replica_convergence_token_digest"] = digest
	p.evidence["stateful_replica_convergence_token_count"] = tokenCount
	return nil
}

// ApplyCommittedTxAlloEpoch closes one source-block epoch. Empty source epochs
// advance the paper time-step clock without fabricating A-TxAllo input.
// Every 20th source epoch executes the paper case-study full G refresh.
func (p *txalloAccountSharding) ApplyCommittedTxAlloEpoch(records []WorkloadRecord, sourceEpoch uint64, statelessDirect bool) error {
	if !p.TxAlloDynamicEnabled() {
		return nil
	}
	history := txalloHistoryFromWorkloadRecords(records)
	if len(history) != len(records) {
		return fmt.Errorf("TxAllo dynamic epoch contains a transaction without an account projection")
	}

	p.mu.Lock()
	if p.allocator == nil || p.allocator.Graph == nil || len(p.allocator.Mapping) == 0 {
		p.mu.Unlock()
		return fmt.Errorf("TxAllo dynamic epoch requires a bootstrapped G allocator")
	}
	expectedEpoch := uint64(intValue(p.evidence["closed_source_epoch_count"]))
	if sourceEpoch != expectedEpoch {
		p.mu.Unlock()
		return fmt.Errorf("TxAllo source epoch closure out of order: got=%d want=%d", sourceEpoch, expectedEpoch)
	}
	replicaReady := false
	if !statelessDirect && p.TxAlloStatefulReplicaEnabled() {
		expectedTokens := txalloExpectedReplicaTokensV229(records)
		expectedDigest := stableJSONDigest(expectedTokens)
		if !p.replicaConvergenceSet || p.replicaConvergedEpoch != sourceEpoch || p.replicaConvergedDigest != expectedDigest || p.replicaConvergedCount != len(expectedTokens) {
			p.evidence["stateful_replica_convergence_confirmed"] = false
			p.mu.Unlock()
			return fmt.Errorf("TxAllo Stateful mapping update requires durable all-node replica convergence for source epoch %d", sourceEpoch)
		}
		replicaReady = true
	}
	beforeMapping := copyMapping(p.allocator.Mapping)
	nextHistory := append(append([]txalloHistoryTx(nil), p.dynamicHistory...), history...)
	candidate := txalloCloneAllocator(p.allocator)
	globalMultiple := p.TxAlloGlobalEpochMultiple()
	periodicG := globalMultiple > 1 && (sourceEpoch+1)%uint64(globalMultiple) == 0
	didUpdate := false
	updateAlgorithm := "none"
	if periodicG {
		candidate = newTxAlloAllocator(
			append([]string(nil), p.shards...),
			p.configuredFloat("eta", 2),
			p.configuredFloat("lambda", 0),
			p.configuredFloat("epsilon", 0),
		)
		candidate.RunG(nextHistory)
		didUpdate = true
		updateAlgorithm = "G-TxAllo"
	} else if len(history) > 0 {
		candidate.RunA(history)
		didUpdate = true
		updateAlgorithm = "A-TxAllo"
	}
	if didUpdate && len(candidate.Mapping) != len(candidate.Graph.Nodes) {
		p.mu.Unlock()
		return fmt.Errorf("TxAllo dynamic mapping incomplete")
	}
	moved := []string{}
	if didUpdate {
		if statelessDirect {
			moved = txalloMovedExistingAccounts(beforeMapping, candidate.Mapping)
		} else {
			moved = txalloStatefulMigrationAccounts(p, beforeMapping, candidate.Mapping, records)
		}
	}
	if !statelessDirect && len(moved) > 0 && !replicaReady {
		p.evidence["stateful_migration_guard_triggered"] = true
		p.evidence["stateful_migration_required_account_count"] = len(moved)
		p.evidence["stateful_migration_required_accounts_digest"] = stableJSONDigest(moved)
		p.evidence["stateful_migration_rejected_source_epoch"] = sourceEpoch
		p.mu.Unlock()
		return fmt.Errorf("TxAllo stateful dynamic mapping requires state migration for %d committed accounts; migration/replicated-state adaptation is not installed", len(moved))
	}

	nextAliases := copyMapping(p.aliases)
	for _, record := range records {
		logicalSender := strings.ToLower(strings.TrimSpace(record.SenderID))
		if logicalSender != "" {
			nextAliases[strings.ToLower(canonicalRuntimeSenderAddress(p.plan, logicalSender))] = logicalSender
		}
		logicalReceiver := strings.ToLower(strings.TrimSpace(record.ReceiverID))
		if logicalReceiver != "" {
			nextAliases["receiver_"+logicalReceiver] = logicalReceiver
		}
	}

	p.dynamicHistory = nextHistory
	p.aliases = nextAliases
	committed := intValue(p.evidence["committed_dynamic_transaction_count"]) + len(history)
	p.evidence["committed_dynamic_transaction_count"] = committed
	p.evidence["closed_source_epoch_count"] = int(sourceEpoch + 1)
	p.evidence["last_closed_source_epoch"] = int(sourceEpoch)
	p.evidence["last_closed_source_epoch_transaction_count"] = len(history)
	p.evidence["pending_or_uncommitted_transactions_used"] = 0
	p.evidence["future_evaluation_transactions_used"] = 0
	p.evidence["last_update_algorithm"] = updateAlgorithm
	p.evidence["stateful_migration_guard_triggered"] = false
	p.evidence["stateful_migration_required_account_count"] = 0
	if replicaReady {
		p.evidence["stateful_migration_policy"] = "paper_replicated_state_all_nodes_durable_before_mapping_publish"
		p.evidence["stateful_replica_convergence_verified_source_epoch"] = sourceEpoch
		p.evidence["stateful_replica_convergence_verified_token_count"] = p.replicaConvergedCount
	}

	initial := intValue(p.evidence["initial_g_history_transaction_count"])
	if initial <= 0 {
		initial = intValue(p.evidence["history_transaction_count"])
	}
	p.evidence["total_allocator_history_transaction_count"] = initial + committed

	var path string
	var snapshot txalloMappingSnapshotV22
	if didUpdate {
		p.allocator = candidate
		p.mappingEpoch++
		if periodicG {
			p.evidence["g_txallo_run_count"] = intValue(p.evidence["g_txallo_run_count"]) + 1
			p.evidence["periodic_g_txallo_run_count"] = intValue(p.evidence["periodic_g_txallo_run_count"]) + 1
		} else {
			p.evidence["a_txallo_run_count"] = intValue(p.evidence["a_txallo_run_count"]) + 1
			p.evidence["a_txallo_transaction_count"] = intValue(p.evidence["a_txallo_transaction_count"]) + len(history)
		}
		objective := p.allocator.Metrics.Objective
		p.evidence["mapping_epoch"] = p.mappingEpoch
		p.evidence["graph_account_count"] = len(p.allocator.Graph.Nodes)
		p.evidence["graph_edge_count"] = len(p.allocator.Graph.Edges)
		p.evidence["mapped_account_count"] = len(p.allocator.Mapping)
		p.evidence["mapping_digest"] = stableJSONDigest(p.allocator.Mapping)
		p.evidence["eta"] = p.allocator.Eta
		p.evidence["lambda"] = p.allocator.Lambda
		p.evidence["epsilon"] = p.allocator.Epsilon
		p.evidence["modeled_throughput"] = objective.Throughput
		p.evidence["modeled_cross_shard_ratio"] = objective.CrossShardRatio
		p.evidence["modeled_workload_stddev"] = objective.WorkloadStdDev
		p.evidence["mapping_nonempty"] = len(p.allocator.Mapping) > 0
		p.evidence["mapping_structurally_complete"] = len(p.allocator.Mapping) == len(p.allocator.Graph.Nodes)
		p.evidence["mapping_operationally_valid"] = len(p.allocator.Mapping) > 0 && len(p.allocator.Mapping) == len(p.allocator.Graph.Nodes)
		path = p.mappingSnapshotPath
		snapshot = p.txalloMappingSnapshotLocked()
	}
	if replicaReady {
		p.replicaConvergenceSet = false
		p.replicaConvergedEpoch = 0
		p.replicaConvergedDigest = ""
		p.replicaConvergedCount = 0
	}
	p.mu.Unlock()

	if didUpdate && strings.TrimSpace(path) != "" {
		// The mutable latest alias remains compatibility evidence only. Runtime
		// watchers never consume it after v22.9.7.
		if err := txalloWriteAtomicJSON(path, snapshot); err != nil {
			return fmt.Errorf("TxAllo mapping latest-alias publish: %w", err)
		}
		historyPath := filepath.Join(filepath.Dir(path), txalloMappingEpochHistoryName)
		if err := txalloAppendMappingEpochHistoryV229(historyPath, snapshot, int64(sourceEpoch), updateAlgorithm); err != nil {
			return fmt.Errorf("TxAllo mapping epoch history publish: %w", err)
		}
		if err := txalloPublishImmutableJSON(txalloMappingSnapshotEpochPath(path, snapshot.Epoch), snapshot); err != nil {
			return fmt.Errorf("TxAllo immutable mapping epoch publish: %w", err)
		}
	}
	return nil
}

func txalloValidSnapshot(snapshot txalloMappingSnapshotV22, shards []string) error {
	if snapshot.SchemaVersion != txalloDynamicMappingSchema || len(snapshot.Mapping) == 0 {
		return fmt.Errorf("TxAllo mapping snapshot schema/mapping invalid")
	}
	if snapshot.MappingDigest == "" || snapshot.MappingDigest != stableJSONDigest(snapshot.Mapping) {
		return fmt.Errorf("TxAllo mapping snapshot digest mismatch")
	}
	if snapshot.AliasesDigest == "" || snapshot.AliasesDigest != stableJSONDigest(snapshot.Aliases) {
		return fmt.Errorf("TxAllo mapping snapshot aliases digest mismatch")
	}
	if snapshot.StateDigest == "" || snapshot.StateDigest != txalloStateDigest(snapshot.Mapping, snapshot.Aliases) {
		return fmt.Errorf("TxAllo mapping snapshot state digest mismatch")
	}
	validShard := map[string]bool{}
	for _, shard := range shards {
		validShard[shard] = true
	}
	for _, shard := range snapshot.Mapping {
		if !validShard[shard] {
			return fmt.Errorf("TxAllo mapping snapshot contains unknown shard %q", shard)
		}
	}
	return nil
}

func (p *txalloAccountSharding) RefreshTxAlloMapping() error {
	if !p.TxAlloDynamicEnabled() {
		return nil
	}
	p.mu.RLock()
	basePath := p.mappingSnapshotPath
	currentEpoch := p.mappingEpoch
	shards := append([]string(nil), p.shards...)
	p.mu.RUnlock()
	if strings.TrimSpace(basePath) == "" {
		return nil
	}
	// v22.9.7 removes Stat/mtime from the protocol. A node at epoch N reads
	// exactly the immutable N+1 file. Absence means "not published yet";
	// transient Windows sharing/lock errors are classified by the watcher and
	// retried without producing an error ACK.
	nextPath := txalloMappingSnapshotEpochPath(basePath, currentEpoch+1)
	raw, err := txalloReadFileStable(nextPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var snapshot txalloMappingSnapshotV22
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return fmt.Errorf("TxAllo mapping snapshot decode epoch=%d: %w", currentEpoch+1, err)
	}
	if err := txalloValidSnapshot(snapshot, shards); err != nil {
		return err
	}
	if snapshot.Epoch != currentEpoch+1 {
		return fmt.Errorf("TxAllo immutable mapping snapshot epoch mismatch: file=%d payload=%d", currentEpoch+1, snapshot.Epoch)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mappingEpoch != currentEpoch {
		return nil
	}
	p.allocator.Mapping = copyMapping(snapshot.Mapping)
	p.aliases = copyMapping(snapshot.Aliases)
	p.mappingEpoch = snapshot.Epoch
	p.evidence["mapping_epoch"] = snapshot.Epoch
	p.evidence["mapping_digest"] = snapshot.MappingDigest
	p.evidence["mapped_account_count"] = len(snapshot.Mapping)
	return nil
}

func (r *NodeRuntime) writeTxAlloMappingAck(epoch uint64, stateDigest string, refreshErr error) error {
	if r == nil || r.plugins.Sharding == nil {
		return nil
	}
	dynamic, ok := r.plugins.Sharding.(txalloMappingRefresher)
	if !ok || !dynamic.TxAlloDynamicEnabled() {
		return nil
	}
	status := "ok"
	errText := ""
	if refreshErr != nil {
		status = "error"
		errText = refreshErr.Error()
	}
	// Latest alias keeps a diagnostic wall-clock timestamp. The immutable ACK
	// uses TimestampMS=0 so retries for the same epoch are byte-identical.
	protocolAck := txalloMappingAckV222{
		SchemaVersion: txalloDynamicMappingAckSchema,
		NodeID:        r.node.NodeID,
		Epoch:         epoch,
		StateDigest:   stateDigest,
		Status:        status,
		Error:         errText,
		TimestampMS:   0,
	}
	latestAck := protocolAck
	latestAck.TimestampMS = time.Now().UnixMilli()
	latestPath := filepath.Join(r.node.DataDir, txalloDynamicMappingAckName)
	if err := txalloWriteAtomicJSON(latestPath, latestAck); err != nil {
		return err
	}
	return txalloPublishImmutableJSON(txalloMappingAckEpochPath(r.node.DataDir, epoch), protocolAck)
}

func (r *NodeRuntime) startTxAlloMappingWatcher(ctx context.Context) {
	if r == nil || r.plugins.Sharding == nil {
		return
	}
	refresher, ok := r.plugins.Sharding.(txalloMappingRefresher)
	if !ok || !refresher.TxAlloDynamicEnabled() {
		return
	}
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		lastPublishedSignature := ""
		for {
			beforeEpoch := refresher.TxAlloMappingEpoch()
			err := refresher.RefreshTxAlloMapping()
			if err != nil && txalloControlFileTransient(err) {
				// A sharing/access/lock window is transport-not-ready, not a mapping
				// rejection. Never freeze an error ACK for a transient condition.
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					continue
				}
			}
			epoch := refresher.TxAlloMappingEpoch()
			digest := refresher.TxAlloMappingStateDigest()
			if err != nil {
				// A non-transient validation/I/O failure while reading N+1 is a
				// permanent fail-closed rejection for that requested epoch.
				epoch = beforeEpoch + 1
				digest = ""
			}
			errText := ""
			if err != nil {
				errText = err.Error()
			}
			signature := fmt.Sprintf("%d|%s|%s", epoch, digest, errText)
			if signature != lastPublishedSignature {
				if writeErr := r.writeTxAlloMappingAck(epoch, digest, err); writeErr == nil {
					// Advance only after durable immutable ACK publication succeeds.
					lastPublishedSignature = signature
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func txalloWaitMappingAcks(ctx context.Context, nodes []NodePlan, epoch uint64, stateDigest string) error {
	if len(nodes) == 0 {
		return fmt.Errorf("TxAllo mapping ACK barrier has no nodes")
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	lastTransient := map[string]string{}
	for {
		complete := true
		pending := make([]string, 0)
		for _, node := range nodes {
			path := txalloMappingAckEpochPath(node.DataDir, epoch)
			raw, err := txalloReadFileStable(path)
			if os.IsNotExist(err) || txalloControlFileTransient(err) {
				complete = false
				pending = append(pending, node.NodeID)
				if err != nil && !os.IsNotExist(err) {
					lastTransient[node.NodeID] = err.Error()
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("TxAllo mapping ACK read node=%s epoch=%d: %w", node.NodeID, epoch, err)
			}
			var ack txalloMappingAckV222
			if err := json.Unmarshal(raw, &ack); err != nil {
				return fmt.Errorf("TxAllo mapping ACK decode node=%s epoch=%d: %w", node.NodeID, epoch, err)
			}
			if ack.SchemaVersion != txalloDynamicMappingAckSchema || ack.NodeID != node.NodeID || ack.Epoch != epoch {
				return fmt.Errorf("TxAllo mapping ACK schema/node/epoch mismatch node=%s epoch=%d", node.NodeID, epoch)
			}
			if ack.Status == "error" {
				return fmt.Errorf("TxAllo node %s rejected mapping epoch %d: %s", node.NodeID, epoch, ack.Error)
			}
			if ack.Status != "ok" || ack.StateDigest != stateDigest {
				return fmt.Errorf("TxAllo node %s mapping ACK digest mismatch at epoch %d", node.NodeID, epoch)
			}
			delete(lastTransient, node.NodeID)
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("TxAllo mapping ACK barrier epoch=%d pending=%s transient=%v: %w", epoch, strings.Join(pending, ","), lastTransient, ctx.Err())
		case <-ticker.C:
		}
	}
}

var txalloEpochFeedMu sync.Mutex

func (r *NodeRuntime) appendTxAlloEpochLifecycle(event LifecycleEvent) {
	if r == nil || r.plugins.Sharding == nil {
		return
	}
	dynamic, ok := r.plugins.Sharding.(interface{ TxAlloDynamicEnabled() bool })
	if !ok || !dynamic.TxAlloDynamicEnabled() {
		return
	}
	stage := strings.ToLower(strings.TrimSpace(event.Stage))
	if stage != "durable_committed" && stage != "sourcefinalize" && stage != "refund" && !(stage == "failed" && event.BlockHeight > 0) {
		return
	}
	logicalID := strings.TrimSpace(event.LogicalTxID)
	if logicalID == "" {
		logicalID = strings.TrimSpace(event.TxID)
	}
	if logicalID == "" {
		return
	}
	row := txalloEpochLifecycleRowV22{SchemaVersion: txalloDynamicEpochFeedSchema, LogicalTxID: logicalID, Stage: stage, Success: event.Success, BlockHeight: event.BlockHeight}
	raw, err := json.Marshal(row)
	if err != nil {
		return
	}
	txalloEpochFeedMu.Lock()
	defer txalloEpochFeedMu.Unlock()
	path := filepath.Join(r.node.DataDir, txalloDynamicEpochFeedName)
	f, err := txalloOpenAppendFileStable(path, 0o644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(raw, '\n'))
	_ = f.Close()
}

type txalloEpochFeedReaderV22 struct {
	offsets map[string]int64
	partial map[string][]byte
	stages  map[string]map[string]bool
}

func newTxAlloEpochFeedReaderV22() *txalloEpochFeedReaderV22 {
	return &txalloEpochFeedReaderV22{offsets: map[string]int64{}, partial: map[string][]byte{}, stages: map[string]map[string]bool{}}
}

func (reader *txalloEpochFeedReaderV22) poll(nodes []NodePlan) error {
	for _, node := range nodes {
		path := filepath.Join(node.DataDir, txalloDynamicEpochFeedName)
		f, err := txalloOpenFileStable(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		offset := reader.offsets[path]
		if info.Size() < offset {
			offset = 0
			reader.partial[path] = nil
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
		chunk, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		reader.offsets[path] = offset + int64(len(chunk))
		buf := append(append([]byte(nil), reader.partial[path]...), chunk...)
		lastNewline := -1
		for i := len(buf) - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				lastNewline = i
				break
			}
		}
		if lastNewline < 0 {
			reader.partial[path] = buf
			continue
		}
		complete := buf[:lastNewline]
		reader.partial[path] = append([]byte(nil), buf[lastNewline+1:]...)
		for _, line := range strings.Split(string(complete), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var row txalloEpochLifecycleRowV22
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				return fmt.Errorf("TxAllo epoch lifecycle feed decode: %w", err)
			}
			if row.SchemaVersion != txalloDynamicEpochFeedSchema || row.LogicalTxID == "" || row.Stage == "" {
				return fmt.Errorf("TxAllo epoch lifecycle feed schema invalid")
			}
			if reader.stages[row.LogicalTxID] == nil {
				reader.stages[row.LogicalTxID] = map[string]bool{}
			}
			stage := strings.ToLower(strings.TrimSpace(row.Stage))
			if stage == "refund" || stage == "failed" {
				reader.stages[row.LogicalTxID][stage] = true
				continue
			}
			if !row.Success {
				// A nominal success-stage record with Success=false must never
				// satisfy the committed-only barrier.
				reader.stages[row.LogicalTxID]["failed"] = true
				continue
			}
			reader.stages[row.LogicalTxID][stage] = true
		}
	}
	return nil
}

func txalloWaitCommittedEpoch(ctx context.Context, nodes []NodePlan, records []WorkloadRecord, crossByLogical map[string]bool, statelessDirect bool, reader *txalloEpochFeedReaderV22) error {
	if len(records) == 0 {
		return nil
	}
	wanted := make([]string, 0, len(records))
	seen := map[string]bool{}
	for _, record := range records {
		logicalID := strings.TrimSpace(firstNonEmpty(record.LogicalID, record.SourceEventID))
		if logicalID == "" || seen[logicalID] {
			continue
		}
		seen[logicalID] = true
		wanted = append(wanted, logicalID)
	}
	sort.Strings(wanted)
	if len(wanted) != len(records) {
		return fmt.Errorf("TxAllo A epoch logical identity is empty or duplicated")
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := reader.poll(nodes); err != nil {
			return err
		}
		complete := true
		for _, logicalID := range wanted {
			stages := reader.stages[logicalID]
			if stages["refund"] || stages["failed"] {
				return fmt.Errorf("TxAllo A epoch transaction %s reached a failed/refunded terminal state", logicalID)
			}
			required := "durable_committed"
			if !statelessDirect && crossByLogical[logicalID] {
				required = "sourcefinalize"
			}
			if !stages[required] {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("TxAllo A epoch commit barrier: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
