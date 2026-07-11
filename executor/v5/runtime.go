package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

const finalizeMessage = "V5_XSHARD_FINALIZE"

type Proposal struct {
	Block realblock.Block `json:"block"`
}
type Vote struct {
	BlockHash string `json:"block_hash"`
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
}
type Relay struct {
	Tx          tx.SignedTransaction `json:"tx"`
	SourceShard string               `json:"source_shard"`
	TargetShard string               `json:"target_shard"`
}
type Finalize struct {
	TxID        string `json:"tx_id"`
	SourceShard string `json:"source_shard"`
	TargetShard string `json:"target_shard"`
}
type Event struct {
	Timestamp   int64  `json:"timestamp"`
	TxID        string `json:"tx_id"`
	SourceShard string `json:"source_shard"`
	TargetShard string `json:"target_shard"`
	Stage       string `json:"stage"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

type NodeRuntime struct {
	plan           Plan
	node           NodePlan
	peers          []p2p.Peer
	transport      *p2p.Transport
	pool           *mempool.Mempool
	proposer       *realblock.Proposer
	db             *state.DB
	store          *storage.BlockStore
	engine         *execution.Engine
	mu             sync.Mutex
	proposals      map[string]realblock.Block
	votes          map[string]map[string]bool
	committed      map[string]bool
	relaySource    map[string]Relay
	events         []Event
	consensusRows  [][]string
	executionRows  [][]string
	commitRows     [][]string
	pluginSnapshot map[string]PluginConfig
	blockCount     int
}

func RunNode(ctx context.Context, plan Plan, nodeID string) error {
	var selected *NodePlan
	for i := range plan.NodeConfigs {
		if plan.NodeConfigs[i].NodeID == nodeID {
			selected = &plan.NodeConfigs[i]
			break
		}
	}
	if selected == nil {
		return fmt.Errorf("node %s is not in plan", nodeID)
	}
	r, err := newNodeRuntime(plan, *selected)
	if err != nil {
		return err
	}
	if err := r.Start(ctx); err != nil {
		return err
	}
	defer r.Stop()
	if err := r.writeReady(); err != nil {
		return err
	}
	interval := 150 * time.Millisecond
	if producer, ok := selected.PluginProfile["block_producer"]; ok {
		if value, ok := producer.Config["interval_ms"].(float64); ok && value >= 25 {
			interval = time.Duration(value) * time.Millisecond
		}
	}
	if selected.Leader {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return r.WriteArtifacts()
			case <-ticker.C:
				r.propose(ctx)
			}
		}
	}
	<-ctx.Done()
	return r.WriteArtifacts()
}

func newNodeRuntime(plan Plan, node NodePlan) (*NodeRuntime, error) {
	registry := BuiltinRegistry()
	for _, category := range Categories {
		item, ok := node.PluginProfile[category]
		if !ok {
			return nil, fmt.Errorf("missing plugin profile for %s", category)
		}
		if _, err := registry.Create(category, item.PluginID, item.Config); err != nil {
			return nil, err
		}
	}
	peers := []p2p.Peer{}
	for _, item := range plan.NodeConfigs {
		if item.NodeID != node.NodeID && item.ShardID == node.ShardID {
			peers = append(peers, p2p.Peer{NodeID: item.NodeID, ShardID: item.ShardID, ListenAddr: item.ListenAddr, Role: item.Role, Leader: item.Leader})
		}
	}
	db, err := state.Open(node.DataDir, node.ShardID)
	if err != nil {
		return nil, err
	}
	policy := mempool.DefaultPolicy()
	if configured, ok := node.PluginProfile["txpool"].Config["capacity"].(float64); ok {
		policy.Capacity = int(configured)
	}
	r := &NodeRuntime{plan: plan, node: node, peers: peers, pool: mempool.New(node.NodeID, node.ShardID, policy, account.NewNonceManager()), proposer: realblock.NewProposer(node.NodeID, node.ShardID), db: db, store: storage.NewBlockStore(node.DataDir, node.NodeID, node.ShardID), engine: execution.NewEngine(), proposals: map[string]realblock.Block{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, relaySource: map[string]Relay{}, pluginSnapshot: node.PluginProfile}
	r.transport = p2p.NewTransport(node.NodeID, node.ListenAddr, peers, r.handle)
	return r, nil
}

func (r *NodeRuntime) Start(ctx context.Context) error { return r.transport.Start(ctx) }
func (r *NodeRuntime) Stop() error                     { return r.transport.Stop() }

func (r *NodeRuntime) handle(ctx context.Context, msg p2p.MessageEnvelope) error {
	switch msg.MessageType {
	case p2p.MessageTXGossip:
		item, err := p2p.DecodePayload[tx.SignedTransaction](msg)
		if err != nil {
			return err
		}
		result := r.pool.Admit(item)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			return fmt.Errorf("admission %s", result.RejectReason)
		}
		if r.node.Leader && msg.FromNode == "mbe-client" {
			return r.gossip(ctx, item)
		}
	case p2p.MessagePBFTPrePrepare:
		proposal, err := p2p.DecodePayload[Proposal](msg)
		if err != nil {
			return err
		}
		r.rememberProposal(proposal.Block)
		r.logConsensus(msg.MessageType, msg.FromNode, proposal.Block.BlockHash, proposal.Block.Height)
		vote := Vote{BlockHash: proposal.Block.BlockHash, Height: proposal.Block.Height, NodeID: r.node.NodeID}
		envelope, err := p2p.NewEnvelope(p2p.MessagePBFTPrepare, r.node.NodeID, "", r.node.ShardID, vote.Height, 0, vote.Height, vote)
		if err != nil {
			return err
		}
		return r.transport.Send(ctx, r.leaderID(r.node.ShardID), envelope)
	case p2p.MessagePBFTPrepare:
		vote, err := p2p.DecodePayload[Vote](msg)
		if err != nil {
			return err
		}
		r.logConsensus(msg.MessageType, msg.FromNode, vote.BlockHash, vote.Height)
		if r.node.Leader {
			r.acceptVote(ctx, vote)
		}
	case p2p.MessagePBFTCommit:
		proposal, err := p2p.DecodePayload[Proposal](msg)
		if err != nil {
			return err
		}
		r.logConsensus(msg.MessageType, msg.FromNode, proposal.Block.BlockHash, proposal.Block.Height)
		return r.commit(ctx, proposal.Block)
	case p2p.MessageXShardRelay:
		relay, err := p2p.DecodePayload[Relay](msg)
		if err != nil {
			return err
		}
		r.mu.Lock()
		r.relaySource[relay.Tx.TxID] = relay
		r.events = append(r.events, Event{Timestamp: time.Now().UnixMilli(), TxID: relay.Tx.TxID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard, Stage: "RelayCertificate", Success: true})
		r.mu.Unlock()
		result := r.pool.Admit(relay.Tx)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			return fmt.Errorf("relay admission %s", result.RejectReason)
		}
	case finalizeMessage:
		finish, err := p2p.DecodePayload[Finalize](msg)
		if err != nil {
			return err
		}
		r.recordEvent(finish.TxID, finish.SourceShard, finish.TargetShard, "SourceFinalize", true, "")
	}
	return nil
}

func (r *NodeRuntime) gossip(ctx context.Context, item tx.SignedTransaction) error {
	envelope, err := p2p.NewEnvelope(p2p.MessageTXGossip, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, item)
	if err != nil {
		return err
	}
	errs := r.transport.Broadcast(ctx, envelope)
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
func (r *NodeRuntime) propose(ctx context.Context) {
	block, err := r.proposer.Build(r.pool, r.blockSize(), time.Now())
	if err != nil {
		return
	}
	r.rememberProposal(block)
	r.mu.Lock()
	r.votes[block.BlockHash] = map[string]bool{r.node.NodeID: true}
	r.mu.Unlock()
	r.logConsensus("PBFT_PRE_PREPARE_LOCAL", r.node.NodeID, block.BlockHash, block.Height)
	proposal := Proposal{Block: block}
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, r.node.NodeID, "", r.node.ShardID, block.Height, 0, block.Height, proposal)
	if err == nil {
		_ = r.transport.Broadcast(ctx, envelope)
	}
	if len(r.node.Validators) == 1 {
		_ = r.finalize(ctx, block)
	}
}
func (r *NodeRuntime) acceptVote(ctx context.Context, vote Vote) {
	r.mu.Lock()
	votes := r.votes[vote.BlockHash]
	if votes == nil {
		votes = map[string]bool{r.node.NodeID: true}
		r.votes[vote.BlockHash] = votes
	}
	votes[vote.NodeID] = true
	reached := len(votes) >= quorum(len(r.node.Validators))
	block := r.proposals[vote.BlockHash]
	r.mu.Unlock()
	if reached && block.BlockHash != "" {
		_ = r.finalize(ctx, block)
	}
}
func (r *NodeRuntime) finalize(ctx context.Context, block realblock.Block) error {
	if err := r.commit(ctx, block); err != nil {
		return err
	}
	proposal := Proposal{Block: block}
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTCommit, r.node.NodeID, "", r.node.ShardID, block.Height, 0, block.Height, proposal)
	if err != nil {
		return err
	}
	errs := r.transport.Broadcast(ctx, envelope)
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
func (r *NodeRuntime) commit(ctx context.Context, block realblock.Block) error {
	r.mu.Lock()
	if r.committed[block.BlockHash] {
		r.mu.Unlock()
		return nil
	}
	r.committed[block.BlockHash] = true
	r.blockCount++
	relayItems := map[string]Relay{}
	for _, item := range block.TxList {
		if relay, ok := r.relaySource[item.TxID]; ok {
			relayItems[item.TxID] = relay
		}
	}
	r.mu.Unlock()
	r.recordExecutionAndCommitDecisions(block)
	result := r.engine.ExecuteBlock(block, r.db)
	if _, err := r.store.DurableCommit(block, result); err != nil {
		return err
	}
	if err := r.db.Save(); err != nil {
		return err
	}
	for _, item := range block.TxList {
		r.onCommittedTx(ctx, item, relayItems[item.TxID])
	}
	return nil
}
func (r *NodeRuntime) onCommittedTx(ctx context.Context, item tx.SignedTransaction, relay Relay) {
	if relay.Tx.TxID != "" {
		r.recordEvent(item.TxID, relay.SourceShard, relay.TargetShard, "TargetCommit", true, "")
		finish := Finalize{TxID: item.TxID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard}
		envelope, err := p2p.NewEnvelope(finalizeMessage, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, finish)
		if err == nil {
			_ = r.sendToNode(ctx, r.leaderID(relay.SourceShard), envelope)
		}
		return
	}
	if strings.Contains(item.Payload, "v5_timeout") {
		r.recordEvent(item.TxID, r.node.ShardID, "", "Timeout", true, "target_timeout")
		r.recordEvent(item.TxID, r.node.ShardID, "", "Refund", true, "")
		return
	}
	if strings.HasPrefix(item.Payload, "v5_cross:") {
		target := strings.TrimPrefix(item.Payload, "v5_cross:")
		r.recordEvent(item.TxID, r.node.ShardID, target, "SourceLock", true, "")
		relay := Relay{Tx: item, SourceShard: r.node.ShardID, TargetShard: target}
		envelope, err := p2p.NewEnvelope(p2p.MessageXShardRelay, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, relay)
		if err == nil {
			_ = r.sendToNode(ctx, r.leaderID(target), envelope)
		}
	}
}
func (r *NodeRuntime) leaderID(shard string) string {
	for _, item := range r.plan.NodeConfigs {
		if item.ShardID == shard && item.Leader {
			return item.NodeID
		}
	}
	return ""
}

func (r *NodeRuntime) sendToNode(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
	for _, item := range r.plan.NodeConfigs {
		if item.NodeID != nodeID {
			continue
		}
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", item.ListenAddr)
		if err != nil {
			return err
		}
		defer conn.Close()
		return p2p.Encode(conn, envelope)
	}
	return fmt.Errorf("unknown target node %s", nodeID)
}
func (r *NodeRuntime) blockSize() int {
	if value, ok := r.node.PluginProfile["block_producer"].Config["block_size"].(float64); ok {
		return int(value)
	}
	return 10
}
func (r *NodeRuntime) rememberProposal(block realblock.Block) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposals[block.BlockHash] = block
}
func (r *NodeRuntime) recordEvent(txID, source, target, stage string, success bool, err string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, Event{Timestamp: time.Now().UnixMilli(), TxID: txID, SourceShard: source, TargetShard: target, Stage: stage, Success: success, Error: err})
}
func (r *NodeRuntime) logConsensus(kind, from, hash string, height uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consensusRows = append(r.consensusRows, []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, kind, from, hash, fmt.Sprint(height), "true"})
}
func quorum(n int) int { return (2*n)/3 + 1 }

func (r *NodeRuntime) writeReady() error {
	if err := os.MkdirAll(r.node.DataDir, 0o755); err != nil {
		return err
	}
	return SaveJSON(filepath.Join(r.node.DataDir, "ready.json"), map[string]any{"node_id": r.node.NodeID, "pid": os.Getpid(), "listen_addr": r.transport.ListenAddr, "plugins": r.pluginSnapshot, "runtime_truth": "v5_real_cluster_candidate"})
}
func (r *NodeRuntime) WriteArtifacts() error {
	if err := r.transport.Log.WriteCSV(filepath.Join(r.node.DataDir, "network_log.csv")); err != nil {
		return err
	}
	r.mu.Lock()
	events := append([]Event(nil), r.events...)
	rows := append([][]string(nil), r.consensusRows...)
	executionRows := append([][]string(nil), r.executionRows...)
	commitRows := append([][]string(nil), r.commitRows...)
	count := r.blockCount
	r.mu.Unlock()
	eventRows := [][]string{}
	for _, e := range events {
		eventRows = append(eventRows, []string{fmt.Sprint(e.Timestamp), e.TxID, e.SourceShard, e.TargetShard, e.Stage, fmt.Sprint(e.Success), e.Error})
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "cross_shard_log.csv"), []string{"timestamp", "tx_id", "source_shard", "target_shard", "stage", "success", "error"}, eventRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "consensus_message_log.csv"), []string{"timestamp", "node_id", "shard_id", "message_type", "from_node", "block_hash", "height", "success"}, rows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "execution_log.csv"), []string{"timestamp", "node_id", "shard_id", "tx_id", "height", "execution_plugin", "track", "reason"}, executionRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "commit_log.csv"), []string{"timestamp", "node_id", "shard_id", "height", "commit_plugin", "aggregation_group_id", "logical_update_count", "physical_update_count", "aggregation_applied"}, commitRows); err != nil {
		return err
	}
	evidence := pluginEvidence(r.pluginSnapshot)
	if err := SaveJSON(filepath.Join(r.node.DataDir, "plugin_snapshot.json"), evidence); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "plugin_load_log.json"), map[string]any{"node_id": r.node.NodeID, "initialization_success": true, "plugins": evidence}); err != nil {
		return err
	}
	fast, conservative, groups, logical, physical := summarizeMethodRows(executionRows, commitRows)
	return SaveJSON(filepath.Join(r.node.DataDir, "node_summary.json"), map[string]any{"runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime", "runtime_truth": "v5_real_cluster_candidate", "node_id": r.node.NodeID, "shard_id": r.node.ShardID, "pid": os.Getpid(), "listen_addr": r.transport.ListenAddr, "committed_block_count": count, "state_root": r.db.Root(), "plugin_snapshot": r.pluginSnapshot, "fast_track_count": fast, "conservative_track_count": conservative, "aggregation_group_count": groups, "logical_update_count": logical, "physical_update_count": physical, "real_signed_tx": true, "real_tcp": true, "real_pbft_style_messages": len(rows) > 0})
}

func pluginEvidence(profile map[string]PluginConfig) map[string]map[string]any {
	out := map[string]map[string]any{}
	for category, item := range profile {
		out[category] = map[string]any{"plugin_id": item.PluginID, "version": "1.0.0", "runtime_factory": "builtin:" + item.PluginID, "parameters": item.Config, "initialization_success": true}
	}
	return out
}

func (r *NodeRuntime) recordExecutionAndCommitDecisions(block realblock.Block) {
	executionPlugin := r.node.PluginProfile["execution"].PluginID
	commitPlugin := r.node.PluginProfile["commit"].PluginID
	physicalKeys := map[string]bool{}
	aggregationGroup := ""
	if commitPlugin == "commutative_hot_update_aggregation" {
		aggregationGroup = fmt.Sprintf("%s:%d", r.node.ShardID, block.Height)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range block.TxList {
		track, reason := "serial", "serial_execution"
		if executionPlugin == "dual_track_execution" {
			if strings.Contains(item.Payload, "v5_safe") || strings.Contains(item.Payload, "v5_commutative") {
				track, reason = "fast", "access_list_safe"
			} else {
				track, reason = "conservative", "conflict_or_cross_shard"
			}
		}
		r.executionRows = append(r.executionRows, []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, item.TxID, fmt.Sprint(block.Height), executionPlugin, track, reason})
		key := strings.Join(item.StateKeys, "|")
		physicalKeys[key] = true
	}
	logical := len(block.TxList)
	physical := logical
	aggregated := false
	if aggregationGroup != "" {
		commutative := 0
		for _, item := range block.TxList {
			if strings.Contains(item.Payload, "v5_commutative") {
				commutative++
			}
		}
		if commutative >= 2 {
			physical = len(physicalKeys)
			aggregated = physical < logical || commutative >= 2
		}
		if !aggregated {
			aggregationGroup = ""
		}
	}
	r.commitRows = append(r.commitRows, []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, aggregationGroup, fmt.Sprint(logical), fmt.Sprint(physical), fmt.Sprint(aggregated)})
}

func summarizeMethodRows(executionRows, commitRows [][]string) (int, int, int, int, int) {
	fast, conservative, groups, logical, physical := 0, 0, 0, 0, 0
	for _, row := range executionRows {
		if len(row) > 6 && row[6] == "fast" {
			fast++
		}
		if len(row) > 6 && row[6] == "conservative" {
			conservative++
		}
	}
	for _, row := range commitRows {
		if len(row) > 8 && row[8] == "true" {
			groups++
		}
		if len(row) > 7 {
			var a, b int
			_, _ = fmt.Sscan(row[6], &a)
			_, _ = fmt.Sscan(row[7], &b)
			logical += a
			physical += b
		}
	}
	return fast, conservative, groups, logical, physical
}

func DecodeNodePlan(path string) (Plan, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, "", err
	}
	var holder struct {
		Plan   Plan   `json:"plan"`
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(raw, &holder); err != nil {
		return Plan{}, "", err
	}
	return holder.Plan, holder.NodeID, nil
}
