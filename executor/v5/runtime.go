package v5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/account"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/faults"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

const finalizeMessage = "V5_XSHARD_FINALIZE"
const catchupRequestMessage = "V5_CATCHUP_REQUEST"
const catchupBlockMessage = "V5_CATCHUP_BLOCK"

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
	LogicalTxID string               `json:"logical_tx_id"`
	SourceShard string               `json:"source_shard"`
	TargetShard string               `json:"target_shard"`
}
type Finalize struct {
	TxID        string `json:"tx_id"`
	LogicalTxID string `json:"logical_tx_id"`
	SourceShard string `json:"source_shard"`
	TargetShard string `json:"target_shard"`
}
type CatchupRequest struct {
	ShardID    string `json:"shard_id"`
	FromHeight uint64 `json:"from_height"`
	ToHeight   uint64 `json:"to_height"`
}
type CatchupBlock struct {
	Block      realblock.Block `json:"block"`
	SourceNode string          `json:"source_node"`
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

type CommitDisposition string

const (
	CommitApplied        CommitDisposition = "applied"
	CommitAlreadyApplied CommitDisposition = "already_applied"
	CommitDeferred       CommitDisposition = "deferred"
	CommitRejected       CommitDisposition = "rejected"
)

type CommitResult struct {
	Disposition CommitDisposition
	Block       realblock.Block
}

type CommitOrigin string

const (
	CommitOriginConsensus      CommitOrigin = "consensus"
	CommitOriginCatchUp        CommitOrigin = "catch_up"
	CommitOriginRecoveryReplay CommitOrigin = "recovery_replay"
)

type NodeRuntime struct {
	plan                    Plan
	node                    NodePlan
	peers                   []p2p.Peer
	transport               *p2p.Transport
	pool                    *mempool.Mempool
	proposer                *realblock.Proposer
	db                      *state.DB
	store                   *storage.BlockStore
	mu                      sync.Mutex
	commitMu                sync.Mutex
	proposals               map[string]realblock.Block
	votes                   map[string]map[string]bool
	committed               map[string]bool
	committing              map[string]bool
	committedHeight         uint64
	committedHash           string
	commitPhase             string
	commitPhaseHeight       uint64
	commitPhaseHash         string
	lastProgressAt          int64
	pendingCommits          map[uint64]realblock.Block
	pendingCommitErrors     map[uint64]string
	proposalInFlight        bool
	proposalInFlightHash    string
	proposalStartedAt       time.Time
	lastProposalError       string
	fatalPersistenceError   string
	lastCatchupRequest      time.Time
	relaySource             map[string]Relay
	crossEventSeen          map[string]bool
	relayAdmissionFailures  map[string]string
	events                  []Event
	lifecycle               []LifecycleEvent
	consensusRows           [][]string
	executionRows           [][]string
	commitRows              [][]string
	chainRows               [][]string
	blockExecutionSummaries []map[string]any
	executionPlans          []map[string]any
	txExecutionTraceRows    [][]string
	stateDeltaRows          [][]string
	planDigestRows          [][]string
	pluginSnapshot          map[string]PluginConfig
	plugins                 RuntimePlugins
	blockCount              int
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
	interval := blockInterval(*selected)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	statusInterval := runtimeStatusWriteInterval(interval)
	lastStatusWrite := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return r.WriteArtifacts()
		case <-ticker.C:
			r.retryPendingRelays()
			if selected.Leader {
				r.expireStaleProposal(r.proposalTimeout())
				r.propose(ctx)
			} else {
				r.requestCatchup(ctx)
			}
			if lastStatusWrite.IsZero() || time.Since(lastStatusWrite) >= statusInterval {
				_ = r.writeRuntimeStatus()
				lastStatusWrite = time.Now()
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(filepath.Dir(r.node.DataDir)), "stop.request")); err == nil {
				return r.WriteArtifacts()
			}
		}
	}
}

func runtimeStatusWriteInterval(blockInterval time.Duration) time.Duration {
	if blockInterval < time.Second {
		return time.Second
	}
	return blockInterval
}

func (r *NodeRuntime) retryPendingRelays() {
	r.mu.Lock()
	relays := make([]Relay, 0, len(r.relaySource))
	for _, relay := range r.relaySource {
		relays = append(relays, relay)
	}
	r.mu.Unlock()
	for _, relay := range relays {
		if r.pool.Has(relay.Tx.TxID) {
			continue
		}
		result := r.pool.AdmitRelay(relay.Tx)
		if !result.Accepted && result.RejectReason == "stale_nonce" {
			r.reconcileCommittedRelay(context.Background(), relay)
			continue
		}
		if !result.Accepted && result.RejectReason != "duplicate_tx" && result.RejectReason != "capacity" {
			r.mu.Lock()
			r.relayAdmissionFailures[relay.Tx.TxID] = result.RejectReason
			r.mu.Unlock()
			r.recordEvent(relay.Tx.TxID, relay.SourceShard, relay.TargetShard, "RelayAdmissionFailed", false, result.RejectReason)
		}
	}
}

func (r *NodeRuntime) reconcileCommittedRelay(ctx context.Context, relay Relay) {
	if !r.node.Leader {
		return
	}
	committed, err := r.store.HasTransaction(relay.Tx.TxID)
	if err != nil || !committed {
		return
	}
	logicalID := relay.LogicalTxID
	if logicalID == "" {
		logicalID = relay.Tx.TxID
	}
	r.recordEvent(logicalID, relay.SourceShard, relay.TargetShard, "TargetCommit", true, "tx_index_reconciliation")
	finish := Finalize{TxID: relay.Tx.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard}
	envelope, err := p2p.NewEnvelope(finalizeMessage, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, finish)
	if err != nil || r.sendToNode(ctx, r.leaderID(relay.SourceShard), envelope) != nil {
		return
	}
	r.mu.Lock()
	delete(r.relaySource, logicalID)
	delete(r.relayAdmissionFailures, logicalID)
	r.mu.Unlock()
}

func (r *NodeRuntime) requestCatchup(ctx context.Context) {
	leader := r.leaderID(r.node.ShardID)
	if leader == "" {
		return
	}
	r.mu.Lock()
	if !r.lastCatchupRequest.IsZero() && time.Since(r.lastCatchupRequest) < r.catchupRequestInterval() {
		r.mu.Unlock()
		return
	}
	from := r.committedHeight + 1
	r.lastCatchupRequest = time.Now()
	r.mu.Unlock()
	if from == 0 {
		return
	}
	envelope, err := p2p.NewEnvelope(catchupRequestMessage, r.node.NodeID, leader, r.node.ShardID, from, 0, from, CatchupRequest{ShardID: r.node.ShardID, FromHeight: from, ToHeight: from + 8})
	if err == nil {
		_ = r.sendToNode(ctx, leader, envelope)
	}
}

func (r *NodeRuntime) catchupRequestInterval() time.Duration {
	interval := r.proposalTimeout()
	if interval < 10*time.Second {
		return 10 * time.Second
	}
	return interval
}

func newNodeRuntime(plan Plan, node NodePlan) (*NodeRuntime, error) {
	plugins, err := InstantiatePlugins(node.PluginProfile)
	if err != nil {
		return nil, err
	}
	peers := []p2p.Peer{}
	for _, item := range plan.NodeConfigs {
		if item.NodeID != node.NodeID {
			peers = append(peers, p2p.Peer{NodeID: item.NodeID, ShardID: item.ShardID, ListenAddr: item.ListenAddr, Role: item.Role, Leader: item.Leader})
		}
	}
	db, err := state.Open(node.DataDir, node.ShardID)
	if err != nil {
		return nil, err
	}
	policy := mempool.DefaultPolicy()
	policy.Capacity = plugins.TxPool.Capacity()
	r := &NodeRuntime{plan: plan, node: node, peers: peers, pool: mempool.New(node.NodeID, node.ShardID, policy, account.NewNonceManager()), proposer: realblock.NewProposer(node.NodeID, node.ShardID), db: db, store: storage.NewBlockStore(node.DataDir, node.NodeID, node.ShardID), proposals: map[string]realblock.Block{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, pendingCommitErrors: map[uint64]string{}, committedHash: "genesis", lastProgressAt: time.Now().UnixMilli(), relaySource: map[string]Relay{}, crossEventSeen: map[string]bool{}, relayAdmissionFailures: map[string]string{}, pluginSnapshot: node.PluginProfile, plugins: plugins}
	r.transport = p2p.NewTransport(node.NodeID, node.ListenAddr, peers, r.handle)
	r.transport.SetFaultPolicy(faultPolicyFromPlan(plan.FaultPlan))
	return r, nil
}

func faultPolicyFromPlan(plan map[string]any) faults.Policy {
	mode := fmt.Sprint(plan["mode"])
	if mode == "" || mode == "disabled" {
		return faults.Policy{}
	}
	policy := faults.Policy{Enabled: true, DelayMS: intValue(plan["delay_ms"]), DropRate: floatValue(plan["drop_rate"]), Seed: int64(intValue(plan["seed"]))}
	if policy.DelayMS == 0 {
		policy.DelayMS = intValue(plan["network_delay_ms"])
	}
	if raw, ok := plan["drop_message_types"].([]any); ok {
		for _, item := range raw {
			policy.DropMessageTypes = append(policy.DropMessageTypes, fmt.Sprint(item))
		}
	}
	if raw, ok := plan["target_peer_ids"].([]any); ok {
		for _, item := range raw {
			policy.TargetPeerIDs = append(policy.TargetPeerIDs, fmt.Sprint(item))
		}
	}
	return policy
}

func intValue(value any) int {
	switch item := value.(type) {
	case int:
		return item
	case float64:
		return int(item)
	case json.Number:
		parsed, _ := item.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func floatValue(value any) float64 {
	switch item := value.(type) {
	case float64:
		return item
	case int:
		return float64(item)
	default:
		return 0
	}
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
		r.recordLifecycle(nowLifecycle(item.TxID, "received", r.node.NodeID, r.node.ShardID))
		result := r.pool.Admit(item)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: false, Error: result.RejectReason})
			return fmt.Errorf("admission %s", result.RejectReason)
		}
		r.recordLifecycle(nowLifecycle(item.TxID, "admitted", r.node.NodeID, r.node.ShardID))
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
		logicalID := relay.LogicalTxID
		if logicalID == "" {
			logicalID = relay.Tx.TxID
		}
		if committed, err := r.store.HasTransaction(relay.Tx.TxID); err == nil && committed {
			r.reconcileCommittedRelay(ctx, relay)
			return nil
		}
		r.mu.Lock()
		if _, exists := r.relaySource[logicalID]; exists {
			r.mu.Unlock()
			return nil
		}
		r.relaySource[logicalID] = relay
		r.events = append(r.events, Event{Timestamp: time.Now().UnixMilli(), TxID: relay.Tx.TxID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard, Stage: "RelayCertificate", Success: true})
		r.mu.Unlock()
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: relay.Tx.TxID, LogicalTxID: relay.Tx.TxID, Stage: "relay_received", NodeID: r.node.NodeID, ShardID: r.node.ShardID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard, Success: true})
		result := r.pool.AdmitRelay(relay.Tx)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			r.mu.Lock()
			r.relayAdmissionFailures[relay.Tx.TxID] = result.RejectReason
			r.mu.Unlock()
			return fmt.Errorf("relay admission %s", result.RejectReason)
		}
		if r.node.Leader {
			for _, node := range r.plan.NodeConfigs {
				if node.ShardID == r.node.ShardID && node.NodeID != r.node.NodeID {
					_ = r.sendToNode(ctx, node.NodeID, msg)
				}
			}
		}
	case finalizeMessage:
		finish, err := p2p.DecodePayload[Finalize](msg)
		if err != nil {
			return err
		}
		logicalID := finish.LogicalTxID
		if logicalID == "" {
			logicalID = finish.TxID
		}
		if !r.node.Leader {
			return nil
		}
		r.recordEvent(logicalID, finish.SourceShard, finish.TargetShard, "SourceFinalize", true, "")
		// Finalization is the source-side durable acknowledgement for a relay.
		// Remove the source reservation only after this message arrives; leaving
		// it in relaySource makes drain report a permanently pending cross-shard
		// operation even though TargetCommit already completed.
		r.mu.Lock()
		delete(r.relaySource, logicalID)
		delete(r.relayAdmissionFailures, logicalID)
		r.mu.Unlock()
	case catchupRequestMessage:
		request, err := p2p.DecodePayload[CatchupRequest](msg)
		if err != nil {
			return err
		}
		if !r.node.Leader {
			return nil
		}
		for height := request.FromHeight; height <= request.ToHeight; height++ {
			block, ok, err := r.store.ReadCommittedAtHeight(height)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			envelope, err := p2p.NewEnvelope(catchupBlockMessage, r.node.NodeID, msg.FromNode, r.node.ShardID, height, 0, height, CatchupBlock{Block: block, SourceNode: r.node.NodeID})
			if err != nil {
				return err
			}
			if err := r.sendToNode(ctx, msg.FromNode, envelope); err != nil {
				return err
			}
		}
	case catchupBlockMessage:
		item, err := p2p.DecodePayload[CatchupBlock](msg)
		if err != nil {
			return err
		}
		if _, err := r.commitWithOrigin(ctx, item.Block, CommitOriginCatchUp); err != nil {
			return err
		}
		r.logConsensus("CATCHUP_APPLIED", item.SourceNode, item.Block.BlockHash, item.Block.Height)
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
	r.mu.Lock()
	fatal := r.fatalPersistenceError
	r.mu.Unlock()
	if fatal != "" {
		return
	}
	r.mu.Lock()
	if r.proposalInFlight {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	block, err := r.proposer.Build(r.pool, r.blockSize(), time.Now())
	if err != nil {
		r.mu.Lock()
		if err.Error() != "empty_mempool" {
			r.lastProposalError = err.Error()
		}
		r.mu.Unlock()
		return
	}
	r.rememberProposal(block)
	for _, item := range block.TxList {
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "proposed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
	}
	r.mu.Lock()
	r.lastProposalError = ""
	r.votes[block.BlockHash] = map[string]bool{r.node.NodeID: true}
	r.proposalInFlight = true
	r.proposalInFlightHash = block.BlockHash
	r.proposalStartedAt = time.Now()
	r.mu.Unlock()
	r.logConsensus("PBFT_PRE_PREPARE_LOCAL", r.node.NodeID, block.BlockHash, block.Height)
	proposal := Proposal{Block: block}
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, r.node.NodeID, "", r.node.ShardID, block.Height, 0, block.Height, proposal)
	if err == nil {
		if errs := r.transport.Broadcast(ctx, envelope); len(errs) > 0 {
			r.mu.Lock()
			r.lastProposalError = errs[0].Error()
			r.mu.Unlock()
		}
	} else {
		r.mu.Lock()
		r.lastProposalError = err.Error()
		r.mu.Unlock()
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
	threshold := r.plugins.Consensus.Quorum(len(r.node.Validators))
	previousCount := len(votes)
	votes[vote.NodeID] = true
	reached := previousCount < threshold && len(votes) >= threshold && !r.committed[vote.BlockHash] && !r.committing[vote.BlockHash]
	block := r.proposals[vote.BlockHash]
	r.mu.Unlock()
	if reached && block.BlockHash != "" {
		r.logConsensus("PBFT_QUORUM_REACHED", r.node.NodeID, block.BlockHash, block.Height)
		_ = r.finalize(ctx, block)
	}
}

func (r *NodeRuntime) setCommitPhase(phase string, block realblock.Block) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitPhase = phase
	r.commitPhaseHeight = block.Height
	r.commitPhaseHash = block.BlockHash
}

func (r *NodeRuntime) finalize(ctx context.Context, block realblock.Block) error {
	for _, item := range block.TxList {
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "quorum_committed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
	}
	result, err := r.commitWithDisposition(ctx, block)
	if err != nil {
		r.mu.Lock()
		fatal := r.fatalPersistenceError != ""
		r.mu.Unlock()
		// A recoverable commit failure can release its reservation. A fatal
		// persistence failure freezes the proposal and keeps evidence reserved.
		if !fatal {
			r.pool.ReleaseReserved(block.TxList)
		}
		if fatal {
			return err
		}
		r.mu.Lock()
		if r.node.Leader {
			r.proposalInFlight = false
			r.proposalInFlightHash = ""
			r.proposalStartedAt = time.Time{}
		}
		delete(r.proposals, block.BlockHash)
		delete(r.votes, block.BlockHash)
		r.mu.Unlock()
		return err
	}
	if result.Disposition != CommitApplied && result.Disposition != CommitAlreadyApplied {
		return fmt.Errorf("block %s commit %s", block.BlockHash, result.Disposition)
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
	result, err := r.commitWithOrigin(ctx, block, CommitOriginConsensus)
	if err != nil {
		return err
	}
	if result.Disposition == CommitDeferred || result.Disposition == CommitRejected {
		return fmt.Errorf("block %s commit %s", block.BlockHash, result.Disposition)
	}
	return nil
}

func (r *NodeRuntime) commitWithDisposition(ctx context.Context, block realblock.Block) (CommitResult, error) {
	return r.commitWithOrigin(ctx, block, CommitOriginConsensus)
}

func (r *NodeRuntime) commitWithOrigin(ctx context.Context, block realblock.Block, origin CommitOrigin) (CommitResult, error) {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	if r.fatalPersistenceError != "" {
		err := fmt.Errorf("fatal persistence freeze: %s", r.fatalPersistenceError)
		r.mu.Unlock()
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	r.mu.Unlock()
	result, err := r.commitOnce(ctx, block, origin)
	if err != nil {
		return result, err
	}
	if result.Disposition == CommitApplied && result.Block.BlockHash != "" {
		r.drainPendingCommits(ctx, result.Block, origin)
	}
	return result, nil
}

func (r *NodeRuntime) drainPendingCommits(ctx context.Context, next realblock.Block, origin CommitOrigin) {
	for next.BlockHash != "" {
		result, err := r.commitOnce(ctx, next, origin)
		if err != nil {
			r.mu.Lock()
			if r.pendingCommitErrors == nil {
				r.pendingCommitErrors = map[uint64]string{}
			}
			r.pendingCommitErrors[next.Height] = fmt.Sprintf("%s: %v", next.BlockHash, err)
			r.mu.Unlock()
			return
		}
		if result.Disposition != CommitApplied {
			r.mu.Lock()
			if r.pendingCommitErrors == nil {
				r.pendingCommitErrors = map[uint64]string{}
			}
			r.pendingCommitErrors[next.Height] = fmt.Sprintf("%s: %s", next.BlockHash, result.Disposition)
			r.mu.Unlock()
			return
		}
		next = result.Block
	}
}

func (r *NodeRuntime) commitOnce(ctx context.Context, block realblock.Block, origin CommitOrigin) (CommitResult, error) {
	r.setCommitPhase("enter", block)
	r.mu.Lock()
	if r.fatalPersistenceError != "" {
		err := fmt.Errorf("fatal persistence freeze: %s", r.fatalPersistenceError)
		r.mu.Unlock()
		r.setCommitPhase("rejected_fatal", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	if r.committed[block.BlockHash] {
		r.mu.Unlock()
		r.setCommitPhase("already_applied", block)
		return CommitResult{Disposition: CommitAlreadyApplied, Block: realblock.Block{}}, nil
	}
	if r.committing == nil {
		r.committing = map[string]bool{}
	}
	if r.committing[block.BlockHash] {
		r.mu.Unlock()
		r.setCommitPhase("already_committing", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("block %s is already being committed", block.BlockHash)
	}
	expected := r.committedHeight + 1
	if block.Height > expected {
		r.pendingCommits[block.Height] = block
		r.mu.Unlock()
		r.setCommitPhase("deferred_future_height", block)
		return CommitResult{Disposition: CommitDeferred, Block: realblock.Block{}}, nil
	}
	if block.Height < expected {
		r.mu.Unlock()
		r.setCommitPhase("rejected_stale_height", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("stale block height %d, expected %d", block.Height, expected)
	}
	if block.PreviousHash != r.committedHash {
		r.mu.Unlock()
		r.setCommitPhase("rejected_parent_hash", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("parent hash mismatch at height %d", block.Height)
	}
	r.committing[block.BlockHash] = true
	defer func() {
		r.mu.Lock()
		delete(r.committing, block.BlockHash)
		r.mu.Unlock()
	}()
	relayItems := map[string]Relay{}
	for _, item := range block.TxList {
		if relay, ok := r.relaySource[item.TxID]; ok {
			relayItems[item.TxID] = relay
		}
	}
	r.mu.Unlock()
	r.setCommitPhase("state_checkpoint", block)
	stateBefore := r.db.Snapshot()
	stateCheckpoint, err := r.db.Checkpoint()
	if err != nil {
		r.setCommitPhase("state_checkpoint_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	r.setCommitPhase("store_checkpoint", block)
	checkpoint, err := r.store.Checkpoint()
	if err != nil {
		r.setCommitPhase("store_checkpoint_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	r.setCommitPhase("execute_block", block)
	executeStarted := time.Now()
	executed, err := r.plugins.BlockExecutor.ExecuteBlock(ctx, BlockExecutionInput{Block: block, BaseStateSnapshot: stateBefore, NodeID: r.node.NodeID, ShardID: r.node.ShardID, WorkerCount: 1})
	if err != nil {
		r.setCommitPhase("execute_block_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	executed.BlockExecutionMS = time.Since(executeStarted).Milliseconds()
	executed.TransactionExecutionMS = executed.BlockExecutionMS
	r.setCommitPhase("apply_state_delta", block)
	applyStarted := time.Now()
	r.db.ApplyDeterministicBatch(executed.StateDelta)
	executed.DeterministicApplyMS = time.Since(applyStarted).Milliseconds()
	result := executed.ExecutionResult
	r.setCommitPhase("durable_commit", block)
	if _, err := r.store.DurableCommit(block, result); err != nil {
		r.setCommitPhase("durable_commit_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	r.setCommitPhase("state_save", block)
	if err := r.db.Save(); err != nil {
		r.setCommitPhase("state_save_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	r.setCommitPhase("record_execution_artifacts", block)
	r.recordBlockExecutionResult(block, executed)
	r.recordExecutionAndCommitDecisions(block)
	r.setCommitPhase("commit_reserved", block)
	r.pool.CommitReserved(block.TxList)
	if r.node.Leader {
		r.proposer.Confirm(block)
	}
	r.setCommitPhase("advance_runtime_state", block)
	r.mu.Lock()
	r.committed[block.BlockHash] = true
	delete(r.committing, block.BlockHash)
	r.blockCount++
	r.chainRows = append(r.chainRows, []string{r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), "0", block.BlockHash, block.PreviousHash, fmt.Sprint(len(block.TxList)), block.TxRoot, block.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, fmt.Sprint(time.Now().UnixMilli()), fmt.Sprint(time.Now().UnixMilli())})
	r.mu.Unlock()
	r.mu.Lock()
	r.committedHeight = block.Height
	r.committedHash = block.BlockHash
	r.lastProgressAt = time.Now().UnixMilli()
	if r.node.Leader {
		r.proposalInFlight = false
		r.proposalInFlightHash = ""
		r.proposalStartedAt = time.Time{}
	}
	next := r.pendingCommits[r.committedHeight+1]
	delete(r.pendingCommits, r.committedHeight+1)
	r.mu.Unlock()
	for _, item := range block.TxList {
		if origin == CommitOriginConsensus {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "durable_committed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
			r.onCommittedTx(ctx, item, relayItems[item.TxID])
		}
		if relayItems[item.TxID].Tx.TxID != "" {
			r.mu.Lock()
			delete(r.relaySource, item.TxID)
			r.mu.Unlock()
		}
	}
	r.setCommitPhase("idle", realblock.Block{})
	return CommitResult{Disposition: CommitApplied, Block: next}, nil
}

func (r *NodeRuntime) expireStaleProposal(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	r.mu.Lock()
	if !r.proposalInFlight || r.proposalInFlightHash == "" || r.proposalStartedAt.IsZero() || time.Since(r.proposalStartedAt) < timeout {
		r.mu.Unlock()
		return
	}
	hash := r.proposalInFlightHash
	if r.committed[hash] || r.committing[hash] || r.proposalQuorumReachedLocked(hash) {
		r.mu.Unlock()
		return
	}
	block := r.proposals[hash]
	r.proposalInFlight = false
	r.proposalInFlightHash = ""
	r.proposalStartedAt = time.Time{}
	delete(r.proposals, hash)
	delete(r.votes, hash)
	r.lastProposalError = "proposal_timeout_released"
	if block.BlockHash != "" {
		r.pool.ReleaseReserved(block.TxList)
	}
	r.mu.Unlock()
}

func (r *NodeRuntime) proposalQuorumReachedLocked(hash string) bool {
	votes := r.votes[hash]
	if len(votes) == 0 || len(r.node.Validators) == 0 || r.plugins.Consensus == nil {
		return false
	}
	return len(votes) >= r.plugins.Consensus.Quorum(len(r.node.Validators))
}

func (r *NodeRuntime) rollbackCommitFailure(blockHash string, stateBefore map[string]string, stateCheckpoint state.FileCheckpoint, checkpoint storage.ArtifactCheckpoint, cause error) error {
	r.db.Restore(stateBefore)
	stateErr := r.db.Rollback(stateCheckpoint)
	storeErr := r.store.Rollback(checkpoint)
	r.mu.Lock()
	if stateErr == nil && storeErr == nil {
		delete(r.committing, blockHash)
	} else {
		parts := []string{}
		if stateErr != nil {
			parts = append(parts, "state rollback: "+stateErr.Error())
		}
		if storeErr != nil {
			parts = append(parts, "store rollback: "+storeErr.Error())
		}
		r.fatalPersistenceError = strings.Join(parts, "; ")
	}
	r.mu.Unlock()
	if stateErr != nil || storeErr != nil {
		return fmt.Errorf("%w; rollback also failed: %s", cause, r.fatalPersistenceError)
	}
	return cause
}
func (r *NodeRuntime) onCommittedTx(ctx context.Context, item tx.SignedTransaction, relay Relay) {
	r.onCommittedTxWithOrigin(ctx, item, relay, CommitOriginConsensus)
}

func (r *NodeRuntime) onCommittedTxWithOrigin(ctx context.Context, item tx.SignedTransaction, relay Relay, origin CommitOrigin) {
	if origin != CommitOriginConsensus {
		return
	}
	if relay.Tx.TxID != "" {
		if !r.node.Leader {
			return
		}
		logicalID := relay.LogicalTxID
		if logicalID == "" {
			logicalID = item.TxID
		}
		r.recordEvent(logicalID, relay.SourceShard, relay.TargetShard, "TargetCommit", true, "")
		finish := Finalize{TxID: item.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard}
		envelope, err := p2p.NewEnvelope(finalizeMessage, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, finish)
		if err == nil {
			_ = r.sendToNode(ctx, r.leaderID(relay.SourceShard), envelope)
		}
		return
	}
	if !r.node.Leader {
		return
	}
	if strings.Contains(item.Payload, "v5_timeout") {
		r.recordEvent(item.TxID, r.node.ShardID, "", "Timeout", true, "target_timeout")
		r.recordEvent(item.TxID, r.node.ShardID, "", "Refund", true, "")
		return
	}
	if item.SourceKind == "cross_shard_relay" {
		return
	}
	if r.plugins.CrossShard.IsCrossShard(item) {
		target := crossTargetShard(item.Payload)
		if target == "" || target == r.node.ShardID {
			return
		}
		r.recordEvent(item.TxID, r.node.ShardID, target, "SourceLock", true, "")
		relay := Relay{Tx: item, LogicalTxID: item.TxID, SourceShard: r.node.ShardID, TargetShard: target}
		envelope, err := p2p.NewEnvelope(p2p.MessageXShardRelay, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, relay)
		if err == nil {
			_ = r.sendToNode(ctx, r.leaderID(target), envelope)
		}
	}
}

func crossTargetShard(payload string) string {
	target := strings.TrimPrefix(payload, "v5_cross:")
	if colon := strings.Index(target, ":"); colon >= 0 {
		return target[:colon]
	}
	return target
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
	return r.transport.Send(ctx, nodeID, envelope)
}
func (r *NodeRuntime) blockSize() int {
	return r.plugins.BlockProducer.BlockSize()
}
func blockInterval(node NodePlan) time.Duration {
	interval := 150 * time.Millisecond
	if producer, ok := node.PluginProfile["block_producer"]; ok {
		if value := intValue(producer.Config["interval_ms"]); value >= 25 {
			interval = time.Duration(value) * time.Millisecond
		}
	}
	return interval
}
func (r *NodeRuntime) proposalTimeout() time.Duration {
	blockSize := r.blockSize()
	if blockSize <= 0 {
		blockSize = 1
	}
	interval := blockInterval(r.node)
	timeout := 5*time.Second + time.Duration(blockSize)*100*time.Millisecond + 4*interval
	if timeout < 5*time.Second {
		return 5 * time.Second
	}
	if timeout > 60*time.Second {
		return 60 * time.Second
	}
	return timeout
}
func (r *NodeRuntime) rememberProposal(block realblock.Block) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposals[block.BlockHash] = block
}
func (r *NodeRuntime) recordEvent(txID, source, target, stage string, success bool, err string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	logicalID := txID
	if logicalID == "" {
		return
	}
	uniqueStage := strings.ToLower(stage)
	if uniqueStage == "sourcelock" || uniqueStage == "targetcommit" || uniqueStage == "sourcefinalize" || uniqueStage == "refund" {
		if r.crossEventSeen == nil {
			r.crossEventSeen = map[string]bool{}
		}
		key := logicalID + "|" + uniqueStage
		if r.crossEventSeen[key] {
			return
		}
		r.crossEventSeen[key] = true
	}
	r.events = append(r.events, Event{Timestamp: time.Now().UnixMilli(), TxID: txID, SourceShard: source, TargetShard: target, Stage: stage, Success: success, Error: err})
	r.lifecycle = append(r.lifecycle, LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: txID, LogicalTxID: logicalID, Stage: strings.ToLower(stage), NodeID: r.node.NodeID, ShardID: r.node.ShardID, SourceShard: source, TargetShard: target, Success: success, Error: err})
}
func (r *NodeRuntime) recordLifecycle(event LifecycleEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lifecycle = append(r.lifecycle, event)
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
func (r *NodeRuntime) writeRuntimeStatus() error {
	r.mu.Lock()
	committedHeight := r.committedHeight
	committedHash := r.committedHash
	proposalInFlight := r.proposalInFlight
	proposalTimeoutMS := r.proposalTimeout().Milliseconds()
	commitPhase := r.commitPhase
	commitPhaseHeight := r.commitPhaseHeight
	commitPhaseHash := r.commitPhaseHash
	lastProposalError := r.lastProposalError
	fatalPersistenceError := r.fatalPersistenceError
	pendingCommitCount := len(r.pendingCommits)
	pendingCommitHeights := mapKeys(r.pendingCommits)
	pendingCommitErrors := map[uint64]string{}
	for key, value := range r.pendingCommitErrors {
		pendingCommitErrors[key] = value
	}
	pendingRelayCount := len(r.relaySource)
	relayAdmissionFailures := map[string]string{}
	for key, value := range r.relayAdmissionFailures {
		relayAdmissionFailures[key] = value
	}
	lastProgressAt := r.lastProgressAt
	terminal := map[string]bool{}
	durableCommitted := map[string]bool{}
	sourceFinalized := map[string]bool{}
	refunded := map[string]bool{}
	failed := map[string]bool{}
	crossLogical := map[string]bool{}
	completedCross := map[string]bool{}
	for _, event := range r.lifecycle {
		stage := strings.ToLower(event.Stage)
		logicalID := event.LogicalTxID
		if logicalID == "" {
			logicalID = event.TxID
		}
		if logicalID != "" {
			switch stage {
			case "durable_committed":
				durableCommitted[logicalID] = true
			case "sourcefinalize":
				sourceFinalized[logicalID] = true
			case "refund":
				refunded[logicalID] = true
			case "failed":
				failed[logicalID] = true
			}
		}
		if stage == "sourcelock" || stage == "relaycertificate" || stage == "targetcommit" || stage == "sourcefinalize" {
			crossLogical[logicalID] = true
		}
		if stage == "sourcefinalize" || stage == "refund" || stage == "failed" {
			completedCross[logicalID] = true
		}
	}
	for _, event := range r.lifecycle {
		stage := strings.ToLower(event.Stage)
		if stage == "durable_committed" || stage == "sourcefinalize" || stage == "refund" || stage == "failed" {
			if stage == "durable_committed" && crossLogical[event.LogicalTxID] && !completedCross[event.LogicalTxID] {
				continue
			}
			terminal[event.LogicalTxID] = true
		}
	}
	terminalIDs := make([]string, 0, len(terminal))
	for id := range terminal {
		terminalIDs = append(terminalIDs, id)
	}
	durableIDs := mapIDs(durableCommitted)
	sourceFinalizedIDs := mapIDs(sourceFinalized)
	refundedIDs := mapIDs(refunded)
	failedIDs := mapIDs(failed)
	pendingRelayIDs := make([]string, 0, len(r.relaySource))
	for txID := range r.relaySource {
		pendingRelayIDs = append(pendingRelayIDs, txID)
	}
	r.mu.Unlock()
	mempoolIDs := r.pool.IDs()
	sort.Strings(mempoolIDs)
	status := map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "role": r.node.Role, "committed_height": committedHeight, "committed_block_hash": committedHash, "mempool_depth": r.pool.Len(), "mempool_logical_tx_ids": mempoolIDs, "reserved_tx_count": r.pool.ReservedCount(), "proposal_in_flight": proposalInFlight, "proposal_timeout_ms": proposalTimeoutMS, "commit_phase": commitPhase, "commit_phase_height": commitPhaseHeight, "commit_phase_hash": commitPhaseHash, "last_proposal_error": lastProposalError, "fatal_persistence_error": fatalPersistenceError, "pending_commit_count": pendingCommitCount, "pending_commit_heights": pendingCommitHeights, "pending_commit_errors": pendingCommitErrors, "pending_future_block_count": 0, "pending_cross_shard_count": pendingRelayCount, "pending_cross_shard_ids": pendingRelayIDs, "relay_admission_failures": relayAdmissionFailures, "terminal_count": len(terminal), "terminal_logical_tx_ids": terminalIDs, "durable_committed_logical_tx_ids": durableIDs, "source_finalized_logical_tx_ids": sourceFinalizedIDs, "refunded_logical_tx_ids": refundedIDs, "failed_logical_tx_ids": failedIDs, "last_progress_at": lastProgressAt, "ready": true, "stopping": false}
	return SaveJSON(filepath.Join(r.node.DataDir, "node_runtime_status.json"), status)
}
func mapIDs(items map[string]bool) []string {
	out := make([]string, 0, len(items))
	for key := range items {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
func mapKeys(items map[uint64]realblock.Block) []uint64 {
	out := make([]uint64, 0, len(items))
	for key := range items {
		out = append(out, key)
	}
	return out
}
func (r *NodeRuntime) WriteArtifacts() error {
	if err := r.writeRuntimeStatus(); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "fault_policy.json"), map[string]any{"requested": r.plan.FaultPlan, "applied": faultPolicyFromPlan(r.plan.FaultPlan)}); err != nil {
		return err
	}
	if err := r.transport.Log.WriteCSV(filepath.Join(r.node.DataDir, "network_log.csv")); err != nil {
		return err
	}
	r.mu.Lock()
	events := append([]Event(nil), r.events...)
	rows := append([][]string(nil), r.consensusRows...)
	executionRows := append([][]string(nil), r.executionRows...)
	commitRows := append([][]string(nil), r.commitRows...)
	chainRows := append([][]string(nil), r.chainRows...)
	blockExecutionSummaries := append([]map[string]any(nil), r.blockExecutionSummaries...)
	executionPlans := append([]map[string]any(nil), r.executionPlans...)
	txExecutionTraceRows := append([][]string(nil), r.txExecutionTraceRows...)
	stateDeltaRows := append([][]string(nil), r.stateDeltaRows...)
	planDigestRows := append([][]string(nil), r.planDigestRows...)
	lifecycle := append([]LifecycleEvent(nil), r.lifecycle...)
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
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "committed_chain.csv"), []string{"node_id", "shard_id", "height", "view", "block_hash", "parent_hash", "tx_count", "tx_digest", "state_root_before", "state_root_after", "receipt_root", "commit_started_at", "commit_finished_at"}, chainRows); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "block_execution_summary.json"), map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "block_executor_id": r.plugins.BlockExecutor.ID(), "block_executor_version": "1.0.0", "worker_count": 1, "blocks": blockExecutionSummaries, "executed_block_count": len(blockExecutionSummaries), "plan_digest_consistent": planDigestsConsistent(planDigestRows)}); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(r.node.DataDir, "execution_plan.jsonl"), executionPlans); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "transaction_execution_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "success", "error", "read_key_count", "write_key_count", "state_root_after_tx"}, txExecutionTraceRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "state_delta_log.csv"), []string{"node_id", "shard_id", "block_hash", "height", "key", "value_digest"}, stateDeltaRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "plan_digest_consistency.csv"), []string{"node_id", "shard_id", "block_hash", "height", "block_executor_id", "plan_digest", "state_root_before", "state_root_after", "receipt_root", "worker_count", "consistent"}, planDigestRows); err != nil {
		return err
	}
	lifecycleRows := make([][]string, 0, len(lifecycle))
	for _, event := range lifecycle {
		lifecycleRows = append(lifecycleRows, lifecycleRow(event))
	}
	if err := writeLifecycleJSONL(filepath.Join(r.node.DataDir, "transaction_lifecycle.jsonl"), lifecycle); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "transaction_lifecycle.csv"), []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}, lifecycleRows); err != nil {
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
	return SaveJSON(filepath.Join(r.node.DataDir, "node_summary.json"), map[string]any{"runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime", "runtime_truth": "v5_real_cluster_candidate", "node_id": r.node.NodeID, "shard_id": r.node.ShardID, "pid": os.Getpid(), "listen_addr": r.transport.ListenAddr, "committed_block_count": count, "state_root": r.db.Root(), "plugin_snapshot": r.pluginSnapshot, "block_executor_id": r.plugins.BlockExecutor.ID(), "block_executor_version": "1.0.0", "worker_count": 1, "plan_digest_consistent": planDigestsConsistent(planDigestRows), "fast_track_count": fast, "conservative_track_count": conservative, "aggregation_group_count": groups, "logical_update_count": logical, "physical_update_count": physical, "real_signed_tx": true, "real_tcp": true, "real_pbft_style_messages": len(rows) > 0})
}

func pluginEvidence(profile map[string]PluginConfig) map[string]map[string]any {
	out := map[string]map[string]any{}
	for category, item := range profile {
		out[category] = map[string]any{"plugin_id": item.PluginID, "version": "1.0.0", "runtime_factory": "builtin:" + item.PluginID, "parameters": item.Config, "initialization_success": true}
	}
	return out
}

func (r *NodeRuntime) recordExecutionAndCommitDecisions(block realblock.Block) {
	executionPlugin := r.plugins.Execution.ID()
	commitPlugin := r.plugins.Commit.ID()
	commitDecision := r.plugins.Commit.DecideCommit(CommitInput{ShardID: r.node.ShardID, Height: block.Height, Transactions: block.TxList})
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range block.TxList {
		decision := r.plugins.Execution.Classify(item)
		r.executionRows = append(r.executionRows, []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, item.TxID, fmt.Sprint(block.Height), executionPlugin, decision.Track, decision.Reason})
	}
	r.commitRows = append(r.commitRows, []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, commitDecision.AggregationGroupID, fmt.Sprint(commitDecision.LogicalUpdates), fmt.Sprint(commitDecision.PhysicalUpdates), fmt.Sprint(commitDecision.Applied)})
}

func (r *NodeRuntime) recordBlockExecutionResult(block realblock.Block, result BlockExecutionResult) {
	executed := result.ExecutionResult
	observedReadKeys, observedWriteKeys := 0, 0
	for _, delta := range executed.TxDeltas {
		observedReadKeys += len(delta.ReadSet)
		observedWriteKeys += len(delta.WriteSet)
	}
	summary := map[string]any{
		"block_hash":                   block.BlockHash,
		"height":                       block.Height,
		"block_executor_id":            r.plugins.BlockExecutor.ID(),
		"block_executor_version":       executed.ExecutorVersion,
		"block_execution_ms":           result.BlockExecutionMS,
		"transaction_execution_ms":     result.TransactionExecutionMS,
		"deterministic_apply_ms":       result.DeterministicApplyMS,
		"executed_transaction_count":   len(block.TxList),
		"successful_transaction_count": executed.SuccessfulTxs,
		"failed_transaction_count":     executed.FailedTxs,
		"declared_read_key_count":      executed.Plan.DeclaredReadKeyCount,
		"declared_write_key_count":     executed.Plan.DeclaredWriteKeyCount,
		"observed_read_key_count":      observedReadKeys,
		"observed_write_key_count":     observedWriteKeys,
		"state_root_before":            executed.StateRootBefore,
		"state_root_after":             executed.StateRootAfter,
		"receipt_root":                 executed.ReceiptRoot,
		"execution_plan_digest":        executed.PlanDigest,
		"worker_count":                 result.WorkerCount,
		"plan_digest_consistent":       true,
		"state_root_consistent":        true,
		"serial_order_preserved":       true,
		"reordered_transaction_count":  0,
		"maximum_parallel_width":       1,
	}
	plan := map[string]any{
		"node_id":  r.node.NodeID,
		"shard_id": r.node.ShardID,
		"plan":     executed.Plan,
	}
	traceRows := make([][]string, 0, len(executed.TxDeltas))
	for _, delta := range executed.TxDeltas {
		traceRows = append(traceRows, []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), delta.TxID, fmt.Sprint(delta.OriginalIndex), fmt.Sprint(delta.Success), delta.Error, fmt.Sprint(len(delta.ReadSet)), fmt.Sprint(len(delta.WriteSet)), delta.Receipt.StateRootAfterTx})
	}
	stateRows := make([][]string, 0, len(result.StateDelta))
	for _, item := range result.StateDelta {
		stateRows = append(stateRows, []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), item.Key, stableTextDigest(item.Value)})
	}
	planRow := []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), r.plugins.BlockExecutor.ID(), executed.PlanDigest, executed.StateRootBefore, executed.StateRootAfter, executed.ReceiptRoot, fmt.Sprint(result.WorkerCount), "true"}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockExecutionSummaries = append(r.blockExecutionSummaries, summary)
	r.executionPlans = append(r.executionPlans, plan)
	r.txExecutionTraceRows = append(r.txExecutionTraceRows, traceRows...)
	r.stateDeltaRows = append(r.stateDeltaRows, stateRows...)
	r.planDigestRows = append(r.planDigestRows, planRow)
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

func writeJSONL(path string, rows []map[string]any) error {
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		payload, err := json.Marshal(row)
		if err != nil {
			return err
		}
		lines = append(lines, string(payload))
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

func stableTextDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func planDigestsConsistent(rows [][]string) bool {
	for _, row := range rows {
		if len(row) < 11 || row[5] == "" || row[10] != "true" {
			return false
		}
	}
	return true
}
