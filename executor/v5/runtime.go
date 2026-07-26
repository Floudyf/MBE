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
	"strconv"
	"strings"
	"sync"
	"time"

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
const catchupRequestMessage = "V5_CATCHUP_REQUEST"
const catchupBlockMessage = "V5_CATCHUP_BLOCK"
const stateFetchRequestMessage = "V5_STATE_FETCH_REQUEST"
const stateFetchResponseMessage = "V5_STATE_FETCH_RESPONSE"
const stateDeltaApplyMessage = "V5_STATE_DELTA_APPLY"
const stateDeltaApplyAckMessage = "V5_STATE_DELTA_APPLY_ACK"
const remoteStateDeltaApplyLagBlocks uint64 = 1

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
type StateFetchRequest struct {
	RequestID      string `json:"request_id"`
	TxID           string `json:"tx_id"`
	BlockHash      string `json:"block_hash"`
	Key            string `json:"key"`
	HomeShard      string `json:"home_shard"`
	ExecutionShard string `json:"execution_shard"`
	AccessKind     string `json:"access_kind"`
}
type StateFetchResponse struct {
	RequestID      string `json:"request_id"`
	TxID           string `json:"tx_id"`
	BlockHash      string `json:"block_hash"`
	Key            string `json:"key"`
	QualifiedKey   string `json:"qualified_key"`
	Value          string `json:"value"`
	HomeShard      string `json:"home_shard"`
	ExecutionShard string `json:"execution_shard"`
	StateRoot      string `json:"state_root"`
	WitnessDigest  string `json:"witness_digest"`
	Success        bool   `json:"success"`
	Error          string `json:"error,omitempty"`
}
type StateDeltaApplyRequest struct {
	RequestID       string   `json:"request_id"`
	TxID            string   `json:"tx_id"`
	TxIDs           []string `json:"tx_ids,omitempty"`
	BlockHash       string   `json:"block_hash"`
	Key             string   `json:"key"`
	Value           string   `json:"value"`
	UpdateSemantics string   `json:"update_semantics,omitempty"`
	Delta           int64    `json:"delta,omitempty"`
	BaseValue       string   `json:"base_value,omitempty"`
	BaseValueDigest string   `json:"base_value_digest,omitempty"`
	ApplyOrigin     string   `json:"apply_origin,omitempty"`
	DeltaKind       string   `json:"delta_kind,omitempty"`
	HasInitialValue bool     `json:"has_initial_value,omitempty"`
	InitialValue    int64    `json:"initial_value"`
	HomeShard       string   `json:"home_shard"`
	ExecutionShard  string   `json:"execution_shard"`
	SourceKey       string   `json:"source_key"`
	SourceHeight    uint64   `json:"source_height"`
}
type StateDeltaApplyAck struct {
	RequestID       string   `json:"request_id"`
	TxID            string   `json:"tx_id"`
	TxIDs           []string `json:"tx_ids,omitempty"`
	BlockHash       string   `json:"block_hash"`
	Key             string   `json:"key"`
	QualifiedKey    string   `json:"qualified_key"`
	ValueDigest     string   `json:"value_digest"`
	UpdateSemantics string   `json:"update_semantics,omitempty"`
	Delta           int64    `json:"delta,omitempty"`
	BaseValueDigest string   `json:"base_value_digest,omitempty"`
	ApplyOrigin     string   `json:"apply_origin,omitempty"`
	DeltaKind       string   `json:"delta_kind,omitempty"`
	HasInitialValue bool     `json:"has_initial_value,omitempty"`
	InitialValue    int64    `json:"initial_value"`
	HomeShard       string   `json:"home_shard"`
	ExecutionShard  string   `json:"execution_shard"`
	StateRoot       string   `json:"state_root"`
	WitnessDigest   string   `json:"witness_digest"`
	Success         bool     `json:"success"`
	Error           string   `json:"error,omitempty"`
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
	schedulerRows           [][]string
	commitRows              [][]string
	logicalPhysicalRows     [][]string
	chainRows               [][]string
	blockExecutionSummaries []map[string]any
	executionPlans          []map[string]any
	txExecutionTraceRows    [][]string
	businessExecutionRows   [][]string
	stateDeltaRows          [][]string
	planDigestRows          [][]string
	remoteStateRows         [][]string
	runtimeEventRows        [][]string
	runtimeMetricCounts     map[string]int64
	stateFetchWaiters       map[string]chan StateFetchResponse
	stateFetchWitnesses     map[string]StateFetchResponse
	stateFetchSnapshots     map[string]map[string]string
	stateApplyWaiters       map[string]chan StateDeltaApplyAck
	pendingStateDeltas      []StateDeltaApplyRequest
	pendingStateDeltaKeys   map[string]bool
	appliedStateDeltaKeys   map[string]bool
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
	interval := r.blockInterval()
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

	ctx := context.Background()

	for _, relay := range relays {
		// 交易仍在内存池时，无需反复访问持久化索引。
		if r.pool.Has(relay.Tx.TxID) {
			continue
		}

		// 交易已经离开内存池时，再判断它是否已经持久化提交。
		committed, err := r.reconcileRelayIfCommitted(ctx, relay)
		if err != nil {
			r.recordRelayAdmissionFailure(
				relay,
				"commit_lookup: "+err.Error(),
			)
			continue
		}
		if committed {
			continue
		}

		if err := r.admitTransaction(relay.Tx); err != nil {
			r.recordRelayAdmissionFailure(relay, err.Error())
			continue
		}

		result := r.pool.AdmitRelay(relay.Tx)

		// 保留入池后的第二次检查，关闭 admission 与提交并发窗口。
		if result.Accepted {
			committed, err = r.reconcileRelayIfCommitted(ctx, relay)
			if err != nil {
				r.pool.Remove(relay.Tx.TxID)
				r.recordRelayAdmissionFailure(
					relay,
					"post_admission_commit_lookup: "+err.Error(),
				)
				continue
			}
			if committed {
				continue
			}
		}

		if !result.Accepted && result.RejectReason == "stale_nonce" {
			r.reconcileCommittedRelay(ctx, relay)
			continue
		}

		if !result.Accepted &&
			result.RejectReason != "duplicate_tx" &&
			result.RejectReason != "capacity" {
			r.recordRelayAdmissionFailure(relay, result.RejectReason)
		}
	}
}

func (r *NodeRuntime) recordRelayAdmissionFailure(relay Relay, reason string) {
	r.mu.Lock()
	r.relayAdmissionFailures[relay.Tx.TxID] = reason
	r.mu.Unlock()
	r.recordEvent(relay.Tx.TxID, relay.SourceShard, relay.TargetShard, "RelayAdmissionFailed", false, reason)
}

func (r *NodeRuntime) reconcileRelayIfCommitted(ctx context.Context, relay Relay) (bool, error) {
	committed, err := r.store.HasTransaction(relay.Tx.TxID)
	if err != nil {
		return false, err
	}
	if !committed {
		return false, nil
	}
	r.pool.Remove(relay.Tx.TxID)
	r.reconcileCommittedRelay(ctx, relay)
	return true, nil
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
	crossShard := r.crossShardPlugin()
	targetEvent := crossShard.TargetCommit(CrossShardFinalizeInput{TxID: relay.Tx.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard})
	targetEvent.Error = "tx_index_reconciliation"
	r.recordCrossShardEvent(targetEvent)
	finish := crossShard.BuildFinalize(CrossShardFinalizeInput{TxID: relay.Tx.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard})
	envelope, err := p2p.NewEnvelope(finalizeMessage, r.node.NodeID, "", r.node.ShardID, 0, 0, 0, finish)
	if err != nil || r.sendToNode(ctx, r.leaderID(relay.SourceShard), envelope) != nil {
		return
	}
	r.mu.Lock()
	delete(r.relaySource, logicalID)
	delete(r.relayAdmissionFailures, logicalID)
	delete(r.relayAdmissionFailures, relay.Tx.TxID)
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
	db, store, err := plugins.StateStorage.Open(StateStorageInput{DataDir: node.DataDir, NodeID: node.NodeID, ShardID: node.ShardID})
	if err != nil {
		return nil, err
	}
	policy := mempool.DefaultPolicy()
	policy.Capacity = plugins.TxPool.Capacity()
	pool := plugins.TxPool.CreatePool(TxPoolInput{NodeID: node.NodeID, ShardID: node.ShardID, Policy: policy})
	r := &NodeRuntime{plan: plan, node: node, peers: peers, pool: pool, proposer: realblock.NewProposer(node.NodeID, node.ShardID), db: db, store: store, proposals: map[string]realblock.Block{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, pendingCommitErrors: map[uint64]string{}, committedHash: "genesis", lastProgressAt: time.Now().UnixMilli(), relaySource: map[string]Relay{}, crossEventSeen: map[string]bool{}, relayAdmissionFailures: map[string]string{}, runtimeMetricCounts: map[string]int64{}, stateFetchWaiters: map[string]chan StateFetchResponse{}, stateFetchWitnesses: map[string]StateFetchResponse{}, stateFetchSnapshots: map[string]map[string]string{}, stateApplyWaiters: map[string]chan StateDeltaApplyAck{}, pendingStateDeltaKeys: map[string]bool{}, appliedStateDeltaKeys: map[string]bool{}, pluginSnapshot: node.PluginProfile, plugins: plugins}
	r.transport = plugins.Network.CreateTransport(NetworkInput{NodeID: node.NodeID, ListenAddr: node.ListenAddr, Peers: peers, Handler: r.handle})
	r.transport.SetFaultPolicy(plugins.Fault.Policy(plan.FaultPlan))
	return r, nil
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

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
		if err := r.admitTransaction(item); err != nil {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: false, Error: "transaction_admission:" + err.Error()})
			return fmt.Errorf("transaction admission %s", err)
		}
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
		committed, err := r.reconcileRelayIfCommitted(ctx, relay)
		if err != nil {
			return fmt.Errorf("relay commit lookup: %w", err)
		}
		if committed {
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
		if err := r.admitTransaction(relay.Tx); err != nil {
			r.mu.Lock()
			r.relayAdmissionFailures[relay.Tx.TxID] = err.Error()
			r.mu.Unlock()
			r.recordEvent(relay.Tx.TxID, relay.SourceShard, relay.TargetShard, "RelayAdmissionFailed", false, err.Error())
			return fmt.Errorf("relay transaction admission %s", err)
		}
		result := r.pool.AdmitRelay(relay.Tx)
		if result.Accepted {
			committed, err = r.reconcileRelayIfCommitted(ctx, relay)
			if err != nil {
				r.pool.Remove(relay.Tx.TxID)
				return fmt.Errorf("relay post-admission commit lookup: %w", err)
			}
			if committed {
				return nil
			}
		}
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
		r.recordCrossShardEvent(r.crossShardPlugin().HandleFinalize(CrossShardFinalizeInput{TxID: finish.TxID, LogicalTxID: logicalID, SourceShard: finish.SourceShard, TargetShard: finish.TargetShard}))
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
	case stateFetchRequestMessage:
		request, err := p2p.DecodePayload[StateFetchRequest](msg)
		if err != nil {
			return err
		}
		return r.handleStateFetchRequest(ctx, msg.FromNode, request)
	case stateFetchResponseMessage:
		response, err := p2p.DecodePayload[StateFetchResponse](msg)
		if err != nil {
			return err
		}
		r.handleStateFetchResponse(response)
	case stateDeltaApplyMessage:
		request, err := p2p.DecodePayload[StateDeltaApplyRequest](msg)
		if err != nil {
			return err
		}
		return r.handleStateDeltaApply(ctx, msg.FromNode, request)
	case stateDeltaApplyAckMessage:
		ack, err := p2p.DecodePayload[StateDeltaApplyAck](msg)
		if err != nil {
			return err
		}
		r.handleStateDeltaApplyAck(ack)
	}
	return nil
}

func (r *NodeRuntime) admitTransaction(item tx.SignedTransaction) error {
	if r.plugins.Admission == nil {
		return fmt.Errorf("transaction admission plugin is not configured")
	}
	return r.plugins.Admission.Admit(item)
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
	nextHeight := r.proposer.NextHeight
	readySystemDeltas := r.readyRemoteStateDeltasForConsensus(nextHeight)
	input := BlockProductionInput{Pool: r.pool, Proposer: r.proposer, Limit: r.blockSize(), Now: time.Now(), SystemDeltaReady: len(readySystemDeltas) > 0}
	if !r.plugins.BlockProducer.ShouldProduce(input) {
		return
	}
	block, err := r.plugins.BlockProducer.BuildCandidate(input)
	if err != nil && err.Error() == "empty_mempool" && len(readySystemDeltas) > 0 {
		block = r.buildSystemDeltaDrainBlock(input.Now, readySystemDeltas)
		err = nil
	}
	if err != nil {
		r.mu.Lock()
		if err.Error() != "empty_mempool" {
			r.lastProposalError = err.Error()
		}
		r.mu.Unlock()
		return
	}
	block = r.scheduleBlock(block)
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

func (r *NodeRuntime) buildSystemDeltaDrainBlock(now time.Time, ready []realblock.SystemStateDelta) realblock.Block {
	if now.IsZero() {
		now = time.Now()
	}
	block := realblock.Block{
		ShardID:            r.node.ShardID,
		Height:             r.proposer.NextHeight,
		PreviousHash:       r.proposer.PreviousHash,
		ProposerID:         r.node.NodeID,
		Timestamp:          now.UnixMilli(),
		TxIDs:              []string{},
		TxList:             []tx.SignedTransaction{},
		StateRootBefore:    "empty",
		StateRootAfter:     "pending_not_executed",
		ReceiptRoot:        "pending_not_executed",
		StateCommit:        false,
		CrossShardProtocol: false,
		SystemStateDeltas:  append([]realblock.SystemStateDelta(nil), ready...),
	}
	realblock.AssignHash(&block)
	return block
}

func (r *NodeRuntime) scheduleBlock(block realblock.Block) realblock.Block {
	if r.plugins.BlockExecutor != nil && r.plugins.BlockExecutor.ID() == metaTrackBlockExecutorID {
		r.recordPlannedDependencyPlan(block)
		block.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(block.Height)
		realblock.AssignHash(&block)
		return block
	}
	schedule := r.plugins.Scheduler.Schedule(block.TxList, r.plugins.Execution)
	ordered := schedule.Ordered
	if len(ordered) != len(block.TxList) {
		return block
	}
	r.recordScheduleEvents(block, schedule.Events, true)
	block.TxList = ordered
	block.TxIDs = make([]string, 0, len(ordered))
	for _, item := range ordered {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	block.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(block.Height)
	realblock.AssignHash(&block)
	return block
}

func (r *NodeRuntime) recordPlannedDependencyPlan(block realblock.Block) {
	classification := batchClassification(block.TxList, r.plugins.Execution)
	events := make([]ScheduleEvent, 0, len(block.TxList))
	for _, item := range block.TxList {
		decision := decisionForTx(item, classification.Decisions, r.plugins.Execution)
		txID := txIdentifier(item)
		blocked := len(classification.Dependencies[txID]) > 0
		queue := "planned_ready"
		reason := "planned_ready:" + decision.Reason
		if blocked {
			queue = "planned_blocked"
			reason = "planned_blocked:" + strings.Join(classification.Dependencies[txID], "|")
		}
		events = append(events, ScheduleEvent{TxID: txID, Track: decision.Track, QueueName: queue, DecisionReason: reason, LocalExecution: true, Blocked: blocked})
	}
	r.recordScheduleEvents(block, events, true)
}

func (r *NodeRuntime) recordScheduleEvents(block realblock.Block, events []ScheduleEvent, planned bool) {
	if len(events) == 0 {
		return
	}
	schedulerPlugin := ""
	if plugin, ok := r.pluginSnapshot["scheduler"]; ok {
		schedulerPlugin = plugin.PluginID
	}
	rows := make([][]string, 0, len(events))
	now := time.Now().UnixMilli()
	txByID := map[string]tx.SignedTransaction{}
	for _, item := range block.TxList {
		txByID[item.TxID] = item
	}
	for index, event := range events {
		if planned {
			if event.Blocked {
				event.QueueName = "planned_blocked"
			} else {
				event.QueueName = "planned_ready"
			}
			if !strings.HasPrefix(event.DecisionReason, "planned_") {
				event.DecisionReason = "planned:" + event.DecisionReason
			}
			event.Wakeup = false
		}
		if item, ok := txByID[event.TxID]; ok && r.executesRemoteHomeState(item) {
			event.LocalExecution = false
			if event.DecisionReason != "" {
				event.DecisionReason += ";"
			}
			event.DecisionReason += "remote_home_access"
		}
		timestamp := now + int64(index)
		rows = append(rows, []string{
			fmt.Sprint(timestamp),
			r.node.NodeID,
			r.node.ShardID,
			fmt.Sprint(block.Height),
			schedulerPlugin,
			event.TxID,
			event.Track,
			event.QueueName,
			event.DecisionReason,
			fmt.Sprint(event.LocalExecution),
			fmt.Sprint(event.StolenWork),
			fmt.Sprint(event.Blocked),
			fmt.Sprint(event.Wakeup),
			fmt.Sprint(event.ReadyQueueDepth),
			fmt.Sprint(event.FastQueueDepth),
			fmt.Sprint(event.ConservativeQueueDepth),
			fmt.Sprint(event.DependencyWaitMS),
			fmt.Sprint(event.SchedulerIdleMS),
		})
	}
	r.mu.Lock()
	r.schedulerRows = append(r.schedulerRows, rows...)
	r.mu.Unlock()
	if planned {
		return
	}
	for _, event := range events {
		if event.Blocked {
			r.emitRuntimeEvent(RuntimeEvent{Type: "TxBlocked", TxID: event.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"track": event.Track, "queue": event.QueueName, "reason": event.DecisionReason}})
		}
		if event.Wakeup {
			r.emitRuntimeEvent(RuntimeEvent{Type: "TxWoken", TxID: event.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"track": event.Track, "queue": event.QueueName, "reason": event.DecisionReason}})
		}
	}
}

func (r *NodeRuntime) executesRemoteHomeState(item tx.SignedTransaction) bool {
	if !r.hasBatchRoutingControlPlane() {
		return false
	}
	shards := r.shardIDs()
	if len(shards) < 2 {
		return false
	}
	for _, access := range item.AccessList {
		if access.Key == "" {
			continue
		}
		homeShard := r.homeShardFor([]string{access.Key}, shards)
		if homeShard != r.node.ShardID {
			return true
		}
	}
	return false
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
	stateBefore := r.plugins.StateStorage.Snapshot(r.db)
	stateCheckpoint, err := r.plugins.StateStorage.Checkpoint(r.db)
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
	remoteDeltas := remoteStateDeltasFromBlock(block, r.node.ShardID)
	executionSnapshot, err := applyStateDeltaToSnapshot(stateBefore, remoteDeltas, r.node.ShardID, block.Height)
	if err != nil {
		r.setCommitPhase("remote_state_cas_rejected", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	r.setCommitPhase("execute_block", block)
	executeStarted := time.Now()
	r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionStarted", BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"tx_count": len(block.TxList)}})
	executionSnapshot, err = r.prepareMetaTrackStateSnapshot(ctx, block, executionSnapshot)
	if err != nil {
		r.setCommitPhase("state_access_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	executed, err := r.plugins.BlockExecutor.ExecuteBlock(ctx, BlockExecutionInput{Block: block, BaseStateSnapshot: executionSnapshot, NodeID: r.node.NodeID, ShardID: r.node.ShardID, WorkerCount: blockExecutorWorkerCountFromProfile(r.pluginSnapshot), Execution: r.plugins.Execution, Scheduler: r.plugins.Scheduler})
	if err != nil {
		r.setCommitPhase("execute_block_error", block)
		r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionFinished", BlockHash: block.BlockHash, Height: block.Height, Success: false, Error: err.Error()})
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	result := executed.ExecutionResult
	receiptSuccessByTxID := make(map[string]bool, len(result.Receipts))
	receiptErrorByTxID := make(map[string]string, len(result.Receipts))
	for _, receipt := range result.Receipts {
		if receipt.TxID == "" {
			r.setCommitPhase("invalid_execution_receipts", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("execution receipt missing transaction id at block height %d", block.Height)
		}
		if _, exists := receiptSuccessByTxID[receipt.TxID]; exists {
			r.setCommitPhase("invalid_execution_receipts", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("duplicate execution receipt for transaction %s at block height %d", receipt.TxID, block.Height)
		}
		receiptSuccessByTxID[receipt.TxID] = receipt.Success
		receiptErrorByTxID[receipt.TxID] = receipt.Error
	}
	if len(receiptSuccessByTxID) != len(block.TxList) {
		r.setCommitPhase("invalid_execution_receipts", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("execution receipt count mismatch at block height %d: receipts=%d transactions=%d", block.Height, len(receiptSuccessByTxID), len(block.TxList))
	}
	blockTxSeen := make(map[string]bool, len(block.TxList))
	for _, item := range block.TxList {
		if item.TxID == "" {
			r.setCommitPhase("invalid_execution_receipts", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("block transaction missing id at block height %d", block.Height)
		}
		if blockTxSeen[item.TxID] {
			r.setCommitPhase("invalid_execution_receipts", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("duplicate transaction id %s in block height %d", item.TxID, block.Height)
		}
		blockTxSeen[item.TxID] = true
		if _, ok := receiptSuccessByTxID[item.TxID]; !ok {
			r.setCommitPhase("invalid_execution_receipts", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, fmt.Errorf("missing execution receipt for transaction %s at block height %d", item.TxID, block.Height)
		}
	}

	executed.BlockExecutionMS = time.Since(executeStarted).Milliseconds()
	executed.TransactionExecutionMS = executed.BlockExecutionMS
	r.recordScheduleEvents(block, executed.ScheduleEvents, false)
	r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionFinished", BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"block_execution_ms": executed.BlockExecutionMS}})
	r.setCommitPhase("build_commit_plan", block)
	commitDecision := r.plugins.Commit.DecideCommit(CommitInput{ShardID: r.node.ShardID, Height: block.Height, Transactions: block.TxList, TxDeltas: executed.ExecutionResult.TxDeltas, StateDelta: executed.StateDelta})
	physicalDelta := commitDecision.PhysicalStateDelta
	if len(physicalDelta) == 0 {
		physicalDelta = executed.StateDelta
	}
	physicalDelta = annotateStateDeltaTxIDs(physicalDelta, executed.ExecutionResult.TxDeltas, block.TxList)
	physicalDelta, err = r.applyMetaTrackRemoteDeltas(ctx, block, physicalDelta, executed.ExecutionResult.TxDeltas)
	if err != nil {
		r.setCommitPhase("state_delta_apply_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	if len(remoteDeltas) > 0 {
		physicalDelta = append(append([]state.StateKV(nil), remoteDeltas...), physicalDelta...)
	}
	r.setCommitPhase("apply_state_delta", block)
	applyStarted := time.Now()
	stateRootBeforeWAL := r.plugins.StateStorage.Root(r.db)
	if err := r.plugins.StateStorage.ApplyBatch(r.db, physicalDelta); err != nil {
		r.setCommitPhase("state_apply_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	executed.DeterministicApplyMS = time.Since(applyStarted).Milliseconds()
	stateRootAfterWAL := r.plugins.StateStorage.Root(r.db)
	r.setCommitPhase("state_wal_append", block)
	persistenceOpts := persistenceOptionsFromProfile(r.pluginSnapshot)
	stateMetrics, err := r.plugins.StateStorage.PersistDelta(r.db, state.DeltaMetadata{BlockHeight: block.Height, BlockHash: block.BlockHash, ParentHash: block.PreviousHash, DeltaID: stableTextDigest(strings.Join([]string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height)}, "|")), StateRootBefore: stateRootBeforeWAL, StateRootAfter: stateRootAfterWAL}, physicalDelta, persistenceOpts)
	if err != nil {
		r.setCommitPhase("state_wal_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	r.setCommitPhase("durable_commit", block)
	storeMetrics, err := r.store.DurableCommitWithMetrics(block, result)
	if err != nil {
		r.setCommitPhase("durable_commit_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	r.setCommitPhase("state_snapshot_if_due", block)
	snapshotMetrics, err := r.plugins.StateStorage.SnapshotIfDue(r.db, block.Height, persistenceOpts)
	if err != nil {
		r.setCommitPhase("state_snapshot_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	stateMetrics.SnapshotWriteMS = snapshotMetrics.SnapshotWriteMS
	stateMetrics.SnapshotCount = snapshotMetrics.SnapshotCount
	stateMetrics.WrittenBytes += snapshotMetrics.WrittenBytes
	executed.PersistenceMetrics = map[string]any{
		"checkpoint_read_ms":          stateMetrics.CheckpointReadMS,
		"wal_append_ms":               stateMetrics.WALAppendMS,
		"wal_sync_ms":                 stateMetrics.WALSyncMS,
		"snapshot_write_ms":           stateMetrics.SnapshotWriteMS,
		"receipt_batch_write_ms":      storeMetrics.ReceiptBatchWriteMS,
		"tx_index_batch_write_ms":     storeMetrics.TxIndexBatchWriteMS,
		"durable_commit_ms":           storeMetrics.DurableCommitMS,
		"persistence_written_bytes":   stateMetrics.WrittenBytes + storeMetrics.WrittenBytes,
		"wal_record_count":            stateMetrics.WALRecordCount,
		"snapshot_count":              stateMetrics.SnapshotCount,
		"durability_mode":             stateMetrics.DurabilityMode,
		"fsync_cadence":               stateMetrics.FSyncCadence,
		"state_root_before_wal":       stateRootBeforeWAL,
		"state_root_after_wal":        stateRootAfterWAL,
		"per_block_full_snapshot":     stateMetrics.SnapshotCount > 0,
		"receipt_tx_index_batch_mode": true,
	}
	r.markRemoteStateDeltasApplied(block.SystemStateDeltas)
	r.setCommitPhase("record_execution_artifacts", block)
	r.recordBlockExecutionResult(block, executed)
	r.recordExecutionAndCommitDecisions(block, commitDecision, physicalDelta)
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
			if receiptSuccessByTxID[item.TxID] {
				r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "durable_committed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
				r.onCommittedTx(ctx, item, relayItems[item.TxID])
			} else {
				executionError := strings.TrimSpace(receiptErrorByTxID[item.TxID])
				if executionError == "" {
					executionError = "execution_failed"
				}
				r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: false, Error: executionError})
			}
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
	stateErr := r.plugins.StateStorage.Rollback(r.db, stateCheckpoint)
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

func persistenceOptionsFromProfile(profile map[string]PluginConfig) state.PersistenceOptions {
	opts := state.DefaultPersistenceOptions()
	if item, ok := profile["state_storage"]; ok {
		if mode := strings.TrimSpace(fmt.Sprint(item.Config["durability_mode"])); mode == "strict" || mode == "batched" {
			opts.DurabilityMode = mode
		}
		if value := intValue(item.Config["fsync_cadence"]); value > 0 {
			opts.FSyncCadence = value
		}
		if value := intValue(item.Config["snapshot_cadence_blocks"]); value > 0 {
			opts.SnapshotCadence = value
		}
	}
	if mode := strings.TrimSpace(os.Getenv("MBE_DURABILITY_MODE")); mode == "strict" || mode == "batched" {
		opts.DurabilityMode = mode
	}
	if value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MBE_FSYNC_CADENCE_BLOCKS"))); err == nil && value > 0 {
		opts.FSyncCadence = value
	}
	if value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MBE_SNAPSHOT_CADENCE_BLOCKS"))); err == nil && value > 0 {
		opts.SnapshotCadence = value
	}
	if opts.DurabilityMode == "strict" {
		opts.FSyncCadence = 1
	}
	return opts
}

func (r *NodeRuntime) homeShardFor(keys, shards []string) string {
	return shardFor(r.plugins.Sharding, keys, shards)
}

func (r *NodeRuntime) prepareMetaTrackStateSnapshot(ctx context.Context, block realblock.Block, stateBefore map[string]string) (map[string]string, error) {
	if !r.hasBatchRoutingControlPlane() {
		return stateBefore, nil
	}
	shardIDs := r.shardIDs()
	if len(shardIDs) < 2 {
		return stateBefore, nil
	}
	next := map[string]string{}
	for key, value := range stateBefore {
		next[key] = value
	}
	seen := map[string]bool{}
	for _, item := range block.TxList {
		for _, access := range item.AccessList {
			if access.Key == "" || strings.Contains(access.Key, "::") {
				continue
			}
			homeShard := r.homeShardFor([]string{access.Key}, shardIDs)
			if homeShard == "" || homeShard == r.node.ShardID {
				continue
			}
			rowKey := item.TxID + "|" + access.Key + "|" + homeShard
			if seen[rowKey] {
				continue
			}
			seen[rowKey] = true
			response, latency, err := r.fetchRemoteState(ctx, block, item, access, homeShard)
			if err != nil {
				return nil, err
			}
			localQualifiedKey := r.node.ShardID + "::" + access.Key
			next[localQualifiedKey] = response.Value
			r.recordRemoteStateAccess(block, item, access, response, latency)
		}
	}
	return next, nil
}

func (r *NodeRuntime) fetchRemoteState(ctx context.Context, block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, homeShard string) (StateFetchResponse, time.Duration, error) {
	targetNode := r.leaderID(homeShard)
	if targetNode == "" {
		return StateFetchResponse{}, 0, fmt.Errorf("metatrack remote state home leader missing for %s", homeShard)
	}
	requestID := stableTextDigest(strings.Join([]string{r.node.NodeID, item.TxID, block.BlockHash, access.Key, homeShard, r.node.ShardID}, "|"))
	waiter := make(chan StateFetchResponse, 1)
	r.mu.Lock()
	if r.stateFetchWaiters == nil {
		r.stateFetchWaiters = map[string]chan StateFetchResponse{}
	}
	r.stateFetchWaiters[requestID] = waiter
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.stateFetchWaiters, requestID)
		r.mu.Unlock()
	}()
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: item.TxID, BlockHash: block.BlockHash, Key: access.Key, HomeShard: homeShard, ExecutionShard: r.node.ShardID, AccessKind: string(access.Mode)})
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
	if err != nil {
		return StateFetchResponse{}, 0, err
	}
	start := time.Now()
	if err := r.sendToNode(ctx, targetNode, envelope); err != nil {
		return StateFetchResponse{}, time.Since(start), err
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case response := <-waiter:
		if !response.Success {
			return response, time.Since(start), fmt.Errorf("metatrack remote state fetch failed: %s", response.Error)
		}
		return response, time.Since(start), nil
	case <-timer.C:
		return StateFetchResponse{}, time.Since(start), fmt.Errorf("metatrack remote state fetch timed out for %s from %s", access.Key, homeShard)
	case <-ctx.Done():
		return StateFetchResponse{}, time.Since(start), ctx.Err()
	}
}

func (r *NodeRuntime) handleStateFetchRequest(ctx context.Context, requester string, request StateFetchRequest) error {
	qualifiedKey := request.HomeShard + "::" + request.Key
	cacheKey := stateFetchWitnessKey(request)
	r.mu.Lock()
	cached, ok := r.stateFetchWitnesses[cacheKey]
	r.mu.Unlock()
	response := cached
	if !ok {
		snapshot := r.stateFetchSnapshot(request)
		value := snapshot[qualifiedKey]
		response = StateFetchResponse{TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, Value: value, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: state.RootOfSnapshot(snapshot), Success: true}
		r.mu.Lock()
		if r.stateFetchWitnesses == nil {
			r.stateFetchWitnesses = map[string]StateFetchResponse{}
		}
		r.stateFetchWitnesses[cacheKey] = response
		r.mu.Unlock()
	}
	response.RequestID = request.RequestID
	response.TxID = request.TxID
	response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
	envelope, err := p2p.NewEnvelope(stateFetchResponseMessage, r.node.NodeID, requester, r.node.ShardID, 0, 0, 0, response)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, requester, envelope)
}

func stateFetchWitnessKey(request StateFetchRequest) string {
	return stableTextDigest(strings.Join([]string{request.BlockHash, request.HomeShard, request.ExecutionShard, request.Key, request.AccessKind}, "|"))
}

func stateFetchSnapshotKey(request StateFetchRequest) string {
	return stableTextDigest(strings.Join([]string{request.BlockHash, request.HomeShard, request.ExecutionShard}, "|"))
}

func stateFetchWitnessDigest(response StateFetchResponse, accessKind string) string {
	return stableTextDigest(strings.Join([]string{response.BlockHash, response.QualifiedKey, response.Value, response.StateRoot, response.HomeShard, response.ExecutionShard, accessKind}, "|"))
}

func (r *NodeRuntime) stateFetchSnapshot(request StateFetchRequest) map[string]string {
	snapshotKey := stateFetchSnapshotKey(request)
	r.mu.Lock()
	snapshot := r.stateFetchSnapshots[snapshotKey]
	r.mu.Unlock()
	if snapshot != nil {
		return copyStringMap(snapshot)
	}
	fresh := r.plugins.StateStorage.Snapshot(r.db)
	r.mu.Lock()
	if r.stateFetchSnapshots == nil {
		r.stateFetchSnapshots = map[string]map[string]string{}
	}
	if snapshot = r.stateFetchSnapshots[snapshotKey]; snapshot == nil {
		snapshot = copyStringMap(fresh)
		r.stateFetchSnapshots[snapshotKey] = snapshot
	}
	r.mu.Unlock()
	return copyStringMap(snapshot)
}

func copyStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func (r *NodeRuntime) handleStateFetchResponse(response StateFetchResponse) {
	r.mu.Lock()
	waiter := r.stateFetchWaiters[response.RequestID]
	r.mu.Unlock()
	if waiter == nil {
		return
	}
	select {
	case waiter <- response:
	default:
	}
}

func (r *NodeRuntime) applyMetaTrackRemoteDeltas(ctx context.Context, block realblock.Block, physicalDelta []state.StateKV, txDeltaGroups ...[]execution.TxDelta) ([]state.StateKV, error) {
	if !r.hasBatchRoutingControlPlane() {
		return physicalDelta, nil
	}
	shardIDs := r.shardIDs()
	if len(shardIDs) < 2 {
		return physicalDelta, nil
	}
	local := make([]state.StateKV, 0, len(physicalDelta))
	var txDeltas []execution.TxDelta
	if len(txDeltaGroups) > 0 {
		txDeltas = txDeltaGroups[0]
	}
	for _, item := range physicalDelta {
		unqualified, ok := unqualifiedLocalKey(item.Key, r.node.ShardID)
		if !ok {
			local = append(local, item)
			continue
		}
		homeShard := r.homeShardFor([]string{unqualified}, shardIDs)
		if homeShard == "" || homeShard == r.node.ShardID {
			local = append(local, item)
			continue
		}
		if !r.node.Leader {
			continue
		}
		remoteItem := metaTrackRemoteWritebackItem(item, unqualified, block.TxList, txDeltas)
		acks, latency, err := r.applyRemoteStateDelta(ctx, block, remoteItem, unqualified, homeShard, txDeltas)
		if err != nil {
			return nil, err
		}
		for _, ack := range acks {
			r.recordRemoteStateApply(block, remoteItem, unqualified, ack, latency)
		}
	}
	return local, nil
}

func metaTrackRemoteWritebackItem(item state.StateKV, unqualifiedKey string, transactions []tx.SignedTransaction, txDeltas []execution.TxDelta) state.StateKV {
	next := item
	if !isStandardAccountStateKey(unqualifiedKey) {
		return item
	}
	if item.UpdateSemantics == "commutative_delta" || len(item.TxIDs) == 0 {
		return annotateMetaTrackRemoteAccountDelta(next, unqualifiedKey, transactions, txDeltas)
	}
	finalValue, err := strconv.ParseInt(item.Value, 10, 64)
	if err != nil {
		return item
	}
	base, ok := remoteWritebackBaseValue(unqualifiedKey, item.TxIDs, transactions, txDeltas)
	if !ok {
		return item
	}
	next.UpdateSemantics = "commutative_delta"
	next.Delta = finalValue - base
	return annotateMetaTrackRemoteAccountDelta(next, unqualifiedKey, transactions, txDeltas)
}

func isStandardAccountStateKey(key string) bool {
	return strings.HasPrefix(key, "balance:") || strings.HasPrefix(key, "nonce:")
}

func annotateMetaTrackRemoteAccountDelta(item state.StateKV, unqualifiedKey string, transactions []tx.SignedTransaction, txDeltas []execution.TxDelta) state.StateKV {
	if item.UpdateSemantics != "commutative_delta" || !isStandardAccountStateKey(unqualifiedKey) {
		return item
	}
	initial, ok := remoteAccountDeltaInitialValue(unqualifiedKey, item.TxIDs, transactions)
	if !ok {
		return item
	}
	item.ApplyOrigin = "metatrack_remote_state"
	if strings.HasPrefix(unqualifiedKey, "nonce:") {
		item.DeltaKind = "account_nonce_delta"
	} else {
		item.DeltaKind = "account_balance_delta"
	}
	item.HasInitialValue = true
	item.InitialValue = initial
	return item
}

func remoteWritebackBaseValue(key string, txIDs []string, transactions []tx.SignedTransaction, txDeltas []execution.TxDelta) (int64, bool) {
	bestValue, found := remoteWritebackBaseObservation(key, txIDs, txDeltas)
	if found && bestValue != "" {
		parsed, err := strconv.ParseInt(bestValue, 10, 64)
		return parsed, err == nil
	}
	return remoteAccountDeltaInitialValue(key, txIDs, transactions)
}

func remoteAccountDeltaInitialValue(key string, txIDs []string, transactions []tx.SignedTransaction) (int64, bool) {
	txByID := make(map[string]tx.SignedTransaction, len(transactions))
	for _, item := range transactions {
		txByID[item.TxID] = item
	}
	if strings.HasPrefix(key, "nonce:") {
		return 0, true
	}
	if strings.HasPrefix(key, "balance:") {
		account := strings.TrimPrefix(key, "balance:")
		for _, txID := range txIDs {
			item, ok := txByID[txID]
			if ok && item.Sender == account {
				return 1_000_000, true
			}
			if ok && item.Receiver == account {
				return 0, true
			}
		}
		return 0, false
	}
	return 0, false
}

func remoteWritebackBaseObservation(key string, txIDs []string, txDeltas []execution.TxDelta) (string, bool) {
	contributor := map[string]bool{}
	for _, txID := range txIDs {
		contributor[txID] = true
	}
	bestIndex := int(^uint(0) >> 1)
	bestValue := ""
	found := false
	for _, delta := range txDeltas {
		if !contributor[delta.TxID] || !delta.Success {
			continue
		}
		if delta.OriginalIndex > bestIndex {
			continue
		}
		for _, read := range delta.ReadSet {
			if !stateKeysReferToSameLogicalKey(read.Key, key) {
				continue
			}
			bestIndex = delta.OriginalIndex
			bestValue = read.Value
			found = true
			break
		}
	}
	return bestValue, found
}

func annotateStateDeltaTxIDs(physicalDelta []state.StateKV, txDeltas []execution.TxDelta, transactions []tx.SignedTransaction) []state.StateKV {
	txByID := map[string]tx.SignedTransaction{}
	for _, item := range transactions {
		if item.TxID != "" {
			txByID[item.TxID] = item
		}
	}
	deltaByID := map[string]execution.TxDelta{}
	for _, delta := range txDeltas {
		if delta.TxID != "" {
			deltaByID[delta.TxID] = delta
		}
	}
	out := make([]state.StateKV, 0, len(physicalDelta))
	for _, item := range physicalDelta {
		txIDs := append([]string(nil), item.TxIDs...)
		seen := map[string]bool{}
		for _, txID := range txIDs {
			seen[txID] = true
		}
		for _, delta := range txDeltas {
			if delta.TxID == "" || seen[delta.TxID] || !writeSetContainsStateKey(delta.WriteSet, item.Key) {
				continue
			}
			txIDs = append(txIDs, delta.TxID)
			seen[delta.TxID] = true
		}
		next := item
		next.TxIDs = txIDs
		if semantics, delta, ok := commutativeDeltaSemanticsFor(item.Key, txIDs, txByID, deltaByID); ok {
			next.UpdateSemantics = semantics
			next.Delta = delta
		}
		out = append(out, next)
	}
	return out
}

func commutativeDeltaSemanticsFor(
	stateKey string,
	txIDs []string,
	txByID map[string]tx.SignedTransaction,
	deltaByID map[string]execution.TxDelta,
) (string, int64, bool) {
	if len(txIDs) == 0 {
		return "", 0, false
	}

	total := int64(0)
	matched := 0

	for _, txID := range txIDs {
		item, ok := txByID[txID]
		if !ok {
			return "", 0, false
		}

		delta, ok := deltaByID[txID]
		if !ok || !delta.Success {
			return "", 0, false
		}

		found := false

		for _, access := range item.AccessList {
			if access.Mode != tx.AccessCommutativeDelta ||
				!stateKeysReferToSameLogicalKey(
					stateKey,
					access.Key,
				) {

				continue
			}

			total += access.Delta
			found = true
		}

		if !found &&
			item.Sender != item.Receiver &&
			item.Value > 0 &&
			stateKeysReferToSameLogicalKey(
				stateKey,
				"balance:"+item.Receiver,
			) {

			total += item.Value
			found = true
		}

		if !found {
			return "", 0, false
		}

		matched++
	}

	return "commutative_delta", total, matched > 0
}

func stateKeysReferToSameLogicalKey(stateKey, logicalKey string) bool {
	if stateKey == logicalKey {
		return true
	}
	if index := strings.Index(stateKey, "::"); index >= 0 && index+2 < len(stateKey) {
		return stateKey[index+2:] == logicalKey
	}
	return false
}

func stateValueDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func writeSetContainsStateKey(writeSet map[string]string, stateKey string) bool {
	if _, ok := writeSet[stateKey]; ok {
		return true
	}
	if index := strings.Index(stateKey, "::"); index >= 0 && index+2 < len(stateKey) {
		_, ok := writeSet[stateKey[index+2:]]
		return ok
	}
	return false
}

func (r *NodeRuntime) applyRemoteStateDelta(ctx context.Context, block realblock.Block, item state.StateKV, unqualifiedKey, homeShard string, txDeltas []execution.TxDelta) ([]StateDeltaApplyAck, time.Duration, error) {
	targetNodes := r.nodeIDsForShard(homeShard)
	if len(targetNodes) == 0 {
		return nil, 0, fmt.Errorf("metatrack remote state apply home nodes missing for %s", homeShard)
	}
	joinedTxIDs := strings.Join(item.TxIDs, "|")
	baseValue := item.BaseValue
	baseValueDigest := item.BaseValueDigest
	if item.UpdateSemantics != "commutative_delta" && baseValueDigest == "" {
		if observed, ok := remoteWritebackBaseObservation(unqualifiedKey, item.TxIDs, txDeltas); ok {
			baseValue = observed
			baseValueDigest = stateValueDigest(observed)
		}
	}
	start := time.Now()
	acks := make([]StateDeltaApplyAck, 0, len(targetNodes))
	for _, targetNode := range targetNodes {
		requestID := stableTextDigest(strings.Join([]string{r.node.NodeID, targetNode, block.BlockHash, joinedTxIDs, item.Key, unqualifiedKey, item.Value, item.UpdateSemantics, fmt.Sprint(item.Delta), baseValueDigest, homeShard, r.node.ShardID}, "|"))
		waiter := make(chan StateDeltaApplyAck, 1)
		r.mu.Lock()
		if r.stateApplyWaiters == nil {
			r.stateApplyWaiters = map[string]chan StateDeltaApplyAck{}
		}
		r.stateApplyWaiters[requestID] = waiter
		r.mu.Unlock()
		request := r.plugins.StateAccess.BuildDeltaApplyRequest(StateDeltaApplyInput{RequestID: requestID, TxID: joinedTxIDs, TxIDs: append([]string(nil), item.TxIDs...), BlockHash: block.BlockHash, Key: unqualifiedKey, Value: item.Value, UpdateSemantics: item.UpdateSemantics, Delta: item.Delta, BaseValue: baseValue, BaseValueDigest: baseValueDigest, ApplyOrigin: item.ApplyOrigin, DeltaKind: item.DeltaKind, HasInitialValue: item.HasInitialValue, InitialValue: item.InitialValue, HomeShard: homeShard, ExecutionShard: r.node.ShardID, SourceKey: item.Key, SourceHeight: block.Height})
		envelope, err := p2p.NewEnvelope(stateDeltaApplyMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
		if err != nil {
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			return nil, time.Since(start), err
		}
		if err := r.sendToNode(ctx, targetNode, envelope); err != nil {
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			return nil, time.Since(start), err
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case ack := <-waiter:
			timer.Stop()
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			if !ack.Success {
				return acks, time.Since(start), fmt.Errorf("metatrack remote state apply failed on %s: %s", targetNode, ack.Error)
			}
			acks = append(acks, ack)
		case <-timer.C:
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			return acks, time.Since(start), fmt.Errorf("metatrack remote state apply timed out for %s to %s/%s", unqualifiedKey, homeShard, targetNode)
		case <-ctx.Done():
			timer.Stop()
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			return acks, time.Since(start), ctx.Err()
		}
	}
	return acks, time.Since(start), nil
}

func (r *NodeRuntime) handleStateDeltaApply(ctx context.Context, requester string, request StateDeltaApplyRequest) error {
	ack := r.handleStateDeltaApplyRequest(request)
	envelope, err := p2p.NewEnvelope(stateDeltaApplyAckMessage, r.node.NodeID, requester, r.node.ShardID, 0, 0, 0, ack)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, requester, envelope)
}

func (r *NodeRuntime) handleStateDeltaApplyRequest(request StateDeltaApplyRequest) StateDeltaApplyAck {
	qualifiedKey := request.HomeShard + "::" + request.Key
	ack := stateDeltaApplyAckFromRequest(request, qualifiedKey, stableTextDigest("queued:"+request.Value), r.plugins.StateStorage.Root(r.db))
	if request.HomeShard != "" && request.HomeShard != r.node.ShardID {
		ack.Success = false
		ack.Error = "wrong_home_shard"
		ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
		return ack
	}
	key := stateDeltaApplyKey(request)
	r.mu.Lock()
	if r.pendingStateDeltaKeys == nil {
		r.pendingStateDeltaKeys = map[string]bool{}
	}
	if r.appliedStateDeltaKeys == nil {
		r.appliedStateDeltaKeys = map[string]bool{}
	}
	if !r.pendingStateDeltaKeys[key] && !r.appliedStateDeltaKeys[key] {
		r.pendingStateDeltaKeys[key] = true
		r.pendingStateDeltas = append(r.pendingStateDeltas, request)
	}
	r.mu.Unlock()
	ack.WitnessDigest = stateDeltaApplyWitnessDigest(ack)
	return ack
}

func (r *NodeRuntime) applyQueuedStateDeltas() {
	// Remote state deltas are intentionally applied only through the home
	// shard's consensus-bound commit path. This helper is kept as a no-op for
	// older call sites/tests that used to flush out-of-band state mutations.
}

func (r *NodeRuntime) flushQueuedStateDeltas() {
	// Artifact writes must not turn remote acknowledgements into state commits.
	// Pending deltas stay pending until a home-shard block commits them.
}

func (r *NodeRuntime) readyRemoteStateDeltasForConsensus(homeBlockHeight uint64) []realblock.SystemStateDelta {
	r.mu.Lock()
	defer r.mu.Unlock()
	ready := make([]StateDeltaApplyRequest, 0, len(r.pendingStateDeltas))
	for _, request := range r.pendingStateDeltas {
		if remoteStateDeltaReadyForHomeBlock(request, homeBlockHeight) {
			ready = append(ready, request)
		}
	}
	sort.SliceStable(ready, func(i, j int) bool {
		return remoteDeltaConsensusOrder(ready[i]) < remoteDeltaConsensusOrder(ready[j])
	})
	out := make([]realblock.SystemStateDelta, 0, len(ready))
	for _, request := range ready {
		out = append(out, systemStateDeltaFromRequest(request))
	}
	return out
}

func (r *NodeRuntime) markRemoteStateDeltasApplied(applied []realblock.SystemStateDelta) {
	if len(applied) == 0 {
		return
	}
	appliedKeys := map[string]bool{}
	for _, item := range applied {
		if item.DeltaID != "" {
			appliedKeys[item.DeltaID] = true
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.appliedStateDeltaKeys == nil {
		r.appliedStateDeltaKeys = map[string]bool{}
	}
	for key := range appliedKeys {
		r.appliedStateDeltaKeys[key] = true
		delete(r.pendingStateDeltaKeys, key)
	}
	pending := make([]StateDeltaApplyRequest, 0, len(r.pendingStateDeltas))
	for _, request := range r.pendingStateDeltas {
		key := stateDeltaApplyKey(request)
		if appliedKeys[key] {
			continue
		}
		pending = append(pending, request)
	}
	r.pendingStateDeltas = pending
}

func systemStateDeltaFromRequest(request StateDeltaApplyRequest) realblock.SystemStateDelta {
	return realblock.SystemStateDelta{
		DeltaID:         stateDeltaApplyKey(request),
		Key:             request.Key,
		Value:           request.Value,
		TxID:            request.TxID,
		TxIDs:           append([]string(nil), request.TxIDs...),
		UpdateSemantics: request.UpdateSemantics,
		Delta:           request.Delta,
		BaseValue:       request.BaseValue,
		BaseValueDigest: request.BaseValueDigest,
		ApplyOrigin:     request.ApplyOrigin,
		DeltaKind:       request.DeltaKind,
		HasInitialValue: request.HasInitialValue,
		InitialValue:    request.InitialValue,
		HomeShard:       request.HomeShard,
		ExecutionShard:  request.ExecutionShard,
		SourceKey:       request.SourceKey,
		SourceHeight:    request.SourceHeight,
		SourceBlockHash: request.BlockHash,
	}
}

func remoteStateDeltasFromBlock(block realblock.Block, homeShard string) []state.StateKV {
	out := make([]state.StateKV, 0, len(block.SystemStateDeltas))
	for _, item := range block.SystemStateDeltas {
		if item.HomeShard != "" && item.HomeShard != homeShard {
			continue
		}
		out = append(out, state.StateKV{
			Key:             item.Key,
			Value:           item.Value,
			TxIDs:           append([]string(nil), item.TxIDs...),
			UpdateSemantics: item.UpdateSemantics,
			Delta:           item.Delta,
			BaseValue:       item.BaseValue,
			BaseValueDigest: item.BaseValueDigest,
			ApplyOrigin:     item.ApplyOrigin,
			DeltaKind:       item.DeltaKind,
			HasInitialValue: item.HasInitialValue,
			InitialValue:    item.InitialValue,
			DeltaID:         item.DeltaID,
			BlockHeight:     block.Height,
		})
	}
	return out
}

func remoteStateDeltaReadyForHomeBlock(request StateDeltaApplyRequest, homeBlockHeight uint64) bool {
	return request.SourceHeight <= homeBlockHeight
}

func remoteDeltaConsensusOrder(request StateDeltaApplyRequest) string {
	return strings.Join([]string{
		fmt.Sprintf("%020d", request.SourceHeight),
		request.BlockHash,
		request.Key,
		fmt.Sprintf("%02d", remoteDeltaSemanticsRank(request.UpdateSemantics)),
		fmt.Sprintf("%020d", request.Delta),
		request.TxID,
		request.ExecutionShard,
		request.SourceKey,
	}, "|")
}

func remoteDeltaSemanticsRank(semantics string) int {
	if semantics == "commutative_delta" {
		return 1
	}
	return 0
}

func applyStateDeltaToSnapshot(snapshot map[string]string, updates []state.StateKV, namespace string, blockHeight uint64) (map[string]string, error) {
	out := make(map[string]string, len(snapshot)+len(updates))
	for key, value := range snapshot {
		out[key] = value
	}
	for _, item := range updates {
		key := item.Key
		if !strings.Contains(key, "::") {
			// The caller passes home-shard snapshots, so unqualified keys belong
			// to that home namespace.
			key = namespace + "::" + item.Key
		}
		if item.UpdateSemantics == "commutative_delta" {
			current, _ := strconv.ParseInt(out[key], 10, 64)
			if out[key] == "" && item.HasInitialValue {
				current = item.InitialValue
			}
			out[key] = strconv.FormatInt(current+item.Delta, 10)
			continue
		}
		if item.BaseValueDigest != "" {
			actual := stateValueDigest(out[key])
			if actual != item.BaseValueDigest {
				if item.BlockHeight == 0 {
					item.BlockHeight = blockHeight
				}
				return nil, state.CASMismatchError{Key: key, ExpectedDigest: item.BaseValueDigest, ActualDigest: actual, DeltaID: item.DeltaID, TxIDs: append([]string(nil), item.TxIDs...), BlockHeight: item.BlockHeight}
			}
		}
		out[key] = item.Value
	}
	return out, nil
}

func stateDeltaApplyAckFromRequest(request StateDeltaApplyRequest, qualifiedKey, valueDigest, stateRoot string) StateDeltaApplyAck {
	return StateDeltaApplyAck{RequestID: request.RequestID, TxID: request.TxID, TxIDs: append([]string(nil), request.TxIDs...), BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, ValueDigest: valueDigest, UpdateSemantics: request.UpdateSemantics, Delta: request.Delta, BaseValueDigest: request.BaseValueDigest, ApplyOrigin: request.ApplyOrigin, DeltaKind: request.DeltaKind, HasInitialValue: request.HasInitialValue, InitialValue: request.InitialValue, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: stateRoot, Success: true}
}

func stateDeltaApplyWitnessDigest(ack StateDeltaApplyAck) string {
	return stableTextDigest(strings.Join([]string{ack.BlockHash, ack.QualifiedKey, ack.ValueDigest, ack.StateRoot, ack.HomeShard, ack.ExecutionShard, ack.UpdateSemantics, fmt.Sprint(ack.Delta), ack.BaseValueDigest, ack.ApplyOrigin, ack.DeltaKind, fmt.Sprint(ack.HasInitialValue), fmt.Sprint(ack.InitialValue)}, "|"))
}

func stateDeltaApplyKey(request StateDeltaApplyRequest) string {
	return stableTextDigest(strings.Join([]string{fmt.Sprint(request.SourceHeight), request.BlockHash, request.SourceKey, request.Key, request.TxID, request.UpdateSemantics, fmt.Sprint(request.Delta), request.BaseValueDigest, request.ApplyOrigin, request.DeltaKind, fmt.Sprint(request.HasInitialValue), fmt.Sprint(request.InitialValue), request.HomeShard, request.ExecutionShard}, "|"))
}

func (r *NodeRuntime) handleStateDeltaApplyAck(ack StateDeltaApplyAck) {
	r.mu.Lock()
	waiter := r.stateApplyWaiters[ack.RequestID]
	r.mu.Unlock()
	if waiter == nil {
		return
	}
	select {
	case waiter <- ack:
	default:
	}
}

func (r *NodeRuntime) recordRemoteStateApply(block realblock.Block, item state.StateKV, unqualifiedKey string, ack StateDeltaApplyAck, latency time.Duration) {
	accessKind := "write_apply"
	if ack.UpdateSemantics != "" {
		accessKind += ":" + ack.UpdateSemantics
	}
	row := []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), block.BlockHash, ack.TxID, unqualifiedKey, ack.QualifiedKey, ack.HomeShard, ack.ExecutionShard, accessKind, fmt.Sprint(latency.Milliseconds()), ack.WitnessDigest, ack.StateRoot, fmt.Sprint(ack.Success), ack.Error}
	r.mu.Lock()
	r.remoteStateRows = append(r.remoteStateRows, row)
	r.mu.Unlock()
	r.emitRuntimeEvent(RuntimeEvent{Type: "RemoteStateApplied", TxID: ack.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: ack.Success, Error: ack.Error, Attributes: map[string]any{"key": unqualifiedKey, "home_shard": ack.HomeShard, "execution_shard": ack.ExecutionShard, "latency_ms": latency.Milliseconds(), "update_semantics": ack.UpdateSemantics}})
}

func (r *NodeRuntime) recordRemoteStateAccess(block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, response StateFetchResponse, latency time.Duration) {
	row := []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), block.BlockHash, item.TxID, access.Key, response.QualifiedKey, response.HomeShard, response.ExecutionShard, string(access.Mode), fmt.Sprint(latency.Milliseconds()), response.WitnessDigest, response.StateRoot, fmt.Sprint(response.Success), response.Error}
	r.mu.Lock()
	r.remoteStateRows = append(r.remoteStateRows, row)
	r.mu.Unlock()
	r.emitRuntimeEvent(RuntimeEvent{Type: "RemoteStateFetched", TxID: item.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: response.Success, Error: response.Error, Attributes: map[string]any{"key": access.Key, "home_shard": response.HomeShard, "execution_shard": response.ExecutionShard, "latency_ms": latency.Milliseconds(), "access_mode": string(access.Mode)}})
}

func (r *NodeRuntime) shardIDs() []string {
	seen := map[string]bool{}
	shards := []string{}
	for _, node := range r.plan.NodeConfigs {
		if node.ShardID == "" || seen[node.ShardID] {
			continue
		}
		seen[node.ShardID] = true
		shards = append(shards, node.ShardID)
	}
	sort.Strings(shards)
	return shards
}

func unqualifiedLocalKey(key, shardID string) (string, bool) {
	prefix := shardID + "::"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	next := strings.TrimPrefix(key, prefix)
	return next, next != ""
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
		crossShard := r.crossShardPlugin()
		r.recordCrossShardEvent(crossShard.TargetCommit(CrossShardFinalizeInput{TxID: item.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard}))
		finish := crossShard.BuildFinalize(CrossShardFinalizeInput{TxID: item.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard})
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
		r.recordCrossShardEvent(r.crossShardPlugin().TimeoutRefund(CrossShardFinalizeInput{TxID: item.TxID, LogicalTxID: item.TxID, SourceShard: r.node.ShardID}, "target_timeout"))
		return
	}
	if item.SourceKind == "cross_shard_relay" {
		return
	}
	crossShard := r.crossShardPlugin()
	if crossShard.IsCrossShard(item) {
		target := crossTargetShard(item.Payload)
		if target == "" || target == r.node.ShardID {
			return
		}
		relayInput := CrossShardRelayInput{Tx: item, LogicalTxID: item.TxID, SourceShard: r.node.ShardID, TargetShard: target}
		r.recordCrossShardEvent(crossShard.SourceLock(relayInput))
		relay := crossShard.BuildRelay(relayInput)
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

func (r *NodeRuntime) nodeIDsForShard(shard string) []string {
	out := []string{}
	for _, item := range r.plan.NodeConfigs {
		if item.ShardID == shard {
			out = append(out, item.NodeID)
		}
	}
	return out
}

func (r *NodeRuntime) sendToNode(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
	return r.transport.Send(ctx, nodeID, envelope)
}
func (r *NodeRuntime) blockSize() int {
	return r.plugins.BlockProducer.BlockSize()
}
func (r *NodeRuntime) blockInterval() time.Duration {
	interval := r.plugins.BlockProducer.Interval()
	if interval < 25*time.Millisecond {
		return 25 * time.Millisecond
	}
	return interval
}
func (r *NodeRuntime) proposalTimeout() time.Duration {
	blockSize := r.blockSize()
	if blockSize <= 0 {
		blockSize = 1
	}
	interval := r.blockInterval()
	timeout := 5*time.Second + time.Duration(blockSize)*100*time.Millisecond + 4*interval
	if consensusTimeout := r.plugins.Consensus.Timeout(); consensusTimeout > timeout {
		timeout = consensusTimeout
	}
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
	lifecycle := LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: txID, LogicalTxID: logicalID, Stage: strings.ToLower(stage), NodeID: r.node.NodeID, ShardID: r.node.ShardID, SourceShard: source, TargetShard: target, Success: success, Error: err}
	r.lifecycle = append(r.lifecycle, lifecycle)
	r.emitRuntimeEventLocked(runtimeEventFromLifecycle(lifecycle))
}

func (r *NodeRuntime) recordCrossShardEvent(event CrossShardEvent) {
	txID := firstNonEmpty(event.LogicalTxID, event.TxID)
	r.recordEvent(txID, event.SourceShard, event.TargetShard, event.Stage, event.Success, event.Error)
}

func (r *NodeRuntime) crossShardPlugin() CrossShardPlugin {
	if r != nil && r.plugins.CrossShard != nil {
		return r.plugins.CrossShard
	}
	return builtinCrossShard{makeBasic("cross_shard", "relay_certificate_protocol", nil)}
}

func (r *NodeRuntime) recordLifecycle(event LifecycleEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lifecycle = append(r.lifecycle, event)
	r.emitRuntimeEventLocked(runtimeEventFromLifecycle(event))
}

func (r *NodeRuntime) emitRuntimeEvent(event RuntimeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emitRuntimeEventLocked(event)
}

func (r *NodeRuntime) emitRuntimeEventLocked(event RuntimeEvent) {
	if event.Type == "" {
		return
	}
	if event.TimestampMS == 0 {
		event.TimestampMS = time.Now().UnixMilli()
	}
	if event.NodeID == "" {
		event.NodeID = r.node.NodeID
	}
	if event.ShardID == "" {
		event.ShardID = r.node.ShardID
	}
	if r.plugins.Metrics != nil {
		if r.runtimeMetricCounts == nil {
			r.runtimeMetricCounts = map[string]int64{}
		}
		for _, metric := range r.plugins.Metrics.Consume(event) {
			if metric.Key == "" {
				continue
			}
			switch value := metric.Value.(type) {
			case int:
				r.runtimeMetricCounts[metric.Key] += int64(value)
			case int64:
				r.runtimeMetricCounts[metric.Key] += value
			case float64:
				r.runtimeMetricCounts[metric.Key] += int64(value)
			default:
				r.runtimeMetricCounts[metric.Key]++
			}
		}
	}
	if r.plugins.Observability != nil {
		row := r.plugins.Observability.Observe(event)
		if len(row) > 0 {
			r.runtimeEventRows = append(r.runtimeEventRows, row)
		}
	}
}

func runtimeEventFromLifecycle(event LifecycleEvent) RuntimeEvent {
	eventType := ""
	switch event.Stage {
	case "received":
		eventType = "TxReceived"
	case "admitted":
		eventType = "TxAdmitted"
	case "failed":
		eventType = "TxFailed"
	case "proposed":
		eventType = "BlockProposed"
	case "prepare", "commit", "vote":
		eventType = "ConsensusMessage"
	case "quorum_committed":
		eventType = "ConsensusReached"
	case "durable_committed":
		eventType = "StateCommitted"
	case "relay_received":
		eventType = "RemoteStateFetched"
	case "sourcelock":
		eventType = "RemoteStateApplied"
	case "targetcommit", "sourcefinalize":
		eventType = "TxFinalized"
	case "refund", "timeout":
		eventType = "TxFailed"
	}
	return RuntimeEvent{TimestampMS: event.TimestampMS, Type: eventType, NodeID: event.NodeID, ShardID: event.ShardID, TxID: event.TxID, Height: event.BlockHeight, Success: event.Success, Error: event.Error, Attributes: map[string]any{"logical_tx_id": event.LogicalTxID, "source_shard": event.SourceShard, "target_shard": event.TargetShard}}
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
	pendingStateDeltaCount := len(r.pendingStateDeltas)
	pendingStateDeltaKeyCount := len(r.pendingStateDeltaKeys)
	readyStateDeltaCount := 0
	for _, request := range r.pendingStateDeltas {
		if remoteStateDeltaReadyForHomeBlock(request, committedHeight+1) {
			readyStateDeltaCount++
		}
	}
	r.mu.Unlock()
	mempoolIDs := r.pool.IDs()
	sort.Strings(mempoolIDs)
	status := map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "role": r.node.Role, "committed_height": committedHeight, "committed_block_hash": committedHash, "mempool_depth": r.pool.Len(), "mempool_logical_tx_ids": mempoolIDs, "reserved_tx_count": r.pool.ReservedCount(), "proposal_in_flight": proposalInFlight, "proposal_timeout_ms": proposalTimeoutMS, "commit_phase": commitPhase, "commit_phase_height": commitPhaseHeight, "commit_phase_hash": commitPhaseHash, "last_proposal_error": lastProposalError, "fatal_persistence_error": fatalPersistenceError, "pending_commit_count": pendingCommitCount, "pending_commit_heights": pendingCommitHeights, "pending_commit_errors": pendingCommitErrors, "pending_future_block_count": 0, "pending_cross_shard_count": pendingRelayCount, "pending_cross_shard_ids": pendingRelayIDs, "pending_state_delta_count": pendingStateDeltaCount, "pending_state_delta_key_count": pendingStateDeltaKeyCount, "ready_state_delta_count": readyStateDeltaCount, "relay_admission_failures": relayAdmissionFailures, "terminal_count": len(terminal), "terminal_logical_tx_ids": terminalIDs, "durable_committed_logical_tx_ids": durableIDs, "source_finalized_logical_tx_ids": sourceFinalizedIDs, "refunded_logical_tx_ids": refundedIDs, "failed_logical_tx_ids": failedIDs, "last_progress_at": lastProgressAt, "ready": true, "stopping": false}
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
	r.flushQueuedStateDeltas()
	if err := r.writeRuntimeStatus(); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "fault_policy.json"), map[string]any{"requested": r.plan.FaultPlan, "applied": r.plugins.Fault.Policy(r.plan.FaultPlan), "fault_plugin_id": r.plugins.Fault.ID()}); err != nil {
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
	logicalPhysicalRows := append([][]string(nil), r.logicalPhysicalRows...)
	chainRows := append([][]string(nil), r.chainRows...)
	blockExecutionSummaries := append([]map[string]any(nil), r.blockExecutionSummaries...)
	executionPlans := append([]map[string]any(nil), r.executionPlans...)
	txExecutionTraceRows := append([][]string(nil), r.txExecutionTraceRows...)
	businessExecutionRows := append([][]string(nil), r.businessExecutionRows...)
	stateDeltaRows := append([][]string(nil), r.stateDeltaRows...)
	planDigestRows := append([][]string(nil), r.planDigestRows...)
	runtimeEventRows := append([][]string(nil), r.runtimeEventRows...)
	runtimeMetricCounts := map[string]int64{}
	for key, value := range r.runtimeMetricCounts {
		runtimeMetricCounts[key] = value
	}
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
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "commit_log.csv"), commitLogHeaders(), commitRows); err != nil {
		return err
	}
	if r.hasBatchRoutingControlPlane() {
		if err := r.writeMetaTrackNodeArtifacts(executionRows, commitRows, logicalPhysicalRows); err != nil {
			return err
		}
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "committed_chain.csv"), []string{"node_id", "shard_id", "height", "view", "block_hash", "parent_hash", "tx_count", "tx_digest", "state_root_before", "state_root_after", "receipt_root", "commit_started_at", "commit_finished_at"}, chainRows); err != nil {
		return err
	}
	artifactWorkerCount := workerCountFromBlockSummaries(blockExecutionSummaries)
	if err := SaveJSON(filepath.Join(r.node.DataDir, "block_execution_summary.json"), map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "block_executor_id": r.plugins.BlockExecutor.ID(), "block_executor_version": blockExecutorVersionFromSummaries(blockExecutionSummaries), "worker_count": artifactWorkerCount, "blocks": blockExecutionSummaries, "executed_block_count": len(blockExecutionSummaries), "plan_digest_consistent": planDigestsConsistent(planDigestRows)}); err != nil {
		return err
	}
	if err := writeJSONL(filepath.Join(r.node.DataDir, "execution_plan.jsonl"), executionPlans); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "transaction_execution_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "success", "error", "read_key_count", "write_key_count", "state_root_after_tx"}, txExecutionTraceRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "business_execute_invocation_count_by_node.csv"), []string{"node_id", "shard_id", "block_height", "block_hash", "tx_id", "track", "attempt", "reason", "success", "final_completion"}, businessExecutionRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "state_delta_log.csv"), []string{"node_id", "shard_id", "block_hash", "height", "key", "value_digest"}, stateDeltaRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "plan_digest_consistency.csv"), []string{"node_id", "shard_id", "block_hash", "height", "block_executor_id", "plan_digest", "state_root_before", "state_root_after", "receipt_root", "worker_count", "consistent"}, planDigestRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "runtime_events.csv"), []string{"timestamp_ms", "event_type", "node_id", "shard_id", "tx_id", "block_hash", "height", "success", "error", "attributes_digest"}, runtimeEventRows); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "runtime_metrics.json"), map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "metrics_plugin_id": r.plugins.Metrics.ID(), "observability_plugin_id": r.plugins.Observability.ID(), "counts": runtimeMetricCounts}); err != nil {
		return err
	}
	if err := r.writeBlockSTMArtifacts(blockExecutionSummaries); err != nil {
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
	remoteSummary := summarizeRemoteStateRows(r.remoteStateRows)
	schedulerSummary := summarizeSchedulerRows(r.schedulerRows)
	return SaveJSON(filepath.Join(r.node.DataDir, "node_summary.json"), map[string]any{"runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime", "runtime_truth": "v5_real_cluster_candidate", "node_id": r.node.NodeID, "shard_id": r.node.ShardID, "pid": os.Getpid(), "listen_addr": r.transport.ListenAddr, "committed_block_count": count, "state_root": r.plugins.StateStorage.Root(r.db), "plugin_snapshot": r.pluginSnapshot, "block_executor_id": r.plugins.BlockExecutor.ID(), "block_executor_version": blockExecutorVersionFromSummaries(blockExecutionSummaries), "worker_count": artifactWorkerCount, "plan_digest_consistent": planDigestsConsistent(planDigestRows), "fast_track_count": fast, "conservative_track_count": conservative, "aggregation_group_count": groups, "logical_update_count": logical, "physical_update_count": physical, "scheduler_event_count": schedulerSummary.total, "scheduler_blocked_count": schedulerSummary.blocked, "scheduler_wakeup_count": schedulerSummary.wakeup, "scheduler_stolen_work_count": schedulerSummary.stolen, "scheduler_local_execution_count": schedulerSummary.local, "scheduler_ready_queue_max_depth": schedulerSummary.readyMax, "scheduler_fast_queue_max_depth": schedulerSummary.fastMax, "scheduler_conservative_queue_max_depth": schedulerSummary.conservativeMax, "scheduler_dependency_wait_ms": schedulerSummary.dependencyWaitMS, "scheduler_idle_ms": schedulerSummary.idleMS, "scheduler_idle_ratio": schedulerSummary.idleRatio(), "remote_state_access_count": remoteSummary.total, "remote_state_read_count": remoteSummary.reads, "remote_state_write_apply_count": remoteSummary.writes, "remote_state_access_failed_count": remoteSummary.failed, "remote_state_access_avg_latency_ms": remoteSummary.avgLatency, "runtime_event_count": len(runtimeEventRows), "runtime_metric_counts": runtimeMetricCounts, "real_signed_tx": true, "real_tcp": true, "real_pbft_style_messages": len(rows) > 0})
}

func (r *NodeRuntime) writeMetaTrackNodeArtifacts(executionRows, commitRows, logicalPhysicalRows [][]string) error {
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "track_classification.csv"), []string{"timestamp", "node_id", "shard_id", "tx_id", "height", "execution_plugin", "track", "reason"}, executionRows); err != nil {
		return err
	}
	r.mu.Lock()
	remoteRows := append([][]string(nil), r.remoteStateRows...)
	schedulerRows := append([][]string(nil), r.schedulerRows...)
	r.mu.Unlock()
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "metatrack_scheduler_trace.csv"), []string{"timestamp", "node_id", "shard_id", "height", "scheduler_plugin", "tx_id", "track", "queue_name", "decision_reason", "local_execution", "stolen_work", "blocked", "wakeup", "ready_queue_depth", "fast_queue_depth", "conservative_queue_depth", "dependency_wait_ms", "scheduler_idle_ms"}, schedulerRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "aggregation_plan.csv"), commitLogHeaders(), commitRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "remote_state_access.csv"), []string{"timestamp", "node_id", "execution_shard", "height", "block_hash", "tx_id", "state_key", "qualified_home_key", "home_shard", "response_execution_shard", "access_kind", "latency_ms", "witness_digest", "home_state_root", "success", "error"}, remoteRows); err != nil {
		return err
	}
	return metrics.WriteCSV(filepath.Join(r.node.DataDir, "logical_physical_update_mapping.csv"), []string{"timestamp", "node_id", "shard_id", "height", "commit_plugin", "aggregation_group_id", "state_key", "value_digest", "logical_tx_ids", "logical_update_count", "physical_update_count", "reduced_physical_write_count", "aggregation_applied", "pre_aggregation_physical_op_count", "post_aggregation_physical_op_count", "aggregated_key_count", "aggregated_logical_delta_count"}, logicalPhysicalRows)
}

func commitLogHeaders() []string {
	return []string{"timestamp", "node_id", "shard_id", "height", "commit_plugin", "aggregation_group_id", "logical_update_count", "physical_update_count", "aggregation_applied", "pre_aggregation_physical_op_count", "post_aggregation_physical_op_count", "aggregated_key_count", "aggregated_logical_delta_count"}
}

func (r *NodeRuntime) writeBlockSTMArtifacts(blocks []map[string]any) error {
	taskRows := [][]string{}
	validationRows := [][]string{}
	abortRows := [][]string{}
	dependencyRows := [][]string{}
	incarnationRows := [][]string{}
	equivalenceRows := []map[string]any{}
	serialEquivalent := true
	total := execution.BlockSTMMetrics{IncarnationHistogram: map[int]int{}}
	for _, blockSummary := range blocks {
		metricsValue, ok := blockSTMMetricsFromSummary(blockSummary)
		if !ok {
			continue
		}
		blockHash := fmt.Sprint(blockSummary["block_hash"])
		height := fmt.Sprint(blockSummary["height"])
		total.WorkerCount = maxInt(total.WorkerCount, metricsValue.WorkerCount)
		total.MaximumParallelWidth = maxInt(total.MaximumParallelWidth, metricsValue.MaximumParallelWidth)
		total.ExecutionTaskCount += metricsValue.ExecutionTaskCount
		total.ValidationTaskCount += metricsValue.ValidationTaskCount
		total.AbortCount += metricsValue.AbortCount
		total.ReexecutionCount += metricsValue.ReexecutionCount
		total.EstimateCount += metricsValue.EstimateCount
		total.DependencyWaitCount += metricsValue.DependencyWaitCount
		total.DependencyResumeCount += metricsValue.DependencyResumeCount
		total.SpeculativeReadCount += metricsValue.SpeculativeReadCount
		total.ValidationFailureCount += metricsValue.ValidationFailureCount
		total.CommittedTransactionCount += metricsValue.CommittedTransactionCount
		total.SerialOracleMS += metricsValue.SerialOracleMS
		total.MaterializationMS += metricsValue.MaterializationMS
		total.IncarnationLimitHitCount += metricsValue.IncarnationLimitHitCount
		total.SerialFallbackCount += metricsValue.SerialFallbackCount
		total.BusinessExecutionCount += metricsValue.BusinessExecutionCount
		total.MaximumIncarnation = maxInt(total.MaximumIncarnation, metricsValue.MaximumIncarnation)
		for incarnation, count := range metricsValue.IncarnationHistogram {
			total.IncarnationHistogram[incarnation] += count
			incarnationRows = append(incarnationRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(incarnation), fmt.Sprint(count)})
		}
		taskRows = append(taskRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(metricsValue.WorkerCount), fmt.Sprint(metricsValue.ExecutionTaskCount), fmt.Sprint(metricsValue.MaximumParallelWidth), fmt.Sprint(metricsValue.SpeculativeReadCount), fmt.Sprint(metricsValue.BusinessExecutionCount)})
		validationRows = append(validationRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(metricsValue.ValidationTaskCount), fmt.Sprint(metricsValue.ValidationFailureCount)})
		abortRows = append(abortRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(metricsValue.AbortCount), fmt.Sprint(metricsValue.ReexecutionCount), fmt.Sprint(metricsValue.MaximumIncarnation)})
		dependencyRows = append(dependencyRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(metricsValue.DependencyWaitCount), fmt.Sprint(metricsValue.DependencyResumeCount), fmt.Sprint(metricsValue.EstimateCount)})
		blockEquivalent := boolFromAny(blockSummary["serial_equivalent"])
		serialEquivalent = serialEquivalent && blockEquivalent
		equivalenceRows = append(equivalenceRows, map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "block_hash": blockHash, "height": blockSummary["height"], "block_executor_id": blockSummary["block_executor_id"], "state_root_before": blockSummary["state_root_before"], "state_root_after": blockSummary["state_root_after"], "receipt_root": blockSummary["receipt_root"], "execution_plan_digest": blockSummary["execution_plan_digest"], "serial_equivalent": blockEquivalent})
	}
	if len(taskRows) == 0 {
		return nil
	}
	if err := SaveJSON(filepath.Join(r.node.DataDir, "block_stm_summary.json"), map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "block_executor_id": execution.BlockSTMExecutorID, "block_stm_metrics": total, "block_count": len(taskRows), "serial_equivalent": serialEquivalent}); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "block_stm_task_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "worker_count", "execution_task_count", "maximum_parallel_width", "speculative_read_count", "business_execution_invocation_count"}, taskRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "block_stm_validation_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "validation_task_count", "validation_failure_count"}, validationRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "block_stm_abort_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "abort_count", "reexecution_count", "maximum_incarnation"}, abortRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "block_stm_dependency_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "dependency_wait_count", "dependency_resume_count", "estimate_count"}, dependencyRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "incarnation_summary.csv"), []string{"node_id", "shard_id", "block_hash", "height", "incarnation", "transaction_count"}, incarnationRows); err != nil {
		return err
	}
	return SaveJSON(filepath.Join(r.node.DataDir, "serial_equivalence.json"), map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "block_executor_id": execution.BlockSTMExecutorID, "serial_equivalent": serialEquivalent, "blocks": equivalenceRows})
}

func pluginEvidence(profile map[string]PluginConfig) map[string]map[string]any {
	out := map[string]map[string]any{}
	for category, item := range profile {
		out[category] = map[string]any{"plugin_id": item.PluginID, "version": "1.0.0", "runtime_factory": "builtin:" + item.PluginID, "parameters": item.Config, "initialization_success": true}
	}
	return out
}

func blockSTMMetricsFromSummary(item map[string]any) (execution.BlockSTMMetrics, bool) {
	switch value := item["block_stm_metrics"].(type) {
	case execution.BlockSTMMetrics:
		return value, true
	case map[string]any:
		return blockSTMMetricsFromMap(value), true
	default:
		return execution.BlockSTMMetrics{}, false
	}
}

func blockSTMMetricsFromMap(value map[string]any) execution.BlockSTMMetrics {
	metricsValue := execution.BlockSTMMetrics{IncarnationHistogram: map[int]int{}}
	metricsValue.WorkerCount = intFromAny(value["worker_count"])
	metricsValue.MaximumParallelWidth = intFromAny(value["maximum_parallel_width"])
	metricsValue.ExecutionTaskCount = intFromAny(value["execution_task_count"])
	metricsValue.ValidationTaskCount = intFromAny(value["validation_task_count"])
	metricsValue.AbortCount = intFromAny(value["abort_count"])
	metricsValue.ReexecutionCount = intFromAny(value["reexecution_count"])
	metricsValue.EstimateCount = intFromAny(value["estimate_count"])
	metricsValue.DependencyWaitCount = intFromAny(value["dependency_wait_count"])
	metricsValue.DependencyResumeCount = intFromAny(value["dependency_resume_count"])
	metricsValue.SpeculativeReadCount = intFromAny(value["speculative_read_count"])
	metricsValue.ValidationFailureCount = intFromAny(value["validation_failure_count"])
	metricsValue.CommittedTransactionCount = intFromAny(value["committed_transaction_count"])
	metricsValue.MaximumIncarnation = intFromAny(value["maximum_incarnation"])
	metricsValue.SerialOracleMS = int64(intFromAny(value["serial_oracle_ms"]))
	metricsValue.MaterializationMS = int64(intFromAny(value["materialization_ms"]))
	metricsValue.IncarnationLimitHitCount = intFromAny(value["incarnation_limit_hit_count"])
	metricsValue.SerialFallbackCount = intFromAny(value["serial_fallback_count"])
	metricsValue.BusinessExecutionCount = intFromAny(value["business_execution_invocation_count"])
	if histogram, ok := value["incarnation_histogram"].(map[string]any); ok {
		for key, count := range histogram {
			metricsValue.IncarnationHistogram[intFromAny(key)] = intFromAny(count)
		}
	}
	return metricsValue
}

func intFromAny(value any) int {
	switch item := value.(type) {
	case int:
		return item
	case int64:
		return int(item)
	case float64:
		return int(item)
	case string:
		var parsed int
		_, _ = fmt.Sscan(item, &parsed)
		return parsed
	default:
		return 0
	}
}

func boolFromAny(value any) bool {
	switch item := value.(type) {
	case bool:
		return item
	case string:
		return item == "true"
	default:
		return false
	}
}

func blockExecutorWorkerCountFromProfile(profile map[string]PluginConfig) int {
	plugin, ok := profile["block_executor"]
	if !ok {
		return 1
	}
	return configuredWorkerCount(plugin.Config, 1)
}

func (r *NodeRuntime) hasBatchRoutingControlPlane() bool {
	if r == nil || r.plugins.Routing == nil {
		return false
	}
	_, ok := r.plugins.Routing.(BatchRoutingPlugin)
	return ok
}

func (r *NodeRuntime) recordExecutionAndCommitDecisions(block realblock.Block, commitDecision CommitDecision, physicalDelta []state.StateKV) {
	executionPlugin := r.plugins.Execution.ID()
	commitPlugin := r.plugins.Commit.ID()
	timestamp := fmt.Sprint(time.Now().UnixMilli())
	classification := batchClassification(block.TxList, r.plugins.Execution)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range block.TxList {
		decision := decisionForTx(item, classification.Decisions, r.plugins.Execution)
		r.executionRows = append(r.executionRows, []string{timestamp, r.node.NodeID, r.node.ShardID, item.TxID, fmt.Sprint(block.Height), executionPlugin, decision.Track, decision.Reason})
	}
	r.commitRows = append(r.commitRows, []string{timestamp, r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, commitDecision.AggregationGroupID, fmt.Sprint(commitDecision.LogicalUpdates), fmt.Sprint(commitDecision.PhysicalUpdates), fmt.Sprint(commitDecision.Applied), fmt.Sprint(commitDecision.PreAggregationPhysicalOps), fmt.Sprint(commitDecision.PostAggregationPhysicalOps), fmt.Sprint(commitDecision.AggregatedKeyCount), fmt.Sprint(commitDecision.AggregatedLogicalDeltaCount)})
	for _, item := range physicalDelta {
		physicalUpdates := 1
		reduced := len(item.TxIDs) - physicalUpdates
		if reduced < 0 {
			reduced = 0
		}
		r.logicalPhysicalRows = append(r.logicalPhysicalRows, []string{timestamp, r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, commitDecision.AggregationGroupID, item.Key, stableTextDigest(item.Value), strings.Join(item.TxIDs, "|"), fmt.Sprint(len(item.TxIDs)), fmt.Sprint(physicalUpdates), fmt.Sprint(reduced), fmt.Sprint(commitDecision.Applied), fmt.Sprint(commitDecision.PreAggregationPhysicalOps), fmt.Sprint(commitDecision.PostAggregationPhysicalOps), fmt.Sprint(commitDecision.AggregatedKeyCount), fmt.Sprint(commitDecision.AggregatedLogicalDeltaCount)})
	}
}

func (r *NodeRuntime) recordBlockExecutionResult(block realblock.Block, result BlockExecutionResult) {
	executed := result.ExecutionResult
	observedReadKeys, observedWriteKeys := 0, 0
	for _, delta := range executed.TxDeltas {
		observedReadKeys += len(delta.ReadSet)
		observedWriteKeys += len(delta.WriteSet)
	}
	summary := map[string]any{
		"block_hash":                     block.BlockHash,
		"height":                         block.Height,
		"block_executor_id":              r.plugins.BlockExecutor.ID(),
		"block_executor_version":         executed.ExecutorVersion,
		"block_execution_ms":             result.BlockExecutionMS,
		"transaction_execution_ms":       result.TransactionExecutionMS,
		"deterministic_apply_ms":         result.DeterministicApplyMS,
		"executed_transaction_count":     len(block.TxList),
		"successful_transaction_count":   executed.SuccessfulTxs,
		"failed_transaction_count":       executed.FailedTxs,
		"declared_read_key_count":        executed.Plan.DeclaredReadKeyCount,
		"declared_write_key_count":       executed.Plan.DeclaredWriteKeyCount,
		"observed_read_key_count":        observedReadKeys,
		"observed_write_key_count":       observedWriteKeys,
		"state_root_before":              executed.StateRootBefore,
		"state_root_after":               executed.StateRootAfter,
		"receipt_root":                   executed.ReceiptRoot,
		"execution_plan_digest":          executed.PlanDigest,
		"worker_count":                   result.WorkerCount,
		"plan_digest_consistent":         true,
		"state_root_consistent":          true,
		"serial_order_preserved":         executed.BlockExecutorID == execution.SerialBlockExecutorID,
		"reordered_transaction_count":    reorderedTransactionCount(executed),
		"maximum_parallel_width":         maximumParallelWidth(executed, result.WorkerCount),
		"system_delta_drain_block_count": boolToInt(len(block.TxList) == 0 && len(block.SystemStateDeltas) > 0),
	}
	for key, value := range result.PersistenceMetrics {
		summary[key] = value
	}
	for key, value := range result.ActualMetrics {
		summary[key] = value
	}
	if executed.BlockSTMMetrics.WorkerCount > 0 {
		summary["block_stm_metrics"] = executed.BlockSTMMetrics
		summary["abort_count"] = executed.BlockSTMMetrics.AbortCount
		summary["reexecution_count"] = executed.BlockSTMMetrics.ReexecutionCount
		summary["dependency_wait_count"] = executed.BlockSTMMetrics.DependencyWaitCount
		summary["validation_failure_count"] = executed.BlockSTMMetrics.ValidationFailureCount
		summary["serial_equivalent"] = executed.SerialEquivalent
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
	businessRows := make([][]string, 0, len(result.BusinessAttempts))
	for _, attempt := range result.BusinessAttempts {
		businessRows = append(businessRows, []string{r.node.NodeID, r.node.ShardID, fmt.Sprint(attempt.BlockHeight), block.BlockHash, attempt.TxID, attempt.Track, fmt.Sprint(attempt.Attempt), attempt.Reason, fmt.Sprint(attempt.Success), fmt.Sprint(attempt.FinalCompletion)})
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
	r.businessExecutionRows = append(r.businessExecutionRows, businessRows...)
	r.stateDeltaRows = append(r.stateDeltaRows, stateRows...)
	r.planDigestRows = append(r.planDigestRows, planRow)
}

func workerCountFromBlockSummaries(items []map[string]any) int {
	max := 1
	for _, item := range items {
		switch value := item["worker_count"].(type) {
		case int:
			if value > max {
				max = value
			}
		case float64:
			if int(value) > max {
				max = int(value)
			}
		}
	}
	return max
}

func blockExecutorVersionFromSummaries(items []map[string]any) string {
	for _, item := range items {
		if value, ok := item["block_executor_version"].(string); ok && value != "" {
			return value
		}
	}
	return "1.0.0"
}

func maximumParallelWidth(result execution.Result, workerCount int) int {
	if result.BlockSTMMetrics.MaximumParallelWidth > 0 {
		return result.BlockSTMMetrics.MaximumParallelWidth
	}
	if result.BlockExecutorID == execution.SerialBlockExecutorID {
		return 1
	}
	if workerCount > 0 {
		return workerCount
	}
	return 1
}

func reorderedTransactionCount(result execution.Result) int {
	for index, original := range result.Plan.OriginalTransactionIdxs {
		if index != original {
			return len(result.Plan.OriginalTransactionIdxs)
		}
	}
	return 0
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

type remoteStateSummary struct {
	total      int
	reads      int
	writes     int
	failed     int
	avgLatency float64
}

func summarizeRemoteStateRows(rows [][]string) remoteStateSummary {
	summary := remoteStateSummary{}
	latencySum := 0
	successful := 0
	for _, row := range rows {
		if len(row) < 16 {
			continue
		}
		if row[14] != "true" {
			summary.failed++
			continue
		}
		successful++
		summary.total++
		if row[10] == "write_apply" {
			summary.writes++
		} else {
			summary.reads++
		}
		latencySum += intFromAny(row[11])
	}
	if successful > 0 {
		summary.avgLatency = float64(latencySum) / float64(successful)
	}
	return summary
}

type schedulerSummary struct {
	total             int
	blocked           int
	wakeup            int
	stolen            int
	local             int
	readyMax          int
	fastMax           int
	conservativeMax   int
	dependencyWaitMS  int
	idleMS            int
	idlePositiveCount int
}

func summarizeSchedulerRows(rows [][]string) schedulerSummary {
	summary := schedulerSummary{total: len(rows)}
	for _, row := range rows {
		if len(row) < 13 {
			continue
		}
		if row[9] == "true" {
			summary.local++
		}
		if row[10] == "true" {
			summary.stolen++
		}
		if row[11] == "true" {
			summary.blocked++
		}
		if row[12] == "true" {
			summary.wakeup++
		}
		if len(row) >= 18 {
			summary.readyMax = maxInt(summary.readyMax, intFromAny(row[13]))
			summary.fastMax = maxInt(summary.fastMax, intFromAny(row[14]))
			summary.conservativeMax = maxInt(summary.conservativeMax, intFromAny(row[15]))
			waitMS := intFromAny(row[16])
			idleMS := intFromAny(row[17])
			summary.dependencyWaitMS += waitMS
			summary.idleMS += idleMS
			if idleMS > 0 {
				summary.idlePositiveCount++
			}
		}
	}
	return summary
}

func (summary schedulerSummary) idleRatio() float64 {
	if summary.total == 0 {
		return 0
	}
	return float64(summary.idlePositiveCount) / float64(summary.total)
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
