package v5

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

const finalizeMessage = "V5_XSHARD_FINALIZE"
const finalizeAckMessage = "V5_XSHARD_FINALIZE_ACK"
const catchupRequestMessage = "V5_CATCHUP_REQUEST"
const catchupBlockMessage = "V5_CATCHUP_BLOCK"
const catchupUnavailableMessage = "V5_CATCHUP_UNAVAILABLE"
const stateFetchRequestMessage = "V5_STATE_FETCH_REQUEST"
const stateFetchResponseMessage = "V5_STATE_FETCH_RESPONSE"
const statelessVersionAdmissionProbeAccessKind = "state_version_admission_probe"
const stateDeltaApplyMessage = "V5_STATE_DELTA_APPLY"
const stateDeltaApplyAckMessage = "V5_STATE_DELTA_APPLY_ACK"
const remoteStateDeltaApplyLagBlocks uint64 = 1
const crossShardRetryInterval = 500 * time.Millisecond
const crossShardSuccessfulSendRetryInterval = 30 * time.Second
const commitMailboxCapacity = 64
const stateFetchMailboxCapacity = 256
const stateFetchWorkerCount = 4
const stateFetchResponseMailboxCapacity = 4096
const stateFetchResponseWorkerCount = 16
const stateFetchDiagnosticHistoryLimit = 16
const stateFetchSnapshotCacheLimit = 16 // MBE_META_TRACK_RAPID_FIX_V3
const schedulerTraceRetentionLimit = 20000
const runtimeEventTraceRetentionLimit = 20000
const maxConcurrentCatchupResponses = 2
const certifiedCatchupBatchSize uint64 = 32
const urgentCatchupRequestInterval = time.Second

// Proposal is retained as a wire/test compatibility shape for historical
// single-validator V5 commit-envelope tests. Multi-validator V5 consensus uses
// the shared pbft.State exclusively.
type Proposal struct {
	Block realblock.Block `json:"block"`
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
	ShardID     string `json:"shard_id"`
	FromHeight  uint64 `json:"from_height"`
	ToHeight    uint64 `json:"to_height"`
	KnownHeight uint64 `json:"known_height,omitempty"`
	KnownHash   string `json:"known_hash,omitempty"`
}
type CatchupBlock struct {
	Block       realblock.Block        `json:"block"`
	SourceNode  string                 `json:"source_node"`
	Certificate pbft.CommitCertificate `json:"commit_certificate"`
}
type CatchupUnavailable struct {
	SourceNode             string `json:"source_node"`
	RequestedFromHeight    uint64 `json:"requested_from_height"`
	CommittedHeight        uint64 `json:"committed_height"`
	StableCheckpointHeight uint64 `json:"stable_checkpoint_height"`
	Reason                 string `json:"reason"`
}
type StateFetchRequest struct {
	RequestID       string `json:"request_id"`
	TxID            string `json:"tx_id"`
	BlockHash       string `json:"block_hash"`
	Key             string `json:"key"`
	HomeShard       string `json:"home_shard"`
	ExecutionShard  string `json:"execution_shard"`
	AccessKind      string `json:"access_kind"`
	RequiredVersion uint64 `json:"required_version,omitempty"`
	Versioned       bool   `json:"versioned,omitempty"`
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
	StateVersion   uint64 `json:"state_version,omitempty"`
	Versioned      bool   `json:"versioned,omitempty"`
	Success        bool   `json:"success"`
	Error          string `json:"error,omitempty"`
}

// StateFetchDiagnostic is bounded runtime evidence for one real MetaTrack RPC.
// It is not part of the protocol or execution result and cannot influence
// routing, snapshots, consensus, or state application.
type StateFetchDiagnostic struct {
	RequestID      string `json:"request_id"`
	TxID           string `json:"tx_id"`
	BlockHash      string `json:"block_hash"`
	BlockHeight    uint64 `json:"block_height"`
	Key            string `json:"key"`
	HomeShard      string `json:"home_shard"`
	ExecutionShard string `json:"execution_shard"`
	Requester      string `json:"requester,omitempty"`
	Stage          string `json:"stage"`
	StartedAtMS    int64  `json:"started_at_ms"`
	SentAtMS       int64  `json:"sent_at_ms,omitempty"`
	FinishedAtMS   int64  `json:"finished_at_ms,omitempty"`
	LatencyMS      int64  `json:"latency_ms,omitempty"`
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
	RoutingOrdinal  uint64   `json:"routing_ordinal,omitempty"`
	PreviousVersion uint64   `json:"previous_version,omitempty"`
	ProducedVersion uint64   `json:"produced_version,omitempty"`
	OrderingNoop    bool     `json:"ordering_noop,omitempty"`
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
	PreviousVersion uint64   `json:"previous_version,omitempty"`
	ProducedVersion uint64   `json:"produced_version,omitempty"`
	OrderingNoop    bool     `json:"ordering_noop,omitempty"`
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

// CommitFailure captures the most recent rejected commit for bounded runtime
// diagnostics. It is deliberately observational: commit and rollback behavior
// remains owned by commitOnce.
type CommitFailure struct {
	Phase     string `json:"phase"`
	Height    uint64 `json:"height"`
	BlockHash string `json:"block_hash"`
	Error     string `json:"error"`
	Timestamp int64  `json:"timestamp_ms"`
}

type CommitOrigin string

const (
	CommitOriginConsensus      CommitOrigin = "consensus"
	CommitOriginCatchUp        CommitOrigin = "catch_up"
	CommitOriginRecoveryReplay CommitOrigin = "recovery_replay"
)

type commitTaskKind string

const (
	commitTaskConsensus commitTaskKind = "consensus"
	commitTaskCatchUp   commitTaskKind = "catch_up"
)

type commitTask struct {
	key    string
	kind   commitTaskKind
	block  realblock.Block
	origin CommitOrigin
}

type deferredPrePrepare struct {
	FromNode  string
	View      uint64
	Sequence  uint64
	Block     realblock.Block
	Signature string
}

// proposalValidationWorkEstimator is an optional, algorithm-neutral runtime
// hook. It reports how much validator-side work a concrete proposal requires.
// The PBFT messages, quorum, vote rules, and commit path remain unchanged.
type proposalValidationWorkEstimator interface {
	EstimateProposalValidationWork(realblock.Block) int
}

type verifiedExecutionPlanRecord struct {
	AlgorithmID   string
	PayloadDigest string
	PlanDigest    string
}

type consensusPlanningProgress struct {
	AlgorithmID string
	Phase       string
	WorkUnits   int64
	DetailCount int64
}

// contextConsensusExecutionPlanner is an optional pre-consensus planning
// extension used only by algorithms whose exact planner can be materially
// expensive. It changes lifecycle/cancellation only: PlanBlockContext must
// produce the same consensus-bound plan as PlanBlock when the context is not
// cancelled.
type contextConsensusExecutionPlanner interface {
	PlanBlockContext(context.Context, realblock.Block, func(consensusPlanningProgress)) (ConsensusExecutionPlanningResult, error)
}

// fatalConsensusPlanningError is an opt-in planner failure contract. Normal
// planner errors remain retryable exactly as before. A planner must explicitly
// mark an error fatal when continuing would only repeat a deterministic,
// evidence-backed impossibility (currently used only by Nezha CG intrinsic
// cycle-space detection).
type fatalConsensusPlanningError interface {
	error
	FatalConsensusPlanning() bool
}

func isFatalConsensusPlanningError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var fatal fatalConsensusPlanningError
	return errors.As(err, &fatal) && fatal.FatalConsensusPlanning()
}

type prePrepareDeferralDisposition uint8

const (
	prePrepareDeferralIgnored prePrepareDeferralDisposition = iota
	prePrepareDeferralStored
	prePrepareDeferralBecameCurrent
)

// stateFetchTask moves potentially blocking snapshot and response work off the
// per-connection reader. Responses remain correlated directly by request ID.
type stateFetchTask struct {
	requester string
	request   StateFetchRequest
}

type stateFetchResponseTask struct {
	requester string
	response  StateFetchResponse
}

type NodeRuntime struct {
	plan                            Plan
	node                            NodePlan
	peers                           []p2p.Peer
	transport                       *p2p.Transport
	consensus                       *pbft.State
	sendToNodeHook                  func(context.Context, string, p2p.MessageEnvelope) error
	pool                            *mempool.Mempool
	proposer                        *realblock.Proposer
	db                              *state.DB
	store                           *storage.BlockStore
	mu                              sync.Mutex
	commitMu                        sync.Mutex
	commitTasks                     chan commitTask
	commitWorkerCancel              context.CancelFunc
	commitWorkerContext             context.Context
	commitWorkerWG                  sync.WaitGroup
	queuedCommitTasks               map[string]bool
	stateFetchTasks                 chan stateFetchTask
	stateFetchWorkerCancel          context.CancelFunc
	stateFetchWorkerContext         context.Context
	stateFetchWorkerWG              sync.WaitGroup
	stateFetchResponseTasks         chan stateFetchResponseTask
	stateFetchResponseWG            sync.WaitGroup
	stateFetchSnapshotMu            sync.Mutex
	proposals                       map[string]realblock.Block
	deferredPrePrepares             map[uint64]deferredPrePrepare
	votes                           map[string]map[string]bool
	committed                       map[string]bool
	committing                      map[string]bool
	committedHeight                 uint64
	committedHash                   string
	commitPhase                     string
	commitPhaseHeight               uint64
	commitPhaseHash                 string
	lastProgressAt                  int64
	pendingCommits                  map[uint64]realblock.Block
	pendingCommitErrors             map[uint64]string
	proposalInFlight                bool
	proposalInFlightHash            string
	proposalStartedAt               time.Time
	proposalLastBroadcastAt         time.Time
	proposalRetransmitCount         int
	proposalWorkUnits               atomic.Int64
	proposalPlanningInFlight        bool
	proposalPlanningView            uint64
	proposalPlanningHeight          uint64
	proposalPlanningAlgorithmID     string
	proposalPlanningPhase           string
	proposalPlanningStartedAt       time.Time
	proposalPlanningProgressAt      time.Time
	proposalPlanningWorkUnits       int64
	proposalPlanningDetailCount     int64
	proposalPlanningCancel          context.CancelFunc
	proposalPlanningGeneration      uint64
	proposalPlanningCancelReason    string
	proposalPlanningWG              sync.WaitGroup
	verifiedExecutionPlans          map[string]verifiedExecutionPlanRecord
	viewChangeStartedAt             time.Time
	viewChangeLastBroadcast         time.Time
	viewChangeRetransmits           int
	viewChangeTarget                uint64
	lastProposalError               string
	lastCommitFailure               CommitFailure
	fatalPersistenceError           string
	fatalExecutionError             string
	fatalPlanningError              string
	blockExecutionProgress          execution.BlockSTMProgress
	lastCatchupRequest              time.Time
	catchupTargetHeight             uint64
	catchupResponsesInFlight        int
	lastCrossShardRetry             time.Time
	relaySource                     map[string]Relay
	pendingOutboundRelays           map[string]Relay
	pendingFinalizeMessages         map[string]Finalize
	outboundRelayRetryAfter         map[string]time.Time
	finalizeRetryAfter              map[string]time.Time
	outboundRelaySendErrors         map[string]string
	finalizeSendErrors              map[string]string
	crossEventSeen                  map[string]bool
	relayAdmissionFailures          map[string]string
	events                          []Event
	lifecycle                       []LifecycleEvent
	consensusRows                   [][]string
	executionRows                   [][]string
	schedulerRows                   [][]string
	schedulerAggregate              schedulerSummary
	schedulerRowsDropped            int64
	commitRows                      [][]string
	logicalPhysicalRows             [][]string
	chainRows                       [][]string
	blockExecutionSummaries         []map[string]any
	executionPlans                  []map[string]any
	proposalEvidence                []map[string]any
	txExecutionTraceRows            [][]string
	observedStateAccessRows         [][]string
	businessExecutionRows           [][]string
	stateDeltaRows                  [][]string
	planDigestRows                  [][]string
	remoteStateRows                 [][]string
	runtimeEventRows                [][]string
	runtimeEventTotal               int64
	runtimeEventRowsDropped         int64
	runtimeMetricCounts             map[string]int64
	stateFetchWaiters               map[string]chan StateFetchResponse
	pendingStateFetches             map[string]StateFetchDiagnostic
	lastStateFetch                  StateFetchDiagnostic
	stateFetchFailures              []StateFetchDiagnostic
	lastStateFetchService           StateFetchDiagnostic
	stateFetchServiceErrors         []StateFetchDiagnostic
	stateFetchWitnesses             map[string]StateFetchResponse
	stateFetchSnapshots             map[string]map[string]string
	stateFetchSnapshotRoots         map[string]string
	stateFetchSnapshotOrder         []string
	stateVersionInitial             map[string]string
	stateVersionValues              map[string]map[uint64]string
	stateVersionMaterialized        map[string]uint64
	stateVersionSignals             map[string]chan struct{}
	stateVersionRemoteSubscriptions map[string]map[string]stateVersionRemoteSubscription
	stateApplyWaiters               map[string]chan StateDeltaApplyAck
	pendingStateDeltas              []StateDeltaApplyRequest
	pendingStateDeltaKeys           map[string]bool
	appliedStateDeltaKeys           map[string]bool
	pluginSnapshot                  map[string]PluginConfig
	plugins                         RuntimePlugins
	blockCount                      int
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
	if r.pbftAuthenticationRequired() {
		if err := ValidatePBFTIdentityPlan(plan); err != nil {
			return fmt.Errorf("pbft identity plan validation: %w", err)
		}
		if _, err := r.pbftSigningPrivateKey(); err != nil {
			return fmt.Errorf("pbft local identity validation: %w", err)
		}
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
			r.retryPendingCrossShardMessages(ctx)
			// Pre-consensus planning must never pin an obsolete primary after a
			// NEW-VIEW is installed. Cancellation is lifecycle-only; the CG
			// planner itself remains exact whenever its context stays live.
			r.cancelStaleProposalPlanning()
			r.mu.Lock()
			fatalPlanning := r.fatalPlanningError
			r.mu.Unlock()
			if fatalPlanning != "" {
				_ = r.writeRuntimeStatus()
				return fmt.Errorf("fatal consensus planning: %s", fatalPlanning)
			}
			r.checkPBFTLiveness(ctx)
			if r.isCurrentLeader() {
				if r.catchupNeeded() {
					r.requestCatchup(ctx)
				} else {
					r.propose(ctx)
				}
			} else {
				// Future PRE-PREPARE replay is idempotent. Running it periodically
				// closes any remaining scheduling gap between parent commit,
				// catch-up completion, and the one-shot replay hooks.
				r.replayDeferredPrePrepare(ctx)
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
	if raw := strings.TrimSpace(os.Getenv("MBE_RUNTIME_STATUS_INTERVAL_MS")); raw != "" {
		if milliseconds, err := strconv.Atoi(raw); err == nil && milliseconds >= 250 {
			return time.Duration(milliseconds) * time.Millisecond
		}
	}
	if blockInterval < time.Second {
		// Large per-node status files contain complete transaction-ID arrays.
		// Rewriting them every second becomes a dominant 10K-scale observer cost.
		return 5 * time.Second
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

func relayLogicalID(relay Relay) string {
	if relay.LogicalTxID != "" {
		return relay.LogicalTxID
	}
	return relay.Tx.TxID
}

func finalizeLogicalID(finish Finalize) string {
	if finish.LogicalTxID != "" {
		return finish.LogicalTxID
	}
	return finish.TxID
}

func (r *NodeRuntime) retryPendingCrossShardMessages(
	ctx context.Context,
) {
	now := time.Now()

	r.mu.Lock()

	if !r.lastCrossShardRetry.IsZero() &&
		now.Sub(r.lastCrossShardRetry) <
			crossShardRetryInterval {
		r.mu.Unlock()
		return
	}

	r.lastCrossShardRetry = now

	relayIDs := make(
		[]string,
		0,
		len(r.pendingOutboundRelays),
	)

	for logicalID := range r.pendingOutboundRelays {
		retryAfter := r.outboundRelayRetryAfter[logicalID]

		if !retryAfter.IsZero() && now.Before(retryAfter) {
			continue
		}

		relayIDs = append(relayIDs, logicalID)
	}

	sort.Strings(relayIDs)

	relays := make([]Relay, 0, len(relayIDs))

	for _, logicalID := range relayIDs {
		relays = append(
			relays,
			r.pendingOutboundRelays[logicalID],
		)
	}

	finalizeIDs := make(
		[]string,
		0,
		len(r.pendingFinalizeMessages),
	)

	for logicalID := range r.pendingFinalizeMessages {
		retryAfter := r.finalizeRetryAfter[logicalID]

		if !retryAfter.IsZero() && now.Before(retryAfter) {
			continue
		}

		finalizeIDs = append(finalizeIDs, logicalID)
	}

	sort.Strings(finalizeIDs)

	finalizes := make([]Finalize, 0, len(finalizeIDs))

	for _, logicalID := range finalizeIDs {
		finalizes = append(
			finalizes,
			r.pendingFinalizeMessages[logicalID],
		)
	}

	r.mu.Unlock()

	for _, relay := range relays {
		if r.sendPendingOutboundRelay(ctx, relay) {
			r.deferOutboundRelayRetry(relay)
		}
	}

	for _, finish := range finalizes {
		if r.sendPendingFinalize(ctx, finish) {
			r.deferFinalizeRetry(finish)
		}
	}
}

func (r *NodeRuntime) deferOutboundRelayRetry(
	relay Relay,
) {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.pendingOutboundRelays[logicalID]; !exists {
		delete(r.outboundRelayRetryAfter, logicalID)
		return
	}

	if r.outboundRelayRetryAfter == nil {
		r.outboundRelayRetryAfter =
			map[string]time.Time{}
	}

	r.outboundRelayRetryAfter[logicalID] =
		time.Now().Add(
			crossShardSuccessfulSendRetryInterval,
		)
}

func (r *NodeRuntime) deferFinalizeRetry(
	finish Finalize,
) {
	logicalID := finalizeLogicalID(finish)

	if logicalID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.pendingFinalizeMessages[logicalID]; !exists {
		delete(r.finalizeRetryAfter, logicalID)
		return
	}

	if r.finalizeRetryAfter == nil {
		r.finalizeRetryAfter =
			map[string]time.Time{}
	}

	r.finalizeRetryAfter[logicalID] =
		time.Now().Add(
			crossShardSuccessfulSendRetryInterval,
		)
}

func (r *NodeRuntime) queueOutboundRelay(
	ctx context.Context,
	relay Relay,
) {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return
	}

	r.mu.Lock()

	if r.pendingOutboundRelays == nil {
		r.pendingOutboundRelays = map[string]Relay{}
	}

	r.pendingOutboundRelays[logicalID] = relay
	delete(r.outboundRelayRetryAfter, logicalID)
	r.lastProgressAt = time.Now().UnixMilli()

	r.mu.Unlock()

	if r.sendPendingOutboundRelay(ctx, relay) {
		r.deferOutboundRelayRetry(relay)
	}
}

func (r *NodeRuntime) sendPendingOutboundRelay(
	ctx context.Context,
	relay Relay,
) bool {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return false
	}

	r.mu.Lock()

	pendingRelay, exists :=
		r.pendingOutboundRelays[logicalID]

	r.mu.Unlock()

	if !exists {
		return true
	}

	relay = pendingRelay

	targetLeader := r.leaderID(relay.TargetShard)

	if targetLeader == "" {
		r.recordOutboundRelaySendFailure(
			relay,
			"target_leader_not_found",
		)
		return false
	}

	envelope, err := p2p.NewEnvelope(
		p2p.MessageXShardRelay,
		r.node.NodeID,
		targetLeader,
		r.node.ShardID,
		0,
		0,
		0,
		relay,
	)

	if err != nil {
		r.recordOutboundRelaySendFailure(
			relay,
			"build_envelope: "+err.Error(),
		)
		return false
	}

	if err := r.sendToNode(
		ctx,
		targetLeader,
		envelope,
	); err != nil {
		r.recordOutboundRelaySendFailure(
			relay,
			"send: "+err.Error(),
		)
		return false
	}

	r.mu.Lock()
	delete(r.outboundRelaySendErrors, logicalID)
	r.lastProgressAt = time.Now().UnixMilli()
	r.mu.Unlock()

	return true
}

func (r *NodeRuntime) recordOutboundRelaySendFailure(
	relay Relay,
	reason string,
) {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return
	}

	r.mu.Lock()

	if r.outboundRelaySendErrors == nil {
		r.outboundRelaySendErrors =
			map[string]string{}
	}

	previous := r.outboundRelaySendErrors[logicalID]
	r.outboundRelaySendErrors[logicalID] = reason

	r.mu.Unlock()

	if previous != reason {
		r.recordEvent(
			logicalID,
			relay.SourceShard,
			relay.TargetShard,
			"RelaySendFailed",
			false,
			reason,
		)
	}
}

func (r *NodeRuntime) queueFinalize(
	ctx context.Context,
	relay Relay,
) {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return
	}

	finish := r.crossShardPlugin().BuildFinalize(
		CrossShardFinalizeInput{
			TxID:        relay.Tx.TxID,
			LogicalTxID: logicalID,
			SourceShard: relay.SourceShard,
			TargetShard: relay.TargetShard,
		},
	)

	r.mu.Lock()

	if r.pendingFinalizeMessages == nil {
		r.pendingFinalizeMessages =
			map[string]Finalize{}
	}

	r.pendingFinalizeMessages[logicalID] = finish
	delete(r.finalizeRetryAfter, logicalID)

	delete(r.relaySource, logicalID)
	delete(r.relayAdmissionFailures, logicalID)
	delete(
		r.relayAdmissionFailures,
		relay.Tx.TxID,
	)

	r.lastProgressAt = time.Now().UnixMilli()

	r.mu.Unlock()

	if r.sendPendingFinalize(ctx, finish) {
		r.deferFinalizeRetry(finish)
	}
}

func (r *NodeRuntime) sendPendingFinalize(
	ctx context.Context,
	finish Finalize,
) bool {
	logicalID := finalizeLogicalID(finish)

	if logicalID == "" {
		return false
	}

	r.mu.Lock()

	pendingFinish, exists :=
		r.pendingFinalizeMessages[logicalID]

	r.mu.Unlock()

	if !exists {
		return true
	}

	finish = pendingFinish

	sourceLeader := r.leaderID(finish.SourceShard)

	if sourceLeader == "" {
		r.recordFinalizeSendFailure(
			finish,
			"source_leader_not_found",
		)
		return false
	}

	envelope, err := p2p.NewEnvelope(
		finalizeMessage,
		r.node.NodeID,
		sourceLeader,
		r.node.ShardID,
		0,
		0,
		0,
		finish,
	)

	if err != nil {
		r.recordFinalizeSendFailure(
			finish,
			"build_envelope: "+err.Error(),
		)
		return false
	}

	if err := r.sendToNode(
		ctx,
		sourceLeader,
		envelope,
	); err != nil {
		r.recordFinalizeSendFailure(
			finish,
			"send: "+err.Error(),
		)
		return false
	}

	r.mu.Lock()
	delete(r.finalizeSendErrors, logicalID)
	r.lastProgressAt = time.Now().UnixMilli()
	r.mu.Unlock()

	// TCP write success only proves local delivery to the socket.
	// Keep Finalize pending until the source leader returns FinalizeAck.
	return true
}

func (r *NodeRuntime) recordFinalizeSendFailure(
	finish Finalize,
	reason string,
) {
	logicalID := finalizeLogicalID(finish)

	if logicalID == "" {
		return
	}

	r.mu.Lock()

	if r.finalizeSendErrors == nil {
		r.finalizeSendErrors = map[string]string{}
	}

	previous := r.finalizeSendErrors[logicalID]
	r.finalizeSendErrors[logicalID] = reason

	r.mu.Unlock()

	if previous != reason {
		r.recordEvent(
			logicalID,
			finish.SourceShard,
			finish.TargetShard,
			"FinalizeSendFailed",
			false,
			reason,
		)
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
	committed, err := r.store.HasTransaction(relay.Tx.TxID)
	if err != nil || !committed {
		return
	}

	if !r.isCurrentLeader() {
		r.clearCommittedRelayReplica(relay)
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
	r.queueFinalize(ctx, relay)
}

func (r *NodeRuntime) catchupNeeded() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.catchupTargetHeight > r.committedHeight
}

func (r *NodeRuntime) noteCatchupTarget(height uint64) {
	if height == 0 {
		return
	}
	r.mu.Lock()
	if height > r.committedHeight && height > r.catchupTargetHeight {
		r.catchupTargetHeight = height
		r.incrementRuntimeMetricLocked("pbft_catchup_target_advanced_count")
	}
	r.mu.Unlock()
}

func (r *NodeRuntime) requestCatchup(ctx context.Context) {
	r.mu.Lock()
	from := r.committedHeight + 1
	knownHeight := r.committedHeight
	knownHash := r.committedHash
	targetHeight := r.catchupTargetHeight
	urgent := targetHeight >= from && targetHeight > knownHeight
	interval := r.catchupRequestInterval(urgent)
	if !r.lastCatchupRequest.IsZero() && time.Since(r.lastCatchupRequest) < interval {
		r.mu.Unlock()
		return
	}
	r.lastCatchupRequest = time.Now()
	r.incrementRuntimeMetricLocked("pbft_catchup_request_count")
	requestOrdinal := r.runtimeMetricCounts["pbft_catchup_request_count"]
	if urgent {
		r.incrementRuntimeMetricLocked("pbft_catchup_urgent_request_count")
	}
	r.mu.Unlock()
	if from == 0 {
		return
	}

	to := from + 8
	if urgent {
		to = from + certifiedCatchupBatchSize - 1
		if targetHeight >= from && targetHeight < to {
			to = targetHeight
		}
	}
	request := CatchupRequest{ShardID: r.node.ShardID, FromHeight: from, ToHeight: to, KnownHeight: knownHeight, KnownHash: knownHash}
	// Rotate a certified catch-up source on each retry. A successful TCP send
	// does not prove that a busy primary can service the requested proof range.
	// Safety is unchanged because catch-up blocks still require PBFT commit certs.
	for _, validator := range rotateCatchupPeers(r.catchupPeerCandidates(), requestOrdinal) {
		envelope, err := p2p.NewEnvelope(catchupRequestMessage, r.node.NodeID, validator, r.node.ShardID, from, r.currentPBFTView(), from, request)
		if err != nil {
			continue
		}
		if err := r.sendCatchupToNode(ctx, validator, envelope); err == nil {
			r.mu.Lock()
			r.incrementRuntimeMetricLocked("pbft_catchup_request_peer_count")
			r.mu.Unlock()
			return
		}
	}
}

// catchupPeerCandidates returns a deterministic, leader-first source order.
// A certified block is authenticated by its PBFT commit certificate, so a
// lagging replica only needs one healthy peer at a time. Broadcasting the same
// catch-up range to every validator multiplies large block payloads without
// adding safety. Failed sends fall through to the next validator for liveness.
func (r *NodeRuntime) catchupPeerCandidates() []string {
	leader := ""
	if state := r.pbftState(); state != nil {
		leader = state.Leader()
	}
	out := make([]string, 0, len(r.node.Validators))
	seen := map[string]bool{}
	appendPeer := func(nodeID string) {
		if nodeID == "" || nodeID == r.node.NodeID || seen[nodeID] {
			return
		}
		seen[nodeID] = true
		out = append(out, nodeID)
	}
	appendPeer(leader)
	for _, validator := range r.node.Validators {
		appendPeer(validator)
	}
	return out
}

func rotateCatchupPeers(peers []string, requestOrdinal int64) []string {
	out := append([]string(nil), peers...)
	if len(out) <= 1 {
		return out
	}
	offset := int((requestOrdinal - 1) % int64(len(out)))
	if offset < 0 {
		offset = 0
	}
	rotated := make([]string, 0, len(out))
	rotated = append(rotated, out[offset:]...)
	rotated = append(rotated, out[:offset]...)
	return rotated
}

func (r *NodeRuntime) catchupRequestInterval(urgentMode ...bool) time.Duration {
	urgent := len(urgentMode) > 0 && urgentMode[0]
	if urgent {
		return urgentCatchupRequestInterval
	}
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
	r := &NodeRuntime{plan: plan, node: node, peers: peers, pool: pool, proposer: realblock.NewProposer(node.NodeID, node.ShardID), db: db, store: store, consensus: pbft.NewState(node.NodeID, node.ShardID, initialLeaderID(plan, node.ShardID), node.Validators), proposals: map[string]realblock.Block{}, verifiedExecutionPlans: map[string]verifiedExecutionPlanRecord{}, deferredPrePrepares: map[uint64]deferredPrePrepare{}, votes: map[string]map[string]bool{}, committed: map[string]bool{}, committing: map[string]bool{}, queuedCommitTasks: map[string]bool{}, pendingCommits: map[uint64]realblock.Block{}, pendingCommitErrors: map[uint64]string{}, committedHash: "genesis", lastProgressAt: time.Now().UnixMilli(), relaySource: map[string]Relay{}, pendingOutboundRelays: map[string]Relay{}, pendingFinalizeMessages: map[string]Finalize{}, outboundRelaySendErrors: map[string]string{}, finalizeSendErrors: map[string]string{}, crossEventSeen: map[string]bool{}, relayAdmissionFailures: map[string]string{}, runtimeMetricCounts: map[string]int64{}, stateFetchWaiters: map[string]chan StateFetchResponse{}, pendingStateFetches: map[string]StateFetchDiagnostic{}, stateFetchWitnesses: map[string]StateFetchResponse{}, stateFetchSnapshots: map[string]map[string]string{}, stateFetchSnapshotRoots: map[string]string{}, stateVersionInitial: map[string]string{}, stateVersionValues: map[string]map[uint64]string{}, stateVersionMaterialized: map[string]uint64{}, stateVersionSignals: map[string]chan struct{}{}, stateVersionRemoteSubscriptions: map[string]map[string]stateVersionRemoteSubscription{}, stateApplyWaiters: map[string]chan StateDeltaApplyAck{}, pendingStateDeltaKeys: map[string]bool{}, appliedStateDeltaKeys: map[string]bool{}, pluginSnapshot: node.PluginProfile, plugins: plugins}
	for qualifiedKey, value := range plugins.StateStorage.Snapshot(db) {
		if key, ok := unqualifiedLocalKey(qualifiedKey, node.ShardID); ok {
			r.stateVersionInitial[key] = value
		}
	}
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

func (r *NodeRuntime) Start(ctx context.Context) error {
	r.startStateFetchWorkers(ctx)
	if err := r.transport.Start(ctx); err != nil {
		r.stopStateFetchWorkers()
		return err
	}
	r.startCommitWorker(ctx)
	return nil
}

func (r *NodeRuntime) Stop() error {
	r.stopProposalPlanning()
	r.stopCommitWorker()
	r.stopStateFetchWorkers()
	return r.transport.Stop()
}

func (r *NodeRuntime) handle(ctx context.Context, msg p2p.MessageEnvelope) error {
	switch msg.MessageType {
	case p2p.MessageTXGossip:
		item, err := p2p.DecodePayload[tx.SignedTransaction](msg)
		if err != nil {
			return err
		}
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "received", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: true})
		if err := r.admitTransaction(item); err != nil {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: false, Error: "transaction_admission:" + err.Error()})
			return fmt.Errorf("transaction admission %s", err)
		}
		result := r.pool.Admit(item)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: false, Error: result.RejectReason})
			return fmt.Errorf("admission %s", result.RejectReason)
		}
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "admitted", NodeID: r.node.NodeID, ShardID: r.node.ShardID, Success: true})
		if msg.FromNode == "mbe-client" {
			// Transaction gossip is not a primary-only consensus action. If the
			// client is still connected to the initial ingress node after a view
			// change, gossiping from that validator ensures the new primary sees
			// subsequent transactions without requiring client-side leader discovery.
			return r.gossip(ctx, item)
		}
	case p2p.MessagePBFTPrePrepare:
		return r.handlePBFTPrePrepare(ctx, msg)
	case p2p.MessagePBFTPrepare:
		return r.handlePBFTPrepare(ctx, msg)
	case p2p.MessagePBFTCommit:
		return r.handlePBFTCommit(ctx, msg)
	case p2p.MessagePBFTViewChange:
		return r.handlePBFTViewChange(ctx, msg)
	case p2p.MessagePBFTNewView:
		return r.handlePBFTNewView(ctx, msg)
	case p2p.MessagePBFTCheckpoint:
		return r.handlePBFTCheckpoint(ctx, msg)
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
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: relay.Tx.TxID, LogicalTxID: tx.SemanticID(relay.Tx), Stage: "relay_received", NodeID: r.node.NodeID, ShardID: r.node.ShardID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard, Success: true})
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
		if r.isCurrentLeader() {
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

		logicalID := finalizeLogicalID(finish)
		if logicalID == "" {
			return fmt.Errorf(
				"finalize has empty logical transaction id",
			)
		}

		if !r.isCurrentLeader() {
			return nil
		}

		if finish.SourceShard != "" &&
			finish.SourceShard != r.node.ShardID {
			return fmt.Errorf(
				"finalize source shard %s does not match node shard %s",
				finish.SourceShard,
				r.node.ShardID,
			)
		}

		r.recordCrossShardEvent(
			r.crossShardPlugin().HandleFinalize(
				CrossShardFinalizeInput{
					TxID:        finish.TxID,
					LogicalTxID: logicalID,
					SourceShard: finish.SourceShard,
					TargetShard: finish.TargetShard,
				},
			),
		)

		// SourceFinalize is durable source-side completion.
		// Duplicate Finalize messages remain idempotent and still receive ACK.
		r.mu.Lock()
		delete(r.pendingOutboundRelays, logicalID)
		delete(r.outboundRelayRetryAfter, logicalID)
		delete(r.outboundRelaySendErrors, logicalID)
		delete(r.relaySource, logicalID)
		delete(r.relayAdmissionFailures, logicalID)
		r.lastProgressAt = time.Now().UnixMilli()
		r.mu.Unlock()

		targetLeader := r.leaderID(finish.TargetShard)
		if targetLeader == "" {
			return fmt.Errorf(
				"finalize ack target leader not found for shard %s",
				finish.TargetShard,
			)
		}

		ackEnvelope, err := p2p.NewEnvelope(
			finalizeAckMessage,
			r.node.NodeID,
			targetLeader,
			r.node.ShardID,
			0,
			0,
			0,
			finish,
		)
		if err != nil {
			return fmt.Errorf(
				"build finalize ack: %w",
				err,
			)
		}

		if err := r.sendToNode(
			ctx,
			targetLeader,
			ackEnvelope,
		); err != nil {
			return fmt.Errorf(
				"send finalize ack: %w",
				err,
			)
		}

	case finalizeAckMessage:
		finish, err := p2p.DecodePayload[Finalize](msg)
		if err != nil {
			return err
		}

		logicalID := finalizeLogicalID(finish)
		if logicalID == "" {
			return fmt.Errorf(
				"finalize ack has empty logical transaction id",
			)
		}

		if !r.isCurrentLeader() {
			return nil
		}

		if finish.TargetShard != "" &&
			finish.TargetShard != r.node.ShardID {
			return fmt.Errorf(
				"finalize ack target shard %s does not match node shard %s",
				finish.TargetShard,
				r.node.ShardID,
			)
		}

		r.mu.Lock()
		delete(r.pendingFinalizeMessages, logicalID)
		delete(r.finalizeRetryAfter, logicalID)
		delete(r.finalizeSendErrors, logicalID)
		r.lastProgressAt = time.Now().UnixMilli()
		r.mu.Unlock()

	case catchupRequestMessage:
		request, err := p2p.DecodePayload[CatchupRequest](msg)
		if err != nil {
			return err
		}
		return r.handleCertifiedCatchupRequest(ctx, msg.FromNode, request)
	case catchupBlockMessage:
		item, err := p2p.DecodePayload[CatchupBlock](msg)
		if err != nil {
			return err
		}
		return r.handleCertifiedCatchupBlock(ctx, msg.FromNode, item)
	case catchupUnavailableMessage:
		item, err := p2p.DecodePayload[CatchupUnavailable](msg)
		if err != nil {
			return err
		}
		return r.handleCatchupUnavailable(msg.FromNode, msg.ShardID, item)
	case stateFetchRequestMessage:
		request, err := p2p.DecodePayload[StateFetchRequest](msg)
		if err != nil {
			return err
		}
		if err := r.enqueueStateFetchTask(msg.FromNode, request); err != nil {
			r.recordStateFetchServiceDiagnostic(msg.FromNode, request, "request_enqueue_error", err)
			return err
		}
		r.recordStateFetchServiceDiagnostic(msg.FromNode, request, "request_enqueued", nil)
		return nil
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

func (r *NodeRuntime) handleCatchupUnavailable(fromNode, shardID string, item CatchupUnavailable) error {
	if fromNode == "" || fromNode != item.SourceNode || !r.pbftState().IsValidator(fromNode) {
		return fmt.Errorf("certified catch-up unavailable source is not a validator")
	}
	if shardID != "" && shardID != r.node.ShardID {
		return fmt.Errorf("certified catch-up unavailable shard mismatch")
	}
	if item.RequestedFromHeight == 0 || item.StableCheckpointHeight > item.CommittedHeight {
		return fmt.Errorf("certified catch-up unavailable metadata is invalid")
	}
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("pbft_catchup_unavailable_count")
	r.lastProposalError = fmt.Sprintf("pbft catch-up unavailable from %s: %s; requested_from=%d; source_committed=%d; stable_checkpoint=%d", item.SourceNode, item.Reason, item.RequestedFromHeight, item.CommittedHeight, item.StableCheckpointHeight)
	r.mu.Unlock()
	// CatchupUnavailable is an unsigned liveness hint, not a PBFT certificate.
	// It must never manufacture a larger recovery target; target heights remain
	// driven by consensus-observed future PRE-PREPARE/checkpoint evidence.
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

func (r *NodeRuntime) deferPrePrepare(fromNode string, block realblock.Block) {
	view := r.currentPBFTView()
	_ = r.deferPBFTPrePrepareWithDisposition(fromNode, pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  fromNode,
		BlockHash: block.BlockHash,
		Block:     block,
	})
}

func (r *NodeRuntime) deferPrePrepareWithDisposition(fromNode string, block realblock.Block) prePrepareDeferralDisposition {
	view := r.currentPBFTView()
	return r.deferPBFTPrePrepareWithDisposition(fromNode, pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  fromNode,
		BlockHash: block.BlockHash,
		Block:     block,
	})
}

func (r *NodeRuntime) handleDeferredPrePrepare(ctx context.Context, fromNode string, block realblock.Block, replayed bool) error {
	view := r.currentPBFTView()
	return r.deferPBFTPrePrepare(ctx, fromNode, pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  fromNode,
		BlockHash: block.BlockHash,
		Block:     block,
	}, replayed)
}

func (r *NodeRuntime) retainDeferredPrePrepare(fromNode string, block realblock.Block) {
	view := r.currentPBFTView()
	_ = r.deferPBFTPrePrepareWithDisposition(fromNode, pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  fromNode,
		BlockHash: block.BlockHash,
		Block:     block,
	})
}

func (r *NodeRuntime) acceptPrePrepare(ctx context.Context, fromNode string, block realblock.Block, replayed bool) error {
	view := r.currentPBFTView()
	pre := pbft.PrePrepare{
		View:      view,
		Sequence:  block.Height,
		Height:    block.Height,
		LeaderID:  fromNode,
		BlockHash: block.BlockHash,
		Block:     block,
	}
	if replayed {
		r.logConsensus("PBFT_PRE_PREPARE_REPLAYED", fromNode, block.BlockHash, block.Height)
	}
	prepare, err := r.pbftState().OnPrePrepare(pre)
	if err != nil {
		return err
	}
	r.rememberProposal(block)
	return r.broadcastPBFTPrepare(ctx, prepare)
}

func (r *NodeRuntime) replayDeferredPrePrepare(ctx context.Context) {
	r.replayDeferredPBFTPrePrepare(ctx)
}

func (r *NodeRuntime) propose(ctx context.Context) {
	if !r.isCurrentLeader() {
		return
	}
	if snapshot := r.pbftState().Snapshot(); snapshot.Stage == pbft.StageViewChange {
		return
	}
	r.mu.Lock()
	fatal := firstNonEmpty(r.fatalPersistenceError, r.fatalExecutionError, r.fatalPlanningError)
	r.mu.Unlock()
	if fatal != "" {
		return
	}
	r.mu.Lock()
	if r.proposalInFlight || r.proposalPlanningInFlight {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	nextHeight := r.proposer.NextHeight
	readySystemDeltas, systemDrainPending := r.remoteStateDeltaDrainState(nextHeight)

	// SystemDeltaReady also acts as the block-producer wake-up signal.
	// Pending deltas that are not ready yet still require deterministic
	// PBFT height-advance blocks, otherwise the home shard can stop below
	// the source-height readiness boundary forever.
	var productionSnapshot map[string]string
	if r.plugins.BlockProducer != nil && (r.plugins.BlockProducer.ID() == groundhogBlockProducerID || r.plugins.BlockProducer.ID() == ariaBlockProducerID) {
		productionSnapshot = r.plugins.StateStorage.Snapshot(r.db)
	}
	routingPluginID := ""
	if r.plugins.Routing != nil {
		routingPluginID = r.plugins.Routing.ID()
	}
	input := BlockProductionInput{
		Pool:              r.pool,
		Proposer:          r.proposer,
		Limit:             r.blockSize(),
		Now:               time.Now(),
		SystemDeltaReady:  systemDrainPending,
		Context:           ctx,
		BaseStateSnapshot: productionSnapshot,
		WorkerCount:       blockExecutorWorkerCountFromProfile(r.pluginSnapshot),
		RoutingPluginID:   routingPluginID,
	}
	if !r.plugins.BlockProducer.ShouldProduce(input) {
		return
	}

	block, err := r.plugins.BlockProducer.BuildCandidate(input)
	if err != nil && errors.Is(err, errMetaTrackBatchProjectionIncomplete) {
		if systemDrainPending {
			block = r.buildSystemDeltaDrainBlock(input.Now, readySystemDeltas)
			err = nil
		} else {
			return
		}
	}
	if err != nil && err.Error() == "empty_mempool" && systemDrainPending {
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
	if r.statelessVersionAdmissionEnabled(block) {
		originalCandidate := append([]tx.SignedTransaction(nil), block.TxList...)
		admitted, deferred, admissionErr := r.admitStatelessVersionCandidate(ctx, block)
		if admissionErr != nil {
			r.pool.ReleaseReserved(originalCandidate)
			r.setLastProposalError(admissionErr)
			return
		}
		if len(deferred) > 0 {
			r.pool.ReleaseReserved(deferred)
		}
		if len(admitted.TxList) == 0 {
			if systemDrainPending {
				block = r.buildSystemDeltaDrainBlock(input.Now, readySystemDeltas)
			} else {
				return
			}
		} else {
			block = admitted
		}
	}
	if planner, ok := r.plugins.Scheduler.(contextConsensusExecutionPlanner); ok && len(block.TxList) > 0 {
		if r.startContextProposalPlanning(ctx, block, planner) {
			return
		}
		// A planning task became active between candidate reservation and the
		// transition above. Do not strand this independently reserved candidate.
		r.pool.ReleaseReserved(block.TxList)
		return
	}

	scheduledBlock, scheduleErr := r.scheduleBlock(block)
	if scheduleErr != nil {
		r.pool.ReleaseReserved(block.TxList)
		r.mu.Lock()
		r.lastProposalError = scheduleErr.Error()
		r.mu.Unlock()
		return
	}
	block = scheduledBlock
	proposalWorkUnits := r.estimateProposalValidationWork(block)
	for _, item := range block.TxList {
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "proposed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
	}
	if err := r.beginPBFTProposal(ctx, block, proposalWorkUnits); err != nil {
		r.pool.ReleaseReserved(block.TxList)
		r.setLastProposalError(err)
	}
}

func (r *NodeRuntime) startContextProposalPlanning(ctx context.Context, block realblock.Block, planner contextConsensusExecutionPlanner) bool {
	if planner == nil || len(block.TxList) == 0 {
		return false
	}
	state := r.pbftState()
	view := state.View()
	if state.Leader() != r.node.NodeID {
		return false
	}
	planCtx, cancel := context.WithCancel(ctx)
	now := time.Now()
	r.mu.Lock()
	if r.proposalInFlight || r.proposalPlanningInFlight || r.committedHeight+1 != block.Height {
		r.mu.Unlock()
		cancel()
		return false
	}
	r.proposalPlanningGeneration++
	generation := r.proposalPlanningGeneration
	r.proposalPlanningInFlight = true
	r.proposalPlanningView = view
	r.proposalPlanningHeight = block.Height
	r.proposalPlanningAlgorithmID = r.plugins.Scheduler.ID()
	r.proposalPlanningPhase = "started"
	r.proposalPlanningStartedAt = now
	r.proposalPlanningProgressAt = now
	r.proposalPlanningWorkUnits = 1
	r.proposalPlanningDetailCount = 0
	r.proposalPlanningCancel = cancel
	r.proposalPlanningCancelReason = ""
	r.lastProgressAt = now.UnixMilli()
	r.mu.Unlock()

	r.proposalPlanningWG.Add(1)
	go func() {
		defer r.proposalPlanningWG.Done()
		report := func(progress consensusPlanningProgress) {
			r.recordProposalPlanningProgress(generation, progress)
		}
		planned, err := planner.PlanBlockContext(planCtx, block, report)
		if err != nil {
			r.pool.ReleaseReserved(block.TxList)
			if isFatalConsensusPlanningError(err) {
				r.markFatalPlanningError(err)
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				r.setLastProposalError(err)
			}
			r.finishProposalPlanning(generation)
			return
		}
		if !r.proposalPlanningStillCurrent(generation, view, block.Height) {
			r.pool.ReleaseReserved(block.TxList)
			r.finishProposalPlanning(generation)
			return
		}

		if len(planned.Deferred) > 0 {
			r.pool.ReleaseReserved(planned.Deferred)
		}
		scheduledBlock := planned.Block
		scheduledBlock.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(scheduledBlock.Height)
		realblock.AssignHash(&scheduledBlock)
		if !r.proposalPlanningStillCurrent(generation, view, block.Height) {
			r.pool.ReleaseReserved(scheduledBlock.TxList)
			r.finishProposalPlanning(generation)
			return
		}

		r.recordScheduleEvents(scheduledBlock, planned.Events, true)
		r.rememberVerifiedExecutionPlan(scheduledBlock)
		proposalWorkUnits := r.estimateProposalValidationWork(scheduledBlock)
		for _, item := range scheduledBlock.TxList {
			r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "proposed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: scheduledBlock.Height, Success: true})
		}
		if err := r.beginPBFTProposal(ctx, scheduledBlock, proposalWorkUnits); err != nil {
			r.pool.ReleaseReserved(scheduledBlock.TxList)
			r.setLastProposalError(err)
		}
		r.finishProposalPlanning(generation)
	}()
	return true
}

func (r *NodeRuntime) recordProposalPlanningProgress(generation uint64, progress consensusPlanningProgress) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.proposalPlanningInFlight || r.proposalPlanningGeneration != generation {
		return
	}
	advanced := progress.WorkUnits > r.proposalPlanningWorkUnits || progress.DetailCount > r.proposalPlanningDetailCount || (progress.Phase != "" && progress.Phase != r.proposalPlanningPhase)
	if !advanced {
		return
	}
	if progress.AlgorithmID != "" {
		r.proposalPlanningAlgorithmID = progress.AlgorithmID
	}
	if progress.Phase != "" {
		r.proposalPlanningPhase = progress.Phase
	}
	if progress.WorkUnits > r.proposalPlanningWorkUnits {
		r.proposalPlanningWorkUnits = progress.WorkUnits
	}
	if progress.DetailCount > r.proposalPlanningDetailCount {
		r.proposalPlanningDetailCount = progress.DetailCount
	}
	r.proposalPlanningProgressAt = now
	r.lastProgressAt = now.UnixMilli()
}

func (r *NodeRuntime) proposalPlanningStillCurrent(generation, view, height uint64) bool {
	state := r.pbftState()
	if state.View() != view || state.Leader() != r.node.NodeID {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.proposalPlanningInFlight && r.proposalPlanningGeneration == generation && r.proposalPlanningView == view && r.proposalPlanningHeight == height && r.committedHeight+1 == height
}

func (r *NodeRuntime) finishProposalPlanning(generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.proposalPlanningGeneration != generation {
		return
	}
	r.proposalPlanningInFlight = false
	r.proposalPlanningCancel = nil
	r.proposalPlanningPhase = "idle"
}

func (r *NodeRuntime) cancelStaleProposalPlanning() {
	state := r.pbftState()
	view := state.View()
	leader := state.Leader()
	var cancel context.CancelFunc
	r.mu.Lock()
	if r.proposalPlanningInFlight && (r.proposalPlanningView != view || leader != r.node.NodeID) {
		cancel = r.proposalPlanningCancel
		r.proposalPlanningCancelReason = fmt.Sprintf("stale_view_or_leader:planned_view=%d current_view=%d current_leader=%s", r.proposalPlanningView, view, leader)
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *NodeRuntime) stopProposalPlanning() {
	var cancel context.CancelFunc
	r.mu.Lock()
	if r.proposalPlanningInFlight {
		cancel = r.proposalPlanningCancel
		r.proposalPlanningCancelReason = "runtime_stop"
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.proposalPlanningWG.Wait()
}

type statelessVersionAdmissionRequirement struct {
	token       string
	key         string
	version     uint64
	homeShard   string
	transaction tx.SignedTransaction
}

type statelessVersionAdmissionProbeResult struct {
	token string
	ready bool
	err   error
}

func (r *NodeRuntime) statelessVersionAdmissionEnabled(block realblock.Block) bool {
	if r == nil || len(block.TxList) == 0 || len(r.shardIDs()) < 2 || !routingUsesStatelessVersionAdmission(r.plugins.Routing) {
		return false
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting != nil && len(item.ExecutionRouting.StateVersions) > 0 {
			return true
		}
	}
	return false
}

func statelessVersionAdmissionToken(key string, version uint64) string {
	return key + "@" + fmt.Sprint(version)
}

// admitStatelessVersionCandidate prevents a PBFT proposal from binding a
// stateless transaction whose exact predecessor version can only be produced
// outside the current candidate and is not yet published at its persistent
// home.  Dependencies produced by another transaction in this same candidate
// remain admissible and are resolved by the existing versioned wave executor.
// The function never substitutes a latest value and never changes a required
// logical version.
func (r *NodeRuntime) admitStatelessVersionCandidate(ctx context.Context, block realblock.Block) (realblock.Block, []tx.SignedTransaction, error) {
	if !r.statelessVersionAdmissionEnabled(block) {
		return block, nil, nil
	}
	shardIDs := r.shardIDs()
	producerIndex := map[string]int{}
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.ProducedVersion == 0 {
				continue
			}
			producerIndex[statelessVersionAdmissionToken(dependency.Key, dependency.ProducedVersion)] = index
		}
	}

	external := map[string]statelessVersionAdmissionRequirement{}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.RequiredVersion == 0 {
				continue
			}
			token := statelessVersionAdmissionToken(dependency.Key, dependency.RequiredVersion)
			if _, internal := producerIndex[token]; internal {
				continue
			}
			homeShard := r.homeShardFor([]string{dependency.Key}, shardIDs)
			if homeShard == "" {
				return realblock.Block{}, nil, fmt.Errorf("stateless version admission has no home shard for %s", dependency.Key)
			}
			external[token] = statelessVersionAdmissionRequirement{token: token, key: dependency.Key, version: dependency.RequiredVersion, homeShard: homeShard, transaction: item}
		}
	}

	externalReady, err := r.probeStatelessVersionAdmissionRequirements(ctx, block, external)
	if err != nil {
		return realblock.Block{}, nil, err
	}
	selected := make([]bool, len(block.TxList))
	progress := true
	for progress {
		progress = false
		for index, item := range block.TxList {
			if selected[index] {
				continue
			}
			ready := true
			if item.ExecutionRouting != nil {
				for _, dependency := range item.ExecutionRouting.StateVersions {
					if dependency.Key == "" || dependency.RequiredVersion == 0 {
						continue
					}
					token := statelessVersionAdmissionToken(dependency.Key, dependency.RequiredVersion)
					if producer, internal := producerIndex[token]; internal {
						if !selected[producer] {
							ready = false
							break
						}
						continue
					}
					if !externalReady[token] {
						ready = false
						break
					}
				}
			}
			if ready {
				selected[index] = true
				progress = true
			}
		}
	}

	admittedItems := make([]tx.SignedTransaction, 0, len(block.TxList))
	deferred := make([]tx.SignedTransaction, 0, len(block.TxList))
	for index, item := range block.TxList {
		if selected[index] {
			admittedItems = append(admittedItems, item)
		} else {
			deferred = append(deferred, item)
		}
	}
	r.addRuntimeMetric("stateless_version_admission_candidate_tx_count", int64(len(block.TxList)))
	r.addRuntimeMetric("stateless_version_admission_admitted_tx_count", int64(len(admittedItems)))
	r.addRuntimeMetric("stateless_version_admission_deferred_event_count", int64(len(deferred)))
	if len(admittedItems) == 0 {
		return realblock.Block{}, deferred, nil
	}
	admitted, err := r.proposer.BuildFromReserved(admittedItems, time.UnixMilli(block.Timestamp))
	if err != nil {
		return realblock.Block{}, deferred, err
	}
	return admitted, deferred, nil
}

func (r *NodeRuntime) probeStatelessVersionAdmissionRequirements(ctx context.Context, block realblock.Block, requirements map[string]statelessVersionAdmissionRequirement) (map[string]bool, error) {
	ready := make(map[string]bool, len(requirements))
	if len(requirements) == 0 {
		return ready, nil
	}
	items := make([]statelessVersionAdmissionRequirement, 0, len(requirements))
	for _, requirement := range requirements {
		items = append(items, requirement)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].token < items[j].token })
	workerCount := 16
	if len(items) < workerCount {
		workerCount = len(items)
	}
	jobs := make(chan statelessVersionAdmissionRequirement, len(items))
	results := make(chan statelessVersionAdmissionProbeResult, len(items))
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for requirement := range jobs {
				isReady, probeErr := r.probeStatelessVersionAdmissionOnce(ctx, block, requirement)
				results <- statelessVersionAdmissionProbeResult{token: requirement.token, ready: isReady, err: probeErr}
			}
		}()
	}
	for _, requirement := range items {
		jobs <- requirement
	}
	close(jobs)
	wg.Wait()
	close(results)
	var firstErr error
	notReady := int64(0)
	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		ready[result.token] = result.ready
		if !result.ready && result.err == nil {
			notReady++
		}
	}
	r.addRuntimeMetric("stateless_version_admission_probe_count", int64(len(items)))
	r.addRuntimeMetric("stateless_version_admission_probe_not_ready_count", notReady)
	if firstErr != nil {
		return ready, firstErr
	}
	return ready, nil
}

func (r *NodeRuntime) probeStatelessVersionAdmissionOnce(ctx context.Context, block realblock.Block, requirement statelessVersionAdmissionRequirement) (bool, error) {
	if requirement.version == 0 {
		return true, nil
	}
	if requirement.homeShard == r.node.ShardID {
		_, ready := r.stateVersionValue(requirement.key, requirement.version)
		return ready, nil
	}
	targetNode := r.leaderID(requirement.homeShard)
	if targetNode == "" {
		return false, fmt.Errorf("stateless version admission home leader missing for %s", requirement.homeShard)
	}
	requestID := stableTextDigest(strings.Join([]string{"admission", r.node.NodeID, requirement.transaction.TxID, fmt.Sprint(block.Height), requirement.key, requirement.homeShard, fmt.Sprint(requirement.version), fmt.Sprint(time.Now().UnixNano())}, "|"))
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
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: requirement.transaction.TxID, BlockHash: block.BlockHash, Key: requirement.key, HomeShard: requirement.homeShard, ExecutionShard: r.node.ShardID, AccessKind: statelessVersionAdmissionProbeAccessKind, RequiredVersion: requirement.version, Versioned: true})
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
	if err != nil {
		return false, err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		return false, err
	}
	timer := time.NewTimer(750 * time.Millisecond)
	defer timer.Stop()
	select {
	case response := <-waiter:
		if response.Success {
			return response.Versioned && response.StateVersion == requirement.version, nil
		}
		if response.Error == "state_version_not_ready" {
			return false, nil
		}
		return false, fmt.Errorf("stateless version admission probe failed: %s", response.Error)
	case <-timer.C:
		return false, fmt.Errorf("stateless version admission probe timed out for %s version %d from %s", requirement.key, requirement.version, requirement.homeShard)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (r *NodeRuntime) addRuntimeMetric(name string, value int64) {
	if value == 0 {
		return
	}
	r.mu.Lock()
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts[name] += value
	r.mu.Unlock()
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

func (r *NodeRuntime) scheduleBlock(block realblock.Block) (realblock.Block, error) {
	if planner, ok := r.plugins.Scheduler.(ConsensusExecutionPlanner); ok {
		planned, err := planner.PlanBlock(block)
		if err != nil {
			return block, err
		}
		if len(planned.Deferred) > 0 {
			r.pool.ReleaseReserved(planned.Deferred)
		}
		block = planned.Block
		r.recordScheduleEvents(block, planned.Events, true)
		block.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(block.Height)
		realblock.AssignHash(&block)
		// The primary has just produced this exact consensus-bound schedule.
		// Record the immutable block/plan identity so execution does not need
		// to rebuild the same deterministic planner output after PBFT.
		r.rememberVerifiedExecutionPlan(block)
		return block, nil
	}
	if r.shouldAttachMetaTrackExecutionPlan() {
		if err := r.attachMetaTrackExecutionPlan(&block); err != nil {
			return block, err
		}
		r.recordPlannedDependencyPlan(block)
		block.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(block.Height)
		realblock.AssignHash(&block)
		return block, nil
	}
	schedule := r.plugins.Scheduler.Schedule(block.TxList, r.plugins.Execution)
	ordered := schedule.Ordered
	if len(ordered) != len(block.TxList) {
		return block, fmt.Errorf("scheduler returned %d transactions for %d input transactions", len(ordered), len(block.TxList))
	}
	r.recordScheduleEvents(block, schedule.Events, true)
	block.TxList = ordered
	block.TxIDs = make([]string, 0, len(ordered))
	for _, item := range ordered {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	block.SystemStateDeltas = r.readyRemoteStateDeltasForConsensus(block.Height)
	realblock.AssignHash(&block)
	return block, nil
}

func (r *NodeRuntime) shouldAttachMetaTrackExecutionPlan() bool {
	if r.plugins.Routing == nil {
		return false
	}
	_, ok := r.plugins.Routing.(BatchRoutingPlugin)
	return ok
}

func (r *NodeRuntime) batchExecutionPlanAlgorithmID() string {
	return batchExecutionPlanAlgorithmIDForRouting(r.plugins.Routing)
}

func (r *NodeRuntime) attachMetaTrackExecutionPlan(block *realblock.Block) error {
	if block == nil {
		return nil
	}
	if _, ok := r.plugins.Routing.(BatchRoutingPlugin); !ok {
		return nil
	}
	payload, err := r.metaTrackExecutionPlanPayload(*block)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	payloadDigest := stableTextDigest(string(raw))
	algorithmID := r.batchExecutionPlanAlgorithmID()
	if algorithmID == "" {
		return fmt.Errorf("batch execution plan algorithm id is empty")
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: algorithmID, PayloadDigest: payloadDigest, PlanDigest: payloadDigest, Payload: append([]byte(nil), raw...)}
	return nil
}

func (r *NodeRuntime) verifyExecutionPlanEnvelope(block realblock.Block) error {
	if err := r.verifyProposalEvidenceEnvelope(block); err != nil {
		return err
	}
	planner, requiresConsensusPlan := r.plugins.Scheduler.(ConsensusExecutionPlanner)
	if block.ExecutionPlan == nil {
		if requiresConsensusPlan {
			return fmt.Errorf("scheduler %s requires a pre-consensus execution plan", r.plugins.Scheduler.ID())
		}
	} else {
		if block.ExecutionPlan.AlgorithmID == "" || block.ExecutionPlan.PayloadDigest == "" || block.ExecutionPlan.PlanDigest == "" || len(block.ExecutionPlan.Payload) == 0 {
			return fmt.Errorf("invalid execution plan envelope")
		}
		if digest := stableTextDigest(string(block.ExecutionPlan.Payload)); digest != block.ExecutionPlan.PayloadDigest {
			return fmt.Errorf("execution plan payload digest mismatch")
		}
		if requiresConsensusPlan {
			// PRE-PREPARE validation fully verifies and remembers this exact
			// immutable block/plan identity. A later COMMIT for the same bytes
			// must not rebuild the same expensive graph/table a second time.
			// Any changed block hash or plan digest misses this provenance and
			// falls back to the full semantic verifier below.
			if !r.hasVerifiedExecutionPlan(block) {
				if err := planner.VerifyBlockPlan(block); err != nil {
					return err
				}
			}
		} else {
			expectedAlgorithmID := r.batchExecutionPlanAlgorithmID()
			if expectedAlgorithmID == "" || block.ExecutionPlan.AlgorithmID != expectedAlgorithmID {
				return fmt.Errorf("execution plan algorithm mismatch: expected=%s got=%s", expectedAlgorithmID, block.ExecutionPlan.AlgorithmID)
			}
			if err := r.verifyMetaTrackExecutionPlanPayload(block); err != nil {
				return err
			}
		}
	}
	expected := realblock.Hash(block)
	if block.BlockHash != expected {
		return fmt.Errorf("block hash mismatch after execution plan verification")
	}
	if requiresConsensusPlan {
		// A backup reaches here only after the scheduler's semantic verifier has
		// fully rebuilt and accepted the exact consensus-bound plan. Keep that
		// provenance locally and reuse it only for this exact immutable block.
		r.rememberVerifiedExecutionPlan(block)
	}
	return nil
}

func verifiedExecutionPlanRecordForBlock(block realblock.Block) (verifiedExecutionPlanRecord, bool) {
	if block.BlockHash == "" || block.ExecutionPlan == nil || block.ExecutionPlan.AlgorithmID == "" || block.ExecutionPlan.PayloadDigest == "" || block.ExecutionPlan.PlanDigest == "" {
		return verifiedExecutionPlanRecord{}, false
	}
	return verifiedExecutionPlanRecord{AlgorithmID: block.ExecutionPlan.AlgorithmID, PayloadDigest: block.ExecutionPlan.PayloadDigest, PlanDigest: block.ExecutionPlan.PlanDigest}, true
}

func (r *NodeRuntime) rememberVerifiedExecutionPlan(block realblock.Block) {
	record, ok := verifiedExecutionPlanRecordForBlock(block)
	if !ok {
		return
	}
	r.mu.Lock()
	if r.verifiedExecutionPlans == nil {
		r.verifiedExecutionPlans = map[string]verifiedExecutionPlanRecord{}
	}
	r.verifiedExecutionPlans[block.BlockHash] = record
	r.mu.Unlock()
}

func (r *NodeRuntime) hasVerifiedExecutionPlan(block realblock.Block) bool {
	record, ok := verifiedExecutionPlanRecordForBlock(block)
	if !ok {
		return false
	}
	r.mu.Lock()
	stored, exists := r.verifiedExecutionPlans[block.BlockHash]
	r.mu.Unlock()
	return exists && stored == record
}

func (r *NodeRuntime) verifyProposalEvidenceEnvelope(block realblock.Block) error {
	envelope := block.ProposalEvidence
	if envelope == nil {
		if r.plugins.BlockProducer != nil && (r.plugins.BlockProducer.ID() == ariaBlockProducerID || r.plugins.BlockProducer.ID() == groundhogBlockProducerID) && len(block.TxList) > 0 {
			return fmt.Errorf("%s requires proposal selection evidence", r.plugins.BlockProducer.ID())
		}
		return nil
	}
	if envelope.AlgorithmID == "" || envelope.PayloadDigest == "" || len(envelope.Payload) == 0 {
		return fmt.Errorf("invalid proposal evidence envelope")
	}
	if digest := stableTextDigest(string(envelope.Payload)); digest != envelope.PayloadDigest {
		return fmt.Errorf("proposal evidence payload digest mismatch")
	}
	expectedAlgorithm := ""
	if r.plugins.BlockProducer != nil {
		switch r.plugins.BlockProducer.ID() {
		case ariaBlockProducerID:
			expectedAlgorithm = ariaCandidateSelectionEvidenceID
		case groundhogBlockProducerID:
			expectedAlgorithm = groundhogCandidateSelectionEvidenceID
		}
	}
	if expectedAlgorithm == "" || envelope.AlgorithmID != expectedAlgorithm {
		return fmt.Errorf("proposal evidence algorithm mismatch: expected=%s got=%s", expectedAlgorithm, envelope.AlgorithmID)
	}
	if expectedAlgorithm == ariaCandidateSelectionEvidenceID {
		_, err := decodeAriaCandidateSelectionEvidence(block)
		return err
	}
	var payload struct {
		ShardID       string   `json:"shard_id"`
		Height        uint64   `json:"height"`
		SelectedTxIDs []string `json:"selected_tx_ids"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return fmt.Errorf("proposal evidence payload decode: %w", err)
	}
	if payload.ShardID != block.ShardID || payload.Height != block.Height {
		return fmt.Errorf("proposal evidence block identity mismatch")
	}
	if strings.Join(payload.SelectedTxIDs, "|") != strings.Join(block.TxIDs, "|") {
		return fmt.Errorf("proposal evidence selected transaction list mismatch")
	}
	return nil
}

func (r *NodeRuntime) verifyMetaTrackExecutionPlanPayload(block realblock.Block) error {
	expected, err := r.metaTrackExecutionPlanPayload(block)
	if err != nil {
		return err
	}
	expectedRaw, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if stableTextDigest(string(expectedRaw)) != block.ExecutionPlan.PayloadDigest {
		return fmt.Errorf("batch execution plan semantic recompute mismatch")
	}
	var received map[string]any
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &received); err != nil {
		return fmt.Errorf("batch execution plan payload decode: %w", err)
	}
	expectedDigest := fmt.Sprint(expected["routing_plan_digest"])
	if expectedDigest == "" {
		return fmt.Errorf("batch execution plan missing expected routing digest")
	}
	if fmt.Sprint(received["routing_plan_digest"]) != expectedDigest {
		return fmt.Errorf("batch execution plan routing digest mismatch")
	}
	return nil
}

func (r *NodeRuntime) metaTrackExecutionPlanPayload(block realblock.Block) (map[string]any, error) {
	if routingUsesSignedBatchExecutionPlan(r.plugins.Routing) {
		return r.signedMetaTrackExecutionPlanPayload(block)
	}
	planner, ok := r.plugins.Routing.(BatchRoutingPlugin)
	if !ok {
		return nil, fmt.Errorf("batch execution plan requires batch routing plugin")
	}
	shards := r.shardIDs()
	records := make([]WorkloadRecord, 0, len(block.TxList))
	orderedIDs := make([]string, 0, len(block.TxList))
	accessDigests := make([]string, 0, len(block.TxList))
	for index, item := range block.TxList {
		txID := txIdentifier(item)
		orderedIDs = append(orderedIDs, txID)
		accessDigest := item.AccessListDigest
		if accessDigest == "" && len(item.AccessList) > 0 {
			accessDigest = CanonicalAccessListDigest(item.AccessList)
		}
		accessDigests = append(accessDigests, accessDigest)
		records = append(records, WorkloadRecord{Index: index, LogicalID: txID, SenderID: item.Sender, ReceiverID: item.Receiver, StateKeys: append([]string(nil), item.StateKeys...), AccessList: append([]tx.AccessItem(nil), item.AccessList...), AccessListDigest: accessDigest, SourceShard: block.ShardID})
	}
	routePlan := planner.PlanBatch(BatchRoutingInput{BatchIndex: int(block.Height), Records: records, ShardIDs: shards, Sharding: r.plugins.Sharding})
	routePlan = bindBatchPlanToExecutionShard(routePlan, block.ShardID)
	return map[string]any{
		"algorithm_id":            r.batchExecutionPlanAlgorithmID(),
		"ordered_transaction_ids": orderedIDs,
		"access_list_digests":     accessDigests,
		"routing_plan_digest":     routePlan.PlanDigest,
		"placement_policy":        routePlan.PlacementPolicy,
		"transaction_policy":      routePlan.TransactionPolicy,
		"placement_budget":        routePlan.PlacementBudget,
		"placement_min_budget":    routePlan.PlacementMinBudget,
		"placement_mu":            routePlan.PlacementMu,
		"access_matrix":           routePlan.AccessMatrix,
		"state_frequency":         routePlan.StateFrequency,
		"coaccess_edges":          routePlan.CoaccessEdges,
		"state_placements":        routePlan.StatePlacements,
		"transaction_placements":  routePlan.TransactionPlacements,
		"remote_access_estimate":  routePlan.RemoteAccessEstimate,
	}, nil
}

func (r *NodeRuntime) signedMetaTrackExecutionPlanPayload(block realblock.Block) (map[string]any, error) {
	batchProjection, err := validateMetaTrackBatchProjection(block, false)
	if err != nil {
		return nil, err
	}
	orderedIDs := make([]string, 0, len(block.TxList))
	accessDigests := make([]string, 0, len(block.TxList))
	placements := make([]TransactionPlacement, 0, len(block.TxList))
	accessMatrix := make([]AccessMatrixRow, 0)
	sourceRouteDigests := map[string]bool{}
	remoteEstimate := 0
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil {
			return nil, fmt.Errorf("metatrack transaction %s is missing signed execution routing metadata", txIdentifier(item))
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return nil, fmt.Errorf("metatrack transaction %s routing metadata: %w", txIdentifier(item), err)
		}
		routing := item.ExecutionRouting
		if routing.ExecutionShard != block.ShardID {
			return nil, fmt.Errorf("metatrack transaction %s routed to %s but proposed in %s", txIdentifier(item), routing.ExecutionShard, block.ShardID)
		}
		txID := txIdentifier(item)
		orderedIDs = append(orderedIDs, txID)
		accessDigest := item.AccessListDigest
		if accessDigest == "" && len(item.AccessList) > 0 {
			accessDigest = CanonicalAccessListDigest(item.AccessList)
		}
		accessDigests = append(accessDigests, accessDigest)
		for _, access := range item.AccessList {
			accessMatrix = append(accessMatrix, AccessMatrixRow{LogicalID: txID, TxIndex: index, Key: access.Key, Mode: access.Mode})
		}
		remote := routing.PredictedRemoteReads + routing.PredictedRemoteWrites
		remoteEstimate += remote
		sourceRouteDigests[routing.RoutePlanDigest] = true
		placements = append(placements, TransactionPlacement{
			LogicalID:             txID,
			TxIndex:               index,
			SenderGroupID:         "sender:" + strings.ToLower(item.Sender),
			RoutingEpoch:          routing.RoutingEpoch,
			HomeShard:             shardFor(r.plugins.Sharding, []string{"nonce:" + item.Sender}, r.shardIDs()),
			ExecutionShard:        routing.ExecutionShard,
			CoaccessGroup:         strings.Join(item.StateKeys, "+"),
			Reason:                routing.RoutingReason,
			PredictedRemoteReads:  routing.PredictedRemoteReads,
			PredictedRemoteWrites: routing.PredictedRemoteWrites,
			RemoteAccessCount:     remote,
		})
	}
	sort.Slice(accessMatrix, func(i, j int) bool {
		if accessMatrix[i].TxIndex != accessMatrix[j].TxIndex {
			return accessMatrix[i].TxIndex < accessMatrix[j].TxIndex
		}
		if accessMatrix[i].Key != accessMatrix[j].Key {
			return accessMatrix[i].Key < accessMatrix[j].Key
		}
		return accessMatrix[i].Mode < accessMatrix[j].Mode
	})
	digests := make([]string, 0, len(sourceRouteDigests))
	for digest := range sourceRouteDigests {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	routingDigestPayload := struct {
		BlockShard                      string                 `json:"block_shard"`
		BlockHeight                     uint64                 `json:"block_height"`
		SourceDigests                   []string               `json:"source_route_plan_digests"`
		RouteBatchSequence              uint64                 `json:"route_batch_sequence,omitempty"`
		RouteBatchTransactionCount      int                    `json:"route_batch_transaction_count,omitempty"`
		RouteBatchShardTransactionCount int                    `json:"route_batch_shard_transaction_count,omitempty"`
		Placements                      []TransactionPlacement `json:"transaction_placements"`
		AccessListDigest                []string               `json:"access_list_digests"`
	}{
		BlockShard: block.ShardID, BlockHeight: block.Height, SourceDigests: digests,
		RouteBatchSequence: batchProjection.Sequence, RouteBatchTransactionCount: batchProjection.TransactionCount, RouteBatchShardTransactionCount: batchProjection.ShardTransactionCount,
		Placements: placements, AccessListDigest: accessDigests,
	}
	raw, err := json.Marshal(routingDigestPayload)
	if err != nil {
		return nil, err
	}
	routingDigest := stableTextDigest(string(raw))
	return map[string]any{
		"algorithm_id":                        r.batchExecutionPlanAlgorithmID(),
		"ordered_transaction_ids":             orderedIDs,
		"access_list_digests":                 accessDigests,
		"routing_plan_digest":                 routingDigest,
		"source_route_plan_digests":           digests,
		"route_batch_sequence":                batchProjection.Sequence,
		"route_batch_transaction_count":       batchProjection.TransactionCount,
		"route_batch_shard_transaction_count": batchProjection.ShardTransactionCount,
		"placement_policy":                    "signed_client_route_entries_v1",
		"transaction_policy":                  "sender_group_remote_cost_v2",
		"access_matrix":                       accessMatrix,
		"transaction_placements":              placements,
		"remote_access_estimate":              remoteEstimate,
	}, nil
}

func bindBatchPlanToExecutionShard(plan BatchRoutingPlan, executionShard string) BatchRoutingPlan {
	if executionShard == "" {
		return plan
	}
	for index := range plan.TransactionPlacements {
		placement := &plan.TransactionPlacements[index]
		if placement.ExecutionShard == executionShard {
			continue
		}
		if placement.RemoteAccessCount <= 0 {
			placement.RemoteAccessCount = 1
		}
		placement.ExecutionShard = executionShard
		if placement.Reason == "" {
			placement.Reason = "consensus_bound_execution_shard"
		} else if !strings.Contains(placement.Reason, "consensus_bound_execution_shard") {
			placement.Reason += ";consensus_bound_execution_shard"
		}
	}
	return plan
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
	batchSummary := summarizeSchedulerRows(rows)
	r.mu.Lock()
	mergeSchedulerSummary(&r.schedulerAggregate, batchSummary)
	remaining := schedulerTraceRetentionLimit - len(r.schedulerRows)
	if remaining > 0 {
		keep := len(rows)
		if keep > remaining {
			keep = remaining
		}
		r.schedulerRows = append(r.schedulerRows, rows[:keep]...)
		if keep < len(rows) {
			r.schedulerRowsDropped += int64(len(rows) - keep)
		}
	} else {
		r.schedulerRowsDropped += int64(len(rows))
	}
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
func (r *NodeRuntime) validatePrePrepare(fromNode string, block realblock.Block) (bool, bool, error) {
	leader := r.leaderID(r.node.ShardID)
	if leader == "" || fromNode != leader {
		return false, false, fmt.Errorf("pre-prepare sender mismatch: from=%s leader=%s", fromNode, leader)
	}
	if block.ProposerID != leader && !r.pbftAllowsCarriedProposal(block) {
		return false, false, fmt.Errorf("pre-prepare proposer mismatch: from=%s proposer=%s leader=%s", fromNode, block.ProposerID, leader)
	}
	if block.ShardID != r.node.ShardID {
		return false, false, fmt.Errorf("pre-prepare shard mismatch: block=%s node=%s", block.ShardID, r.node.ShardID)
	}
	if block.BlockHash == "" || realblock.Hash(block) != block.BlockHash {
		return false, false, fmt.Errorf("pre-prepare block hash mismatch at height %d", block.Height)
	}
	if realblock.TxRoot(block.TxIDs) != block.TxRoot {
		return false, false, fmt.Errorf("pre-prepare transaction root mismatch at height %d", block.Height)
	}
	if err := r.verifyExecutionPlanEnvelope(block); err != nil {
		return false, false, err
	}

	r.mu.Lock()
	expectedHeight := r.committedHeight + 1
	committedHash := r.committedHash
	fatal := firstNonEmpty(r.fatalPersistenceError, r.fatalExecutionError)
	r.mu.Unlock()
	if fatal != "" {
		return false, false, fmt.Errorf("node execution frozen: %s", fatal)
	}
	if block.Height < expectedHeight {
		r.mu.Lock()
		r.lastProposalError = fmt.Sprintf("stale pre-prepare height %d, expected %d", block.Height, expectedHeight)
		r.mu.Unlock()
		return false, false, nil
	}
	if block.Height > expectedHeight {
		r.mu.Lock()
		r.lastProposalError = fmt.Sprintf("future pre-prepare height %d, expected %d", block.Height, expectedHeight)
		r.mu.Unlock()
		return false, true, nil
	}
	if block.PreviousHash != committedHash {
		r.mu.Lock()
		r.lastProposalError = fmt.Sprintf("pre-prepare parent mismatch at height %d", block.Height)
		r.mu.Unlock()
		return false, true, nil
	}
	return true, false, nil
}

func (r *NodeRuntime) validateConsensusCommit(fromNode string, block realblock.Block) (bool, bool, error) {
	leader := r.leaderID(r.node.ShardID)
	if leader == "" || fromNode != leader {
		return false, false, fmt.Errorf("commit sender mismatch: from=%s leader=%s", fromNode, leader)
	}
	if block.ProposerID != leader && !r.pbftAllowsCarriedProposal(block) {
		return false, false, fmt.Errorf("commit proposer mismatch: from=%s proposer=%s leader=%s", fromNode, block.ProposerID, leader)
	}
	if block.ShardID != r.node.ShardID {
		return false, false, fmt.Errorf("commit shard mismatch: block=%s node=%s", block.ShardID, r.node.ShardID)
	}
	if block.BlockHash == "" || realblock.Hash(block) != block.BlockHash {
		return false, false, fmt.Errorf("commit block hash mismatch at height %d", block.Height)
	}
	if realblock.TxRoot(block.TxIDs) != block.TxRoot {
		return false, false, fmt.Errorf("commit transaction root mismatch at height %d", block.Height)
	}
	if err := r.verifyExecutionPlanEnvelope(block); err != nil {
		return false, false, err
	}

	r.mu.Lock()
	expectedHeight := r.committedHeight + 1
	committedHash := r.committedHash
	remembered := r.proposals[block.BlockHash]
	fatal := firstNonEmpty(r.fatalPersistenceError, r.fatalExecutionError)
	r.mu.Unlock()
	if fatal != "" {
		return false, false, fmt.Errorf("node execution frozen: %s", fatal)
	}
	if block.Height < expectedHeight {
		return false, false, nil
	}
	if block.Height > expectedHeight {
		return false, true, nil
	}
	if block.PreviousHash != committedHash {
		return false, true, nil
	}
	if remembered.BlockHash == "" || remembered.Height != block.Height {
		return false, true, nil
	}
	return true, false, nil
}

func stringInSlice(value string, values []string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) updateBlockExecutionProgress(progress execution.BlockSTMProgress) {
	if progress.LastProgressAtMS == 0 {
		progress.LastProgressAtMS = time.Now().UnixMilli()
	}
	r.mu.Lock()
	if progress.BlockHeight > r.blockExecutionProgress.BlockHeight ||
		(progress.BlockHeight == r.blockExecutionProgress.BlockHeight &&
			progress.LastProgressAtMS >= r.blockExecutionProgress.LastProgressAtMS) {
		r.blockExecutionProgress = progress
	}
	r.lastProgressAt = progress.LastProgressAtMS
	r.mu.Unlock()
}

func isDeterministicExecutionError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"block-stm ",
		"execution receipt ",
		"missing execution receipt",
		"duplicate execution receipt",
		"execution plan ",
		"ordered materialization",
		"state root mismatch",
		"missing validated incarnation",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) markFatalPlanningError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.fatalPlanningError = err.Error()
	r.lastProposalError = err.Error()
	r.lastProgressAt = time.Now().UnixMilli()
	r.mu.Unlock()
}

func (r *NodeRuntime) markFatalExecutionError(block realblock.Block, err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.fatalExecutionError = err.Error()
	r.lastProposalError = err.Error()
	r.lastCommitFailure = CommitFailure{
		Phase:     r.commitPhase,
		Height:    block.Height,
		BlockHash: block.BlockHash,
		Error:     err.Error(),
		Timestamp: time.Now().UnixMilli(),
	}
	r.lastProgressAt = time.Now().UnixMilli()
	r.mu.Unlock()
}

func (r *NodeRuntime) rejectDeterministicExecution(block realblock.Block, phase string, err error) (CommitResult, error) {
	r.setCommitPhase(phase, block)
	r.markFatalExecutionError(block, err)
	return CommitResult{Disposition: CommitRejected, Block: block}, err
}

func (r *NodeRuntime) startStateFetchWorkers(parent context.Context) {
	r.mu.Lock()
	if r.stateFetchWorkerCancel != nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	tasks := make(chan stateFetchTask, stateFetchMailboxCapacity)
	r.stateFetchWorkerContext = ctx
	r.stateFetchWorkerCancel = cancel
	r.stateFetchTasks = tasks
	r.stateFetchResponseTasks = make(chan stateFetchResponseTask, stateFetchResponseMailboxCapacity)
	r.stateFetchWorkerWG.Add(stateFetchWorkerCount)
	r.stateFetchResponseWG.Add(stateFetchResponseWorkerCount)
	r.mu.Unlock()
	for range stateFetchWorkerCount {
		go r.runStateFetchWorker(ctx, tasks)
	}
	for range stateFetchResponseWorkerCount {
		go r.runStateFetchResponseWorker(ctx, r.stateFetchResponseTasks)
	}
}

func (r *NodeRuntime) stopStateFetchWorkers() {
	r.mu.Lock()
	cancel := r.stateFetchWorkerCancel
	r.stateFetchWorkerCancel = nil
	r.stateFetchWorkerContext = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.stateFetchWorkerWG.Wait()
	r.stateFetchResponseWG.Wait()
	r.mu.Lock()
	r.stateFetchTasks = nil
	r.stateFetchResponseTasks = nil
	r.mu.Unlock()
}

func (r *NodeRuntime) enqueueStateFetchResponse(ctx context.Context, requester string, response StateFetchResponse) error {
	r.mu.Lock()
	tasks := r.stateFetchResponseTasks
	r.mu.Unlock()
	if tasks == nil {
		return fmt.Errorf("state fetch response service is not running")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case tasks <- stateFetchResponseTask{requester: requester, response: response}:
		return nil
	}
}

func (r *NodeRuntime) enqueueStateFetchTask(requester string, request StateFetchRequest) error {
	if strings.TrimSpace(request.RequestID) == "" {
		return fmt.Errorf("state fetch request has empty request id")
	}
	r.mu.Lock()
	ctx := r.stateFetchWorkerContext
	tasks := r.stateFetchTasks
	r.mu.Unlock()
	if ctx == nil || tasks == nil {
		return fmt.Errorf("state fetch service is not running")
	}
	task := stateFetchTask{requester: requester, request: request}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case tasks <- task:
		return nil
	}
}

func (r *NodeRuntime) runStateFetchWorker(ctx context.Context, tasks <-chan stateFetchTask) {
	defer r.stateFetchWorkerWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:
			if ctx.Err() != nil {
				return
			}
			if err := r.handleStateFetchRequest(ctx, task.requester, task.request); err != nil && ctx.Err() == nil {
				r.recordStateFetchWorkerError(err)
			}
		}
	}
}

func (r *NodeRuntime) runStateFetchResponseWorker(ctx context.Context, tasks <-chan stateFetchResponseTask) {
	defer r.stateFetchResponseWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:
			if ctx.Err() != nil {
				return
			}
			envelope, err := p2p.NewEnvelope(stateFetchResponseMessage, r.node.NodeID, task.requester, r.node.ShardID, 0, 0, 0, task.response)
			if err == nil {
				err = r.sendStateAccessToNode(ctx, task.requester, envelope)
			}
			if err != nil && ctx.Err() == nil {
				r.recordStateFetchWorkerError(err)
				r.recordStateFetchServiceDiagnostic(task.requester, StateFetchRequest{RequestID: task.response.RequestID, TxID: task.response.TxID, BlockHash: task.response.BlockHash, Key: task.response.Key, HomeShard: task.response.HomeShard, ExecutionShard: task.response.ExecutionShard}, "response_send_error", err)
			} else if err == nil {
				r.recordStateFetchServiceDiagnostic(task.requester, StateFetchRequest{RequestID: task.response.RequestID, TxID: task.response.TxID, BlockHash: task.response.BlockHash, Key: task.response.Key, HomeShard: task.response.HomeShard, ExecutionShard: task.response.ExecutionShard}, "response_sent", nil)
			}
		}
	}
}

func (r *NodeRuntime) recordStateFetchWorkerError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.runtimeMetricCounts["state_fetch_service_error_count"]++
	r.mu.Unlock()
}

func appendStateFetchDiagnostic(history []StateFetchDiagnostic, item StateFetchDiagnostic) []StateFetchDiagnostic {
	history = append(history, item)
	if len(history) > stateFetchDiagnosticHistoryLimit {
		history = append([]StateFetchDiagnostic(nil), history[len(history)-stateFetchDiagnosticHistoryLimit:]...)
	}
	return history
}

func (r *NodeRuntime) recordStateFetchServiceDiagnostic(requester string, request StateFetchRequest, stage string, err error) {
	item := StateFetchDiagnostic{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, Requester: requester, Stage: stage, FinishedAtMS: time.Now().UnixMilli()}
	if err != nil {
		item.Error = err.Error()
	}
	r.mu.Lock()
	r.lastStateFetchService = item
	if err != nil {
		r.stateFetchServiceErrors = appendStateFetchDiagnostic(r.stateFetchServiceErrors, item)
	}
	r.mu.Unlock()
}

func (r *NodeRuntime) beginStateFetch(block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, homeShard, requestID string) {
	diagnostic := StateFetchDiagnostic{RequestID: requestID, TxID: item.TxID, BlockHash: block.BlockHash, BlockHeight: block.Height, Key: access.Key, HomeShard: homeShard, ExecutionShard: r.node.ShardID, Stage: "created", StartedAtMS: time.Now().UnixMilli()}
	r.mu.Lock()
	if r.pendingStateFetches == nil {
		r.pendingStateFetches = map[string]StateFetchDiagnostic{}
	}
	r.pendingStateFetches[requestID] = diagnostic
	r.mu.Unlock()
}

func (r *NodeRuntime) markStateFetchSent(requestID string) {
	r.mu.Lock()
	diagnostic, ok := r.pendingStateFetches[requestID]
	if ok {
		diagnostic.Stage = "sent"
		diagnostic.SentAtMS = time.Now().UnixMilli()
		r.pendingStateFetches[requestID] = diagnostic
	}
	r.mu.Unlock()
}

func (r *NodeRuntime) finishStateFetch(requestID, stage string, started time.Time, err error) {
	r.mu.Lock()
	diagnostic := r.pendingStateFetches[requestID]
	delete(r.pendingStateFetches, requestID)
	diagnostic.Stage = stage
	diagnostic.FinishedAtMS = time.Now().UnixMilli()
	diagnostic.LatencyMS = time.Since(started).Milliseconds()
	if err != nil {
		diagnostic.Error = err.Error()
		r.stateFetchFailures = appendStateFetchDiagnostic(r.stateFetchFailures, diagnostic)
	}
	r.lastStateFetch = diagnostic
	r.mu.Unlock()
}

func (r *NodeRuntime) startCommitWorker(parent context.Context) {
	r.mu.Lock()
	if r.commitWorkerCancel != nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.commitWorkerContext = ctx
	r.commitWorkerCancel = cancel
	r.commitTasks = make(chan commitTask, commitMailboxCapacity)
	if r.queuedCommitTasks == nil {
		r.queuedCommitTasks = map[string]bool{}
	}
	r.commitWorkerWG.Add(1)
	r.mu.Unlock()
	go r.runCommitWorker(ctx)
}

func (r *NodeRuntime) stopCommitWorker() {
	r.mu.Lock()
	cancel := r.commitWorkerCancel
	r.commitWorkerCancel = nil
	r.commitWorkerContext = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.commitWorkerWG.Wait()
	r.mu.Lock()
	r.commitTasks = nil
	r.queuedCommitTasks = map[string]bool{}
	r.mu.Unlock()
}

func (r *NodeRuntime) enqueueCommitTask(kind commitTaskKind, block realblock.Block, origin CommitOrigin) error {
	if block.BlockHash == "" {
		return fmt.Errorf("commit task has empty block hash")
	}
	key := string(kind) + "|" + string(origin) + "|" + block.BlockHash
	r.mu.Lock()
	ctx := r.commitWorkerContext
	tasks := r.commitTasks
	if ctx == nil || tasks == nil {
		r.mu.Unlock()
		return fmt.Errorf("commit worker is not running")
	}
	if r.queuedCommitTasks[key] {
		r.mu.Unlock()
		return nil
	}
	r.queuedCommitTasks[key] = true
	r.mu.Unlock()
	task := commitTask{key: key, kind: kind, block: block, origin: origin}
	select {
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.queuedCommitTasks, key)
		r.mu.Unlock()
		return ctx.Err()
	case tasks <- task:
		return nil
	default:
		r.mu.Lock()
		delete(r.queuedCommitTasks, key)
		r.mu.Unlock()
		return fmt.Errorf("commit mailbox is full")
	}
}

func (r *NodeRuntime) runCommitWorker(ctx context.Context) {
	defer r.commitWorkerWG.Done()
	r.mu.Lock()
	tasks := r.commitTasks
	r.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-tasks:
			if ctx.Err() != nil {
				return
			}
			err := r.executeCommitTask(ctx, task)
			if err != nil && ctx.Err() == nil {
				r.recordCommitWorkerError(err)
			}
			r.mu.Lock()
			delete(r.queuedCommitTasks, task.key)
			r.mu.Unlock()
		}
	}
}

func (r *NodeRuntime) executeCommitTask(ctx context.Context, task commitTask) error {
	switch task.kind {
	case commitTaskConsensus:
		return r.commit(ctx, task.block)
	case commitTaskCatchUp:
		result, err := r.commitWithOrigin(ctx, task.block, task.origin)
		if err != nil {
			return err
		}
		if result.Disposition == CommitApplied || result.Disposition == CommitAlreadyApplied {
			r.mu.Lock()
			r.incrementRuntimeMetricLocked("pbft_catchup_success_count")
			r.mu.Unlock()
		}
		r.logConsensus("CATCHUP_APPLIED", task.block.ProposerID, task.block.BlockHash, task.block.Height)
		return nil
	default:
		return fmt.Errorf("unknown commit task kind %q", task.kind)
	}
}

func (r *NodeRuntime) recordCommitWorkerError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastProposalError = err.Error()
	r.mu.Unlock()
}

func (r *NodeRuntime) setCommitPhase(phase string, block realblock.Block) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitPhase = phase
	r.commitPhaseHeight = block.Height
	r.commitPhaseHash = block.BlockHash
}

func (r *NodeRuntime) recordCommitFailure(block realblock.Block, err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.lastProposalError = err.Error()
	r.lastCommitFailure = CommitFailure{
		Phase:     r.commitPhase,
		Height:    block.Height,
		BlockHash: block.BlockHash,
		Error:     err.Error(),
		Timestamp: time.Now().UnixMilli(),
	}
	r.mu.Unlock()
}

// finalize is a legacy synchronous test helper retained for current V5 tests
// that exercise commit failure/rollback behavior directly. Production PBFT
// reaches commitWithDisposition only after a commit certificate in
// enqueuePBFTCommittedBlock/executeCommitTask.
func (r *NodeRuntime) finalize(ctx context.Context, block realblock.Block) error {
	for _, item := range block.TxList {
		r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "quorum_committed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
	}
	result, err := r.commitWithDisposition(ctx, block)
	if err != nil {
		r.mu.Lock()
		fatal := r.fatalPersistenceError != "" || r.fatalExecutionError != ""
		r.mu.Unlock()
		if !fatal {
			r.pool.ReleaseReserved(block.TxList)
		}
		if fatal {
			return err
		}
		r.mu.Lock()
		if r.isCurrentLeaderLocked() {
			r.proposalInFlight = false
			r.proposalInFlightHash = ""
			r.proposalStartedAt = time.Time{}
			r.proposalLastBroadcastAt = time.Time{}
			r.proposalWorkUnits.Store(0)
		}
		delete(r.proposals, block.BlockHash)
		delete(r.votes, block.BlockHash)
		delete(r.committing, block.BlockHash)
		r.mu.Unlock()
		return err
	}
	if result.Disposition != CommitApplied && result.Disposition != CommitAlreadyApplied {
		return fmt.Errorf("block %s commit %s", block.BlockHash, result.Disposition)
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
	if r.fatalExecutionError != "" {
		err := fmt.Errorf("fatal execution freeze: %s", r.fatalExecutionError)
		r.mu.Unlock()
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	r.mu.Unlock()
	result, err := r.commitOnce(ctx, block, origin)
	if err != nil {
		r.recordCommitFailure(block, err)
		return result, err
	}
	if result.Disposition == CommitApplied {
		if origin == CommitOriginConsensus || origin == CommitOriginCatchUp || origin == CommitOriginRecoveryReplay {
			r.pbftState().MarkDurableCommit(block)
			r.maybeBroadcastPBFTCheckpoint(ctx, block)
		}
		r.replayDeferredPrePrepare(ctx)
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
		if origin == CommitOriginConsensus || origin == CommitOriginCatchUp || origin == CommitOriginRecoveryReplay {
			r.pbftState().MarkDurableCommit(next)
			r.maybeBroadcastPBFTCheckpoint(ctx, next)
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
	if r.fatalExecutionError != "" {
		err := fmt.Errorf("fatal execution freeze: %s", r.fatalExecutionError)
		r.mu.Unlock()
		r.setCommitPhase("rejected_fatal_execution", block)
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
	// The built-in durable store can atomically capture the KV snapshot and the
	// exact matching authenticated root. This removes cross-block full-tree
	// reconstruction without ever pairing a snapshot with a different DB version.
	// Other storage plugins and multi-shard remote overlays retain the full-build
	// executor fallback.
	var stateBefore map[string]string
	var baseStateCommitment *state.Commitment
	if len(r.shardIDs()) == 1 {
		if snapshotter, ok := r.plugins.StateStorage.(AuthenticatedStateSnapshotter); ok {
			stateBefore, baseStateCommitment = snapshotter.SnapshotWithCommitment(r.db)
		} else {
			stateBefore = r.plugins.StateStorage.Snapshot(r.db)
		}
	} else {
		stateBefore = r.plugins.StateStorage.Snapshot(r.db)
	}
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
	r.publishSystemStateDeltaVersions(block.SystemStateDeltas)
	remoteDeltas := r.materializableRemoteStateDeltas(block, r.node.ShardID)
	executionSnapshot, err := applyStateDeltaToSnapshot(stateBefore, remoteDeltas, r.node.ShardID, block.Height)
	if len(remoteDeltas) > 0 {
		baseStateCommitment = nil
	}
	if err != nil {
		r.setCommitPhase("remote_state_cas_rejected", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	var remoteStateReadiness map[string]bool
	var remoteStateFetch RemoteStateFetchFunc
	versionedWaveExecution := r.versionedRemoteWaveExecutionEnabled(block)
	if r.nativeMetaTrackStateReadyEnabled() {
		r.setCommitPhase("remote_state_state_ready_setup", block)
		remoteStateReadiness, remoteStateFetch = r.metaTrackStateReadyInputs(block)
	} else if !versionedWaveExecution {
		r.setCommitPhase("remote_state_prefetch", block)
		executionSnapshot, err = r.prepareMetaTrackStateSnapshot(ctx, block, executionSnapshot)
		if err != nil {
			r.setCommitPhase("state_access_error", block)
			return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
		}
	}
	r.setCommitPhase("validate_execution_plan", block)
	if err := r.validateMetaTrackPlanDrivesExecution(block); err != nil {
		return r.rejectDeterministicExecution(block, "execution_plan_shard_mismatch", err)
	}
	// Only an exact block/plan identity that was produced locally by PlanBlock
	// or fully verified by the scheduler before PBFT may use the lightweight
	// execution-side projection guard. Catch-up/recovery/direct paths stay
	// fail-closed and retain full planner recomputation.
	executionPlanVerified := origin == CommitOriginConsensus && r.hasVerifiedExecutionPlan(block)
	r.setCommitPhase("execute_block", block)
	executeStarted := time.Now()
	r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionStarted", BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"tx_count": len(block.TxList)}})
	r.updateBlockExecutionProgress(execution.BlockSTMProgress{BlockHeight: block.Height, TransactionCount: len(block.TxList), CurrentTxnIndex: -1, LastProgressAtMS: time.Now().UnixMilli()})
	var executed BlockExecutionResult
	if versionedWaveExecution {
		executed, err = r.executeVersionedRemoteBlock(ctx, block, executionSnapshot)
	} else {
		executed, err = r.plugins.BlockExecutor.ExecuteBlock(ctx, BlockExecutionInput{Block: block, BaseStateSnapshot: executionSnapshot, BaseStateCommitment: baseStateCommitment, NodeID: r.node.NodeID, ShardID: r.node.ShardID, WorkerCount: blockExecutorWorkerCountFromProfile(r.pluginSnapshot), Execution: r.plugins.Execution, Scheduler: r.plugins.Scheduler, ExecutionPlanVerified: executionPlanVerified, Progress: r.updateBlockExecutionProgress, RemoteStateReadiness: remoteStateReadiness, RemoteStateFetch: remoteStateFetch, StateVersionPublish: r.stateVersionPublisher(block)})
	}
	if err != nil {
		r.setCommitPhase("execute_block_error", block)
		if isDeterministicExecutionError(err) {
			r.markFatalExecutionError(block, err)
		}
		r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionFinished", BlockHash: block.BlockHash, Height: block.Height, Success: false, Error: err.Error()})
		return CommitResult{Disposition: CommitRejected, Block: block}, err
	}
	result := executed.ExecutionResult
	receiptSuccessByTxID := make(map[string]bool, len(result.Receipts))
	receiptErrorByTxID := make(map[string]string, len(result.Receipts))
	for _, receipt := range result.Receipts {
		if receipt.TxID == "" {
			err := fmt.Errorf("execution receipt missing transaction id at block height %d", block.Height)
			return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
		}
		if _, exists := receiptSuccessByTxID[receipt.TxID]; exists {
			err := fmt.Errorf("duplicate execution receipt for transaction %s at block height %d", receipt.TxID, block.Height)
			return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
		}
		receiptSuccessByTxID[receipt.TxID] = receipt.Success
		receiptErrorByTxID[receipt.TxID] = receipt.Error
	}
	if len(receiptSuccessByTxID) != len(block.TxList) {
		err := fmt.Errorf("execution receipt count mismatch at block height %d: receipts=%d transactions=%d", block.Height, len(receiptSuccessByTxID), len(block.TxList))
		return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
	}
	blockTxSeen := make(map[string]bool, len(block.TxList))
	for _, item := range block.TxList {
		if item.TxID == "" {
			err := fmt.Errorf("block transaction missing id at block height %d", block.Height)
			return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
		}
		if blockTxSeen[item.TxID] {
			err := fmt.Errorf("duplicate transaction id %s in block height %d", item.TxID, block.Height)
			return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
		}
		blockTxSeen[item.TxID] = true
		if _, ok := receiptSuccessByTxID[item.TxID]; !ok {
			err := fmt.Errorf("missing execution receipt for transaction %s at block height %d", item.TxID, block.Height)
			return r.rejectDeterministicExecution(block, "invalid_execution_receipts", err)
		}
	}

	executed.BlockExecutionMS = time.Since(executeStarted).Milliseconds()
	// Preserve executor-owned phase timing. BlockExecutionMS is the common wall
	// clock envelope; TransactionExecutionMS/DeterministicApplyMS/StateCommitmentMS
	// are measured by the executor and must never be overwritten by that envelope.
	if executed.TransactionExecutionMS == 0 && executed.ExecutionResult.TransactionExecutionMS > 0 {
		executed.TransactionExecutionMS = executed.ExecutionResult.TransactionExecutionMS
	}
	if executed.DeterministicApplyMS == 0 && executed.ExecutionResult.DeterministicMaterializationMS > 0 {
		executed.DeterministicApplyMS = executed.ExecutionResult.DeterministicMaterializationMS
	}
	if executed.StateCommitmentMS == 0 && executed.ExecutionResult.StateCommitmentMS > 0 {
		executed.StateCommitmentMS = executed.ExecutionResult.StateCommitmentMS
	}
	if executed.StateRootVersion == "" {
		executed.StateRootVersion = executed.ExecutionResult.StateRootVersion
	}
	r.recordScheduleEvents(block, executed.ScheduleEvents, false)
	r.emitRuntimeEvent(RuntimeEvent{Type: "ExecutionFinished", BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"block_execution_ms": executed.BlockExecutionMS}})
	r.setCommitPhase("build_commit_plan", block)
	commitDecision := r.plugins.Commit.DecideCommit(CommitInput{ShardID: r.node.ShardID, Height: block.Height, Transactions: block.TxList, TxDeltas: executed.ExecutionResult.TxDeltas, StateDelta: executed.StateDelta, BaseStateSnapshot: executionSnapshot})
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
	physicalDelta = annotateVersionedLocalStateDelta(physicalDelta, block.TxList, executed.ExecutionResult.TxDeltas, r.node.ShardID, r.homeShardFor, r.shardIDs())
	if len(remoteDeltas) > 0 {
		physicalDelta = append(append([]state.StateKV(nil), remoteDeltas...), physicalDelta...)
	}
	physicalDelta = r.filterVersionedPhysicalDelta(physicalDelta)
	r.setCommitPhase("apply_state_delta", block)
	applyStarted := time.Now()
	stateRootBeforeWAL := r.plugins.StateStorage.Root(r.db)
	if err := r.plugins.StateStorage.ApplyBatch(r.db, physicalDelta); err != nil {
		r.setCommitPhase("state_apply_error", block)
		return CommitResult{Disposition: CommitRejected, Block: block}, r.rollbackCommitFailure(block.BlockHash, stateBefore, stateCheckpoint, checkpoint, err)
	}
	executed.StateDBApplyMS = time.Since(applyStarted).Milliseconds()
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
		"state_db_apply_ms":           executed.StateDBApplyMS,
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
	r.markLocalVersionedMaterialized(block.TxList)
	r.setCommitPhase("record_execution_artifacts", block)
	r.recordProposalEvidence(block)
	r.recordBlockExecutionResult(block, executed)
	r.recordExecutionAndCommitDecisions(block, commitDecision, physicalDelta)
	r.setCommitPhase("commit_reserved", block)
	r.pool.CommitReserved(block.TxList)
	// Every production validator advances its local proposer head after durable
	// commit so any replica can safely become the primary in a later PBFT view.
	// Some focused unit tests construct NodeRuntime literals without a proposer.
	if r.proposer != nil {
		r.proposer.Confirm(block)
	}
	r.setCommitPhase("advance_runtime_state", block)
	commitView := r.currentPBFTView()
	r.mu.Lock()
	r.committed[block.BlockHash] = true
	delete(r.committing, block.BlockHash)
	// The durable store and PBFT commit certificate are the post-commit source
	// of truth. Keeping the full proposal and PREPARE vote map here retains the
	// complete block payload indefinitely; Aria proposals can contain the entire
	// candidate batch in proposal evidence. Release this in-flight bookkeeping as
	// soon as durable commit has succeeded.
	delete(r.proposals, block.BlockHash)
	delete(r.votes, block.BlockHash)
	delete(r.verifiedExecutionPlans, block.BlockHash)
	r.blockCount++
	r.chainRows = append(r.chainRows, []string{r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), fmt.Sprint(commitView), block.BlockHash, block.PreviousHash, fmt.Sprint(len(block.TxList)), block.TxRoot, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, fmt.Sprint(time.Now().UnixMilli()), fmt.Sprint(time.Now().UnixMilli())})
	r.mu.Unlock()
	r.mu.Lock()
	r.committedHeight = block.Height
	r.committedHash = block.BlockHash
	if r.catchupTargetHeight > 0 && r.committedHeight >= r.catchupTargetHeight {
		r.catchupTargetHeight = 0
		r.incrementRuntimeMetricLocked("pbft_catchup_target_reached_count")
	}
	r.lastProgressAt = time.Now().UnixMilli()
	if r.proposalInFlightHash == block.BlockHash {
		r.proposalInFlight = false
		r.proposalInFlightHash = ""
		r.proposalStartedAt = time.Time{}
		r.proposalLastBroadcastAt = time.Time{}
		r.proposalRetransmitCount = 0
		r.proposalWorkUnits.Store(0)
		r.lastProposalError = ""
	}
	next := r.pendingCommits[r.committedHeight+1]
	delete(r.pendingCommits, r.committedHeight+1)
	r.mu.Unlock()
	for _, item := range block.TxList {
		if origin == CommitOriginConsensus || origin == CommitOriginCatchUp {
			if receiptSuccessByTxID[item.TxID] {
				relay := relayItems[item.TxID]
				r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "durable_committed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: true})
				if origin == CommitOriginCatchUp && relay.Tx.TxID != "" {
					// Certified catch-up reconstructs this replica's local durable state.
					// Cross-shard protocol side effects are exactly-once leader actions and
					// must not be re-emitted by replay. Drop stale relay bookkeeping so
					// retryPendingRelays cannot later turn this replay into a finalize.
					r.clearCommittedRelayReplica(relay)
				}
				r.onCommittedTxWithOrigin(ctx, item, relay, origin)
			} else {
				executionError := strings.TrimSpace(receiptErrorByTxID[item.TxID])
				if executionError == "" {
					executionError = "execution_failed"
				}
				r.recordLifecycle(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "failed", NodeID: r.node.NodeID, ShardID: r.node.ShardID, BlockHeight: block.Height, Success: false, Error: executionError})
			}
		}
	}
	r.setCommitPhase("idle", realblock.Block{})
	return CommitResult{Disposition: CommitApplied, Block: next}, nil
}

func (r *NodeRuntime) incrementRuntimeMetricLocked(name string) {
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts[name]++
}

func (r *NodeRuntime) expireStaleProposal(timeout time.Duration) {
	// Compatibility helper retained for focused tests. Production liveness is
	// driven by checkPBFTLiveness(ctx), which retransmits the same digest and
	// eventually initiates VIEW-CHANGE. A same-view timeout must never delete
	// the proposal/votes or release its reservation.
	if timeout <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.proposalInFlight || r.proposalInFlightHash == "" || r.proposalStartedAt.IsZero() || time.Since(r.proposalStartedAt) < timeout {
		return
	}
	hash := r.proposalInFlightHash
	if r.committed[hash] || r.committing[hash] {
		return
	}
	if r.proposalQuorumReachedLocked(hash) {
		r.lastProposalError = "prepare_quorum_waiting_commit_certificate"
		return
	}
	r.lastProposalError = "proposal_timeout_preserved_for_retransmit"
	r.incrementRuntimeMetricLocked("pbft_same_digest_timeout_preserved_count")
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

func (r *NodeRuntime) nativeMetaTrackStateReadyEnabled() bool {
	return r != nil && r.plugins.BlockExecutor != nil &&
		routingUsesNativeVersionedStateReady(r.plugins.Routing) &&
		r.plugins.BlockExecutor.ID() == metaTrackBlockExecutorID && len(r.shardIDs()) > 1
}

func (r *NodeRuntime) versionedRemoteWaveExecutionEnabled(block realblock.Block) bool {
	if len(block.TxList) == 0 || len(r.shardIDs()) < 2 || !r.hasBatchRoutingControlPlane() {
		return false
	}
	if r.plugins.BlockExecutor != nil && r.plugins.BlockExecutor.ID() == metaTrackBlockExecutorID {
		return false
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting != nil && len(item.ExecutionRouting.StateVersions) > 0 {
			return true
		}
	}
	return false
}

type versionedStateProbe struct {
	token     string
	item      tx.SignedTransaction
	access    tx.AccessItem
	homeShard string
}

type versionedStateProbeResult struct {
	token   string
	value   string
	ready   bool
	latency time.Duration
	err     error
}

func (r *NodeRuntime) probeStateAccessOnce(ctx context.Context, block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, homeShard string) (string, bool, time.Duration, error) {
	dependency, hasDependency := stateVersionDependencyForKey(item, access.Key)
	versioned := hasDependency && isVersionedStateAccess(access)
	started := time.Now()
	if versioned && homeShard == r.node.ShardID {
		value, ready := r.stateVersionValue(access.Key, dependency.RequiredVersion)
		return value, ready, time.Since(started), nil
	}
	if homeShard == "" || homeShard == r.node.ShardID {
		return "", true, time.Since(started), nil
	}
	targetNode := r.leaderID(homeShard)
	if targetNode == "" {
		return "", false, time.Since(started), fmt.Errorf("remote state home leader missing for %s", homeShard)
	}
	requestID := stableTextDigest(strings.Join([]string{"probe", r.node.NodeID, item.TxID, block.BlockHash, access.Key, homeShard, r.node.ShardID, fmt.Sprint(dependency.RequiredVersion), fmt.Sprint(versioned), fmt.Sprint(time.Now().UnixNano())}, "|"))
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
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: item.TxID, BlockHash: block.BlockHash, Key: access.Key, HomeShard: homeShard, ExecutionShard: r.node.ShardID, AccessKind: string(access.Mode), RequiredVersion: dependency.RequiredVersion, Versioned: versioned})
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
	if err != nil {
		return "", false, time.Since(started), err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		return "", false, time.Since(started), err
	}
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case response := <-waiter:
		if response.Success {
			r.recordRemoteStateAccess(block, item, access, response, time.Since(started))
			return response.Value, true, time.Since(started), nil
		}
		if versioned && response.Error == "state_version_not_ready" {
			return "", false, time.Since(started), nil
		}
		return "", false, time.Since(started), fmt.Errorf("remote state probe failed: %s", response.Error)
	case <-timer.C:
		return "", false, time.Since(started), nil
	case <-ctx.Done():
		return "", false, time.Since(started), ctx.Err()
	}
}

func versionedStateAccesses(item tx.SignedTransaction, shardIDs []string, home func([]string, []string) string, executionShard string) []versionedStateProbe {
	out := make([]versionedStateProbe, 0, len(item.AccessList))
	seen := map[string]bool{}
	for _, access := range item.AccessList {
		if access.Key == "" || strings.Contains(access.Key, "::") {
			continue
		}
		dependency, hasDependency := stateVersionDependencyForKey(item, access.Key)
		versioned := hasDependency && isVersionedStateAccess(access)
		homeShard := home([]string{access.Key}, shardIDs)
		if !versioned && (homeShard == "" || homeShard == executionShard) {
			continue
		}
		token := stateReadinessToken(item, access)
		if seen[token] {
			continue
		}
		seen[token] = true
		if versioned {
			_ = dependency
		}
		out = append(out, versionedStateProbe{token: token, item: item, access: access, homeShard: homeShard})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].token < out[j].token })
	return out
}

func (r *NodeRuntime) executeVersionedRemoteBlock(ctx context.Context, block realblock.Block, base map[string]string) (BlockExecutionResult, error) {
	if r.plugins.BlockExecutor == nil {
		return BlockExecutionResult{}, fmt.Errorf("versioned remote execution requires block executor")
	}
	shardIDs := r.shardIDs()
	indexByTxID := map[string]int{}
	ordinalOwner := map[uint64]string{}
	for index, item := range block.TxList {
		indexByTxID[item.TxID] = index
		if item.ExecutionRouting != nil && item.ExecutionRouting.RoutingOrdinal > 0 {
			ordinalOwner[item.ExecutionRouting.RoutingOrdinal] = item.TxID
		}
	}
	probesByTx := map[string][]versionedStateProbe{}
	probeByToken := map[string]versionedStateProbe{}
	for _, item := range block.TxList {
		probes := versionedStateAccesses(item, shardIDs, r.homeShardFor, r.node.ShardID)
		probesByTx[item.TxID] = probes
		for _, probe := range probes {
			if _, exists := probeByToken[probe.token]; !exists {
				probeByToken[probe.token] = probe
			}
		}
	}

	completed := map[string]bool{}
	resolved := map[string]string{}
	probeReady := map[string]bool{}
	remaining := len(block.TxList)
	working := copyStringMap(base)
	resultByIndex := make([]execution.TxDelta, len(block.TxList))
	receiptByIndex := make([]execution.Receipt, len(block.TxList))
	setByIndex := make([]bool, len(block.TxList))
	actualMetrics := map[string]any{}
	waveCount := 0
	stateWaitCount := 0
	stateReadyCount := 0
	probeCount := 0
	probeLatencyMS := int64(0)
	maxWaveWidth := 0
	workerCount := 1
	var mergedSTM execution.BlockSTMMetrics
	allSerialEquivalent := true
	started := time.Now()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()

	internalReady := func(item tx.SignedTransaction) bool {
		if item.ExecutionRouting == nil {
			return true
		}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.RequiredVersion == 0 {
				continue
			}
			if owner := ordinalOwner[dep.RequiredVersion]; owner != "" && !completed[owner] {
				return false
			}
		}
		return true
	}
	stateReady := func(item tx.SignedTransaction) bool {
		for _, probe := range probesByTx[item.TxID] {
			if !probeReady[probe.token] {
				return false
			}
		}
		return true
	}
	probeFrontier := func(frontier []tx.SignedTransaction) error {
		unique := map[string]versionedStateProbe{}
		for _, item := range frontier {
			for _, probe := range probesByTx[item.TxID] {
				if !probeReady[probe.token] {
					unique[probe.token] = probe
				}
			}
		}
		if len(unique) == 0 {
			return nil
		}
		items := make([]versionedStateProbe, 0, len(unique))
		for _, probe := range unique {
			items = append(items, probe)
		}
		sort.Slice(items, func(i, j int) bool {
			leftDep, _ := stateVersionDependencyForKey(items[i].item, items[i].access.Key)
			rightDep, _ := stateVersionDependencyForKey(items[j].item, items[j].access.Key)
			if leftDep.RequiredVersion != rightDep.RequiredVersion {
				return leftDep.RequiredVersion < rightDep.RequiredVersion
			}
			return items[i].token < items[j].token
		})
		jobs := make(chan versionedStateProbe, len(items))
		results := make(chan versionedStateProbeResult, len(items))
		workers := stateFetchWorkerCount
		if workers > len(items) {
			workers = len(items)
		}
		if workers < 1 {
			workers = 1
		}
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for probe := range jobs {
					value, ready, latency, err := r.probeStateAccessOnce(ctx, block, probe.item, probe.access, probe.homeShard)
					results <- versionedStateProbeResult{token: probe.token, value: value, ready: ready, latency: latency, err: err}
				}
			}()
		}
		for _, probe := range items {
			jobs <- probe
		}
		close(jobs)
		wg.Wait()
		close(results)
		for outcome := range results {
			probeCount++
			probeLatencyMS += outcome.latency.Milliseconds()
			if outcome.err != nil {
				return outcome.err
			}
			if outcome.ready {
				if !probeReady[outcome.token] {
					stateReadyCount++
				}
				probeReady[outcome.token] = true
				resolved[outcome.token] = outcome.value
			}
		}
		return nil
	}

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return BlockExecutionResult{}, ctx.Err()
		case <-deadline.C:
			return BlockExecutionResult{}, fmt.Errorf("versioned state-ready execution timed out at block %d with %d/%d transactions remaining", block.Height, remaining, len(block.TxList))
		default:
		}
		frontier := make([]tx.SignedTransaction, 0, remaining)
		for _, item := range block.TxList {
			if completed[item.TxID] || !internalReady(item) {
				continue
			}
			frontier = append(frontier, item)
		}
		if len(frontier) == 0 {
			return BlockExecutionResult{}, fmt.Errorf("versioned state dependency cycle at block %d", block.Height)
		}
		if err := probeFrontier(frontier); err != nil {
			return BlockExecutionResult{}, err
		}
		ready := make([]tx.SignedTransaction, 0, len(frontier))
		for _, item := range frontier {
			if stateReady(item) {
				ready = append(ready, item)
			} else {
				stateWaitCount++
			}
		}
		if len(ready) == 0 {
			select {
			case <-ctx.Done():
				return BlockExecutionResult{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}

		// A shared block snapshot can represent only one exact historical version
		// for a logical key. Greedily build a compatible ready wave; remaining
		// ready transactions are handled by the next wave without any block-wide
		// barrier.
		wave := make([]tx.SignedTransaction, 0, len(ready))
		waveVersionByKey := map[string]uint64{}
		for _, item := range ready {
			compatible := true
			for _, probe := range probesByTx[item.TxID] {
				dep, ok := stateVersionDependencyForKey(item, probe.access.Key)
				if !ok || !isVersionedStateAccess(probe.access) {
					continue
				}
				if existing, exists := waveVersionByKey[probe.access.Key]; exists && existing != dep.RequiredVersion {
					compatible = false
					break
				}
			}
			if !compatible {
				continue
			}
			wave = append(wave, item)
			for _, probe := range probesByTx[item.TxID] {
				if dep, ok := stateVersionDependencyForKey(item, probe.access.Key); ok && isVersionedStateAccess(probe.access) {
					waveVersionByKey[probe.access.Key] = dep.RequiredVersion
				}
			}
		}
		if len(wave) == 0 {
			wave = append(wave, ready[0])
		}
		if len(wave) > maxWaveWidth {
			maxWaveWidth = len(wave)
		}
		waveCount++
		waveSnapshot := copyStringMap(working)
		for _, item := range wave {
			for _, probe := range probesByTx[item.TxID] {
				if value, ok := resolved[probe.token]; ok {
					waveSnapshot[qualifyStateKey(block.ShardID, probe.access.Key)] = value
				}
			}
		}
		waveBlock := block
		waveBlock.TxList = append([]tx.SignedTransaction(nil), wave...)
		waveBlock.TxIDs = make([]string, 0, len(wave))
		for _, item := range wave {
			waveBlock.TxIDs = append(waveBlock.TxIDs, item.TxID)
		}
		waveSnapshot, err := r.prepareLegacyRemoteStateSnapshot(ctx, waveBlock, waveSnapshot)
		if err != nil {
			return BlockExecutionResult{}, err
		}
		waveResult, err := r.plugins.BlockExecutor.ExecuteBlock(ctx, BlockExecutionInput{Block: waveBlock, BaseStateSnapshot: waveSnapshot, NodeID: r.node.NodeID, ShardID: r.node.ShardID, WorkerCount: blockExecutorWorkerCountFromProfile(r.pluginSnapshot), Execution: r.plugins.Execution, Scheduler: r.plugins.Scheduler, Progress: r.updateBlockExecutionProgress})
		if err != nil {
			return BlockExecutionResult{}, err
		}
		if waveResult.WorkerCount > workerCount {
			workerCount = waveResult.WorkerCount
		}
		mergeBlockSTMMetrics(&mergedSTM, waveResult.ExecutionResult.BlockSTMMetrics)
		if waveResult.ExecutionResult.BlockExecutorID == execution.BlockSTMExecutorID && !waveResult.ExecutionResult.SerialEquivalent {
			allSerialEquivalent = false
		}
		for _, delta := range waveResult.ExecutionResult.TxDeltas {
			fullIndex, ok := indexByTxID[delta.TxID]
			if !ok {
				return BlockExecutionResult{}, fmt.Errorf("versioned wave returned unknown transaction %s", delta.TxID)
			}
			delta.OriginalIndex = fullIndex
			item := block.TxList[fullIndex]
			if err := r.publishTransactionStateVersions(ctx, block, item, delta, waveSnapshot); err != nil {
				return BlockExecutionResult{}, err
			}
			if delta.Success {
				for key, value := range delta.WriteSet {
					working[qualifyStateKey(block.ShardID, key)] = value
				}
			}
			resultByIndex[fullIndex] = delta
			receiptByIndex[fullIndex] = delta.Receipt
			setByIndex[fullIndex] = true
			completed[delta.TxID] = true
			remaining--
		}
	}

	// Normalize the block-level execution evidence in consensus order so remote
	// response timing and wave grouping cannot change receipt roots or state roots.
	normalized := copyStringMap(base)
	merged := execution.Result{BlockHash: block.BlockHash, Height: block.Height, StateRootBefore: state.RootOfSnapshot(base), Deterministic: true, StateUpdates: map[string]string{}, WorkerCount: workerCount, BlockExecutorID: r.plugins.BlockExecutor.ID(), BlockSTMMetrics: mergedSTM}
	if r.plugins.BlockExecutor.ID() == "block_stm_block_executor" {
		merged.BlockExecutorID = execution.BlockSTMExecutorID
		merged.SerialEquivalent = allSerialEquivalent
	}
	for index := range block.TxList {
		if !setByIndex[index] {
			return BlockExecutionResult{}, fmt.Errorf("versioned execution missing transaction result at index %d", index)
		}
		delta := resultByIndex[index]
		if delta.Success {
			for key, value := range delta.WriteSet {
				normalized[qualifyStateKey(block.ShardID, key)] = value
			}
			merged.SuccessfulTxs++
		} else {
			merged.FailedTxs++
		}
		receipt := receiptByIndex[index]
		receipt.StateRootAfterTx = state.RootOfSnapshot(normalized)
		delta.Receipt = receipt
		resultByIndex[index] = delta
		merged.Receipts = append(merged.Receipts, receipt)
		merged.TxDeltas = append(merged.TxDeltas, delta)
	}
	merged.StateRootAfter = state.RootOfSnapshot(normalized)
	merged.ReceiptRoot = execution.ReceiptRoot(merged.Receipts)
	for key, value := range normalized {
		merged.StateUpdates[key] = value
	}
	merged.StateDelta = executionStateDelta(base, normalized)
	if block.ExecutionPlan != nil && block.ExecutionPlan.PlanDigest != "" {
		merged.PlanDigest = block.ExecutionPlan.PlanDigest
	} else {
		merged.PlanDigest = stableTextDigest(strings.Join(block.TxIDs, "|"))
	}
	actualMetrics["versioned_state_ready_wave_count"] = waveCount
	actualMetrics["versioned_state_ready_wait_observation_count"] = stateWaitCount
	actualMetrics["versioned_state_ready_resolved_token_count"] = stateReadyCount
	actualMetrics["versioned_state_probe_count"] = probeCount
	actualMetrics["versioned_state_probe_latency_ms"] = probeLatencyMS
	actualMetrics["versioned_state_ready_max_wave_width"] = maxWaveWidth
	actualMetrics["versioned_state_ready_scheduler_mode"] = "per_transaction_per_key_version_frontier"
	actualMetrics["versioned_state_ready_execution_ms"] = time.Since(started).Milliseconds()
	return BlockExecutionResult{ExecutionResult: merged, StateDelta: stateKVsFromExecutionDelta(merged.StateDelta), PlanDigest: merged.PlanDigest, WorkerCount: workerCount, ActualMetrics: actualMetrics}, nil
}

func mergeBlockSTMMetrics(dst *execution.BlockSTMMetrics, src execution.BlockSTMMetrics) {
	if dst == nil {
		return
	}
	if src.WorkerCount > dst.WorkerCount {
		dst.WorkerCount = src.WorkerCount
	}
	if src.MaximumParallelWidth > dst.MaximumParallelWidth {
		dst.MaximumParallelWidth = src.MaximumParallelWidth
	}
	if src.MaximumIncarnation > dst.MaximumIncarnation {
		dst.MaximumIncarnation = src.MaximumIncarnation
	}
	if src.MaximumConcurrentExecutions > dst.MaximumConcurrentExecutions {
		dst.MaximumConcurrentExecutions = src.MaximumConcurrentExecutions
	}
	if src.SchedulerQueuePeak > dst.SchedulerQueuePeak {
		dst.SchedulerQueuePeak = src.SchedulerQueuePeak
	}
	dst.ExecutionTaskCount += src.ExecutionTaskCount
	dst.ValidationTaskCount += src.ValidationTaskCount
	dst.AbortCount += src.AbortCount
	dst.DependencyAbortCount += src.DependencyAbortCount
	dst.ValidationAbortCount += src.ValidationAbortCount
	dst.ReexecutionCount += src.ReexecutionCount
	dst.EstimateCount += src.EstimateCount
	dst.EstimateMarkCount += src.EstimateMarkCount
	dst.EstimateReadCount += src.EstimateReadCount
	dst.DependencyWaitCount += src.DependencyWaitCount
	dst.DependencyResumeCount += src.DependencyResumeCount
	dst.ValidatedSpeculativeResultCount += src.ValidatedSpeculativeResultCount
	dst.SpeculativeReadCount += src.SpeculativeReadCount
	dst.ValidationFailureCount += src.ValidationFailureCount
	dst.CommittedTransactionCount += src.CommittedTransactionCount
	dst.StaleTaskCount += src.StaleTaskCount
	dst.SerialOracleMS += src.SerialOracleMS
	dst.MaterializationMS += src.MaterializationMS
	dst.IncarnationLimitHitCount += src.IncarnationLimitHitCount
	dst.SerialFallbackCount += src.SerialFallbackCount
	dst.BusinessExecutionCount += src.BusinessExecutionCount
	if dst.IncarnationHistogram == nil {
		dst.IncarnationHistogram = map[int]int{}
	}
	for incarnation, count := range src.IncarnationHistogram {
		dst.IncarnationHistogram[incarnation] += count
	}
}

func (r *NodeRuntime) metaTrackStateReadyInputs(block realblock.Block) (map[string]bool, RemoteStateFetchFunc) {
	readiness := map[string]bool{}
	shardIDs := r.shardIDs()
	for _, item := range block.TxList {
		for _, access := range item.AccessList {
			if access.Key == "" || strings.Contains(access.Key, "::") {
				continue
			}
			homeShard := r.homeShardFor([]string{access.Key}, shardIDs)
			_, versioned := stateVersionDependencyForKey(item, access.Key)
			versioned = versioned && isVersionedStateAccess(access)
			if versioned || (homeShard != "" && homeShard != r.node.ShardID) {
				// Every exact-version access is resolved through StateReady, even when
				// this execution shard is also the persistent home. The latest DB
				// value may already be a newer logical version, so the scheduler must
				// overlay the exact required version before dispatch.
				readiness[stateReadinessToken(item, access)] = false
			}
		}
	}
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		homeShard := r.homeShardFor([]string{access.Key}, shardIDs)
		token := stateReadinessToken(item, access)
		dependency, hasDependency := stateVersionDependencyForKey(item, access.Key)
		versioned := hasDependency && isVersionedStateAccess(access)
		if versioned && homeShard == r.node.ShardID {
			started := time.Now()
			value, err := r.waitForLocalStateVersion(ctx, access.Key, dependency.RequiredVersion)
			if err != nil {
				return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, HomeShard: homeShard, StateVersion: dependency.RequiredVersion, LatencyMS: time.Since(started).Milliseconds()}, err
			}
			return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, Value: value, HomeShard: homeShard, StateVersion: dependency.RequiredVersion, LatencyMS: time.Since(started).Milliseconds()}, nil
		}
		if homeShard == "" || homeShard == r.node.ShardID {
			return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, Value: "", HomeShard: homeShard}, nil
		}
		response, latency, err := r.fetchRemoteState(ctx, block, item, access, homeShard)
		if err != nil {
			return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, HomeShard: homeShard, StateVersion: response.StateVersion, LatencyMS: latency.Milliseconds()}, err
		}
		r.recordRemoteStateAccess(block, item, access, response, latency)
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: token, Value: response.Value, HomeShard: homeShard, StateVersion: response.StateVersion, LatencyMS: latency.Milliseconds()}, nil
	}
	return readiness, fetch
}

func (r *NodeRuntime) prepareMetaTrackStateSnapshot(ctx context.Context, block realblock.Block, stateBefore map[string]string) (map[string]string, error) {
	return r.prepareRemoteStateSnapshot(ctx, block, stateBefore, false)
}

func (r *NodeRuntime) prepareLegacyRemoteStateSnapshot(ctx context.Context, block realblock.Block, stateBefore map[string]string) (map[string]string, error) {
	return r.prepareRemoteStateSnapshot(ctx, block, stateBefore, true)
}

func (r *NodeRuntime) prepareRemoteStateSnapshot(ctx context.Context, block realblock.Block, stateBefore map[string]string, skipVersioned bool) (map[string]string, error) {
	if !r.hasBatchRoutingControlPlane() {
		return stateBefore, nil
	}
	shardIDs := r.shardIDs()
	if len(shardIDs) < 2 {
		return stateBefore, nil
	}

	next := make(map[string]string, len(stateBefore))
	for key, value := range stateBefore {
		next[key] = value
	}

	type prefetchTask struct {
		rowKey    string
		item      tx.SignedTransaction
		access    tx.AccessItem
		homeShard string
	}
	type prefetchResult struct {
		task     prefetchTask
		response StateFetchResponse
		latency  time.Duration
		err      error
	}

	tasks := make([]prefetchTask, 0)
	seen := map[string]bool{}
	for _, item := range block.TxList {
		for _, access := range item.AccessList {
			if access.Key == "" || strings.Contains(access.Key, "::") {
				continue
			}
			if skipVersioned {
				if _, ok := stateVersionDependencyForKey(item, access.Key); ok && isVersionedStateAccess(access) {
					continue
				}
			}
			homeShard := r.homeShardFor([]string{access.Key}, shardIDs)
			if homeShard == "" || homeShard == r.node.ShardID {
				continue
			}
			rowKey := strings.Join([]string{block.BlockHash, r.node.ShardID, homeShard, access.Key}, "|")
			if seen[rowKey] {
				continue
			}
			seen[rowKey] = true
			tasks = append(tasks, prefetchTask{
				rowKey:    rowKey,
				item:      item,
				access:    access,
				homeShard: homeShard,
			})
		}
	}
	if len(tasks) == 0 {
		return next, nil
	}

	workerCount := stateFetchWorkerCount
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}
	jobs := make(chan prefetchTask, len(tasks))
	results := make(chan prefetchResult, len(tasks))
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				response, latency, err := r.fetchRemoteState(
					ctx,
					block,
					task.item,
					task.access,
					task.homeShard,
				)
				results <- prefetchResult{
					task:     task,
					response: response,
					latency:  latency,
					err:      err,
				}
			}
		}()
	}
	for _, task := range tasks {
		jobs <- task
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	collected := make([]prefetchResult, 0, len(tasks))
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		collected = append(collected, result)
	}

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].task.rowKey < collected[j].task.rowKey
	})
	for _, result := range collected {
		localQualifiedKey := r.node.ShardID + "::" + result.task.access.Key
		next[localQualifiedKey] = result.response.Value
		r.recordRemoteStateAccess(
			block,
			result.task.item,
			result.task.access,
			result.response,
			result.latency,
		)
	}

	r.mu.Lock()
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts["metatrack_prefetch_unique_key_count"] += int64(len(tasks))
	r.runtimeMetricCounts["metatrack_prefetch_worker_count"] = int64(workerCount)
	r.mu.Unlock()

	return next, nil
}

func (r *NodeRuntime) validateMetaTrackPlanDrivesExecution(block realblock.Block) error {
	if !routingUsesSignedBatchExecutionPlan(r.plugins.Routing) {
		return nil
	}
	if block.ExecutionPlan == nil {
		return nil
	}
	if block.ExecutionPlan.AlgorithmID != "metatrack_batch_execution_plan_v1" {
		return fmt.Errorf("metatrack execution plan algorithm mismatch: got %s", block.ExecutionPlan.AlgorithmID)
	}
	var payload struct {
		TransactionPlacements []TransactionPlacement `json:"transaction_placements"`
	}
	if err := json.Unmarshal(block.ExecutionPlan.Payload, &payload); err != nil {
		return fmt.Errorf("batch execution plan payload decode: %w", err)
	}
	placementByTx := map[string]TransactionPlacement{}
	for _, placement := range payload.TransactionPlacements {
		placementByTx[placement.LogicalID] = placement
	}
	for _, item := range block.TxList {
		txID := txIdentifier(item)
		placement, ok := placementByTx[txID]
		if !ok {
			return fmt.Errorf("metatrack execution plan missing transaction placement for %s", txID)
		}
		if item.ExecutionRouting == nil {
			return fmt.Errorf("metatrack execution metadata missing for %s", txID)
		}
		if err := tx.ValidateExecutionRouting(item); err != nil {
			return fmt.Errorf("metatrack execution metadata invalid for %s: %w", txID, err)
		}
		if placement.ExecutionShard != item.ExecutionRouting.ExecutionShard || placement.RoutingEpoch != item.ExecutionRouting.RoutingEpoch || placement.PredictedRemoteReads != item.ExecutionRouting.PredictedRemoteReads || placement.PredictedRemoteWrites != item.ExecutionRouting.PredictedRemoteWrites {
			return fmt.Errorf("metatrack signed route mismatch for %s", txID)
		}
		if placement.ExecutionShard != r.node.ShardID {
			return fmt.Errorf("metatrack plan drives execution mismatch for %s: plan_execution_shard=%s runtime_shard=%s", txID, placement.ExecutionShard, r.node.ShardID)
		}
	}
	return nil
}

func stateVersionDependencyForKey(item tx.SignedTransaction, key string) (tx.StateVersionDependency, bool) {
	if item.ExecutionRouting == nil {
		return tx.StateVersionDependency{}, false
	}
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.Key == key {
			return dependency, true
		}
	}
	return tx.StateVersionDependency{}, false
}

func isVersionedStateAccess(access tx.AccessItem) bool {
	if access.Key == "" || access.Mode == tx.AccessCommutativeDelta {
		return false
	}
	return !isStandardAccountStateKey(access.Key)
}

func (r *NodeRuntime) stateVersionValue(key string, version uint64) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stateVersionValueLocked(key, version)
}

// waitForLocalStateVersion implements MetaTrack StateReady semantics for an
// exact logical version whose persistent home is this execution shard. A
// missing exact version is a suspended scheduling state, not an execution
// failure. The wait ends only when the version is published or the enclosing
// run/block context is cancelled.
func (r *NodeRuntime) waitForLocalStateVersion(ctx context.Context, key string, version uint64) (string, error) {
	for {
		r.mu.Lock()
		if value, ready := r.stateVersionValueLocked(key, version); ready {
			r.mu.Unlock()
			return value, nil
		}
		signal := r.stateVersionSignalLocked(key, version)
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-signal:
		}
	}
}

func (r *NodeRuntime) publishStateVersionLocked(key string, version uint64, value string) {
	if version == 0 || key == "" {
		return
	}
	if r.stateVersionValues == nil {
		r.stateVersionValues = map[string]map[uint64]string{}
	}
	if r.stateVersionValues[key] == nil {
		r.stateVersionValues[key] = map[uint64]string{}
	}
	if existing, ok := r.stateVersionValues[key][version]; ok && existing != value {
		return
	}
	r.stateVersionValues[key][version] = value
	waitKey := stateVersionWaitKey(key, version)
	if signal := r.stateVersionSignals[waitKey]; signal != nil {
		close(signal)
		delete(r.stateVersionSignals, waitKey)
	}
}

func (r *NodeRuntime) publishStateVersion(key string, version uint64, value string) {
	r.mu.Lock()
	r.publishStateVersionLocked(key, version, value)
	subscriptions := r.takeStateVersionRemoteSubscriptionsLocked(key, version)
	workerCtx := r.stateFetchWorkerContext
	r.mu.Unlock()
	if len(subscriptions) == 0 {
		return
	}
	if workerCtx == nil {
		workerCtx = context.Background()
	}
	if err := r.notifyStateVersionRemoteSubscriptions(workerCtx, key, version, value, subscriptions); err != nil && workerCtx.Err() == nil {
		r.recordStateFetchWorkerError(err)
	}
}

func (r *NodeRuntime) stateVersionPublisher(block realblock.Block) StateVersionPublishFunc {
	if len(r.shardIDs()) < 2 || !r.hasBatchRoutingControlPlane() {
		return nil
	}
	return func(ctx context.Context, item tx.SignedTransaction, delta execution.TxDelta, exactSnapshot map[string]string) error {
		return r.publishTransactionStateVersions(ctx, block, item, delta, exactSnapshot)
	}
}

func (r *NodeRuntime) publishTransactionStateVersions(ctx context.Context, block realblock.Block, item tx.SignedTransaction, delta execution.TxDelta, exactSnapshot map[string]string) error {
	if item.ExecutionRouting == nil || len(item.ExecutionRouting.StateVersions) == 0 {
		return nil
	}
	accessByKey := map[string]tx.AccessItem{}
	for _, access := range item.AccessList {
		if access.Key != "" {
			accessByKey[access.Key] = access
		}
	}
	shardIDs := r.shardIDs()
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.ProducedVersion == 0 {
			continue
		}
		access, ok := accessByKey[dependency.Key]
		if !ok || !isVersionedStateAccess(access) {
			continue
		}
		value, wrote := writeSetLogicalValue(delta.WriteSet, dependency.Key)
		if !delta.Success || !wrote {
			value = exactSnapshot[qualifyStateKey(r.node.ShardID, dependency.Key)]
		}
		orderingNoop := !delta.Success || !wrote
		homeShard := r.homeShardFor([]string{dependency.Key}, shardIDs)
		if homeShard == "" {
			return fmt.Errorf("versioned state %s has no persistent home shard", dependency.Key)
		}
		if homeShard == r.node.ShardID {
			r.publishStateVersion(dependency.Key, dependency.ProducedVersion, value)
			continue
		}
		// Exact-version publication ownership must remain stable even if PBFT
		// changes view while this already-consensus-bound block is executing.
		// The original block proposer remains an owner; the current leader is a
		// bounded failover owner. Home-side stateDeltaApplyKey deduplicates an
		// identical duplicate publication from those at-most-two validators.
		if r.node.NodeID != block.ProposerID && !r.isCurrentLeader() {
			continue
		}
		versionItem := state.StateKV{
			Key:             qualifyStateKey(r.node.ShardID, dependency.Key),
			Value:           value,
			TxIDs:           []string{item.TxID},
			ApplyOrigin:     "versioned_remote_home",
			RoutingOrdinal:  dependency.ProducedVersion,
			PreviousVersion: dependency.RequiredVersion,
			ProducedVersion: dependency.ProducedVersion,
			OrderingNoop:    orderingNoop,
		}
		acks, latency, err := r.applyRemoteStateDelta(ctx, block, versionItem, dependency.Key, homeShard, []execution.TxDelta{delta})
		if err != nil {
			return err
		}
		for _, ack := range acks {
			r.recordRemoteStateApply(block, versionItem, dependency.Key, ack, latency)
		}
	}
	return nil
}

func (r *NodeRuntime) fetchRemoteState(ctx context.Context, block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, homeShard string) (response StateFetchResponse, latency time.Duration, fetchErr error) {
	targetNode := r.leaderID(homeShard)
	if targetNode == "" {
		return StateFetchResponse{}, 0, fmt.Errorf("remote state home leader missing for %s", homeShard)
	}
	dependency, hasDependency := stateVersionDependencyForKey(item, access.Key)
	versioned := hasDependency && isVersionedStateAccess(access)
	requiredVersion := uint64(0)
	if versioned {
		requiredVersion = dependency.RequiredVersion
	}
	requestID := stableTextDigest(strings.Join([]string{r.node.NodeID, item.TxID, block.BlockHash, access.Key, homeShard, r.node.ShardID, fmt.Sprint(requiredVersion), fmt.Sprint(versioned)}, "|"))
	start := time.Now()
	r.beginStateFetch(block, item, access, homeShard, requestID)
	outcome := "response_received"
	defer func() { r.finishStateFetch(requestID, outcome, start, fetchErr) }()
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
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: item.TxID, BlockHash: block.BlockHash, Key: access.Key, HomeShard: homeShard, ExecutionShard: r.node.ShardID, AccessKind: string(access.Mode), RequiredVersion: requiredVersion, Versioned: versioned})
	buildEnvelope := func() (p2p.MessageEnvelope, error) {
		return p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
	}
	send := func() error {
		envelope, err := buildEnvelope()
		if err != nil {
			return err
		}
		return r.sendStateAccessToNode(ctx, targetNode, envelope)
	}
	if err := send(); err != nil {
		outcome = "send_error"
		return StateFetchResponse{}, time.Since(start), err
	}
	r.markStateFetchSent(requestID)
	// Exact-version StateReady waits are causally released by predecessor
	// publication. Treating a temporarily unavailable exact version as a fixed
	// 10-second fetch failure converts normal dependency waiting into a
	// deterministic block failure. Keep the legacy bounded timeout only for
	// unversioned/current-state RPCs; versioned requests remain cancellable by
	// the enclosing context and keep retrying state_version_not_ready.
	var deadline <-chan time.Time
	var deadlineTimer *time.Timer
	if !versioned {
		deadlineTimer = time.NewTimer(10 * time.Second)
		deadline = deadlineTimer.C
		defer deadlineTimer.Stop()
	}
	for {
		select {
		case response := <-waiter:
			if response.Success {
				return response, time.Since(start), nil
			}
			if versioned && response.Error == "state_version_not_ready" {
				continue
			}
			outcome = "response_error"
			return response, time.Since(start), fmt.Errorf("remote state fetch failed: %s", response.Error)
		case <-deadline:
			outcome = "timeout"
			return StateFetchResponse{}, time.Since(start), fmt.Errorf("remote state fetch timed out for %s version %d from %s", access.Key, requiredVersion, homeShard)
		case <-ctx.Done():
			outcome = "context_cancelled"
			return StateFetchResponse{}, time.Since(start), ctx.Err()
		}
	}
}

func (r *NodeRuntime) handleStateFetchRequest(ctx context.Context, requester string, request StateFetchRequest) error {
	qualifiedKey := request.HomeShard + "::" + request.Key
	if request.HomeShard != "" && request.HomeShard != r.node.ShardID {
		response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateVersion: request.RequiredVersion, Versioned: request.Versioned, Success: false, Error: "wrong_home_shard"}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		return r.enqueueStateFetchResponse(ctx, requester, response)
	}
	if request.Versioned {
		r.mu.Lock()
		value, ready := r.stateVersionValueLocked(request.Key, request.RequiredVersion)
		if !ready {
			if request.AccessKind == statelessVersionAdmissionProbeAccessKind {
				r.mu.Unlock()
				response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateVersion: request.RequiredVersion, Versioned: true, Success: false, Error: "state_version_not_ready"}
				response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
				return r.enqueueStateFetchResponse(ctx, requester, response)
			}
			r.registerStateVersionRemoteSubscriptionLocked(requester, request)
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
		response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, Value: value, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: r.plugins.StateStorage.Root(r.db), StateVersion: request.RequiredVersion, Versioned: true, Success: true}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		return r.enqueueStateFetchResponse(ctx, requester, response)
	}

	cacheKey := stateFetchWitnessKey(request)
	r.mu.Lock()
	cached, ok := r.stateFetchWitnesses[cacheKey]
	r.mu.Unlock()
	response := cached
	if !ok {
		snapshot, snapshotRoot := r.stateFetchSnapshot(request)
		response = StateFetchResponse{TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, Value: snapshot[qualifiedKey], HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: snapshotRoot, Success: true}
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
	return r.enqueueStateFetchResponse(ctx, requester, response)
}

func stateFetchWitnessKey(request StateFetchRequest) string {
	return stableTextDigest(strings.Join([]string{request.BlockHash, request.HomeShard, request.ExecutionShard, request.Key, request.AccessKind, fmt.Sprint(request.RequiredVersion), fmt.Sprint(request.Versioned)}, "|"))
}

func stateFetchSnapshotKey(request StateFetchRequest) string {
	return stableTextDigest(strings.Join([]string{request.BlockHash, request.HomeShard, request.ExecutionShard}, "|"))
}

func stateFetchWitnessDigest(response StateFetchResponse, accessKind string) string {
	return stableTextDigest(strings.Join([]string{response.BlockHash, response.QualifiedKey, response.Value, response.StateRoot, response.HomeShard, response.ExecutionShard, accessKind, fmt.Sprint(response.StateVersion), fmt.Sprint(response.Versioned)}, "|"))
}

func (r *NodeRuntime) stateFetchSnapshot(request StateFetchRequest) (map[string]string, string) {
	snapshotKey := stateFetchSnapshotKey(request)

	// Snapshot() copies the whole home-shard state. Serialize only cache misses so
	// concurrent requests for different keys in one block share one immutable copy.
	r.stateFetchSnapshotMu.Lock()
	defer r.stateFetchSnapshotMu.Unlock()

	r.mu.Lock()
	snapshot := r.stateFetchSnapshots[snapshotKey]
	snapshotRoot := r.stateFetchSnapshotRoots[snapshotKey]
	r.mu.Unlock()
	if snapshot != nil && snapshotRoot != "" {
		return snapshot, snapshotRoot
	}

	fresh := r.plugins.StateStorage.Snapshot(r.db)
	freshRoot := state.RootOfSnapshot(fresh)

	r.mu.Lock()
	if r.stateFetchSnapshots == nil {
		r.stateFetchSnapshots = map[string]map[string]string{}
	}
	if r.stateFetchSnapshotRoots == nil {
		r.stateFetchSnapshotRoots = map[string]string{}
	}

	if snapshot = r.stateFetchSnapshots[snapshotKey]; snapshot == nil {
		snapshot = fresh
		snapshotRoot = freshRoot
		r.stateFetchSnapshots[snapshotKey] = snapshot
		r.stateFetchSnapshotRoots[snapshotKey] = snapshotRoot
		r.stateFetchSnapshotOrder = append(r.stateFetchSnapshotOrder, snapshotKey)

		if len(r.stateFetchSnapshotOrder) > stateFetchSnapshotCacheLimit {
			oldest := r.stateFetchSnapshotOrder[0]
			r.stateFetchSnapshotOrder = append([]string(nil), r.stateFetchSnapshotOrder[1:]...)
			delete(r.stateFetchSnapshots, oldest)
			delete(r.stateFetchSnapshotRoots, oldest)

			// Witness rows carry the evicted root and are cheap to regenerate.
			r.stateFetchWitnesses = map[string]StateFetchResponse{}
		}
	} else {
		snapshotRoot = r.stateFetchSnapshotRoots[snapshotKey]
	}
	r.mu.Unlock()

	return snapshot, snapshotRoot
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

func annotateVersionedLocalStateDelta(items []state.StateKV, transactions []tx.SignedTransaction, txDeltas []execution.TxDelta, localShard string, homeFor func([]string, []string) string, shardIDs []string) []state.StateKV {
	if len(items) == 0 {
		return items
	}
	deltaByID := map[string]execution.TxDelta{}
	for _, delta := range txDeltas {
		deltaByID[delta.TxID] = delta
	}
	out := make([]state.StateKV, 0, len(items))
	for _, item := range items {
		next := item
		logicalKey, ok := unqualifiedLocalKey(item.Key, localShard)
		if !ok || homeFor([]string{logicalKey}, shardIDs) != localShard {
			out = append(out, next)
			continue
		}
		var latest tx.StateVersionDependency
		for _, transaction := range transactions {
			dependency, ok := stateVersionDependencyForKey(transaction, logicalKey)
			if !ok || dependency.ProducedVersion == 0 || dependency.ProducedVersion <= latest.ProducedVersion {
				continue
			}
			versioned := false
			for _, access := range transaction.AccessList {
				if access.Key == logicalKey && isVersionedStateAccess(access) {
					versioned = true
					break
				}
			}
			if !versioned {
				continue
			}
			delta, exists := deltaByID[transaction.TxID]
			if exists && delta.Success {
				if _, wrote := writeSetLogicalValue(delta.WriteSet, logicalKey); !wrote {
					// A declared writer that produced no physical update still advances
					// the logical version as a no-op; the current physical item retains
					// the last real value and can carry that later version.
				}
			}
			latest = dependency
		}
		if latest.ProducedVersion > 0 {
			next.RoutingOrdinal = latest.ProducedVersion
			next.PreviousVersion = latest.RequiredVersion
			next.ProducedVersion = latest.ProducedVersion
			next.BaseValue = ""
			next.BaseValueDigest = ""
		}
		out = append(out, next)
	}
	return out
}

func (r *NodeRuntime) filterVersionedPhysicalDelta(items []state.StateKV) []state.StateKV {
	if len(items) == 0 {
		return items
	}
	r.mu.Lock()
	materialized := make(map[string]uint64, len(r.stateVersionMaterialized))
	for key, version := range r.stateVersionMaterialized {
		materialized[key] = version
	}
	r.mu.Unlock()
	out := make([]state.StateKV, 0, len(items))
	for _, item := range items {
		logicalKey := item.Key
		if index := strings.Index(logicalKey, "::"); index >= 0 && index+2 < len(logicalKey) {
			logicalKey = logicalKey[index+2:]
		}
		if item.ProducedVersion > 0 && item.UpdateSemantics != "commutative_delta" {
			if item.ProducedVersion <= materialized[logicalKey] {
				continue
			}
			materialized[logicalKey] = item.ProducedVersion
			if item.OrderingNoop {
				continue
			}
		}
		out = append(out, item)
	}
	return out
}

func (r *NodeRuntime) markLocalVersionedMaterialized(transactions []tx.SignedTransaction) {
	shardIDs := r.shardIDs()
	versions := map[string]uint64{}
	for _, item := range transactions {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.ProducedVersion == 0 || dependency.Key == "" {
				continue
			}
			versioned := false
			for _, access := range item.AccessList {
				if access.Key == dependency.Key && isVersionedStateAccess(access) {
					versioned = true
					break
				}
			}
			if !versioned || r.homeShardFor([]string{dependency.Key}, shardIDs) != r.node.ShardID {
				continue
			}
			if dependency.ProducedVersion > versions[dependency.Key] {
				versions[dependency.Key] = dependency.ProducedVersion
			}
		}
	}
	if len(versions) == 0 {
		return
	}
	r.mu.Lock()
	if r.stateVersionMaterialized == nil {
		r.stateVersionMaterialized = map[string]uint64{}
	}
	for key, version := range versions {
		if version > r.stateVersionMaterialized[key] {
			r.stateVersionMaterialized[key] = version
		}
	}
	r.mu.Unlock()
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
		if !r.isCurrentLeader() {
			continue
		}
		remoteItems := metaTrackRemoteWritebackItems(item, unqualified, block.TxList, txDeltas)
		for _, remoteItem := range remoteItems {
			if remoteItem.ProducedVersion > 0 && remoteItem.UpdateSemantics != "commutative_delta" {
				// Exact-version writers are published transaction-by-transaction by
				// StateReady execution. Re-sending the block-level final delta would
				// duplicate a logical version and reintroduce source-block grouping.
				continue
			}
			acks, latency, err := r.applyRemoteStateDelta(ctx, block, remoteItem, unqualified, homeShard, txDeltas)
			if err != nil {
				return nil, err
			}
			for _, ack := range acks {
				r.recordRemoteStateApply(block, remoteItem, unqualified, ack, latency)
			}
		}
	}
	return local, nil
}

func metaTrackRemoteWritebackItems(item state.StateKV, unqualifiedKey string, transactions []tx.SignedTransaction, txDeltas []execution.TxDelta) []state.StateKV {
	annotated := metaTrackRemoteWritebackItem(item, unqualifiedKey, transactions, txDeltas)
	annotated.RoutingOrdinal = maxRoutingOrdinalForTxIDs(annotated.TxIDs, transactions)
	if annotated.UpdateSemantics == "commutative_delta" || isStandardAccountStateKey(unqualifiedKey) {
		return []state.StateKV{annotated}
	}

	// Non-commutative remote-home writes must retain the consensus/workload
	// transaction order.  A block-level final value is insufficient because
	// Serial and Block-STM may group the same logical writers into different
	// source blocks.  Emit one deterministic writeback per successful logical
	// writer and let the home shard consensus order them by routing ordinal.
	txByID := make(map[string]tx.SignedTransaction, len(transactions))
	for _, item := range transactions {
		txByID[item.TxID] = item
	}
	out := make([]state.StateKV, 0, len(txDeltas))
	for _, delta := range txDeltas {
		if !delta.Success {
			continue
		}
		value, ok := writeSetLogicalValue(delta.WriteSet, unqualifiedKey)
		if !ok {
			continue
		}
		next := annotated
		next.Value = value
		next.TxIDs = []string{delta.TxID}
		txItem := txByID[delta.TxID]
		next.RoutingOrdinal = routingOrdinalForTransaction(txItem)
		if dependency, ok := stateVersionDependencyForKey(txItem, unqualifiedKey); ok && dependency.ProducedVersion > 0 {
			for _, access := range txItem.AccessList {
				if access.Key == unqualifiedKey && isVersionedStateAccess(access) {
					next.PreviousVersion = dependency.RequiredVersion
					next.ProducedVersion = dependency.ProducedVersion
					break
				}
			}
		}
		next.BaseValue = ""
		next.BaseValueDigest = ""
		for _, read := range delta.ReadSet {
			if stateKeysReferToSameLogicalKey(read.Key, unqualifiedKey) {
				next.BaseValue = read.Value
				next.BaseValueDigest = stateValueDigest(read.Value)
				break
			}
		}
		out = append(out, next)
	}
	if len(out) == 0 {
		return []state.StateKV{annotated}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].RoutingOrdinal != out[j].RoutingOrdinal {
			return out[i].RoutingOrdinal < out[j].RoutingOrdinal
		}
		return strings.Join(out[i].TxIDs, "|") < strings.Join(out[j].TxIDs, "|")
	})
	return out
}

func writeSetLogicalValue(writeSet map[string]string, logicalKey string) (string, bool) {
	if value, ok := writeSet[logicalKey]; ok {
		return value, true
	}
	for key, value := range writeSet {
		if stateKeysReferToSameLogicalKey(key, logicalKey) {
			return value, true
		}
	}
	return "", false
}

func routingOrdinalForTransaction(item tx.SignedTransaction) uint64 {
	if item.ExecutionRouting != nil && item.ExecutionRouting.RoutingOrdinal > 0 {
		return item.ExecutionRouting.RoutingOrdinal
	}
	return 0
}

func maxRoutingOrdinalForTxIDs(txIDs []string, transactions []tx.SignedTransaction) uint64 {
	wanted := make(map[string]bool, len(txIDs))
	for _, txID := range txIDs {
		wanted[txID] = true
	}
	var out uint64
	for _, item := range transactions {
		if wanted[item.TxID] {
			if ordinal := routingOrdinalForTransaction(item); ordinal > out {
				out = ordinal
			}
		}
	}
	return out
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
		requestID := stableTextDigest(strings.Join([]string{r.node.NodeID, targetNode, block.BlockHash, joinedTxIDs, item.Key, unqualifiedKey, item.Value, item.UpdateSemantics, fmt.Sprint(item.Delta), baseValueDigest, fmt.Sprint(item.RoutingOrdinal), fmt.Sprint(item.PreviousVersion), fmt.Sprint(item.ProducedVersion), fmt.Sprint(item.OrderingNoop), homeShard, r.node.ShardID}, "|"))
		waiter := make(chan StateDeltaApplyAck, 1)
		r.mu.Lock()
		if r.stateApplyWaiters == nil {
			r.stateApplyWaiters = map[string]chan StateDeltaApplyAck{}
		}
		r.stateApplyWaiters[requestID] = waiter
		r.mu.Unlock()
		request := r.plugins.StateAccess.BuildDeltaApplyRequest(StateDeltaApplyInput{RequestID: requestID, TxID: joinedTxIDs, TxIDs: append([]string(nil), item.TxIDs...), BlockHash: block.BlockHash, Key: unqualifiedKey, Value: item.Value, UpdateSemantics: item.UpdateSemantics, Delta: item.Delta, BaseValue: baseValue, BaseValueDigest: baseValueDigest, ApplyOrigin: item.ApplyOrigin, DeltaKind: item.DeltaKind, HasInitialValue: item.HasInitialValue, InitialValue: item.InitialValue, HomeShard: homeShard, ExecutionShard: r.node.ShardID, SourceKey: item.Key, SourceHeight: block.Height, RoutingOrdinal: item.RoutingOrdinal, PreviousVersion: item.PreviousVersion, ProducedVersion: item.ProducedVersion, OrderingNoop: item.OrderingNoop})
		envelope, err := p2p.NewEnvelope(stateDeltaApplyMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, 0, block.Height, request)
		if err != nil {
			r.mu.Lock()
			delete(r.stateApplyWaiters, requestID)
			r.mu.Unlock()
			return nil, time.Since(start), err
		}
		if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
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
	return r.sendStateAccessToNode(ctx, requester, envelope)
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
	if request.ProducedVersion > 0 && request.UpdateSemantics != "commutative_delta" {
		r.publishStateVersion(request.Key, request.ProducedVersion, request.Value)
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

func (r *NodeRuntime) remoteStateDeltaDrainState(
	homeBlockHeight uint64,
) ([]realblock.SystemStateDelta, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	pending := len(r.pendingStateDeltas) > 0
	ready := make([]StateDeltaApplyRequest, 0, len(r.pendingStateDeltas))

	for _, request := range r.pendingStateDeltas {
		if remoteStateDeltaReadyForHomeBlock(request, homeBlockHeight) {
			ready = append(ready, request)
		}
	}

	sort.SliceStable(ready, func(i, j int) bool {
		return remoteDeltaConsensusOrder(ready[i]) <
			remoteDeltaConsensusOrder(ready[j])
	})

	out := make([]realblock.SystemStateDelta, 0, len(ready))
	for _, request := range ready {
		out = append(out, systemStateDeltaFromRequest(request))
	}

	return out, pending
}

func (r *NodeRuntime) readyRemoteStateDeltasForConsensus(
	homeBlockHeight uint64,
) []realblock.SystemStateDelta {
	ready, _ := r.remoteStateDeltaDrainState(homeBlockHeight)
	return ready
}

func (r *NodeRuntime) markRemoteStateDeltasApplied(applied []realblock.SystemStateDelta) {
	if len(applied) == 0 {
		return
	}
	appliedKeys := map[string]bool{}
	versions := map[string]uint64{}
	for _, item := range applied {
		if item.DeltaID != "" {
			appliedKeys[item.DeltaID] = true
		}
		if item.ProducedVersion > versions[item.Key] {
			versions[item.Key] = item.ProducedVersion
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.appliedStateDeltaKeys == nil {
		r.appliedStateDeltaKeys = map[string]bool{}
	}
	if r.stateVersionMaterialized == nil {
		r.stateVersionMaterialized = map[string]uint64{}
	}
	for key, version := range versions {
		if version > r.stateVersionMaterialized[key] {
			r.stateVersionMaterialized[key] = version
		}
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
		RoutingOrdinal:  request.RoutingOrdinal,
		PreviousVersion: request.PreviousVersion,
		ProducedVersion: request.ProducedVersion,
		OrderingNoop:    request.OrderingNoop,
		SourceBlockHash: request.BlockHash,
	}
}

func (r *NodeRuntime) publishSystemStateDeltaVersions(items []realblock.SystemStateDelta) {
	for _, item := range items {
		if item.ProducedVersion == 0 || item.UpdateSemantics == "commutative_delta" || item.Key == "" {
			continue
		}
		r.publishStateVersion(item.Key, item.ProducedVersion, item.Value)
	}
}

func (r *NodeRuntime) materializableRemoteStateDeltas(block realblock.Block, homeShard string) []state.StateKV {
	r.mu.Lock()
	materialized := make(map[string]uint64, len(r.stateVersionMaterialized))
	for key, version := range r.stateVersionMaterialized {
		materialized[key] = version
	}
	r.mu.Unlock()

	out := make([]state.StateKV, 0, len(block.SystemStateDeltas))
	for _, item := range block.SystemStateDeltas {
		if item.HomeShard != "" && item.HomeShard != homeShard {
			continue
		}
		if item.ProducedVersion > 0 && item.UpdateSemantics != "commutative_delta" {
			if item.ProducedVersion <= materialized[item.Key] {
				continue
			}
			materialized[item.Key] = item.ProducedVersion
			if item.OrderingNoop {
				continue
			}
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
			RoutingOrdinal:  item.RoutingOrdinal,
			PreviousVersion: item.PreviousVersion,
			ProducedVersion: item.ProducedVersion,
			OrderingNoop:    item.OrderingNoop,
		})
	}
	return out
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
			RoutingOrdinal:  item.RoutingOrdinal,
			PreviousVersion: item.PreviousVersion,
			ProducedVersion: item.ProducedVersion,
			OrderingNoop:    item.OrderingNoop,
		})
	}
	return out
}

func remoteStateDeltaReadyForHomeBlock(request StateDeltaApplyRequest, homeBlockHeight uint64) bool {
	if request.ProducedVersion > 0 && request.UpdateSemantics != "commutative_delta" {
		return true
	}
	return request.SourceHeight <= homeBlockHeight
}

func remoteDeltaConsensusOrder(request StateDeltaApplyRequest) string {
	if request.ProducedVersion > 0 && request.UpdateSemantics != "commutative_delta" {
		return strings.Join([]string{
			"versioned",
			request.Key,
			fmt.Sprintf("%020d", request.ProducedVersion),
			request.TxID,
			request.ExecutionShard,
			request.BlockHash,
		}, "|")
	}
	ordinal := request.RoutingOrdinal
	if ordinal == 0 {
		ordinal = ^uint64(0)
	}
	return strings.Join([]string{
		"legacy",
		fmt.Sprintf("%020d", ordinal),
		request.Key,
		fmt.Sprintf("%020d", request.SourceHeight),
		request.BlockHash,
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
		if item.OrderingNoop {
			continue
		}
		if item.UpdateSemantics == "commutative_delta" {
			current, _ := strconv.ParseInt(out[key], 10, 64)
			if out[key] == "" && item.HasInitialValue {
				current = item.InitialValue
			}
			out[key] = strconv.FormatInt(current+item.Delta, 10)
			continue
		}
		if item.ProducedVersion == 0 && item.BaseValueDigest != "" {
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
	return StateDeltaApplyAck{RequestID: request.RequestID, TxID: request.TxID, TxIDs: append([]string(nil), request.TxIDs...), BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: qualifiedKey, ValueDigest: valueDigest, UpdateSemantics: request.UpdateSemantics, Delta: request.Delta, BaseValueDigest: request.BaseValueDigest, ApplyOrigin: request.ApplyOrigin, DeltaKind: request.DeltaKind, HasInitialValue: request.HasInitialValue, InitialValue: request.InitialValue, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, PreviousVersion: request.PreviousVersion, ProducedVersion: request.ProducedVersion, OrderingNoop: request.OrderingNoop, StateRoot: stateRoot, Success: true}
}

func stateDeltaApplyWitnessDigest(ack StateDeltaApplyAck) string {
	return stableTextDigest(strings.Join([]string{ack.BlockHash, ack.QualifiedKey, ack.ValueDigest, ack.StateRoot, ack.HomeShard, ack.ExecutionShard, ack.UpdateSemantics, fmt.Sprint(ack.Delta), ack.BaseValueDigest, ack.ApplyOrigin, ack.DeltaKind, fmt.Sprint(ack.HasInitialValue), fmt.Sprint(ack.InitialValue), fmt.Sprint(ack.PreviousVersion), fmt.Sprint(ack.ProducedVersion), fmt.Sprint(ack.OrderingNoop)}, "|"))
}

func stateDeltaApplyKey(request StateDeltaApplyRequest) string {
	return stableTextDigest(strings.Join([]string{fmt.Sprint(request.RoutingOrdinal), fmt.Sprint(request.SourceHeight), request.BlockHash, request.SourceKey, request.Key, request.TxID, request.UpdateSemantics, fmt.Sprint(request.Delta), request.BaseValueDigest, request.ApplyOrigin, request.DeltaKind, fmt.Sprint(request.HasInitialValue), fmt.Sprint(request.InitialValue), fmt.Sprint(request.PreviousVersion), fmt.Sprint(request.ProducedVersion), fmt.Sprint(request.OrderingNoop), request.HomeShard, request.ExecutionShard}, "|"))
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
	deltaID := item.DeltaID
	if deltaID == "" {
		deltaID = stateDeltaApplyKey(StateDeltaApplyRequest{TxID: ack.TxID, TxIDs: append([]string(nil), ack.TxIDs...), BlockHash: block.BlockHash, Key: unqualifiedKey, UpdateSemantics: ack.UpdateSemantics, Delta: ack.Delta, BaseValueDigest: ack.BaseValueDigest, ApplyOrigin: ack.ApplyOrigin, DeltaKind: ack.DeltaKind, HasInitialValue: ack.HasInitialValue, InitialValue: ack.InitialValue, HomeShard: ack.HomeShard, ExecutionShard: ack.ExecutionShard, SourceKey: item.Key, SourceHeight: block.Height})
	}
	row := []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), block.BlockHash, ack.TxID, unqualifiedKey, ack.QualifiedKey, ack.HomeShard, ack.ExecutionShard, accessKind, fmt.Sprint(latency.Milliseconds()), ack.WitnessDigest, ack.StateRoot, fmt.Sprint(ack.Success), ack.Error, deltaID, fmt.Sprint(block.Height), block.BlockHash, ack.UpdateSemantics}
	r.mu.Lock()
	r.remoteStateRows = append(r.remoteStateRows, row)
	r.mu.Unlock()
	r.emitRuntimeEvent(RuntimeEvent{Type: "RemoteStateApplied", TxID: ack.TxID, BlockHash: block.BlockHash, Height: block.Height, Success: ack.Success, Error: ack.Error, Attributes: map[string]any{"key": unqualifiedKey, "home_shard": ack.HomeShard, "execution_shard": ack.ExecutionShard, "latency_ms": latency.Milliseconds(), "update_semantics": ack.UpdateSemantics}})
}

func (r *NodeRuntime) recordRemoteStateAccess(block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, response StateFetchResponse, latency time.Duration) {
	row := []string{fmt.Sprint(time.Now().UnixMilli()), r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), block.BlockHash, item.TxID, access.Key, response.QualifiedKey, response.HomeShard, response.ExecutionShard, string(access.Mode), fmt.Sprint(latency.Milliseconds()), response.WitnessDigest, response.StateRoot, fmt.Sprint(response.Success), response.Error, "", "", "", ""}
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
func (r *NodeRuntime) clearCommittedRelayReplica(
	relay Relay,
) {
	logicalID := relayLogicalID(relay)

	if logicalID == "" {
		return
	}

	r.mu.Lock()

	delete(r.relaySource, logicalID)
	delete(r.relaySource, relay.Tx.TxID)

	delete(r.relayAdmissionFailures, logicalID)
	delete(
		r.relayAdmissionFailures,
		relay.Tx.TxID,
	)

	r.lastProgressAt = time.Now().UnixMilli()

	r.mu.Unlock()
}

func (r *NodeRuntime) onCommittedTx(ctx context.Context, item tx.SignedTransaction, relay Relay) {
	r.onCommittedTxWithOrigin(ctx, item, relay, CommitOriginConsensus)
}

func (r *NodeRuntime) onCommittedTxWithOrigin(ctx context.Context, item tx.SignedTransaction, relay Relay, origin CommitOrigin) {
	if origin != CommitOriginConsensus {
		return
	}
	if relay.Tx.TxID != "" {
		if !r.isCurrentLeader() {
			r.clearCommittedRelayReplica(relay)
			return
		}
		logicalID := relay.LogicalTxID
		if logicalID == "" {
			logicalID = item.TxID
		}
		crossShard := r.crossShardPlugin()
		r.recordCrossShardEvent(crossShard.TargetCommit(CrossShardFinalizeInput{TxID: item.TxID, LogicalTxID: logicalID, SourceShard: relay.SourceShard, TargetShard: relay.TargetShard}))
		r.queueFinalize(ctx, relay)
		return
	}
	if !r.isCurrentLeader() {
		return
	}
	if strings.Contains(item.Payload, "v5_timeout") {
		r.recordEvent(item.TxID, r.node.ShardID, "", "Timeout", true, "target_timeout")
		r.recordCrossShardEvent(r.crossShardPlugin().TimeoutRefund(CrossShardFinalizeInput{TxID: item.TxID, LogicalTxID: tx.SemanticID(item), SourceShard: r.node.ShardID}, "target_timeout"))
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
		relayInput := CrossShardRelayInput{Tx: item, LogicalTxID: tx.SemanticID(item), SourceShard: r.node.ShardID, TargetShard: target}
		r.recordCrossShardEvent(crossShard.SourceLock(relayInput))
		relay := crossShard.BuildRelay(relayInput)
		r.queueOutboundRelay(ctx, relay)
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
	if shard == r.node.ShardID {
		if state := r.pbftState(); state != nil {
			if leader := state.Leader(); leader != "" {
				return leader
			}
		}
	}
	return initialLeaderID(r.plan, shard)
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
	if r.sendToNodeHook != nil {
		return r.sendToNodeHook(ctx, nodeID, envelope)
	}
	if r.transport == nil {
		return fmt.Errorf("transport is not configured")
	}
	return r.transport.Send(ctx, nodeID, envelope)
}

func (r *NodeRuntime) sendStateAccessToNode(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
	if r.sendToNodeHook != nil {
		return r.sendToNodeHook(ctx, nodeID, envelope)
	}
	if r.transport == nil {
		return fmt.Errorf("transport is not configured")
	}
	return r.transport.SendStateAccess(ctx, nodeID, envelope)
}

func (r *NodeRuntime) sendCatchupToNode(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
	if r.sendToNodeHook != nil {
		return r.sendToNodeHook(ctx, nodeID, envelope)
	}
	if r.transport == nil {
		return fmt.Errorf("transport is not configured")
	}
	return r.transport.SendCatchup(ctx, nodeID, envelope)
}
func (r *NodeRuntime) blockSize() int {
	if r.plugins.BlockProducer == nil {
		return 1
	}
	return r.plugins.BlockProducer.BlockSize()
}
func (r *NodeRuntime) blockInterval() time.Duration {
	if r.plugins.BlockProducer == nil {
		return 25 * time.Millisecond
	}
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
	workUnits := int(r.proposalWorkUnits.Load())
	if workUnits < blockSize {
		workUnits = blockSize
	}
	interval := r.blockInterval()
	timeout := 5*time.Second + time.Duration(workUnits)*100*time.Millisecond + 4*interval
	if r.plugins.Consensus != nil {
		if consensusTimeout := r.plugins.Consensus.Timeout(); consensusTimeout > timeout {
			timeout = consensusTimeout
		}
	}
	if timeout < 5*time.Second {
		return 5 * time.Second
	}
	if timeout > 60*time.Second {
		return 60 * time.Second
	}
	return timeout
}

func (r *NodeRuntime) estimateProposalValidationWork(block realblock.Block) int {
	workUnits := r.blockSize()
	if workUnits < 1 {
		workUnits = 1
	}
	if len(block.TxList) > workUnits {
		workUnits = len(block.TxList)
	}
	if estimator, ok := r.plugins.BlockProducer.(proposalValidationWorkEstimator); ok {
		if estimated := estimator.EstimateProposalValidationWork(block); estimated > workUnits {
			workUnits = estimated
		}
	}
	return workUnits
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
			r.runtimeEventTotal++
			if len(r.runtimeEventRows) < runtimeEventTraceRetentionLimit {
				r.runtimeEventRows = append(r.runtimeEventRows, row)
			} else {
				r.runtimeEventRowsDropped++
			}
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

type lifecycleStatusSets struct {
	terminal          map[string]bool
	durableCommitted  map[string]bool
	sourceFinalized   map[string]bool
	refunded          map[string]bool
	failed            map[string]bool
	executionFailed   map[string]bool
	admissionRejected map[string]bool
}

func classifyLifecycleStatus(events []LifecycleEvent) lifecycleStatusSets {
	sets := lifecycleStatusSets{
		terminal:          map[string]bool{},
		durableCommitted:  map[string]bool{},
		sourceFinalized:   map[string]bool{},
		refunded:          map[string]bool{},
		failed:            map[string]bool{},
		executionFailed:   map[string]bool{},
		admissionRejected: map[string]bool{},
	}
	crossLogical := map[string]bool{}
	completedCross := map[string]bool{}

	for _, event := range events {
		stage := strings.ToLower(event.Stage)
		logicalID := event.LogicalTxID
		if logicalID == "" {
			logicalID = event.TxID
		}
		if logicalID == "" {
			continue
		}
		switch stage {
		case "durable_committed":
			sets.durableCommitted[logicalID] = true
		case "sourcefinalize":
			sets.sourceFinalized[logicalID] = true
			completedCross[logicalID] = true
		case "refund":
			sets.refunded[logicalID] = true
			completedCross[logicalID] = true
		case "failed":
			sets.failed[logicalID] = true
			if event.BlockHeight > 0 {
				// A failed receipt emitted after a committed block is a
				// deterministic execution outcome, not a mempool observation.
				sets.executionFailed[logicalID] = true
			} else {
				// Gossip/admission can be replica-local and out of order. Keep
				// it visible for diagnostics, but do not make it node-terminal.
				sets.admissionRejected[logicalID] = true
			}
		}
		if stage == "sourcelock" || stage == "relaycertificate" || stage == "targetcommit" || stage == "sourcefinalize" {
			crossLogical[logicalID] = true
		}
	}

	for _, event := range events {
		stage := strings.ToLower(event.Stage)
		logicalID := event.LogicalTxID
		if logicalID == "" {
			logicalID = event.TxID
		}
		if logicalID == "" {
			continue
		}
		switch stage {
		case "durable_committed":
			if crossLogical[logicalID] && !completedCross[logicalID] {
				continue
			}
			sets.terminal[logicalID] = true
		case "sourcefinalize", "refund":
			sets.terminal[logicalID] = true
		case "failed":
			// Admission rejection has BlockHeight==0 and is only observational.
			// A committed-block execution failure is terminal for an intra-shard
			// transaction. Legacy cross-shard execution still waits for its
			// source-finalize/refund protocol outcome.
			if event.BlockHeight == 0 {
				continue
			}
			if crossLogical[logicalID] && !completedCross[logicalID] {
				continue
			}
			sets.terminal[logicalID] = true
		}
	}
	return sets
}

func (r *NodeRuntime) writeRuntimeStatus() error {
	r.mu.Lock()
	committedHeight := r.committedHeight
	committedHash := r.committedHash
	catchupTargetHeight := r.catchupTargetHeight
	proposalInFlight := r.proposalInFlight
	proposalInFlightHash := r.proposalInFlightHash
	proposalRetransmitCount := r.proposalRetransmitCount
	proposalPlanningInFlight := r.proposalPlanningInFlight
	proposalPlanningView := r.proposalPlanningView
	proposalPlanningHeight := r.proposalPlanningHeight
	proposalPlanningAlgorithmID := r.proposalPlanningAlgorithmID
	proposalPlanningPhase := r.proposalPlanningPhase
	proposalPlanningWorkUnits := r.proposalPlanningWorkUnits
	proposalPlanningDetailCount := r.proposalPlanningDetailCount
	proposalPlanningCancelReason := r.proposalPlanningCancelReason
	proposalPlanningStartedAtMS := int64(0)
	if !r.proposalPlanningStartedAt.IsZero() {
		proposalPlanningStartedAtMS = r.proposalPlanningStartedAt.UnixMilli()
	}
	proposalPlanningProgressAtMS := int64(0)
	if !r.proposalPlanningProgressAt.IsZero() {
		proposalPlanningProgressAtMS = r.proposalPlanningProgressAt.UnixMilli()
	}
	viewChangeTarget := r.viewChangeTarget
	proposalWorkDetailsAvailable := true
	proposalLogicalTxIDs := []string{}
	proposalSystemStateDeltaCount := 0
	if proposalInFlight {
		proposalBlock, found := r.proposals[proposalInFlightHash]
		if proposalInFlightHash == "" || !found || proposalBlock.BlockHash == "" {
			proposalWorkDetailsAvailable = false
		} else {
			proposalIDs := map[string]bool{}
			for _, item := range proposalBlock.TxList {
				logicalID := strings.TrimSpace(item.TxID)
				if logicalID != "" {
					proposalIDs[logicalID] = true
				}
			}
			proposalLogicalTxIDs = mapIDs(proposalIDs)
			proposalSystemStateDeltaCount = len(proposalBlock.SystemStateDeltas)
		}
	}
	proposalWorkUnits := r.proposalWorkUnits.Load()
	proposalTimeoutMS := r.proposalTimeout().Milliseconds()
	proposalVoteCount := 0
	proposalQuorum := 0
	proposalQuorumReached := false
	proposalAgeMS := int64(0)
	proposalCommitting := false
	proposalFinalizeQueued := false
	if proposalInFlight && proposalInFlightHash != "" {
		proposalVoteCount = len(r.votes[proposalInFlightHash])
		if r.plugins.Consensus != nil && len(r.node.Validators) > 0 {
			proposalQuorum = r.plugins.Consensus.Quorum(len(r.node.Validators))
			proposalQuorumReached = proposalVoteCount >= proposalQuorum
		}
		if !r.proposalStartedAt.IsZero() {
			proposalAgeMS = time.Since(r.proposalStartedAt).Milliseconds()
		}
		proposalCommitting = r.committing[proposalInFlightHash]
		commitKey := string(commitTaskConsensus) + "|" + string(CommitOriginConsensus) + "|" + proposalInFlightHash
		proposalFinalizeQueued = r.queuedCommitTasks[commitKey]
	}
	commitWorkerRunning := r.commitWorkerContext != nil && r.commitTasks != nil
	commitTaskQueueDepth := 0
	commitTaskQueueCapacity := 0
	if r.commitTasks != nil {
		commitTaskQueueDepth = len(r.commitTasks)
		commitTaskQueueCapacity = cap(r.commitTasks)
	}
	pendingFutureBlockHeights := make([]uint64, 0, len(r.deferredPrePrepares))
	for height := range r.deferredPrePrepares {
		pendingFutureBlockHeights = append(pendingFutureBlockHeights, height)
	}
	sort.Slice(pendingFutureBlockHeights, func(i, j int) bool { return pendingFutureBlockHeights[i] < pendingFutureBlockHeights[j] })
	pendingFutureBlockCount := len(pendingFutureBlockHeights)
	pbftQuorumFinalizeRetryCount := r.runtimeMetricCounts["pbft_quorum_finalize_retry_count"]
	pbftCatchupMetrics := map[string]int64{}
	for _, key := range []string{
		"pbft_catchup_request_count",
		"pbft_catchup_urgent_request_count",
		"pbft_catchup_request_peer_count",
		"pbft_catchup_block_count",
		"pbft_catchup_success_count",
		"pbft_catchup_failure_count",
		"pbft_catchup_block_body_failure_count",
		"pbft_catchup_certificate_auth_failure_count",
		"pbft_catchup_certificate_accept_failure_count",
		"pbft_catchup_parent_mismatch_count",
		"pbft_catchup_enqueue_failure_count",
		"pbft_catchup_source_block_identity_failure_count",
		"pbft_catchup_unavailable_count",
		"pbft_catchup_response_busy_count",
		"pbft_catchup_blocks_served_count",
		"pbft_catchup_indexed_range_read_count",
		"pbft_catchup_stale_block_drop_count",
		"pbft_catchup_unavailable_send_failure_count",
	} {
		pbftCatchupMetrics[key] = r.runtimeMetricCounts[key]
	}
	commitPhase := r.commitPhase
	commitPhaseHeight := r.commitPhaseHeight
	commitPhaseHash := r.commitPhaseHash
	lastProposalError := r.lastProposalError
	lastCommitFailure := r.lastCommitFailure
	lastStateFetch := r.lastStateFetch
	lastStateFetchService := r.lastStateFetchService
	stateFetchFailures := append([]StateFetchDiagnostic(nil), r.stateFetchFailures...)
	stateFetchServiceErrors := append([]StateFetchDiagnostic(nil), r.stateFetchServiceErrors...)
	pendingStateFetches := make([]StateFetchDiagnostic, 0, len(r.pendingStateFetches))
	for _, diagnostic := range r.pendingStateFetches {
		pendingStateFetches = append(pendingStateFetches, diagnostic)
	}
	sort.Slice(pendingStateFetches, func(i, j int) bool { return pendingStateFetches[i].RequestID < pendingStateFetches[j].RequestID })
	fatalPersistenceError := r.fatalPersistenceError
	fatalExecutionError := r.fatalExecutionError
	fatalPlanningError := r.fatalPlanningError
	blockExecutionProgress := r.blockExecutionProgress
	pendingCommitCount := len(r.pendingCommits)
	pendingCommitHeights := mapKeys(r.pendingCommits)
	pendingCommitErrors := map[uint64]string{}
	for key, value := range r.pendingCommitErrors {
		pendingCommitErrors[key] = value
	}
	outboundRelaySendErrors := map[string]string{}
	for key, value := range r.outboundRelaySendErrors {
		outboundRelaySendErrors[key] = value
	}

	finalizeSendErrors := map[string]string{}
	for key, value := range r.finalizeSendErrors {
		finalizeSendErrors[key] = value
	}
	relayAdmissionFailures := map[string]string{}
	for key, value := range r.relayAdmissionFailures {
		relayAdmissionFailures[key] = value
	}
	lastProgressAt := r.lastProgressAt
	lifecycleSets := classifyLifecycleStatus(r.lifecycle)
	terminalIDs := mapIDs(lifecycleSets.terminal)
	durableIDs := mapIDs(lifecycleSets.durableCommitted)
	sourceFinalizedIDs := mapIDs(lifecycleSets.sourceFinalized)
	refundedIDs := mapIDs(lifecycleSets.refunded)
	failedIDs := mapIDs(lifecycleSets.failed)
	executionFailedIDs := mapIDs(lifecycleSets.executionFailed)
	admissionRejectedIDs := mapIDs(lifecycleSets.admissionRejected)
	pendingCrossShardSet := map[string]bool{}

	pendingRelaySourceIDs := make(
		[]string,
		0,
		len(r.relaySource),
	)
	for logicalID := range r.relaySource {
		pendingRelaySourceIDs = append(
			pendingRelaySourceIDs,
			logicalID,
		)
		pendingCrossShardSet[logicalID] = true
	}
	sort.Strings(pendingRelaySourceIDs)

	pendingOutboundRelayIDs := make(
		[]string,
		0,
		len(r.pendingOutboundRelays),
	)
	for logicalID := range r.pendingOutboundRelays {
		pendingOutboundRelayIDs = append(
			pendingOutboundRelayIDs,
			logicalID,
		)
		pendingCrossShardSet[logicalID] = true
	}
	sort.Strings(pendingOutboundRelayIDs)

	pendingFinalizeMessageIDs := make(
		[]string,
		0,
		len(r.pendingFinalizeMessages),
	)
	for logicalID := range r.pendingFinalizeMessages {
		pendingFinalizeMessageIDs = append(
			pendingFinalizeMessageIDs,
			logicalID,
		)
		pendingCrossShardSet[logicalID] = true
	}
	sort.Strings(pendingFinalizeMessageIDs)

	pendingCrossShardIDs := mapIDs(
		pendingCrossShardSet,
	)

	pendingRelaySourceCount :=
		len(pendingRelaySourceIDs)
	pendingOutboundRelayCount :=
		len(pendingOutboundRelayIDs)
	pendingFinalizeMessageCount :=
		len(pendingFinalizeMessageIDs)
	pendingCrossShardCount :=
		len(pendingCrossShardIDs)
	pendingStateDeltaCount := len(r.pendingStateDeltas)
	pendingStateDeltaKeyCount := len(r.pendingStateDeltaKeys)
	readyStateDeltaCount := 0
	for _, request := range r.pendingStateDeltas {
		if remoteStateDeltaReadyForHomeBlock(request, committedHeight+1) {
			readyStateDeltaCount++
		}
	}
	stateFetchRequestQueueDepth := len(r.stateFetchTasks)
	stateFetchResponseQueueDepth := len(r.stateFetchResponseTasks)
	r.mu.Unlock()

	pbftSnapshot := r.pbftState().Snapshot()
	pbftPrepareVoteCount := 0
	pbftCommitVoteCount := 0
	if proposalInFlightHash != "" {
		pbftPrepareVoteCount = r.pbftState().PrepareVoteCount(proposalInFlightHash)
		pbftCommitVoteCount = r.pbftState().CommitVoteCount(proposalInFlightHash)
	}
	pbftViewChangeVoteCount := 0
	if viewChangeTarget > pbftSnapshot.View {
		pbftViewChangeVoteCount = r.pbftState().ViewChangeVoteCount(viewChangeTarget)
	}

	mempoolIDs := r.pool.IDs()
	sort.Strings(mempoolIDs)
	status := map[string]any{"node_id": r.node.NodeID, "shard_id": r.node.ShardID, "role": r.node.Role, "committed_height": committedHeight, "committed_block_hash": committedHash, "mempool_depth": r.pool.Len(), "mempool_logical_tx_ids": mempoolIDs, "reserved_tx_count": r.pool.ReservedCount(), "proposal_in_flight": proposalInFlight, "proposal_in_flight_hash": proposalInFlightHash, "proposal_work_details_available": proposalWorkDetailsAvailable, "proposal_logical_tx_ids": proposalLogicalTxIDs, "proposal_system_state_delta_count": proposalSystemStateDeltaCount, "proposal_validation_work_units": proposalWorkUnits, "proposal_timeout_ms": proposalTimeoutMS, "proposal_planning_in_flight": proposalPlanningInFlight, "proposal_planning_view": proposalPlanningView, "proposal_planning_height": proposalPlanningHeight, "proposal_planning_algorithm_id": proposalPlanningAlgorithmID, "proposal_planning_phase": proposalPlanningPhase, "proposal_planning_started_at_ms": proposalPlanningStartedAtMS, "proposal_planning_progress_at_ms": proposalPlanningProgressAtMS, "proposal_planning_work_units": proposalPlanningWorkUnits, "proposal_planning_detail_count": proposalPlanningDetailCount, "proposal_planning_cancel_reason": proposalPlanningCancelReason, "proposal_vote_count": proposalVoteCount, "proposal_quorum": proposalQuorum, "proposal_quorum_reached": proposalQuorumReached, "proposal_age_ms": proposalAgeMS, "proposal_committing": proposalCommitting, "proposal_finalize_queued": proposalFinalizeQueued, "pbft_quorum_finalize_retry_count": pbftQuorumFinalizeRetryCount, "pbft_view": pbftSnapshot.View, "pbft_current_leader": pbftSnapshot.LeaderID, "pbft_stage": pbftSnapshot.Stage, "pbft_prepare_vote_count": pbftPrepareVoteCount, "pbft_prepare_quorum": pbftSnapshot.PrepareQuorum, "pbft_commit_vote_count": pbftCommitVoteCount, "pbft_commit_quorum": pbftSnapshot.CommitQuorum, "pbft_commit_certificate_count": pbftSnapshot.CommitCertificateCount, "pbft_last_consensus_progress_at_ms": pbftSnapshot.LastProgressAtMS, "pbft_preprepare_retransmit_count": proposalRetransmitCount, "pbft_view_change_target": viewChangeTarget, "pbft_view_change_vote_count": pbftViewChangeVoteCount, "pbft_low_watermark": pbftSnapshot.LowWatermark, "pbft_high_watermark": pbftSnapshot.HighWatermark, "pbft_stable_checkpoint_height": pbftSnapshot.StableCheckpointHeight, "pbft_catchup_target_height": catchupTargetHeight, "pbft_catchup_metrics": pbftCatchupMetrics, "commit_worker_running": commitWorkerRunning, "commit_task_queue_depth": commitTaskQueueDepth, "commit_task_queue_capacity": commitTaskQueueCapacity, "commit_phase": commitPhase, "commit_phase_height": commitPhaseHeight, "commit_phase_hash": commitPhaseHash, "last_proposal_error": lastProposalError, "last_commit_failure": lastCommitFailure, "last_state_fetch": lastStateFetch, "pending_state_fetch_count": len(pendingStateFetches), "pending_state_fetches": pendingStateFetches, "state_fetch_failures": stateFetchFailures, "last_state_fetch_service": lastStateFetchService, "state_fetch_service_errors": stateFetchServiceErrors, "fatal_persistence_error": fatalPersistenceError, "fatal_execution_error": fatalExecutionError, "fatal_planning_error": fatalPlanningError, "block_execution_progress": blockExecutionProgress, "block_execution_height": blockExecutionProgress.BlockHeight, "block_execution_progress_at_ms": blockExecutionProgress.LastProgressAtMS, "block_execution_validated_count": blockExecutionProgress.ValidatedCount, "block_execution_task_count": blockExecutionProgress.ExecutionTaskCount, "block_validation_task_count": blockExecutionProgress.ValidationTaskCount, "block_execution_abort_count": blockExecutionProgress.AbortCount, "block_execution_reexecution_count": blockExecutionProgress.ReexecutionCount, "block_execution_scheduler_queue_length": blockExecutionProgress.SchedulerQueueLen, "pending_commit_count": pendingCommitCount, "pending_commit_heights": pendingCommitHeights, "pending_commit_errors": pendingCommitErrors, "pending_future_block_count": pendingFutureBlockCount, "pending_future_block_heights": pendingFutureBlockHeights, "pending_cross_shard_count": pendingCrossShardCount, "pending_cross_shard_ids": pendingCrossShardIDs, "pending_relay_source_count": pendingRelaySourceCount, "pending_relay_source_ids": pendingRelaySourceIDs, "pending_outbound_relay_count": pendingOutboundRelayCount, "pending_outbound_relay_ids": pendingOutboundRelayIDs, "pending_finalize_message_count": pendingFinalizeMessageCount, "pending_finalize_message_ids": pendingFinalizeMessageIDs, "outbound_relay_send_errors": outboundRelaySendErrors, "finalize_send_errors": finalizeSendErrors, "state_fetch_request_queue_depth": stateFetchRequestQueueDepth, "state_fetch_request_queue_capacity": stateFetchMailboxCapacity, "state_fetch_response_queue_depth": stateFetchResponseQueueDepth, "state_fetch_response_queue_capacity": stateFetchResponseMailboxCapacity, "pending_state_delta_count": pendingStateDeltaCount, "pending_state_delta_key_count": pendingStateDeltaKeyCount, "ready_state_delta_count": readyStateDeltaCount, "relay_admission_failures": relayAdmissionFailures, "terminal_count": len(lifecycleSets.terminal), "terminal_logical_tx_ids": terminalIDs, "durable_committed_logical_tx_ids": durableIDs, "source_finalized_logical_tx_ids": sourceFinalizedIDs, "refunded_logical_tx_ids": refundedIDs, "failed_logical_tx_ids": failedIDs, "execution_failed_logical_tx_ids": executionFailedIDs, "admission_rejected_logical_tx_ids": admissionRejectedIDs, "last_progress_at": lastProgressAt, "ready": true, "stopping": false}
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
	proposalEvidence := append([]map[string]any(nil), r.proposalEvidence...)
	schedulerAggregate := r.schedulerAggregate
	schedulerRowsRetained := len(r.schedulerRows)
	schedulerRowsDropped := r.schedulerRowsDropped
	runtimeEventTotal := r.runtimeEventTotal
	runtimeEventRowsDropped := r.runtimeEventRowsDropped
	txExecutionTraceRows := append([][]string(nil), r.txExecutionTraceRows...)
	observedStateAccessRows := append([][]string(nil), r.observedStateAccessRows...)
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
	if err := writeJSONL(filepath.Join(r.node.DataDir, "proposal_selection_evidence.jsonl"), proposalEvidence); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "transaction_execution_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "success", "error", "read_key_count", "write_key_count", "state_root_after_tx"}, txExecutionTraceRows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "observed_state_access.csv"), []string{"node_id", "shard_id", "block_hash", "height", "tx_id", "original_index", "access_type", "state_key", "value_digest", "source"}, observedStateAccessRows); err != nil {
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
	methodSummary := summarizeMethodRows(executionRows, commitRows)
	remoteSummary := summarizeRemoteStateRows(r.remoteStateRows)
	schedulerSummary := schedulerAggregate
	blockProduction := summarizeBlockProductionRows(chainRows)
	businessStateDigest := canonicalBusinessStateDigest(r.plugins.StateStorage.Snapshot(r.db))
	stateReadySummary := summarizeStateReadyEvidence(blockExecutionSummaries)
	return SaveJSON(filepath.Join(r.node.DataDir, "node_summary.json"), map[string]any{"runtime_stage": "v5_1_real_plugin_driven_multi_process_multishard_runtime", "runtime_truth": "v5_real_cluster_candidate", "node_id": r.node.NodeID, "shard_id": r.node.ShardID, "pid": os.Getpid(), "listen_addr": r.transport.ListenAddr, "committed_block_count": count, "state_root": r.plugins.StateStorage.Root(r.db), "business_state_digest": businessStateDigest, "state_ready_wait_count": stateReadySummary.waitCount, "state_ready_resume_count": stateReadySummary.resumeCount, "state_prefetch_wait_ms": stateReadySummary.waitMS, "remote_state_fetch_count": stateReadySummary.fetchCount, "remote_state_fetch_completed_count": stateReadySummary.fetchCompletedCount, "state_ready_scheduler_mode": stateReadySummary.mode, "versioned_state_ready_wave_count": stateReadySummary.versionedWaveCount, "versioned_state_ready_wait_observation_count": stateReadySummary.versionedWaitCount, "versioned_state_ready_resolved_token_count": stateReadySummary.versionedResolvedCount, "versioned_state_probe_count": stateReadySummary.versionedProbeCount, "versioned_state_probe_latency_ms": stateReadySummary.versionedProbeLatencyMS, "versioned_state_ready_max_wave_width": stateReadySummary.versionedMaxWaveWidth, "versioned_state_ready_scheduler_mode": stateReadySummary.versionedMode, "plugin_snapshot": r.pluginSnapshot, "block_executor_id": r.plugins.BlockExecutor.ID(), "block_executor_version": blockExecutorVersionFromSummaries(blockExecutionSummaries), "worker_count": artifactWorkerCount, "configured_block_size": r.blockSize(), "configured_block_interval_ms": int(r.blockInterval().Milliseconds()), "actual_committed_block_count": blockProduction.count, "actual_average_tx_per_block": blockProduction.averageTxPerBlock, "actual_min_tx_per_block": blockProduction.minTxPerBlock, "actual_max_tx_per_block": blockProduction.maxTxPerBlock, "actual_block_interval_mean_ms": blockProduction.intervalMeanMS, "actual_block_interval_p95_ms": blockProduction.intervalP95MS, "plan_digest_consistent": planDigestsConsistent(planDigestRows), "fast_track_count": methodSummary.fastTrackCount, "conservative_track_count": methodSummary.conservativeTrackCount, "aggregation_group_count": methodSummary.aggregationGroupCount, "logical_update_count": methodSummary.logicalUpdateCount, "physical_update_count": methodSummary.physicalUpdateCount, "logical_update_count_deprecated": true, "physical_update_count_deprecated": true, "executed_logical_transaction_count": methodSummary.executedLogicalTransactionCount, "executed_transaction_instance_count": methodSummary.executedTransactionInstanceCount, "pre_aggregation_physical_op_count": methodSummary.preAggregationPhysicalOps, "post_aggregation_physical_op_count": methodSummary.postAggregationPhysicalOps, "aggregated_key_count": methodSummary.aggregatedKeyCount, "aggregated_logical_delta_count": methodSummary.aggregatedLogicalDeltaCount, "physical_ops_saved_count": methodSummary.physicalOpsSavedCount(), "aggregation_reduction_ratio": methodSummary.aggregationReductionRatio(), "scheduler_event_count": schedulerSummary.total, "scheduler_blocked_count": schedulerSummary.blocked, "scheduler_wakeup_count": schedulerSummary.wakeup, "scheduler_stolen_work_count": schedulerSummary.stolen, "scheduler_local_execution_count": schedulerSummary.local, "scheduler_ready_queue_max_depth": schedulerSummary.readyMax, "scheduler_fast_queue_max_depth": schedulerSummary.fastMax, "scheduler_conservative_queue_max_depth": schedulerSummary.conservativeMax, "scheduler_dependency_wait_ms": schedulerSummary.dependencyWaitMS, "scheduler_idle_ms": schedulerSummary.idleMS, "scheduler_idle_ratio": schedulerSummary.idleRatio(), "scheduler_trace_retained_count": schedulerRowsRetained, "scheduler_trace_dropped_count": schedulerRowsDropped, "scheduler_trace_truncated": schedulerRowsDropped > 0, "remote_state_access_count": remoteSummary.total, "remote_state_read_count": remoteSummary.reads, "remote_state_write_apply_count": remoteSummary.writes, "remote_operation_unknown_kind_count": remoteSummary.unknown, "physical_remote_operation_count": remoteSummary.total, "physical_remote_fetch_count": remoteSummary.reads, "physical_remote_writeback_count": remoteSummary.writes, "physical_remote_failed_count": remoteSummary.failed, "remote_state_access_failed_count": remoteSummary.failed, "remote_state_access_avg_latency_ms": remoteSummary.avgLatency, "runtime_event_count": runtimeEventTotal, "runtime_event_trace_retained_count": len(runtimeEventRows), "runtime_event_trace_dropped_count": runtimeEventRowsDropped, "runtime_event_trace_truncated": runtimeEventRowsDropped > 0, "runtime_metric_counts": runtimeMetricCounts, "real_signed_tx": true, "real_tcp": true, "real_pbft_style_messages": len(rows) > 0})
}

type stateReadyEvidenceSummary struct {
	waitCount               int64
	resumeCount             int64
	waitMS                  int64
	fetchCount              int64
	fetchCompletedCount     int64
	mode                    string
	versionedWaveCount      int64
	versionedWaitCount      int64
	versionedResolvedCount  int64
	versionedProbeCount     int64
	versionedProbeLatencyMS int64
	versionedMaxWaveWidth   int64
	versionedMode           string
}

func summarizeStateReadyEvidence(blocks []map[string]any) stateReadyEvidenceSummary {
	out := stateReadyEvidenceSummary{}
	for _, block := range blocks {
		out.waitCount += int64FromAny(block["state_wait_blocked_count"])
		out.resumeCount += int64FromAny(block["state_ready_wakeup_count"])
		out.waitMS += int64FromAny(block["remote_state_fetch_latency_ms"])
		out.fetchCount += int64FromAny(block["remote_state_fetch_count"])
		out.fetchCompletedCount += int64FromAny(block["remote_state_fetch_completed_count"])
		if mode := strings.TrimSpace(fmt.Sprint(block["state_ready_scheduler_mode"])); mode != "" && mode != "<nil>" {
			out.mode = mode
		}
		out.versionedWaveCount += int64FromAny(block["versioned_state_ready_wave_count"])
		out.versionedWaitCount += int64FromAny(block["versioned_state_ready_wait_observation_count"])
		out.versionedResolvedCount += int64FromAny(block["versioned_state_ready_resolved_token_count"])
		out.versionedProbeCount += int64FromAny(block["versioned_state_probe_count"])
		out.versionedProbeLatencyMS += int64FromAny(block["versioned_state_probe_latency_ms"])
		if width := int64FromAny(block["versioned_state_ready_max_wave_width"]); width > out.versionedMaxWaveWidth {
			out.versionedMaxWaveWidth = width
		}
		if mode := strings.TrimSpace(fmt.Sprint(block["versioned_state_ready_scheduler_mode"])); mode != "" && mode != "<nil>" {
			out.versionedMode = mode
		}
	}
	return out
}

func int64FromAny(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		return parsed
	}
}

func canonicalBusinessStateDigest(snapshot map[string]string) string {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		logical := key
		if index := strings.Index(logical, "::"); index >= 0 && index+2 < len(logical) {
			logical = logical[index+2:]
		}
		if strings.HasPrefix(logical, "relay_commit:") || strings.HasPrefix(logical, "protocol:") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]string, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, key+"="+snapshot[key])
	}
	return stableTextDigest(strings.Join(rows, "\n"))
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
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "remote_state_access.csv"), []string{"timestamp", "node_id", "execution_shard", "height", "block_hash", "tx_id", "state_key", "qualified_home_key", "home_shard", "response_execution_shard", "access_kind", "latency_ms", "witness_digest", "home_state_root", "success", "error", "delta_id", "source_height", "source_block_hash", "update_semantics"}, remoteRows); err != nil {
		return err
	}
	return metrics.WriteCSV(filepath.Join(r.node.DataDir, "logical_physical_update_mapping.csv"), []string{"timestamp", "node_id", "shard_id", "height", "commit_plugin", "aggregation_group_id", "state_key", "value_digest", "logical_tx_ids", "logical_update_count", "physical_update_count", "reduced_physical_write_count", "aggregation_applied", "pre_aggregation_physical_op_count", "post_aggregation_physical_op_count", "aggregated_key_count", "aggregated_logical_delta_count"}, logicalPhysicalRows)
}

func commitLogHeaders() []string {
	return []string{"timestamp", "node_id", "shard_id", "height", "commit_plugin", "aggregation_group_id", "logical_update_count", "physical_update_count", "aggregation_applied", "pre_aggregation_physical_op_count", "post_aggregation_physical_op_count", "aggregated_key_count", "aggregated_logical_delta_count", "atomic_reservation_count", "constraint_fallback_count", "constraint_fallback_reasons"}
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
		total.DependencyAbortCount += metricsValue.DependencyAbortCount
		total.ValidationAbortCount += metricsValue.ValidationAbortCount
		total.ReexecutionCount += metricsValue.ReexecutionCount
		total.EstimateCount += metricsValue.EstimateCount
		total.EstimateMarkCount += metricsValue.EstimateMarkCount
		total.EstimateReadCount += metricsValue.EstimateReadCount
		total.DependencyWaitCount += metricsValue.DependencyWaitCount
		total.DependencyResumeCount += metricsValue.DependencyResumeCount
		total.ValidatedSpeculativeResultCount += metricsValue.ValidatedSpeculativeResultCount
		total.SpeculativeReadCount += metricsValue.SpeculativeReadCount
		total.ValidationFailureCount += metricsValue.ValidationFailureCount
		total.CommittedTransactionCount += metricsValue.CommittedTransactionCount
		total.MaximumConcurrentExecutions = maxInt(total.MaximumConcurrentExecutions, metricsValue.MaximumConcurrentExecutions)
		total.SchedulerQueuePeak = maxInt(total.SchedulerQueuePeak, metricsValue.SchedulerQueuePeak)
		total.StaleTaskCount += metricsValue.StaleTaskCount
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
		abortRows = append(abortRows, []string{r.node.NodeID, r.node.ShardID, blockHash, height, fmt.Sprint(metricsValue.AbortCount), fmt.Sprint(metricsValue.ReexecutionCount), fmt.Sprint(metricsValue.MaximumIncarnation), fmt.Sprint(metricsValue.DependencyAbortCount), fmt.Sprint(metricsValue.ValidationAbortCount)})
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
	if err := metrics.WriteCSV(filepath.Join(r.node.DataDir, "block_stm_abort_trace.csv"), []string{"node_id", "shard_id", "block_hash", "height", "abort_count", "reexecution_count", "maximum_incarnation", "dependency_abort_count", "validation_abort_count"}, abortRows); err != nil {
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
	metricsValue.DependencyAbortCount = intFromAny(value["dependency_abort_count"])
	metricsValue.ValidationAbortCount = intFromAny(value["validation_abort_count"])
	metricsValue.ReexecutionCount = intFromAny(value["reexecution_count"])
	metricsValue.EstimateCount = intFromAny(value["estimate_count"])
	metricsValue.EstimateMarkCount = intFromAny(value["estimate_mark_count"])
	metricsValue.EstimateReadCount = intFromAny(value["estimate_read_count"])
	metricsValue.DependencyWaitCount = intFromAny(value["dependency_wait_count"])
	metricsValue.DependencyResumeCount = intFromAny(value["dependency_resume_count"])
	metricsValue.ValidatedSpeculativeResultCount = intFromAny(value["validated_speculative_result_count"])
	metricsValue.SpeculativeReadCount = intFromAny(value["speculative_read_count"])
	metricsValue.ValidationFailureCount = intFromAny(value["validation_failure_count"])
	metricsValue.CommittedTransactionCount = intFromAny(value["committed_transaction_count"])
	metricsValue.MaximumIncarnation = intFromAny(value["maximum_incarnation"])
	metricsValue.MaximumConcurrentExecutions = intFromAny(value["maximum_concurrent_executions"])
	metricsValue.SchedulerQueuePeak = intFromAny(value["scheduler_queue_peak"])
	metricsValue.StaleTaskCount = intFromAny(value["stale_task_count"])
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

func (r *NodeRuntime) recordProposalEvidence(block realblock.Block) {
	if block.ProposalEvidence == nil {
		return
	}
	var payload any
	if err := json.Unmarshal(block.ProposalEvidence.Payload, &payload); err != nil {
		payload = string(block.ProposalEvidence.Payload)
	} else if object, ok := payload.(map[string]any); ok {
		switch block.ProposalEvidence.AlgorithmID {
		case ariaCandidateSelectionEvidenceID, groundhogCandidateSelectionEvidenceID:
			compactProposalAuditPayload(object)
		}
	}
	row := map[string]any{
		"node_id":           r.node.NodeID,
		"shard_id":          r.node.ShardID,
		"block_hash":        block.BlockHash,
		"height":            block.Height,
		"algorithm_id":      block.ProposalEvidence.AlgorithmID,
		"payload_digest":    block.ProposalEvidence.PayloadDigest,
		"selected_tx_count": len(block.TxList),
		"payload":           payload,
	}
	r.mu.Lock()
	r.proposalEvidence = append(r.proposalEvidence, row)
	r.mu.Unlock()
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
	r.commitRows = append(r.commitRows, []string{timestamp, r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, commitDecision.AggregationGroupID, fmt.Sprint(commitDecision.LogicalUpdates), fmt.Sprint(commitDecision.PhysicalUpdates), fmt.Sprint(commitDecision.Applied), fmt.Sprint(commitDecision.PreAggregationPhysicalOps), fmt.Sprint(commitDecision.PostAggregationPhysicalOps), fmt.Sprint(commitDecision.AggregatedKeyCount), fmt.Sprint(commitDecision.AggregatedLogicalDeltaCount), fmt.Sprint(commitDecision.AtomicReservationCount), fmt.Sprint(commitDecision.ConstraintFallbackCount), strings.Join(commitDecision.ConstraintFallbackReasons, "|")})
	for _, item := range physicalDelta {
		physicalUpdates := 1
		reduced := len(item.TxIDs) - physicalUpdates
		if reduced < 0 {
			reduced = 0
		}
		r.logicalPhysicalRows = append(r.logicalPhysicalRows, []string{timestamp, r.node.NodeID, r.node.ShardID, fmt.Sprint(block.Height), commitPlugin, commitDecision.AggregationGroupID, item.Key, stableTextDigest(item.Value), strings.Join(item.TxIDs, "|"), fmt.Sprint(len(item.TxIDs)), fmt.Sprint(physicalUpdates), fmt.Sprint(reduced), fmt.Sprint(commitDecision.Applied), fmt.Sprint(commitDecision.PreAggregationPhysicalOps), fmt.Sprint(commitDecision.PostAggregationPhysicalOps), fmt.Sprint(commitDecision.AggregatedKeyCount), fmt.Sprint(commitDecision.AggregatedLogicalDeltaCount)})
	}
}

func blockExecutionTimingFields(result BlockExecutionResult) map[string]any {
	return map[string]any{
		"block_execution_ms":               result.BlockExecutionMS,
		"transaction_execution_ms":         result.TransactionExecutionMS,
		"deterministic_materialization_ms": result.DeterministicApplyMS,
		"deterministic_apply_ms":           result.DeterministicApplyMS,
		"state_db_apply_ms":                result.StateDBApplyMS,
		"state_commitment_ms":              result.StateCommitmentMS,
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
		"state_root_version":             result.StateRootVersion,
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
	for key, value := range blockExecutionTimingFields(result) {
		summary[key] = value
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
		summary["dependency_abort_count"] = executed.BlockSTMMetrics.DependencyAbortCount
		summary["validation_abort_count"] = executed.BlockSTMMetrics.ValidationAbortCount
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
	observedRows := [][]string{}
	for _, delta := range executed.TxDeltas {
		traceRows = append(traceRows, []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), delta.TxID, fmt.Sprint(delta.OriginalIndex), fmt.Sprint(delta.Success), delta.Error, fmt.Sprint(len(delta.ReadSet)), fmt.Sprint(len(delta.WriteSet)), delta.Receipt.StateRootAfterTx})
		for _, read := range delta.ReadSet {
			observedRows = append(observedRows, []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), delta.TxID, fmt.Sprint(delta.OriginalIndex), "read", read.Key, read.ValueDigest, read.Source})
		}
		writeKeys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			writeKeys = append(writeKeys, key)
		}
		sort.Strings(writeKeys)
		for _, key := range writeKeys {
			observedRows = append(observedRows, []string{r.node.NodeID, r.node.ShardID, block.BlockHash, fmt.Sprint(block.Height), delta.TxID, fmt.Sprint(delta.OriginalIndex), "write", key, stableTextDigest(delta.WriteSet[key]), "write_set"})
		}
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
	r.observedStateAccessRows = append(r.observedStateAccessRows, observedRows...)
	r.businessExecutionRows = append(r.businessExecutionRows, businessRows...)
	r.stateDeltaRows = append(r.stateDeltaRows, stateRows...)
	r.planDigestRows = append(r.planDigestRows, planRow)
}

func compactProposalAuditPayload(payload map[string]any) {
	// proposal_selection_evidence.jsonl is an observational audit copy. The
	// consensus-bound Block.ProposalEvidence payload is never mutated here, and
	// the durable block store independently preserves/restores the complete
	// proposal evidence. Repeated candidate/selection vectors can therefore be
	// represented by count+digest commitments in this secondary artifact.
	// Keep the compact identity/selection vectors needed for direct audit and
	// downstream evidence consumers. Consensus Evidence Fidelity v1.1 changed
	// Aria's wire from full candidate_transactions to deferred_transactions only;
	// both are large signed-transaction payloads and must stay out of this
	// secondary audit copy. The consensus-bound block payload remains untouched.
	fields := []string{"candidate_transactions", "deferred_transactions", "trace"}
	compacted := false
	for _, key := range fields {
		value, ok := payload[key]
		if !ok || value == nil {
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil {
			continue
		}
		payload[key+"_audit_digest"] = stableTextDigest(string(raw))
		// Preserve the v6 audit contract used by runtime_resource_hardening_test.go
		// and existing artifact consumers. The marker describes only the secondary
		// proposal_selection_evidence.jsonl copy; consensus-bound evidence is intact.
		switch key {
		case "candidate_transactions":
			payload["candidate_transactions_retained"] = false
			if count, ok := payload["candidate_count"]; ok {
				payload["candidate_transactions_omitted_count"] = count
			}
		case "deferred_transactions":
			payload["deferred_transactions_retained"] = false
			if ids, ok := payload["deferred_tx_ids"].([]any); ok {
				payload["deferred_transactions_omitted_count"] = len(ids)
			}
		}
		switch typed := value.(type) {
		case []any:
			payload[key+"_audit_count"] = len(typed)
			if key != "deferred_transactions" && len(typed) > 0 {
				limit := len(typed)
				if limit > 16 {
					limit = 16
				}
				payload[key+"_audit_sample"] = append([]any(nil), typed[:limit]...)
			}
		case map[string]any:
			payload[key+"_audit_count"] = len(typed)
		default:
			payload[key+"_audit_count"] = 1
		}
		delete(payload, key)
		compacted = true
	}
	if compacted {
		payload["audit_payload_compacted"] = true
		payload["audit_payload_compaction_version"] = "proposal_selection_audit_digest_v2"
	}
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

type methodMetricSummary struct {
	fastTrackCount                   int
	conservativeTrackCount           int
	aggregationGroupCount            int
	logicalUpdateCount               int
	physicalUpdateCount              int
	executedLogicalTransactionCount  int
	executedTransactionInstanceCount int
	preAggregationPhysicalOps        int
	postAggregationPhysicalOps       int
	aggregatedKeyCount               int
	aggregatedLogicalDeltaCount      int
}

type blockProductionSummary struct {
	count             int
	averageTxPerBlock float64
	minTxPerBlock     int
	maxTxPerBlock     int
	intervalMeanMS    float64
	intervalP95MS     int64
}

func summarizeBlockProductionRows(chainRows [][]string) blockProductionSummary {
	if len(chainRows) == 0 {
		return blockProductionSummary{}
	}
	txCounts := []int{}
	finishedAt := []int64{}
	for _, row := range chainRows {
		if len(row) < 13 {
			continue
		}
		txCount := 0
		_, _ = fmt.Sscan(row[6], &txCount)
		txCounts = append(txCounts, txCount)
		var finished int64
		_, _ = fmt.Sscan(row[12], &finished)
		if finished > 0 {
			finishedAt = append(finishedAt, finished)
		}
	}
	if len(txCounts) == 0 {
		return blockProductionSummary{}
	}
	out := blockProductionSummary{count: len(txCounts), minTxPerBlock: txCounts[0], maxTxPerBlock: txCounts[0]}
	sumTx := 0
	for _, value := range txCounts {
		sumTx += value
		if value < out.minTxPerBlock {
			out.minTxPerBlock = value
		}
		if value > out.maxTxPerBlock {
			out.maxTxPerBlock = value
		}
	}
	out.averageTxPerBlock = float64(sumTx) / float64(len(txCounts))
	sort.Slice(finishedAt, func(i, j int) bool { return finishedAt[i] < finishedAt[j] })
	intervals := []int64{}
	for index := 1; index < len(finishedAt); index++ {
		if delta := finishedAt[index] - finishedAt[index-1]; delta >= 0 {
			intervals = append(intervals, delta)
		}
	}
	if len(intervals) > 0 {
		var sum int64
		for _, value := range intervals {
			sum += value
		}
		out.intervalMeanMS = float64(sum) / float64(len(intervals))
		out.intervalP95MS = int64Percentile(intervals, 0.95)
	}
	return out
}

func int64Percentile(values []int64, p float64) int64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]int64(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	index := int(float64(len(copyValues)-1) * p)
	if index < 0 {
		index = 0
	}
	if index >= len(copyValues) {
		index = len(copyValues) - 1
	}
	return copyValues[index]
}

func (s methodMetricSummary) physicalOpsSavedCount() int {
	if s.preAggregationPhysicalOps <= s.postAggregationPhysicalOps {
		return 0
	}
	return s.preAggregationPhysicalOps - s.postAggregationPhysicalOps
}

func (s methodMetricSummary) aggregationReductionRatio() float64 {
	if s.preAggregationPhysicalOps <= 0 {
		return 0
	}
	return float64(s.physicalOpsSavedCount()) / float64(s.preAggregationPhysicalOps)
}

func summarizeMethodRows(executionRows, commitRows [][]string) methodMetricSummary {
	summary := methodMetricSummary{}
	logicalTxIDs := map[string]bool{}
	for _, row := range executionRows {
		if len(row) > 6 && row[6] == "fast" {
			summary.fastTrackCount++
		}
		if len(row) > 6 && row[6] == "conservative" {
			summary.conservativeTrackCount++
		}
		if len(row) > 3 && strings.TrimSpace(row[3]) != "" {
			logicalTxIDs[strings.TrimSpace(row[3])] = true
		}
	}
	summary.executedTransactionInstanceCount = len(executionRows)
	summary.executedLogicalTransactionCount = len(logicalTxIDs)
	for _, row := range commitRows {
		if len(row) > 8 && row[8] == "true" {
			summary.aggregationGroupCount++
		}
		if len(row) > 7 {
			var a, b int
			_, _ = fmt.Sscan(row[6], &a)
			_, _ = fmt.Sscan(row[7], &b)
			summary.logicalUpdateCount += a
			summary.physicalUpdateCount += b
		}
		if len(row) > 12 {
			var pre, post, keys, logicalDeltas int
			_, _ = fmt.Sscan(row[9], &pre)
			_, _ = fmt.Sscan(row[10], &post)
			_, _ = fmt.Sscan(row[11], &keys)
			_, _ = fmt.Sscan(row[12], &logicalDeltas)
			summary.preAggregationPhysicalOps += pre
			summary.postAggregationPhysicalOps += post
			summary.aggregatedKeyCount += keys
			summary.aggregatedLogicalDeltaCount += logicalDeltas
		}
	}
	return summary
}

type remoteStateSummary struct {
	total      int
	reads      int
	writes     int
	failed     int
	unknown    int
	avgLatency float64
}

func IsRemoteWritebackKind(kind string) bool {
	return strings.HasPrefix(strings.TrimSpace(kind), "write_apply")
}

func NormalizeRemoteOperationKind(kind string) string {
	trimmed := strings.TrimSpace(kind)
	if IsRemoteWritebackKind(trimmed) {
		return "writeback"
	}
	switch trimmed {
	case "read", "read_write", "commutative_delta":
		return "fetch"
	default:
		return "unknown"
	}
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
		switch NormalizeRemoteOperationKind(row[10]) {
		case "writeback":
			summary.writes++
		case "fetch":
			summary.reads++
		default:
			summary.unknown++
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

func mergeSchedulerSummary(target *schedulerSummary, addition schedulerSummary) {
	if target == nil {
		return
	}
	target.total += addition.total
	target.blocked += addition.blocked
	target.wakeup += addition.wakeup
	target.stolen += addition.stolen
	target.local += addition.local
	target.readyMax = maxInt(target.readyMax, addition.readyMax)
	target.fastMax = maxInt(target.fastMax, addition.fastMax)
	target.conservativeMax = maxInt(target.conservativeMax, addition.conservativeMax)
	target.dependencyWaitMS += addition.dependencyWaitMS
	target.idleMS += addition.idleMS
	target.idlePositiveCount += addition.idlePositiveCount
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
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			_ = file.Close()
			return err
		}
	}
	return file.Close()
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

// MBE_FORMAL_RUNTIME_CLOSURE_20260820_V7

// MBE_PBFT_CATCHUP_TAIL_CLOSURE_20260821_V5

// MBE_PBFT_DURABLE_IDENTITY_CLOSURE_20260821_V7
