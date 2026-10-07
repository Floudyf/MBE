package v5

import (
	"context"
	"crypto/ed25519"
	"errors"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
	statepkg "metaverse-chainlab/executor/realism/state"
)

const (
	porygonPaperPartitionRootRequestMessage = "PORYGON_PAPER_PARTITION_ROOT_REQUEST_V1"
	porygonPaperPartitionRootAckMessage     = "PORYGON_PAPER_PARTITION_ROOT_ACK_V1"
)

// Paper2 separates the dynamic ESC role from the fixed Storage Role. An ESC
// member must therefore never certify the root of whatever physical partition
// happens to be co-located on that validator.  It asks the replicas of the
// *logical* target partition to compute the prospective root from Proposal.T
// plus the canonical U/ITx update set and accepts only a strict-majority root.
type PorygonPaperPartitionRootRequest struct {
	RequestID           string               `json:"request_id"`
	RequesterNode       string               `json:"requester_node"`
	BlockHash           string               `json:"block_hash"`
	Height              uint64               `json:"height"`
	TStateHeight        uint64               `json:"t_state_height"`
	PartitionID         string               `json:"partition_id"`
	TPartitionRoot      string               `json:"t_partition_root"`
	CanonicalBaseHeight uint64               `json:"canonical_base_height"`
	CanonicalBaseRoot   string               `json:"canonical_base_root"`
	Updates             []PorygonStateUpdate `json:"updates"`
	UpdateDigest        string               `json:"update_digest"`
}

type PorygonPaperPartitionRootAck struct {
	RequestID           string `json:"request_id"`
	RequesterNode       string `json:"requester_node"`
	BlockHash           string `json:"block_hash"`
	Height              uint64 `json:"height"`
	TStateHeight        uint64 `json:"t_state_height"`
	PartitionID         string `json:"partition_id"`
	TPartitionRoot      string `json:"t_partition_root"`
	CanonicalBaseHeight uint64 `json:"canonical_base_height"`
	CanonicalBaseRoot   string `json:"canonical_base_root"`
	UpdateDigest        string `json:"update_digest"`
	ProspectiveRoot     string `json:"prospective_root"`
	NodeID              string `json:"node_id"`
	Signature           string `json:"signature"`
}

type porygonPaperPartitionRootWaitState struct {
	mu      sync.Mutex
	waiters map[string]chan PorygonPaperPartitionRootAck
}

var porygonPaperPartitionRootStates sync.Map // map[*NodeRuntime]*porygonPaperPartitionRootWaitState

func (r *NodeRuntime) porygonPaperPartitionRootState() *porygonPaperPartitionRootWaitState {
	if value, ok := porygonPaperPartitionRootStates.Load(r); ok {
		return value.(*porygonPaperPartitionRootWaitState)
	}
	created := &porygonPaperPartitionRootWaitState{waiters: map[string]chan PorygonPaperPartitionRootAck{}}
	actual, _ := porygonPaperPartitionRootStates.LoadOrStore(r, created)
	return actual.(*porygonPaperPartitionRootWaitState)
}

func porygonPaperPartitionRootAckBytes(ack PorygonPaperPartitionRootAck) []byte {
	copyAck := ack
	copyAck.Signature = ""
	raw, _ := json.Marshal(copyAck)
	return raw
}

func (r *NodeRuntime) signPorygonPaperPartitionRootAck(ack PorygonPaperPartitionRootAck) (PorygonPaperPartitionRootAck, error) {
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return ack, err
	}
	ack.NodeID = r.node.NodeID
	ack.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonPaperPartitionRootAckBytes(ack)))
	return ack, nil
}

func (r *NodeRuntime) verifyPorygonPaperPartitionRootAck(ack PorygonPaperPartitionRootAck) error {
	if !containsString(r.porygonStoragePartitionMembers(ack.PartitionID), ack.NodeID) {
		return fmt.Errorf("Porygon Paper2 partition-root signer %s not in Storage Role %s", ack.NodeID, ack.PartitionID)
	}
	key, err := r.pbftPublicKey(ack.NodeID)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(ack.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, porygonPaperPartitionRootAckBytes(ack), sig) {
		return fmt.Errorf("Porygon Paper2 partition-root ACK signature invalid for %s", ack.NodeID)
	}
	return nil
}

func (r *NodeRuntime) porygonPaperProposalForIdentity(blockHash string, height uint64) (PorygonProposalBody, error) {
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	item := pipeline.blocks[height]
	if item == nil || item.Block.BlockHash != blockHash {
		pipeline.mu.Unlock()
		return PorygonProposalBody{}, fmt.Errorf("Porygon Paper2 proposal unavailable for height %d", height)
	}
	block := item.Block
	pipeline.mu.Unlock()
	proposal, err := porygonProposalFromBlock(block)
	if err != nil {
		return PorygonProposalBody{}, err
	}
	if proposal.Version != porygonCompactProposalVersion {
		return PorygonProposalBody{}, fmt.Errorf("Porygon Paper2 compact proposal required")
	}
	return proposal, nil
}

func porygonPaperProspectivePartitionRoot(snapshot map[string]string, partitionID string, updates []PorygonStateUpdate) string {
	next := copyRegistryStringMap(snapshot)
	for _, update := range porygonCanonicalStateUpdates(updates) {
		next[qualifyStateKey(partitionID, update.Key)] = update.Value
	}
	return statepkg.RootOfSnapshot(next)
}

var errPorygonPaperPartitionRootNotReady = errors.New("Porygon Paper2 partition-root local state not ready")

// porygonPaperPartitionSnapshotReadiness distinguishes normal asynchronous
// pipeline lag from an already-present but contradictory certified state. A
// missing local snapshot/root is retryable; a present disagreement is fatal.
func (r *NodeRuntime) porygonPaperPartitionSnapshotReadiness(height uint64, partitionID, partitionRoot string) (map[string]string, bool, error) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	snapshot := copyRegistryStringMap(state.certifiedSnapshots[height])
	expected := state.certifiedPartitionRoots[height][partitionID]
	state.mu.Unlock()
	if snapshot == nil || expected == "" {
		return nil, false, nil
	}
	if expected != partitionRoot {
		return nil, true, fmt.Errorf("Porygon agreed partition root mismatch at height %d partition %s: local=%s requested=%s", height, partitionID, expected, partitionRoot)
	}
	if actual := statepkg.RootOfSnapshot(snapshot); actual != partitionRoot {
		return nil, true, fmt.Errorf("Porygon agreed partition snapshot/root mismatch at height %d partition %s: snapshot=%s certified=%s", height, partitionID, actual, partitionRoot)
	}
	return snapshot, true, nil
}

func (r *NodeRuntime) buildPorygonPaperPartitionRootAck(request PorygonPaperPartitionRootRequest) (PorygonPaperPartitionRootAck, error) {
	if request.PartitionID != r.stateAccessPartitionID() {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root request sent to wrong Storage Role")
	}
	if request.RequestID == "" || request.BlockHash == "" || request.Height == 0 || request.TPartitionRoot == "" || request.CanonicalBaseRoot == "" {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root request identity incomplete")
	}
	updates := porygonCanonicalStateUpdates(request.Updates)
	if err := porygonPaperValidateCollapsedStateUpdates(updates); err != nil {
		return PorygonPaperPartitionRootAck{}, err
	}
	if request.UpdateDigest == "" || request.UpdateDigest != porygonUpdateDigest(updates) {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root update digest mismatch")
	}
	// T(h-2) remains the execution-read anchor. A replica that has not yet
	// certified that historical T state is merely slow; a replica that has it
	// under a different root has observed a deterministic contradiction.
	if _, ready, err := r.porygonPaperPartitionSnapshotReadiness(request.TStateHeight, request.PartitionID, request.TPartitionRoot); err != nil {
		return PorygonPaperPartitionRootAck{}, err
	} else if !ready {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("%w: T state height=%d partition=%s", errPorygonPaperPartitionRootNotReady, request.TStateHeight, request.PartitionID)
	}
	baseHeight, baseRoot, ok := r.porygonPaperCanonicalPartitionBase(request.Height, request.PartitionID)
	if !ok {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("%w: canonical base height=%d partition=%s", errPorygonPaperPartitionRootNotReady, request.Height-1, request.PartitionID)
	}
	if request.CanonicalBaseHeight != baseHeight || request.CanonicalBaseRoot != baseRoot {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 canonical partition base mismatch for %s at height %d: local=%d/%s requested=%d/%s", request.PartitionID, request.Height, baseHeight, baseRoot, request.CanonicalBaseHeight, request.CanonicalBaseRoot)
	}
	snapshot, ready, err := r.porygonPaperPartitionSnapshotReadiness(baseHeight, request.PartitionID, baseRoot)
	if err != nil {
		return PorygonPaperPartitionRootAck{}, err
	}
	if !ready {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("%w: canonical snapshot height=%d partition=%s", errPorygonPaperPartitionRootNotReady, baseHeight, request.PartitionID)
	}
	root := porygonPaperProspectivePartitionRoot(snapshot, request.PartitionID, updates)
	return r.signPorygonPaperPartitionRootAck(PorygonPaperPartitionRootAck{
		RequestID: request.RequestID, RequesterNode: request.RequesterNode,
		BlockHash: request.BlockHash, Height: request.Height, TStateHeight: request.TStateHeight,
		PartitionID: request.PartitionID, TPartitionRoot: request.TPartitionRoot,
		CanonicalBaseHeight: baseHeight, CanonicalBaseRoot: baseRoot,
		UpdateDigest: request.UpdateDigest, ProspectiveRoot: root,
	})
}

func (r *NodeRuntime) handlePorygonPaperPartitionRootRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonPaperPartitionRootRequest](msg)
	if err != nil {
		return err
	}
	if request.RequesterNode != msg.FromNode {
		return fmt.Errorf("Porygon Paper2 partition-root requester mismatch")
	}
	if request.PartitionID != r.stateAccessPartitionID() {
		return nil
	}
	r.addPorygonRuntimeMetric("porygon_partition_root_request_received_count", 1)
	ack, err := r.buildPorygonPaperPartitionRootAck(request)
	if err != nil {
		if errors.Is(err, errPorygonPaperPartitionRootNotReady) {
			// Normal asynchronous pipeline lag: do not poison the connection and do
			// not manufacture a negative certificate. The requester retransmits the
			// exact immutable request to missing Storage replicas.
			r.addPorygonRuntimeMetric("porygon_partition_root_not_ready_count", 1)
			return nil
		}
		return err
	}
	env, err := p2p.NewEnvelope(porygonPaperPartitionRootAckMessage, r.node.NodeID, request.RequesterNode, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, ack)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, request.RequesterNode, env)
}

func (r *NodeRuntime) handlePorygonPaperPartitionRootAck(msg p2p.MessageEnvelope) error {
	ack, err := p2p.DecodePayload[PorygonPaperPartitionRootAck](msg)
	if err != nil {
		return err
	}
	if ack.NodeID != msg.FromNode || ack.RequesterNode != r.node.NodeID {
		return fmt.Errorf("Porygon Paper2 partition-root ACK sender/requester mismatch")
	}
	if err := r.verifyPorygonPaperPartitionRootAck(ack); err != nil {
		return err
	}
	state := r.porygonPaperPartitionRootState()
	state.mu.Lock()
	waiter := state.waiters[ack.RequestID]
	state.mu.Unlock()
	if waiter == nil {
		return nil
	}
	select {
	case waiter <- ack:
	default:
	}
	return nil
}

func (r *NodeRuntime) porygonPaperCertifiedPartitionRoot(ctx context.Context, proposal PorygonProposalBody, blockHash string, height uint64, partitionID string, updates []PorygonStateUpdate) (string, error) {
	partitionRoot := proposal.TPartitionRoots[partitionID]
	if partitionRoot == "" {
		return "", fmt.Errorf("Porygon Proposal T missing partition root for %s", partitionID)
	}
	updates = porygonCanonicalStateUpdates(updates)
	if err := porygonPaperValidateCollapsedStateUpdates(updates); err != nil {
		return "", err
	}
	updateDigest := porygonUpdateDigest(updates)
	members := r.porygonStoragePartitionMembers(partitionID)
	threshold := porygonMultiShardUpdateThreshold(len(members))
	if len(members) == 0 {
		return "", fmt.Errorf("Porygon Paper2 Storage Role %s has no replicas", partitionID)
	}

	started := time.Now()
	timeout := r.proposalTimeout()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// This cadence is transport recovery only. It never changes quorum,
	// acceptance, state selection, or any Porygon algorithmic decision.
	recovery := time.NewTicker(50 * time.Millisecond)
	defer recovery.Stop()

	// B_h can execute from T(h-2) before this replica has finished certifying
	// canonical state h-1. Wait locally for the immutable h-1 base instead of
	// turning normal pipeline skew into a deterministic execution failure.
	var baseHeight uint64
	var baseRoot string
	for {
		var ok bool
		baseHeight, baseRoot, ok = r.porygonPaperCanonicalPartitionBase(height, partitionID)
		if ok {
			break
		}
		r.addPorygonRuntimeMetric("porygon_partition_root_local_base_not_ready_count", 1)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
			r.addPorygonRuntimeMetric("porygon_partition_root_timeout_count", 1)
			return "", fmt.Errorf("Porygon Paper2 partition-root local base timeout: height=%d partition=%s requester=%s required_base=%d", height, partitionID, r.node.NodeID, height-1)
		case <-recovery.C:
		}
	}

	requestID := stableTextDigest(fmt.Sprintf("paper2-partition-root|%s|%d|%s|%s|%s|%d|%s|%s", blockHash, height, r.node.NodeID, partitionID, partitionRoot, baseHeight, baseRoot, updateDigest))
	request := PorygonPaperPartitionRootRequest{
		RequestID: requestID, RequesterNode: r.node.NodeID, BlockHash: blockHash, Height: height,
		TStateHeight: proposal.TStateHeight, PartitionID: partitionID, TPartitionRoot: partitionRoot,
		CanonicalBaseHeight: baseHeight, CanonicalBaseRoot: baseRoot,
		Updates: updates, UpdateDigest: updateDigest,
	}
	waiter := make(chan PorygonPaperPartitionRootAck, len(members)+1)
	state := r.porygonPaperPartitionRootState()
	state.mu.Lock()
	state.waiters[requestID] = waiter
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		delete(state.waiters, requestID)
		state.mu.Unlock()
	}()

	byRoot := map[string]map[string]bool{}
	seen := map[string]bool{}
	r.addPorygonRuntimeMetric("porygon_partition_root_request_count", 1)
	sendToMember := func(nodeID string, retry bool) error {
		if seen[nodeID] {
			return nil
		}
		if retry {
			r.addPorygonRuntimeMetric("porygon_partition_root_retry_send_count", 1)
		}
		if nodeID == r.node.NodeID {
			ack, err := r.buildPorygonPaperPartitionRootAck(request)
			if err != nil {
				if errors.Is(err, errPorygonPaperPartitionRootNotReady) {
					r.addPorygonRuntimeMetric("porygon_partition_root_not_ready_count", 1)
					return nil
				}
				return err
			}
			select {
			case waiter <- ack:
			default:
			}
			return nil
		}
		env, err := p2p.NewEnvelope(porygonPaperPartitionRootRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, height, r.currentPBFTView(), height, request)
		if err != nil {
			return err
		}
		if err := r.sendToNode(ctx, nodeID, env); err != nil {
			r.addPorygonRuntimeMetric("porygon_partition_root_send_error_count", 1)
			return nil
		}
		return nil
	}
	for _, nodeID := range members {
		if err := sendToMember(nodeID, false); err != nil {
			return "", err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
			r.addPorygonRuntimeMetric("porygon_partition_root_timeout_count", 1)
			voters := sortedBoolKeys(seen)
			missing := make([]string, 0, len(members))
			for _, nodeID := range members {
				if !seen[nodeID] {
					missing = append(missing, nodeID)
				}
			}
			sort.Strings(missing)
			return "", fmt.Errorf("Porygon Paper2 partition-root quorum timeout: height=%d partition=%s requester=%s threshold=%d received=%d voters=%v missing=%v base_height=%d", height, partitionID, r.node.NodeID, threshold, len(seen), voters, missing, baseHeight)
		case <-recovery.C:
			r.addPorygonRuntimeMetric("porygon_partition_root_retry_round_count", 1)
			for _, nodeID := range members {
				if err := sendToMember(nodeID, true); err != nil {
					return "", err
				}
			}
		case ack := <-waiter:
			if seen[ack.NodeID] {
				continue
			}
			if ack.RequestID != requestID || ack.BlockHash != blockHash || ack.Height != height || ack.TStateHeight != proposal.TStateHeight || ack.PartitionID != partitionID || ack.TPartitionRoot != partitionRoot || ack.CanonicalBaseHeight != baseHeight || ack.CanonicalBaseRoot != baseRoot || ack.UpdateDigest != updateDigest {
				return "", fmt.Errorf("Porygon Paper2 partition-root ACK binding mismatch from %s", ack.NodeID)
			}
			if err := r.verifyPorygonPaperPartitionRootAck(ack); err != nil {
				return "", err
			}
			seen[ack.NodeID] = true
			r.addPorygonRuntimeMetric("porygon_partition_root_ack_count", 1)
			if byRoot[ack.ProspectiveRoot] == nil {
				byRoot[ack.ProspectiveRoot] = map[string]bool{}
			}
			byRoot[ack.ProspectiveRoot][ack.NodeID] = true
			selected := ""
			for root, voters := range byRoot {
				if len(voters) >= threshold {
					if selected != "" && selected != root {
						return "", fmt.Errorf("multiple Porygon Paper2 Storage Role roots reached majority for %s", partitionID)
					}
					selected = root
				}
			}
			if selected != "" {
				r.addPorygonRuntimeMetric("porygon_paper_storage_root_quorum_count", 1)
				r.addPorygonRuntimeMetric("porygon_partition_root_quorum_wait_us", time.Since(started).Microseconds())
				return selected, nil
			}
		}
	}
}

// porygonPaperPartitionRootProjection keeps the existing executor callback
// shape while changing its Paper2 meaning: the later ESC owns execution of U,
// and fixed Storage Roles only authenticate the prospective subtree root.
func (r *NodeRuntime) porygonPaperPartitionRootProjection(ctx context.Context, blockHash string, height uint64, updates map[string][]PorygonStateUpdate) (PorygonMultiShardUpdateCertificate, error) {
	proposal, err := r.porygonPaperProposalForIdentity(blockHash, height)
	if err != nil {
		return PorygonMultiShardUpdateCertificate{}, err
	}
	shards := make([]string, 0, len(updates))
	for shard := range updates {
		shards = append(shards, shard)
	}
	sort.Strings(shards)
	cert := PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: 0, HandoffEnforced: false}
	for _, shard := range shards {
		items := porygonCanonicalStateUpdates(updates[shard])
		if err := porygonPaperValidateCollapsedStateUpdates(items); err != nil {
			return PorygonMultiShardUpdateCertificate{}, err
		}
		root, rootErr := r.porygonPaperCertifiedPartitionRoot(ctx, proposal, blockHash, height, shard, items)
		if rootErr != nil && porygonV50RetryablePaperRootFailure(rootErr) {
			fallback, changed := porygonV50RemoveProposalUFromPartition(proposal, shard, r.porygonExecutionShardCount(), items)
			if changed {
				root, rootErr = r.porygonPaperCertifiedPartitionRoot(ctx, proposal, blockHash, height, shard, fallback)
				if rootErr == nil {
					items = fallback
					cert.DeferredPartitions = append(cert.DeferredPartitions, shard)
					r.addPorygonRuntimeMetric("porygon_v50_partition_update_deferred_count", 1)
				}
			}
		}
		if rootErr != nil {
			return PorygonMultiShardUpdateCertificate{}, rootErr
		}
		cert.Partitions = append(cert.Partitions, PorygonPartitionRootCertificate{PartitionID: shard, UpdateDigest: porygonUpdateDigest(items), ProspectiveRoot: root})
	}
	sort.Strings(cert.DeferredPartitions)
	cert.GlobalStateRoot = porygonGlobalRootFromPartitions(cert.Partitions)
	cert.CertificateDigest = stableJSONDigest(struct {
		BlockHash          string
		Height             uint64
		DeferredPartitions []string
		Partitions         []PorygonPartitionRootCertificate
	}{cert.BlockHash, cert.Height, cert.DeferredPartitions, cert.Partitions})
	return cert, nil
}
