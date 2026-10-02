package v5

import (
	"context"
	"crypto/ed25519"
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
	RequestID      string               `json:"request_id"`
	RequesterNode  string               `json:"requester_node"`
	BlockHash      string               `json:"block_hash"`
	Height         uint64               `json:"height"`
	TStateHeight   uint64               `json:"t_state_height"`
	PartitionID    string               `json:"partition_id"`
	TPartitionRoot string               `json:"t_partition_root"`
	Updates        []PorygonStateUpdate `json:"updates"`
	UpdateDigest   string               `json:"update_digest"`
}

type PorygonPaperPartitionRootAck struct {
	RequestID       string `json:"request_id"`
	RequesterNode   string `json:"requester_node"`
	BlockHash       string `json:"block_hash"`
	Height          uint64 `json:"height"`
	TStateHeight    uint64 `json:"t_state_height"`
	PartitionID     string `json:"partition_id"`
	TPartitionRoot  string `json:"t_partition_root"`
	UpdateDigest    string `json:"update_digest"`
	ProspectiveRoot string `json:"prospective_root"`
	NodeID          string `json:"node_id"`
	Signature       string `json:"signature"`
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

func (r *NodeRuntime) buildPorygonPaperPartitionRootAck(request PorygonPaperPartitionRootRequest) (PorygonPaperPartitionRootAck, error) {
	if request.PartitionID != r.stateAccessPartitionID() {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root request sent to wrong Storage Role")
	}
	if request.RequestID == "" || request.BlockHash == "" || request.Height == 0 || request.TPartitionRoot == "" {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root request identity incomplete")
	}
	updates := porygonCanonicalStateUpdates(request.Updates)
	if request.UpdateDigest == "" || request.UpdateDigest != porygonUpdateDigest(updates) {
		return PorygonPaperPartitionRootAck{}, fmt.Errorf("Porygon Paper2 partition-root update digest mismatch")
	}
	snapshot, err := r.porygonPaperPartitionSnapshot(request.TStateHeight, request.PartitionID, request.TPartitionRoot)
	if err != nil {
		return PorygonPaperPartitionRootAck{}, err
	}
	root := porygonPaperProspectivePartitionRoot(snapshot, request.PartitionID, updates)
	return r.signPorygonPaperPartitionRootAck(PorygonPaperPartitionRootAck{
		RequestID: request.RequestID, RequesterNode: request.RequesterNode,
		BlockHash: request.BlockHash, Height: request.Height, TStateHeight: request.TStateHeight,
		PartitionID: request.PartitionID, TPartitionRoot: request.TPartitionRoot,
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
	ack, err := r.buildPorygonPaperPartitionRootAck(request)
	if err != nil {
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
	updateDigest := porygonUpdateDigest(updates)
	requestID := stableTextDigest(fmt.Sprintf("paper2-partition-root|%s|%d|%s|%s|%s|%s", blockHash, height, r.node.NodeID, partitionID, partitionRoot, updateDigest))
	request := PorygonPaperPartitionRootRequest{
		RequestID: requestID, RequesterNode: r.node.NodeID, BlockHash: blockHash, Height: height,
		TStateHeight: proposal.TStateHeight, PartitionID: partitionID, TPartitionRoot: partitionRoot,
		Updates: updates, UpdateDigest: updateDigest,
	}
	members := r.porygonStoragePartitionMembers(partitionID)
	threshold := porygonMultiShardUpdateThreshold(len(members))
	if len(members) == 0 {
		return "", fmt.Errorf("Porygon Paper2 Storage Role %s has no replicas", partitionID)
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

	for _, nodeID := range members {
		if nodeID == r.node.NodeID {
			ack, err := r.buildPorygonPaperPartitionRootAck(request)
			if err != nil {
				return "", err
			}
			waiter <- ack
			continue
		}
		env, err := p2p.NewEnvelope(porygonPaperPartitionRootRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, height, r.currentPBFTView(), height, request)
		if err != nil {
			return "", err
		}
		if err := r.sendToNode(ctx, nodeID, env); err != nil {
			continue
		}
	}

	byRoot := map[string]map[string]bool{}
	seen := map[string]bool{}
	timer := time.NewTimer(r.proposalTimeout())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
			return "", fmt.Errorf("Porygon Paper2 partition-root quorum timeout for %s", partitionID)
		case ack := <-waiter:
			if seen[ack.NodeID] {
				continue
			}
			if ack.RequestID != requestID || ack.BlockHash != blockHash || ack.Height != height || ack.TStateHeight != proposal.TStateHeight || ack.PartitionID != partitionID || ack.TPartitionRoot != partitionRoot || ack.UpdateDigest != updateDigest {
				return "", fmt.Errorf("Porygon Paper2 partition-root ACK binding mismatch from %s", ack.NodeID)
			}
			if err := r.verifyPorygonPaperPartitionRootAck(ack); err != nil {
				return "", err
			}
			seen[ack.NodeID] = true
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
		root, err := r.porygonPaperCertifiedPartitionRoot(ctx, proposal, blockHash, height, shard, items)
		if err != nil {
			return PorygonMultiShardUpdateCertificate{}, err
		}
		cert.Partitions = append(cert.Partitions, PorygonPartitionRootCertificate{PartitionID: shard, UpdateDigest: porygonUpdateDigest(items), ProspectiveRoot: root})
	}
	cert.GlobalStateRoot = porygonGlobalRootFromPartitions(cert.Partitions)
	cert.CertificateDigest = stableJSONDigest(struct {
		BlockHash  string
		Height     uint64
		Partitions []PorygonPartitionRootCertificate
	}{cert.BlockHash, cert.Height, cert.Partitions})
	return cert, nil
}
