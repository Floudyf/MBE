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
	"metaverse-chainlab/executor/realism/state"
)

const (
	porygonMultiShardUpdateMessage                   = "PORYGON_MULTI_SHARD_UPDATE_V1"
	porygonMultiShardUpdateAckMessage                = "PORYGON_MULTI_SHARD_UPDATE_ACK_V1"
	porygonMultiShardUpdateCertificateMessage        = "PORYGON_MULTI_SHARD_UPDATE_CERTIFICATE_V1"
	porygonMultiShardUpdateCertificateRequestMessage = "PORYGON_MULTI_SHARD_UPDATE_CERTIFICATE_REQUEST_V1"
)

type PorygonStateUpdate struct {
	TxID          string `json:"tx_id"`
	OriginalIndex int    `json:"original_index"`
	Key           string `json:"key"`
	Value         string `json:"value"`
}

type PorygonPartitionUpdateRequest struct {
	BlockHash          string                          `json:"block_hash"`
	Height             uint64                          `json:"height"`
	PartitionID        string                          `json:"partition_id"`
	Updates            []PorygonStateUpdate            `json:"updates"`
	UpdateDigest       string                          `json:"update_digest"`
	Attempt            int                             `json:"attempt"`
	ProtocolRound      uint64                          `json:"protocol_round"`
	HandoffCertificate PorygonUpdateHandoffCertificate `json:"handoff_certificate"`
}

type PorygonPartitionUpdateAck struct {
	BlockHash                string `json:"block_hash"`
	Height                   uint64 `json:"height"`
	PartitionID              string `json:"partition_id"`
	UpdateDigest             string `json:"update_digest"`
	ProspectiveRoot          string `json:"prospective_root"`
	NodeID                   string `json:"node_id"`
	Attempt                  int    `json:"attempt"`
	ProtocolRound            uint64 `json:"protocol_round"`
	HandoffCertificateDigest string `json:"handoff_certificate_digest"`
	Signature                string `json:"signature"`
}

type PorygonPartitionRootCertificate struct {
	PartitionID              string                          `json:"partition_id"`
	UpdateDigest             string                          `json:"update_digest"`
	ProspectiveRoot          string                          `json:"prospective_root"`
	ProtocolRound            uint64                          `json:"protocol_round"`
	HandoffCertificateDigest string                          `json:"handoff_certificate_digest"`
	HandoffCertificate       PorygonUpdateHandoffCertificate `json:"handoff_certificate"`
	Voters                   []string                        `json:"voters"`
	Acks                     []PorygonPartitionUpdateAck     `json:"acks"`
}

type PorygonMultiShardUpdateCertificate struct {
	BlockHash           string                            `json:"block_hash"`
	Height              uint64                            `json:"height"`
	Attempt             int                               `json:"attempt"`
	HandoffEnforced     bool                              `json:"handoff_enforced,omitempty"`
	DeferredPartitions  []string                          `json:"deferred_partitions,omitempty"`
	Partitions          []PorygonPartitionRootCertificate `json:"partitions"`
	GlobalStateRoot     string                            `json:"global_state_root"`
	CertificateDigest   string                            `json:"certificate_digest"`
	RolledBack          bool                              `json:"rolled_back"`
	RollbackCertificate *PorygonRollbackCertificate       `json:"rollback_certificate,omitempty"`
}

type PorygonMultiShardUpdateFunc func(context.Context, string, uint64, map[string][]PorygonStateUpdate) (PorygonMultiShardUpdateCertificate, error)

type porygonPreparedPartitionState struct {
	BlockHash   string
	Height      uint64
	PartitionID string
	Snapshot    map[string]string
	Root        string
}

type porygonMultiShardState struct {
	mu       sync.Mutex
	acks     map[string]map[string]map[string]PorygonPartitionUpdateAck
	certs    map[string]PorygonMultiShardUpdateCertificate
	prepared map[uint64]map[string]porygonPreparedPartitionState
}

var porygonMultiShardStates sync.Map
var porygonLatestGlobalRoots sync.Map // map[*NodeRuntime]PorygonMultiShardUpdateCertificate

func (r *NodeRuntime) porygonMultiShardState() *porygonMultiShardState {
	if v, ok := porygonMultiShardStates.Load(r); ok {
		return v.(*porygonMultiShardState)
	}
	created := &porygonMultiShardState{
		acks:     map[string]map[string]map[string]PorygonPartitionUpdateAck{},
		certs:    map[string]PorygonMultiShardUpdateCertificate{},
		prepared: map[uint64]map[string]porygonPreparedPartitionState{},
	}
	actual, _ := porygonMultiShardStates.LoadOrStore(r, created)
	return actual.(*porygonMultiShardState)
}

func porygonCanonicalStateUpdates(updates []PorygonStateUpdate) []PorygonStateUpdate {
	copyUpdates := append([]PorygonStateUpdate(nil), updates...)
	sort.SliceStable(copyUpdates, func(i, j int) bool {
		if copyUpdates[i].OriginalIndex != copyUpdates[j].OriginalIndex {
			return copyUpdates[i].OriginalIndex < copyUpdates[j].OriginalIndex
		}
		if copyUpdates[i].Key != copyUpdates[j].Key {
			return copyUpdates[i].Key < copyUpdates[j].Key
		}
		return copyUpdates[i].TxID < copyUpdates[j].TxID
	})
	return copyUpdates
}

func porygonUpdateDigest(updates []PorygonStateUpdate) string {
	return stableJSONDigest(porygonCanonicalStateUpdates(updates))
}

func porygonUpdateAckBytes(ack PorygonPartitionUpdateAck) []byte {
	copyAck := ack
	copyAck.Signature = ""
	raw, _ := json.Marshal(copyAck)
	return raw
}

func (r *NodeRuntime) signPorygonUpdateAck(ack PorygonPartitionUpdateAck) (PorygonPartitionUpdateAck, error) {
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return ack, err
	}
	ack.NodeID = r.node.NodeID
	ack.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonUpdateAckBytes(ack)))
	return ack, nil
}
func (r *NodeRuntime) verifyPorygonUpdateAck(ack PorygonPartitionUpdateAck) error {
	members := r.porygonStoragePartitionMembers(ack.PartitionID)
	if !containsString(members, ack.NodeID) {
		return fmt.Errorf("Porygon update ACK signer %s not storage replica of %s", ack.NodeID, ack.PartitionID)
	}
	key, err := r.pbftPublicKey(ack.NodeID)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(ack.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, porygonUpdateAckBytes(ack), sig) {
		return fmt.Errorf("Porygon update ACK signature invalid")
	}
	return nil
}

func (r *NodeRuntime) porygonStoragePartitionMembers(partitionID string) []string {
	out := []string{}
	for _, node := range r.plan.NodeConfigs {
		if effectiveExecutionShardID(node) == partitionID {
			out = append(out, node.NodeID)
		}
	}
	sort.Strings(out)
	return out
}

func porygonProspectivePartitionRoot(snapshot map[string]string, partitionID string, updates []PorygonStateUpdate) string {
	next := copyRegistryStringMap(snapshot)
	for _, u := range porygonCanonicalStateUpdates(updates) {
		next[qualifyStateKey(partitionID, u.Key)] = u.Value
	}
	return state.RootOfSnapshot(next)
}

func porygonApplyPartitionUpdates(snapshot map[string]string, partitionID string, updates []PorygonStateUpdate) map[string]string {
	next := copyRegistryStringMap(snapshot)
	for _, u := range porygonCanonicalStateUpdates(updates) {
		next[qualifyStateKey(partitionID, u.Key)] = u.Value
	}
	return next
}

func (r *NodeRuntime) porygonPreparedPartitionBase(height uint64, partitionID string) map[string]string {
	if r.porygonPipelineEnabledRuntime() && height > 1 {
		state := r.porygonMultiShardState()
		state.mu.Lock()
		if byPartition := state.prepared[height-1]; byPartition != nil {
			if previous, ok := byPartition[partitionID]; ok && previous.Snapshot != nil {
				snapshot := copyRegistryStringMap(previous.Snapshot)
				state.mu.Unlock()
				return snapshot
			}
		}
		state.mu.Unlock()
		// Every validator already holds its local partition projection from the
		// E-stage result. If this replica missed the previous M request but later
		// received the certificate, that execution snapshot is the exact same
		// predecessor epoch and avoids falling back to stale durable state.
		if snapshot, _, ok := r.porygonPipelinePreparedSnapshot(height - 1); ok {
			return snapshot
		}
	}
	return r.plugins.StateStorage.Snapshot(r.db)
}

func (r *NodeRuntime) porygonPreparePartitionState(blockHash string, height uint64, partitionID string, updates []PorygonStateUpdate) (map[string]string, string) {
	base := r.porygonPreparedPartitionBase(height, partitionID)
	next := porygonApplyPartitionUpdates(base, partitionID, updates)
	root := state.RootOfSnapshot(next)
	if r.porygonPipelineEnabledRuntime() {
		ms := r.porygonMultiShardState()
		ms.mu.Lock()
		if ms.prepared == nil {
			ms.prepared = map[uint64]map[string]porygonPreparedPartitionState{}
		}
		if ms.prepared[height] == nil {
			ms.prepared[height] = map[string]porygonPreparedPartitionState{}
		}
		ms.prepared[height][partitionID] = porygonPreparedPartitionState{BlockHash: blockHash, Height: height, PartitionID: partitionID, Snapshot: copyRegistryStringMap(next), Root: root}
		ms.mu.Unlock()
	}
	return next, root
}

func (r *NodeRuntime) porygonDropPreparedPartitionsFrom(height uint64) {
	if !r.porygonPipelineEnabledRuntime() {
		return
	}
	ms := r.porygonMultiShardState()
	ms.mu.Lock()
	for candidate := range ms.prepared {
		if candidate >= height {
			delete(ms.prepared, candidate)
		}
	}
	ms.mu.Unlock()
}

func (r *NodeRuntime) porygonGCPreparedPartitions(durableHeight uint64) {
	if !r.porygonPipelineEnabledRuntime() {
		return
	}
	ms := r.porygonMultiShardState()
	ms.mu.Lock()
	for candidate := range ms.prepared {
		if candidate+1 < durableHeight {
			delete(ms.prepared, candidate)
		}
	}
	ms.mu.Unlock()
}

func (r *NodeRuntime) handlePorygonMultiShardUpdate(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonPartitionUpdateRequest](msg)
	if err != nil {
		return err
	}
	if request.PartitionID != r.stateAccessPartitionID() {
		return nil
	}
	if request.UpdateDigest != porygonUpdateDigest(request.Updates) {
		return fmt.Errorf("Porygon multi-shard update digest mismatch")
	}
	if request.ProtocolRound != porygonUpdateProtocolRound(request.Height, request.Attempt) {
		return fmt.Errorf("Porygon multi-shard update protocol-round mismatch")
	}
	if err := r.validatePorygonUpdateHandoffCertificate(request.HandoffCertificate); err != nil {
		return fmt.Errorf("Porygon multi-shard update handoff: %w", err)
	}
	if request.HandoffCertificate.BlockHash != request.BlockHash || request.HandoffCertificate.Height != request.Height ||
		request.HandoffCertificate.Attempt != request.Attempt || request.HandoffCertificate.ProtocolRound != request.ProtocolRound ||
		request.HandoffCertificate.PartitionID != request.PartitionID || request.HandoffCertificate.UpdateDigest != request.UpdateDigest {
		return fmt.Errorf("Porygon multi-shard update handoff binding mismatch")
	}
	_, root := r.porygonPreparePartitionState(request.BlockHash, request.Height, request.PartitionID, request.Updates)
	ack, err := r.signPorygonUpdateAck(PorygonPartitionUpdateAck{BlockHash: request.BlockHash, Height: request.Height, PartitionID: request.PartitionID, UpdateDigest: request.UpdateDigest, ProspectiveRoot: root, Attempt: request.Attempt, ProtocolRound: request.ProtocolRound, HandoffCertificateDigest: request.HandoffCertificate.CertificateDigest})
	if err != nil {
		return err
	}
	leader := r.leaderID(r.node.ShardID)
	if leader == r.node.NodeID {
		return r.acceptPorygonUpdateAck(ack)
	}
	env, err := p2p.NewEnvelope(porygonMultiShardUpdateAckMessage, r.node.NodeID, leader, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, ack)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, leader, env)
}

func (r *NodeRuntime) acceptPorygonUpdateAck(ack PorygonPartitionUpdateAck) error {
	if err := r.verifyPorygonUpdateAck(ack); err != nil {
		return err
	}
	state := r.porygonMultiShardState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.acks[ack.BlockHash] == nil {
		state.acks[ack.BlockHash] = map[string]map[string]PorygonPartitionUpdateAck{}
	}
	if state.acks[ack.BlockHash][ack.PartitionID] == nil {
		state.acks[ack.BlockHash][ack.PartitionID] = map[string]PorygonPartitionUpdateAck{}
	}
	state.acks[ack.BlockHash][ack.PartitionID][ack.NodeID] = ack
	return nil
}
func (r *NodeRuntime) handlePorygonMultiShardUpdateAck(msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("Porygon update ACK sent to non-leader")
	}
	ack, err := p2p.DecodePayload[PorygonPartitionUpdateAck](msg)
	if err != nil {
		return err
	}
	if ack.NodeID != msg.FromNode {
		return fmt.Errorf("Porygon update ACK sender mismatch")
	}
	return r.acceptPorygonUpdateAck(ack)
}

func porygonGlobalRootFromPartitions(partitions []PorygonPartitionRootCertificate) string {
	copyParts := append([]PorygonPartitionRootCertificate(nil), partitions...)
	sort.Slice(copyParts, func(i, j int) bool { return copyParts[i].PartitionID < copyParts[j].PartitionID })
	rows := make([]string, 0, len(copyParts))
	for _, p := range copyParts {
		rows = append(rows, p.PartitionID+"|"+p.ProspectiveRoot)
	}
	return stableJSONDigest(rows)
}
func porygonMultiShardCertDigest(cert PorygonMultiShardUpdateCertificate) string {
	copyCert := cert
	copyCert.CertificateDigest = ""
	copyCert.Partitions = make([]PorygonPartitionRootCertificate, len(cert.Partitions))
	for i, part := range cert.Partitions {
		copyPart := part
		copyPart.Voters = append([]string(nil), part.Voters...)
		copyPart.Acks = append([]PorygonPartitionUpdateAck(nil), part.Acks...)
		copyCert.Partitions[i] = copyPart
		sort.Strings(copyCert.Partitions[i].Voters)
		sort.Slice(copyCert.Partitions[i].Acks, func(a, b int) bool {
			return copyCert.Partitions[i].Acks[a].NodeID < copyCert.Partitions[i].Acks[b].NodeID
		})
	}
	sort.Slice(copyCert.Partitions, func(i, j int) bool { return copyCert.Partitions[i].PartitionID < copyCert.Partitions[j].PartitionID })
	raw, _ := json.Marshal(copyCert)
	return stableTextDigest(string(raw))
}

func (r *NodeRuntime) porygonTryBuildUpdateCertificate(blockHash string, height uint64, attempt int, updates map[string][]PorygonStateUpdate, handoffs map[string]PorygonUpdateHandoffCertificate) (PorygonMultiShardUpdateCertificate, bool, error) {
	state := r.porygonMultiShardState()
	state.mu.Lock()
	defer state.mu.Unlock()
	cert := PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: attempt, HandoffEnforced: true}
	shards := make([]string, 0, len(updates))
	for sid := range updates {
		shards = append(shards, sid)
	}
	sort.Strings(shards)
	for _, sid := range shards {
		members := r.porygonStoragePartitionMembers(sid)
		threshold := porygonMultiShardUpdateThreshold(len(members))
		expectedDigest := porygonUpdateDigest(updates[sid])
		handoff, ok := handoffs[sid]
		if !ok || handoff.CertificateDigest == "" {
			return PorygonMultiShardUpdateCertificate{}, false, fmt.Errorf("missing Porygon update handoff certificate for %s", sid)
		}
		if err := r.validatePorygonUpdateHandoffCertificate(handoff); err != nil {
			return PorygonMultiShardUpdateCertificate{}, false, err
		}
		byRoot := map[string][]PorygonPartitionUpdateAck{}
		for _, ack := range state.acks[blockHash][sid] {
			if ack.Attempt == attempt && ack.UpdateDigest == expectedDigest && ack.ProtocolRound == handoff.ProtocolRound && ack.HandoffCertificateDigest == handoff.CertificateDigest {
				byRoot[ack.ProspectiveRoot] = append(byRoot[ack.ProspectiveRoot], ack)
			}
		}
		var selected []PorygonPartitionUpdateAck
		var root string
		for candidate, acks := range byRoot {
			if len(acks) >= threshold {
				if selected != nil && root != candidate {
					return PorygonMultiShardUpdateCertificate{}, false, fmt.Errorf("multiple Porygon partition roots reached majority for %s", sid)
				}
				root = candidate
				selected = acks
			}
		}
		if selected == nil {
			return PorygonMultiShardUpdateCertificate{}, false, nil
		}
		voters := make([]string, 0, len(selected))
		for _, ack := range selected {
			voters = append(voters, ack.NodeID)
		}
		sort.Strings(voters)
		cert.Partitions = append(cert.Partitions, PorygonPartitionRootCertificate{PartitionID: sid, UpdateDigest: expectedDigest, ProspectiveRoot: root, ProtocolRound: handoff.ProtocolRound, HandoffCertificateDigest: handoff.CertificateDigest, HandoffCertificate: handoff, Voters: voters, Acks: selected})
	}
	cert.GlobalStateRoot = porygonGlobalRootFromPartitions(cert.Partitions)
	cert.CertificateDigest = porygonMultiShardCertDigest(cert)
	state.certs[blockHash] = cert
	return cert, true, nil
}

func (r *NodeRuntime) broadcastPorygonMultiShardCertificate(ctx context.Context, cert PorygonMultiShardUpdateCertificate) {
	for _, nodeID := range r.node.Validators {
		if nodeID == "" || nodeID == r.node.NodeID {
			continue
		}
		_ = r.sendPorygonMultiShardCertificate(ctx, nodeID, cert)
	}
}
func (r *NodeRuntime) validatePorygonMultiShardCertificate(cert PorygonMultiShardUpdateCertificate) error {
	if cert.RolledBack {
		return fmt.Errorf("Porygon multi-shard certificate is rolled back")
	}
	if cert.CertificateDigest != porygonMultiShardCertDigest(cert) || cert.GlobalStateRoot != porygonGlobalRootFromPartitions(cert.Partitions) {
		return fmt.Errorf("Porygon multi-shard certificate invalid")
	}
	if len(cert.Partitions) != r.porygonExecutionShardCount() {
		return fmt.Errorf("Porygon multi-shard certificate partition coverage mismatch: got=%d want=%d", len(cert.Partitions), r.porygonExecutionShardCount())
	}
	seenPartitions := map[string]bool{}
	for _, part := range cert.Partitions {
		if seenPartitions[part.PartitionID] {
			return fmt.Errorf("Porygon multi-shard certificate duplicate partition %s", part.PartitionID)
		}
		seenPartitions[part.PartitionID] = true
		if cert.HandoffEnforced {
			if err := r.validatePorygonUpdateHandoffCertificate(part.HandoffCertificate); err != nil {
				return err
			}
			if part.HandoffCertificateDigest != part.HandoffCertificate.CertificateDigest || part.ProtocolRound != part.HandoffCertificate.ProtocolRound ||
				part.HandoffCertificate.BlockHash != cert.BlockHash || part.HandoffCertificate.Height != cert.Height || part.HandoffCertificate.Attempt != cert.Attempt ||
				part.HandoffCertificate.PartitionID != part.PartitionID || part.HandoffCertificate.UpdateDigest != part.UpdateDigest {
				return fmt.Errorf("Porygon multi-shard handoff certificate binding mismatch for %s", part.PartitionID)
			}
		} else if part.ProtocolRound != 0 || part.HandoffCertificateDigest != "" || part.HandoffCertificate.CertificateDigest != "" {
			return fmt.Errorf("legacy Porygon multi-shard certificate unexpectedly carries handoff fields for %s", part.PartitionID)
		}
		members := r.porygonStoragePartitionMembers(part.PartitionID)
		threshold := porygonMultiShardUpdateThreshold(len(members))
		if len(part.Acks) < threshold {
			return fmt.Errorf("Porygon multi-shard ACK threshold not met")
		}
		seenVoters := map[string]bool{}
		ackVoters := make([]string, 0, len(part.Acks))
		for _, ack := range part.Acks {
			if ack.BlockHash != cert.BlockHash || ack.Height != cert.Height || ack.Attempt != cert.Attempt ||
				ack.PartitionID != part.PartitionID || ack.UpdateDigest != part.UpdateDigest || ack.ProspectiveRoot != part.ProspectiveRoot {
				return fmt.Errorf("Porygon multi-shard ACK certificate binding mismatch for %s", ack.NodeID)
			}
			if cert.HandoffEnforced && (ack.ProtocolRound != part.ProtocolRound || ack.HandoffCertificateDigest != part.HandoffCertificateDigest) {
				return fmt.Errorf("Porygon multi-shard ACK handoff binding mismatch for %s", ack.NodeID)
			}
			if !cert.HandoffEnforced && (ack.ProtocolRound != 0 || ack.HandoffCertificateDigest != "") {
				return fmt.Errorf("legacy Porygon multi-shard ACK unexpectedly carries handoff fields for %s", ack.NodeID)
			}
			if seenVoters[ack.NodeID] {
				return fmt.Errorf("Porygon multi-shard duplicate ACK voter %s", ack.NodeID)
			}
			seenVoters[ack.NodeID] = true
			ackVoters = append(ackVoters, ack.NodeID)
			if err := r.verifyPorygonUpdateAck(ack); err != nil {
				return err
			}
		}
		sort.Strings(ackVoters)
		voters := append([]string(nil), part.Voters...)
		sort.Strings(voters)
		if !sameStringList(voters, ackVoters) {
			return fmt.Errorf("Porygon multi-shard voter/ACK mismatch for %s", part.PartitionID)
		}
	}
	return nil
}

func (r *NodeRuntime) handlePorygonMultiShardUpdateCertificate(msg p2p.MessageEnvelope) error {
	cert, err := p2p.DecodePayload[PorygonMultiShardUpdateCertificate](msg)
	if err != nil {
		return err
	}
	if r.porygonPipelineEnabledRuntime() && !cert.HandoffEnforced {
		return fmt.Errorf("Porygon pipeline certificate missing following-ESC handoff proof")
	}
	if err := r.validatePorygonMultiShardCertificate(cert); err != nil {
		return err
	}
	state := r.porygonMultiShardState()
	state.mu.Lock()
	state.certs[cert.BlockHash] = cert
	state.mu.Unlock()
	porygonLatestGlobalRoots.Store(r, cert)
	return nil
}

func (r *NodeRuntime) sendPorygonMultiShardCertificate(ctx context.Context, nodeID string, cert PorygonMultiShardUpdateCertificate) error {
	env, err := p2p.NewEnvelope(porygonMultiShardUpdateCertificateMessage, r.node.NodeID, nodeID, r.node.ShardID, cert.Height, r.currentPBFTView(), cert.Height, cert)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, nodeID, env)
}

func (r *NodeRuntime) handlePorygonMultiShardUpdateCertificateRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return nil
	}
	type requestBody struct {
		BlockHash string `json:"block_hash"`
		Height    uint64 `json:"height"`
	}
	req, err := p2p.DecodePayload[requestBody](msg)
	if err != nil {
		return err
	}
	state := r.porygonMultiShardState()
	state.mu.Lock()
	cert, ok := state.certs[req.BlockHash]
	state.mu.Unlock()
	if !ok || cert.Height != req.Height {
		return nil
	}
	return r.sendPorygonMultiShardCertificate(ctx, msg.FromNode, cert)
}

func (r *NodeRuntime) porygonMultiShardUpdate(ctx context.Context, blockHash string, height uint64, updates map[string][]PorygonStateUpdate) (PorygonMultiShardUpdateCertificate, error) {
	// A read-only/no-write CTx batch still needs an M-stage certificate so the
	// cross-round pending frontier can advance.  Empty updates therefore become
	// one authenticated no-op update per storage partition instead of bypassing M.
	if updates == nil {
		updates = map[string][]PorygonStateUpdate{}
	}
	// Include every storage partition so the certificate contains a complete
	// protocol global-root vector, even when one partition has no writes.
	for i := 0; i < r.porygonExecutionShardCount(); i++ {
		sid := fmt.Sprintf("s%d", i)
		if _, ok := updates[sid]; !ok {
			updates[sid] = nil
		}
	}
	const maxAttempts = 2
	if r.isCurrentLeader() {
	attemptLoop:
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			attemptCtx, cancelAttempt := context.WithTimeout(ctx, 750*time.Millisecond)
			handoffs := map[string]PorygonUpdateHandoffCertificate{}
			shards := make([]string, 0, len(updates))
			for sid := range updates {
				shards = append(shards, sid)
			}
			sort.Strings(shards)
			for _, sid := range shards {
				items := porygonCanonicalStateUpdates(updates[sid])
				updates[sid] = items
				handoffRequest, members, err := r.porygonBuildUpdateHandoffRequest(blockHash, height, attempt, sid, porygonUpdateDigest(items))
				if err != nil {
					cancelAttempt()
					return PorygonMultiShardUpdateCertificate{}, err
				}
				handoff, err := r.collectPorygonUpdateHandoffCertificate(attemptCtx, handoffRequest, members)
				if err != nil {
					cancelAttempt()
					r.addPorygonRuntimeMetric("porygon_multi_shard_update_retry_count", 1)
					continue attemptLoop
				}
				handoffs[sid] = handoff
				request := PorygonPartitionUpdateRequest{BlockHash: blockHash, Height: height, PartitionID: sid, Updates: items, UpdateDigest: porygonUpdateDigest(items), Attempt: attempt, ProtocolRound: handoff.ProtocolRound, HandoffCertificate: handoff}
				for _, nodeID := range r.porygonStoragePartitionMembers(sid) {
					if nodeID == r.node.NodeID {
						_, root := r.porygonPreparePartitionState(blockHash, height, sid, items)
						ack, _ := r.signPorygonUpdateAck(PorygonPartitionUpdateAck{BlockHash: blockHash, Height: height, PartitionID: sid, UpdateDigest: request.UpdateDigest, ProspectiveRoot: root, Attempt: attempt, ProtocolRound: request.ProtocolRound, HandoffCertificateDigest: request.HandoffCertificate.CertificateDigest})
						_ = r.acceptPorygonUpdateAck(ack)
						continue
					}
					env, err := p2p.NewEnvelope(porygonMultiShardUpdateMessage, r.node.NodeID, nodeID, r.node.ShardID, height, r.currentPBFTView(), height, request)
					if err != nil {
						cancelAttempt()
						return PorygonMultiShardUpdateCertificate{}, err
					}
					if err := r.sendToNode(attemptCtx, nodeID, env); err != nil {
						continue
					}
				}
			}
			ticker := time.NewTicker(2 * time.Millisecond)
			for {
				cert, ready, err := r.porygonTryBuildUpdateCertificate(blockHash, height, attempt, updates, handoffs)
				if err != nil {
					ticker.Stop()
					cancelAttempt()
					return PorygonMultiShardUpdateCertificate{}, err
				}
				if ready {
					ticker.Stop()
					cancelAttempt()
					if err := r.validatePorygonMultiShardCertificate(cert); err != nil {
						return PorygonMultiShardUpdateCertificate{}, err
					}
					r.broadcastPorygonMultiShardCertificate(ctx, cert)
					porygonLatestGlobalRoots.Store(r, cert)
					r.addPorygonRuntimeMetric("porygon_multi_shard_update_certificate_count", 1)
					r.addPorygonRuntimeMetric("porygon_multi_shard_update_protocol_round", int64(porygonUpdateProtocolRound(height, attempt)))
					return cert, nil
				}
				select {
				case <-ctx.Done():
					ticker.Stop()
					cancelAttempt()
					return PorygonMultiShardUpdateCertificate{}, ctx.Err()
				case <-attemptCtx.Done():
					ticker.Stop()
					cancelAttempt()
					r.addPorygonRuntimeMetric("porygon_multi_shard_update_retry_count", 1)
					continue attemptLoop
				case <-ticker.C:
				}
			}
		}
		r.addPorygonRuntimeMetric("porygon_multi_shard_update_rollback_count", 1)
		if len(porygonRollbackMask(blockHash)) > 0 {
			return PorygonMultiShardUpdateCertificate{}, fmt.Errorf("Porygon rollback-recovery update exhausted retries")
		}
		rollback, err := r.porygonTriggerRollback(ctx, blockHash, height, maxAttempts, updates)
		if err != nil {
			return PorygonMultiShardUpdateCertificate{}, err
		}
		return PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: maxAttempts, RolledBack: true, RollbackCertificate: &rollback}, nil
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	recovery := time.NewTicker(50 * time.Millisecond)
	defer recovery.Stop()
	for {
		state := r.porygonMultiShardState()
		state.mu.Lock()
		cert, ok := state.certs[blockHash]
		state.mu.Unlock()
		if ok {
			porygonLatestGlobalRoots.Store(r, cert)
			return cert, nil
		}
		if rollback, rolledBack := r.porygonRollbackCertificate(blockHash); rolledBack {
			return PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: rollback.FailedAttempt, RolledBack: true, RollbackCertificate: &rollback}, nil
		}
		select {
		case <-ctx.Done():
			return PorygonMultiShardUpdateCertificate{}, ctx.Err()
		case <-ticker.C:
		case <-recovery.C:
			leader := r.leaderID(r.node.ShardID)
			if leader != "" {
				req := map[string]any{"block_hash": blockHash, "height": height}
				env, err := p2p.NewEnvelope(porygonMultiShardUpdateCertificateRequestMessage, r.node.NodeID, leader, r.node.ShardID, height, r.currentPBFTView(), height, req)
				if err == nil {
					_ = r.sendToNode(ctx, leader, env)
				}
			}
		}
	}
}

func (r *NodeRuntime) porygonLatestGlobalRootEvidence() (PorygonMultiShardUpdateCertificate, bool) {
	value, ok := porygonLatestGlobalRoots.Load(r)
	if !ok {
		return PorygonMultiShardUpdateCertificate{}, false
	}
	return value.(PorygonMultiShardUpdateCertificate), true
}
