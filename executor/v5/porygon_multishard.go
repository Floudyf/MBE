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
	BlockHash    string               `json:"block_hash"`
	Height       uint64               `json:"height"`
	PartitionID  string               `json:"partition_id"`
	Updates      []PorygonStateUpdate `json:"updates"`
	UpdateDigest string               `json:"update_digest"`
	Attempt      int                  `json:"attempt"`
}

type PorygonPartitionUpdateAck struct {
	BlockHash       string `json:"block_hash"`
	Height          uint64 `json:"height"`
	PartitionID     string `json:"partition_id"`
	UpdateDigest    string `json:"update_digest"`
	ProspectiveRoot string `json:"prospective_root"`
	NodeID          string `json:"node_id"`
	Attempt         int    `json:"attempt"`
	Signature       string `json:"signature"`
}

type PorygonPartitionRootCertificate struct {
	PartitionID     string                      `json:"partition_id"`
	UpdateDigest    string                      `json:"update_digest"`
	ProspectiveRoot string                      `json:"prospective_root"`
	Voters          []string                    `json:"voters"`
	Acks            []PorygonPartitionUpdateAck `json:"acks"`
}

type PorygonMultiShardUpdateCertificate struct {
	BlockHash         string                            `json:"block_hash"`
	Height            uint64                            `json:"height"`
	Attempt           int                               `json:"attempt"`
	Partitions        []PorygonPartitionRootCertificate `json:"partitions"`
	GlobalStateRoot   string                            `json:"global_state_root"`
	CertificateDigest string                            `json:"certificate_digest"`
	RolledBack        bool                              `json:"rolled_back"`
}

type PorygonMultiShardUpdateFunc func(context.Context, string, uint64, map[string][]PorygonStateUpdate) (PorygonMultiShardUpdateCertificate, error)

type porygonMultiShardState struct {
	mu    sync.Mutex
	acks  map[string]map[string]map[string]PorygonPartitionUpdateAck
	certs map[string]PorygonMultiShardUpdateCertificate
}

var porygonMultiShardStates sync.Map
var porygonLatestGlobalRoots sync.Map // map[*NodeRuntime]PorygonMultiShardUpdateCertificate

func (r *NodeRuntime) porygonMultiShardState() *porygonMultiShardState {
	if v, ok := porygonMultiShardStates.Load(r); ok {
		return v.(*porygonMultiShardState)
	}
	created := &porygonMultiShardState{acks: map[string]map[string]map[string]PorygonPartitionUpdateAck{}, certs: map[string]PorygonMultiShardUpdateCertificate{}}
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
	snapshot := r.plugins.StateStorage.Snapshot(r.db)
	root := porygonProspectivePartitionRoot(snapshot, request.PartitionID, request.Updates)
	ack, err := r.signPorygonUpdateAck(PorygonPartitionUpdateAck{BlockHash: request.BlockHash, Height: request.Height, PartitionID: request.PartitionID, UpdateDigest: request.UpdateDigest, ProspectiveRoot: root, Attempt: request.Attempt})
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
	for i := range copyCert.Partitions {
		sort.Strings(copyCert.Partitions[i].Voters)
		sort.Slice(copyCert.Partitions[i].Acks, func(a, b int) bool {
			return copyCert.Partitions[i].Acks[a].NodeID < copyCert.Partitions[i].Acks[b].NodeID
		})
	}
	sort.Slice(copyCert.Partitions, func(i, j int) bool { return copyCert.Partitions[i].PartitionID < copyCert.Partitions[j].PartitionID })
	raw, _ := json.Marshal(copyCert)
	return stableTextDigest(string(raw))
}

func (r *NodeRuntime) porygonTryBuildUpdateCertificate(blockHash string, height uint64, attempt int, updates map[string][]PorygonStateUpdate) (PorygonMultiShardUpdateCertificate, bool, error) {
	state := r.porygonMultiShardState()
	state.mu.Lock()
	defer state.mu.Unlock()
	cert := PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: attempt}
	shards := make([]string, 0, len(updates))
	for sid := range updates {
		shards = append(shards, sid)
	}
	sort.Strings(shards)
	for _, sid := range shards {
		members := r.porygonStoragePartitionMembers(sid)
		threshold := porygonMultiShardUpdateThreshold(len(members))
		expectedDigest := porygonUpdateDigest(updates[sid])
		byRoot := map[string][]PorygonPartitionUpdateAck{}
		for _, ack := range state.acks[blockHash][sid] {
			if ack.Attempt == attempt && ack.UpdateDigest == expectedDigest {
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
		cert.Partitions = append(cert.Partitions, PorygonPartitionRootCertificate{PartitionID: sid, UpdateDigest: expectedDigest, ProspectiveRoot: root, Voters: voters, Acks: selected})
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
	if len(updates) == 0 {
		return PorygonMultiShardUpdateCertificate{}, nil
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
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			for sid, items := range updates {
				items = porygonCanonicalStateUpdates(items)
				updates[sid] = items
				request := PorygonPartitionUpdateRequest{BlockHash: blockHash, Height: height, PartitionID: sid, Updates: items, UpdateDigest: porygonUpdateDigest(items), Attempt: attempt}
				for _, nodeID := range r.porygonStoragePartitionMembers(sid) {
					if nodeID == r.node.NodeID {
						snapshot := r.plugins.StateStorage.Snapshot(r.db)
						root := porygonProspectivePartitionRoot(snapshot, sid, items)
						ack, _ := r.signPorygonUpdateAck(PorygonPartitionUpdateAck{BlockHash: blockHash, Height: height, PartitionID: sid, UpdateDigest: request.UpdateDigest, ProspectiveRoot: root, Attempt: attempt})
						_ = r.acceptPorygonUpdateAck(ack)
						continue
					}
					env, err := p2p.NewEnvelope(porygonMultiShardUpdateMessage, r.node.NodeID, nodeID, r.node.ShardID, height, r.currentPBFTView(), height, request)
					if err != nil {
						return PorygonMultiShardUpdateCertificate{}, err
					}
					if err := r.sendToNode(ctx, nodeID, env); err != nil {
						continue
					}
				}
			}
			deadline := time.NewTimer(750 * time.Millisecond)
			ticker := time.NewTicker(2 * time.Millisecond)
			for {
				cert, ready, err := r.porygonTryBuildUpdateCertificate(blockHash, height, attempt, updates)
				if err != nil {
					ticker.Stop()
					deadline.Stop()
					return PorygonMultiShardUpdateCertificate{}, err
				}
				if ready {
					ticker.Stop()
					deadline.Stop()
					if err := r.validatePorygonMultiShardCertificate(cert); err != nil {
						return PorygonMultiShardUpdateCertificate{}, err
					}
					r.broadcastPorygonMultiShardCertificate(ctx, cert)
					porygonLatestGlobalRoots.Store(r, cert)
					r.addPorygonRuntimeMetric("porygon_multi_shard_update_certificate_count", 1)
					return cert, nil
				}
				select {
				case <-ctx.Done():
					ticker.Stop()
					deadline.Stop()
					return PorygonMultiShardUpdateCertificate{}, ctx.Err()
				case <-deadline.C:
					ticker.Stop()
					goto retry
				case <-ticker.C:
				}
			}
		retry:
			r.addPorygonRuntimeMetric("porygon_multi_shard_update_retry_count", 1)
		}
		r.addPorygonRuntimeMetric("porygon_multi_shard_update_rollback_count", 1)
		return PorygonMultiShardUpdateCertificate{BlockHash: blockHash, Height: height, Attempt: maxAttempts, RolledBack: true}, fmt.Errorf("Porygon multi-shard update exhausted retries")
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
