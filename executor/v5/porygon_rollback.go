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

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	statepkg "metaverse-chainlab/executor/realism/state"
)

const (
	porygonRollbackRequestMessage     = "PORYGON_ROLLBACK_REQUEST_V1"
	porygonRollbackAckMessage         = "PORYGON_ROLLBACK_ACK_V1"
	porygonRollbackCertificateMessage = "PORYGON_ROLLBACK_CERTIFICATE_V1"
)

type PorygonRollbackRequest struct {
	BlockHash          string `json:"block_hash"`
	Height             uint64 `json:"height"`
	FailedAttempt      int    `json:"failed_attempt"`
	PartitionID        string `json:"partition_id"`
	PreviousRoot       string `json:"previous_root"`
	FailedUpdateDigest string `json:"failed_update_digest"`
}

type PorygonRollbackAck struct {
	BlockHash          string `json:"block_hash"`
	Height             uint64 `json:"height"`
	FailedAttempt      int    `json:"failed_attempt"`
	PartitionID        string `json:"partition_id"`
	PreviousRoot       string `json:"previous_root"`
	FailedUpdateDigest string `json:"failed_update_digest"`
	NodeID             string `json:"node_id"`
	Signature          string `json:"signature"`
}

type PorygonRollbackPartitionCertificate struct {
	PartitionID        string               `json:"partition_id"`
	PreviousRoot       string               `json:"previous_root"`
	FailedUpdateDigest string               `json:"failed_update_digest"`
	Voters             []string             `json:"voters"`
	Acks               []PorygonRollbackAck `json:"acks"`
}

type PorygonRollbackCertificate struct {
	BlockHash               string                                `json:"block_hash"`
	Height                  uint64                                `json:"height"`
	FailedAttempt           int                                   `json:"failed_attempt"`
	Partitions              []PorygonRollbackPartitionCertificate `json:"partitions"`
	PreviousGlobalStateRoot string                                `json:"previous_global_state_root"`
	CertificateDigest       string                                `json:"certificate_digest"`
}

type porygonRollbackState struct {
	mu      sync.Mutex
	acks    map[string]map[string]map[string]PorygonRollbackAck
	certs   map[string]PorygonRollbackCertificate
	signals map[string]chan struct{}
}

var porygonRollbackStates sync.Map // map[*NodeRuntime]*porygonRollbackState
var porygonRollbackMasks sync.Map  // map[blockHash]map[txID]bool

func (r *NodeRuntime) porygonRollbackState() *porygonRollbackState {
	if value, ok := porygonRollbackStates.Load(r); ok {
		return value.(*porygonRollbackState)
	}
	created := &porygonRollbackState{
		acks:    map[string]map[string]map[string]PorygonRollbackAck{},
		certs:   map[string]PorygonRollbackCertificate{},
		signals: map[string]chan struct{}{},
	}
	actual, _ := porygonRollbackStates.LoadOrStore(r, created)
	return actual.(*porygonRollbackState)
}

func porygonRollbackAckBytes(ack PorygonRollbackAck) []byte {
	copyAck := ack
	copyAck.Signature = ""
	raw, _ := json.Marshal(copyAck)
	return raw
}

func (r *NodeRuntime) signPorygonRollbackAck(ack PorygonRollbackAck) (PorygonRollbackAck, error) {
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return ack, err
	}
	ack.NodeID = r.node.NodeID
	ack.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonRollbackAckBytes(ack)))
	return ack, nil
}

func (r *NodeRuntime) verifyPorygonRollbackAck(ack PorygonRollbackAck) error {
	if !containsString(r.porygonStoragePartitionMembers(ack.PartitionID), ack.NodeID) {
		return fmt.Errorf("Porygon rollback ACK signer %s is not storage replica of %s", ack.NodeID, ack.PartitionID)
	}
	key, err := r.pbftPublicKey(ack.NodeID)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(ack.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, porygonRollbackAckBytes(ack), sig) {
		return fmt.Errorf("Porygon rollback ACK signature invalid")
	}
	return nil
}

func porygonRollbackPartitionDigest(part PorygonRollbackPartitionCertificate) string {
	copyPart := part
	copyPart.Voters = append([]string(nil), part.Voters...)
	copyPart.Acks = append([]PorygonRollbackAck(nil), part.Acks...)
	sort.Strings(copyPart.Voters)
	sort.Slice(copyPart.Acks, func(i, j int) bool { return copyPart.Acks[i].NodeID < copyPart.Acks[j].NodeID })
	return stableJSONDigest(copyPart)
}

func porygonRollbackGlobalRoot(parts []PorygonRollbackPartitionCertificate) string {
	copyParts := append([]PorygonRollbackPartitionCertificate(nil), parts...)
	sort.Slice(copyParts, func(i, j int) bool { return copyParts[i].PartitionID < copyParts[j].PartitionID })
	rows := make([]string, 0, len(copyParts))
	for _, part := range copyParts {
		rows = append(rows, part.PartitionID+"|"+part.PreviousRoot)
	}
	return stableJSONDigest(rows)
}

func porygonRollbackCertificateDigest(cert PorygonRollbackCertificate) string {
	copyCert := cert
	copyCert.CertificateDigest = ""
	copyCert.Partitions = make([]PorygonRollbackPartitionCertificate, len(cert.Partitions))
	for i, part := range cert.Partitions {
		copyPart := part
		copyPart.Voters = append([]string(nil), part.Voters...)
		copyPart.Acks = append([]PorygonRollbackAck(nil), part.Acks...)
		copyCert.Partitions[i] = copyPart
		sort.Strings(copyCert.Partitions[i].Voters)
		sort.Slice(copyCert.Partitions[i].Acks, func(a, b int) bool {
			return copyCert.Partitions[i].Acks[a].NodeID < copyCert.Partitions[i].Acks[b].NodeID
		})
	}
	sort.Slice(copyCert.Partitions, func(i, j int) bool { return copyCert.Partitions[i].PartitionID < copyCert.Partitions[j].PartitionID })
	return stableJSONDigest(copyCert)
}

func (r *NodeRuntime) porygonRollbackBaseSnapshot(height uint64) (map[string]string, string, error) {
	if height == 0 {
		return nil, "", fmt.Errorf("Porygon rollback invalid height 0")
	}
	if height > 1 {
		if snapshot, root, ok := r.porygonPipelinePreparedSnapshot(height - 1); ok {
			return snapshot, root, nil
		}
	}
	snapshot := r.plugins.StateStorage.Snapshot(r.db)
	return snapshot, statepkg.RootOfSnapshot(snapshot), nil
}

func (r *NodeRuntime) handlePorygonRollbackRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonRollbackRequest](msg)
	if err != nil {
		return err
	}
	if request.PartitionID != r.stateAccessPartitionID() {
		return nil
	}
	snapshot, _, err := r.porygonRollbackBaseSnapshot(request.Height)
	if err != nil {
		return err
	}
	previousRoot := statepkg.RootOfSnapshot(snapshot)
	if request.PreviousRoot != "" && previousRoot != request.PreviousRoot {
		return fmt.Errorf("Porygon rollback previous-root mismatch for %s", request.PartitionID)
	}
	ack, err := r.signPorygonRollbackAck(PorygonRollbackAck{
		BlockHash: request.BlockHash, Height: request.Height, FailedAttempt: request.FailedAttempt,
		PartitionID: request.PartitionID, PreviousRoot: previousRoot, FailedUpdateDigest: request.FailedUpdateDigest,
	})
	if err != nil {
		return err
	}
	leader := r.leaderID(r.node.ShardID)
	if leader == r.node.NodeID {
		return r.acceptPorygonRollbackAck(ack)
	}
	env, err := p2p.NewEnvelope(porygonRollbackAckMessage, r.node.NodeID, leader, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, ack)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, leader, env)
}

func (r *NodeRuntime) acceptPorygonRollbackAck(ack PorygonRollbackAck) error {
	if err := r.verifyPorygonRollbackAck(ack); err != nil {
		return err
	}
	state := r.porygonRollbackState()
	state.mu.Lock()
	if state.acks[ack.BlockHash] == nil {
		state.acks[ack.BlockHash] = map[string]map[string]PorygonRollbackAck{}
	}
	if state.acks[ack.BlockHash][ack.PartitionID] == nil {
		state.acks[ack.BlockHash][ack.PartitionID] = map[string]PorygonRollbackAck{}
	}
	state.acks[ack.BlockHash][ack.PartitionID][ack.NodeID] = ack
	signal := state.signals[ack.BlockHash]
	state.mu.Unlock()
	if signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *NodeRuntime) handlePorygonRollbackAck(msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("Porygon rollback ACK sent to non-leader")
	}
	ack, err := p2p.DecodePayload[PorygonRollbackAck](msg)
	if err != nil {
		return err
	}
	if ack.NodeID != msg.FromNode {
		return fmt.Errorf("Porygon rollback ACK sender mismatch")
	}
	return r.acceptPorygonRollbackAck(ack)
}

func (r *NodeRuntime) tryBuildPorygonRollbackCertificate(blockHash string, height uint64, failedAttempt int, requests map[string]PorygonRollbackRequest) (PorygonRollbackCertificate, bool, error) {
	state := r.porygonRollbackState()
	state.mu.Lock()
	defer state.mu.Unlock()
	cert := PorygonRollbackCertificate{BlockHash: blockHash, Height: height, FailedAttempt: failedAttempt}
	shards := make([]string, 0, len(requests))
	for sid := range requests {
		shards = append(shards, sid)
	}
	sort.Strings(shards)
	for _, sid := range shards {
		request := requests[sid]
		threshold := porygonMultiShardUpdateThreshold(len(r.porygonStoragePartitionMembers(sid)))
		byRoot := map[string][]PorygonRollbackAck{}
		for _, ack := range state.acks[blockHash][sid] {
			if ack.Height == height && ack.FailedAttempt == failedAttempt && ack.FailedUpdateDigest == request.FailedUpdateDigest {
				byRoot[ack.PreviousRoot] = append(byRoot[ack.PreviousRoot], ack)
			}
		}
		var acks []PorygonRollbackAck
		previousRoot := ""
		for candidate, rows := range byRoot {
			if len(rows) < threshold {
				continue
			}
			if acks != nil && previousRoot != candidate {
				return PorygonRollbackCertificate{}, false, fmt.Errorf("multiple Porygon rollback roots reached majority for %s", sid)
			}
			previousRoot = candidate
			acks = rows
		}
		if acks == nil {
			return PorygonRollbackCertificate{}, false, nil
		}
		sort.Slice(acks, func(i, j int) bool { return acks[i].NodeID < acks[j].NodeID })
		acks = acks[:threshold]
		voters := make([]string, 0, len(acks))
		for _, ack := range acks {
			voters = append(voters, ack.NodeID)
		}
		cert.Partitions = append(cert.Partitions, PorygonRollbackPartitionCertificate{
			PartitionID: sid, PreviousRoot: previousRoot, FailedUpdateDigest: request.FailedUpdateDigest,
			Voters: voters, Acks: acks,
		})
	}
	cert.PreviousGlobalStateRoot = porygonRollbackGlobalRoot(cert.Partitions)
	cert.CertificateDigest = porygonRollbackCertificateDigest(cert)
	state.certs[blockHash] = cert
	return cert, true, nil
}

func (r *NodeRuntime) validatePorygonRollbackCertificate(cert PorygonRollbackCertificate) error {
	if cert.BlockHash == "" || cert.Height == 0 || cert.FailedAttempt < 1 || len(cert.Partitions) != r.porygonExecutionShardCount() {
		return fmt.Errorf("Porygon rollback certificate identity/coverage invalid")
	}
	if cert.CertificateDigest != porygonRollbackCertificateDigest(cert) || cert.PreviousGlobalStateRoot != porygonRollbackGlobalRoot(cert.Partitions) {
		return fmt.Errorf("Porygon rollback certificate digest/root invalid")
	}
	seen := map[string]bool{}
	for _, part := range cert.Partitions {
		if seen[part.PartitionID] {
			return fmt.Errorf("Porygon rollback duplicate partition %s", part.PartitionID)
		}
		seen[part.PartitionID] = true
		threshold := porygonMultiShardUpdateThreshold(len(r.porygonStoragePartitionMembers(part.PartitionID)))
		if len(part.Acks) < threshold {
			return fmt.Errorf("Porygon rollback threshold not met for %s", part.PartitionID)
		}
		ackVoters := []string{}
		seenVoters := map[string]bool{}
		for _, ack := range part.Acks {
			if ack.BlockHash != cert.BlockHash || ack.Height != cert.Height || ack.FailedAttempt != cert.FailedAttempt || ack.PartitionID != part.PartitionID || ack.PreviousRoot != part.PreviousRoot || ack.FailedUpdateDigest != part.FailedUpdateDigest {
				return fmt.Errorf("Porygon rollback ACK binding mismatch for %s", ack.NodeID)
			}
			if seenVoters[ack.NodeID] {
				return fmt.Errorf("Porygon rollback duplicate voter %s", ack.NodeID)
			}
			seenVoters[ack.NodeID] = true
			ackVoters = append(ackVoters, ack.NodeID)
			if err := r.verifyPorygonRollbackAck(ack); err != nil {
				return err
			}
		}
		sort.Strings(ackVoters)
		voters := append([]string(nil), part.Voters...)
		sort.Strings(voters)
		if !sameStringList(voters, ackVoters) {
			return fmt.Errorf("Porygon rollback voter/ACK mismatch for %s", part.PartitionID)
		}
		_ = porygonRollbackPartitionDigest(part)
	}
	return nil
}

func (r *NodeRuntime) broadcastPorygonRollbackCertificate(ctx context.Context, cert PorygonRollbackCertificate) {
	for _, nodeID := range r.node.Validators {
		if nodeID == "" || nodeID == r.node.NodeID {
			continue
		}
		env, err := p2p.NewEnvelope(porygonRollbackCertificateMessage, r.node.NodeID, nodeID, r.node.ShardID, cert.Height, r.currentPBFTView(), cert.Height, cert)
		if err == nil {
			_ = r.sendToNode(ctx, nodeID, env)
		}
	}
}

func (r *NodeRuntime) handlePorygonRollbackCertificate(msg p2p.MessageEnvelope) error {
	cert, err := p2p.DecodePayload[PorygonRollbackCertificate](msg)
	if err != nil {
		return err
	}
	if err := r.validatePorygonRollbackCertificate(cert); err != nil {
		return err
	}
	state := r.porygonRollbackState()
	state.mu.Lock()
	state.certs[cert.BlockHash] = cert
	signal := state.signals[cert.BlockHash]
	state.mu.Unlock()
	if signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return r.porygonPipelineApplyRollback(cert)
}

func (r *NodeRuntime) porygonRollbackCertificate(blockHash string) (PorygonRollbackCertificate, bool) {
	state := r.porygonRollbackState()
	state.mu.Lock()
	defer state.mu.Unlock()
	cert, ok := state.certs[blockHash]
	return cert, ok
}

// porygonTriggerRollback creates an explicit cross-partition rollback proof.
// Porygon pipeline state is provisional until M succeeds, so storage replicas
// attest the previous-version roots rather than mutating durable DB state.
func (r *NodeRuntime) porygonTriggerRollback(ctx context.Context, blockHash string, height uint64, failedAttempt int, updates map[string][]PorygonStateUpdate) (PorygonRollbackCertificate, error) {
	if !r.isCurrentLeader() {
		return PorygonRollbackCertificate{}, fmt.Errorf("Porygon rollback trigger requires current OC leader")
	}
	requests := map[string]PorygonRollbackRequest{}
	for i := 0; i < r.porygonExecutionShardCount(); i++ {
		sid := fmt.Sprintf("s%d", i)
		items := porygonCanonicalStateUpdates(updates[sid])
		request := PorygonRollbackRequest{
			BlockHash: blockHash, Height: height, FailedAttempt: failedAttempt, PartitionID: sid,
			PreviousRoot: "", FailedUpdateDigest: porygonUpdateDigest(items),
		}
		requests[sid] = request
	}
	rollbackState := r.porygonRollbackState()
	rollbackState.mu.Lock()
	if rollbackState.signals[blockHash] == nil {
		rollbackState.signals[blockHash] = make(chan struct{}, 1)
	}
	signal := rollbackState.signals[blockHash]
	rollbackState.mu.Unlock()

	for sid, request := range requests {
		for _, nodeID := range r.porygonStoragePartitionMembers(sid) {
			if nodeID == r.node.NodeID {
				baseSnapshot, _, baseErr := r.porygonRollbackBaseSnapshot(height)
				if baseErr != nil {
					return PorygonRollbackCertificate{}, baseErr
				}
				ack, signErr := r.signPorygonRollbackAck(PorygonRollbackAck{
					BlockHash: blockHash, Height: height, FailedAttempt: failedAttempt, PartitionID: sid,
					PreviousRoot: statepkg.RootOfSnapshot(baseSnapshot), FailedUpdateDigest: request.FailedUpdateDigest,
				})
				if signErr != nil {
					return PorygonRollbackCertificate{}, signErr
				}
				if err := r.acceptPorygonRollbackAck(ack); err != nil {
					return PorygonRollbackCertificate{}, err
				}
				continue
			}
			env, err := p2p.NewEnvelope(porygonRollbackRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, height, r.currentPBFTView(), height, request)
			if err != nil {
				return PorygonRollbackCertificate{}, err
			}
			_ = r.sendToNode(ctx, nodeID, env)
		}
	}

	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		cert, ready, err := r.tryBuildPorygonRollbackCertificate(blockHash, height, failedAttempt, requests)
		if err != nil {
			return PorygonRollbackCertificate{}, err
		}
		if ready {
			if err := r.validatePorygonRollbackCertificate(cert); err != nil {
				return PorygonRollbackCertificate{}, err
			}
			r.broadcastPorygonRollbackCertificate(ctx, cert)
			if err := r.porygonPipelineApplyRollback(cert); err != nil {
				return PorygonRollbackCertificate{}, err
			}
			r.addPorygonRuntimeMetric("porygon_rollback_certificate_count", 1)
			return cert, nil
		}
		select {
		case <-ctx.Done():
			return PorygonRollbackCertificate{}, ctx.Err()
		case <-timeout.C:
			return PorygonRollbackCertificate{}, fmt.Errorf("Porygon rollback certificate timeout")
		case <-ticker.C:
		case <-signal:
		}
	}
}

func porygonInstallRollbackMask(blockHash string, txIDs []string) {
	mask := map[string]bool{}
	for _, txID := range txIDs {
		if txID != "" {
			mask[txID] = true
		}
	}
	porygonRollbackMasks.Store(blockHash, mask)
}

func porygonRollbackMask(blockHash string) map[string]bool {
	value, ok := porygonRollbackMasks.Load(blockHash)
	if !ok {
		return nil
	}
	stored := value.(map[string]bool)
	out := make(map[string]bool, len(stored))
	for txID, enabled := range stored {
		out[txID] = enabled
	}
	return out
}

func porygonApplyRollbackMask(block realblock.Block, plan porygonExecutionPlan) (porygonExecutionPlan, int) {
	mask := porygonRollbackMask(block.BlockHash)
	if len(mask) == 0 {
		return plan, 0
	}
	copyPlan := plan
	copyPlan.Assignments = append([]porygonTxAssignment(nil), plan.Assignments...)
	rolledBack := 0
	for i := range copyPlan.Assignments {
		assignment := &copyPlan.Assignments[i]
		if !mask[assignment.TxID] || !assignment.CrossShard || assignment.Abandoned {
			continue
		}
		assignment.Abandoned = true
		assignment.ConflictReason = "oc_cross_shard_update_rollback_abandoned"
		assignment.Wave = -1
		copyPlan.AbandonedCrossShardTransactionCnt++
		copyPlan.RollbackAbandonedCTxCnt++
		rolledBack++
	}
	if rolledBack == 0 {
		return copyPlan, 0
	}
	waves := make([][]string, len(plan.Waves))
	for waveIndex, wave := range plan.Waves {
		for _, txID := range wave {
			if mask[txID] {
				continue
			}
			waves[waveIndex] = append(waves[waveIndex], txID)
		}
	}
	copyPlan.Waves = waves
	return copyPlan, rolledBack
}

func (r *NodeRuntime) porygonPipelineApplyRollback(cert PorygonRollbackCertificate) error {
	if !r.porygonPipelineEnabledRuntime() {
		return fmt.Errorf("Porygon rollback received outside pipeline mode")
	}
	if err := r.validatePorygonRollbackCertificate(cert); err != nil {
		return err
	}
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	if cert.Height <= pipeline.durableHeight {
		pipeline.mu.Unlock()
		return fmt.Errorf("Porygon cannot roll back durable height %d", cert.Height)
	}
	if pipeline.rollbackDigests == nil {
		pipeline.rollbackDigests = map[uint64]string{}
	}
	if existing := pipeline.rollbackDigests[cert.Height]; existing != "" {
		pipeline.mu.Unlock()
		if existing != cert.CertificateDigest {
			return fmt.Errorf("Porygon conflicting rollback certificate at height %d", cert.Height)
		}
		return nil
	}
	failed := pipeline.blocks[cert.Height]
	if failed == nil || failed.Block.BlockHash != cert.BlockHash {
		pipeline.mu.Unlock()
		return fmt.Errorf("Porygon rollback does not bind ordered block at height %d", cert.Height)
	}
	plan, err := porygonPipelinePlan(failed.Block)
	if err != nil {
		pipeline.mu.Unlock()
		return err
	}
	rollbackIDs := []string{}
	for _, assignment := range plan.Assignments {
		if assignment.CrossShard && !assignment.Abandoned {
			rollbackIDs = append(rollbackIDs, assignment.TxID)
		}
	}
	if len(rollbackIDs) == 0 {
		pipeline.mu.Unlock()
		return fmt.Errorf("Porygon rollback height %d has no retained CTx", cert.Height)
	}
	porygonInstallRollbackMask(cert.BlockHash, rollbackIDs)
	pipeline.rollbackDigests[cert.Height] = cert.CertificateDigest
	if pipeline.executedHeight >= cert.Height {
		pipeline.executedHeight = cert.Height - 1
	}
	if pipeline.protocolCommittedHeight >= cert.Height {
		pipeline.protocolCommittedHeight = cert.Height - 1
	}
	heights := make([]uint64, 0)
	blocks := make([]realblock.Block, 0)
	for height := cert.Height; height <= pipeline.orderedHeight; height++ {
		item := pipeline.blocks[height]
		if item == nil {
			continue
		}
		item.Generation++
		if item.Generation == 0 {
			item.Generation = 1
		}
		item.Phase = porygonPipelineOrdered
		item.Execution = BlockExecutionResult{}
		item.PreparedSnapshot = nil
		item.PreparedRoot = ""
		item.BaseSnapshotRoot = ""
		item.PartitionUpdates = nil
		item.ProtocolCertificate = PorygonMultiShardUpdateCertificate{}
		item.ExecutionStartedAt = time.Time{}
		item.ExecutionFinishedAt = time.Time{}
		item.CommitStartedAt = time.Time{}
		item.CommitFinishedAt = time.Time{}
		heights = append(heights, height)
		blocks = append(blocks, item.Block)
	}
	pipeline.mu.Unlock()

	// Multi-Shard Update keeps per-partition provisional roots independently
	// from the execution snapshot. A rollback must invalidate the failed height
	// and every speculative descendant before any ordered block is re-executed.
	r.porygonDropPreparedPartitionsFrom(cert.Height)

	r.addPorygonRuntimeMetric("porygon_pipeline_rollback_count", 1)
	r.addPorygonRuntimeMetric("porygon_pipeline_rollback_abandoned_ctx_count", int64(len(rollbackIDs)))
	r.addPorygonRuntimeMetric("porygon_pipeline_descendant_reexecution_count", int64(max(0, len(heights)-1)))
	for _, block := range blocks {
		select {
		case <-r.commitWorkerContext.Done():
			return r.commitWorkerContext.Err()
		case pipeline.execQ <- block:
		}
	}
	return nil
}
