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

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
)

const (
	porygonESCBatchResultMessage             = "PORYGON_ESC_BATCH_RESULT_V2"
	porygonESCBatchCertificateMessage        = "PORYGON_ESC_BATCH_CERTIFICATE_V2"
	porygonESCBatchCertificateRequestMessage = "PORYGON_ESC_BATCH_CERTIFICATE_REQUEST_V2"
)

type PorygonBatchTxResult struct {
	TxID    string            `json:"tx_id"`
	Receipt execution.Receipt `json:"receipt"`
	Delta   execution.TxDelta `json:"delta"`
}

type PorygonESCBatchResult struct {
	BlockHash            string                 `json:"block_hash"`
	Height               uint64                 `json:"height"`
	ExecutionShardID     string                 `json:"execution_shard_id"`
	CommitteeEpochDigest string                 `json:"committee_epoch_digest"`
	SenderNodeID         string                 `json:"sender_node_id"`
	Results              []PorygonBatchTxResult `json:"results"`
	StateRoot            string                 `json:"state_root"`
	DeferredPartitions   []string               `json:"deferred_partitions,omitempty"`
	ResultDigest         string                 `json:"result_digest"`
	BusinessExecutionUS  int64                  `json:"business_execution_us"`
}

type PorygonESCBatchAttestation struct {
	NodeID               string `json:"node_id"`
	BlockHash            string `json:"block_hash"`
	Height               uint64 `json:"height"`
	ExecutionShardID     string `json:"execution_shard_id"`
	CommitteeEpochDigest string `json:"committee_epoch_digest"`
	ResultDigest         string `json:"result_digest"`
	Signature            string `json:"signature"`
}

type PorygonESCBatchResultWire struct {
	Result      PorygonESCBatchResult      `json:"result"`
	Attestation PorygonESCBatchAttestation `json:"attestation"`
}

type PorygonESCBatchCertificateEntry struct {
	ExecutionShardID string                       `json:"execution_shard_id"`
	ResultDigest     string                       `json:"result_digest"`
	Threshold        int                          `json:"threshold"`
	Voters           []string                     `json:"voters"`
	Attestations     []PorygonESCBatchAttestation `json:"attestations"`
	Result           PorygonESCBatchResult        `json:"result"`
}

type PorygonESCBatchCertificate struct {
	BlockHash            string                            `json:"block_hash"`
	Height               uint64                            `json:"height"`
	CommitteeEpochDigest string                            `json:"committee_epoch_digest"`
	LeaderNodeID         string                            `json:"leader_node_id"`
	Entries              []PorygonESCBatchCertificateEntry `json:"entries"`
	CertificateDigest    string                            `json:"certificate_digest"`
}

type PorygonBatchExchangeFunc func(context.Context, PorygonESCBatchResult, []string) (PorygonESCBatchCertificate, error)

type porygonBatchBucket struct {
	result       PorygonESCBatchResult
	voters       map[string]bool
	attestations map[string]PorygonESCBatchAttestation
}

type porygonBatchExchangeState struct {
	mu           sync.Mutex
	results      map[string]map[string]map[string]*porygonBatchBucket
	senderDigest map[string]map[string]map[string]string
	certs        map[string]PorygonESCBatchCertificate
}

var porygonBatchExchangeStates sync.Map // map[*NodeRuntime]*porygonBatchExchangeState

func (r *NodeRuntime) porygonBatchState() *porygonBatchExchangeState {
	if value, ok := porygonBatchExchangeStates.Load(r); ok {
		return value.(*porygonBatchExchangeState)
	}
	created := &porygonBatchExchangeState{
		results:      map[string]map[string]map[string]*porygonBatchBucket{},
		senderDigest: map[string]map[string]map[string]string{},
		certs:        map[string]PorygonESCBatchCertificate{},
	}
	actual, _ := porygonBatchExchangeStates.LoadOrStore(r, created)
	return actual.(*porygonBatchExchangeState)
}

func porygonESCBatchResultDigest(result PorygonESCBatchResult) string {
	copyResult := result
	copyResult.SenderNodeID = ""
	copyResult.ResultDigest = ""
	copyResult.BusinessExecutionUS = 0
	copyResult.Results = append([]PorygonBatchTxResult(nil), copyResult.Results...)
	copyResult.DeferredPartitions = append([]string(nil), copyResult.DeferredPartitions...)
	sort.Strings(copyResult.DeferredPartitions)
	sort.Slice(copyResult.Results, func(i, j int) bool { return copyResult.Results[i].TxID < copyResult.Results[j].TxID })
	raw, _ := json.Marshal(copyResult)
	return stableTextDigest(string(raw))
}

func porygonSealBatchResult(result PorygonESCBatchResult) PorygonESCBatchResult {
	result.ResultDigest = porygonESCBatchResultDigest(result)
	return result
}

func porygonBatchAttestationBytes(att PorygonESCBatchAttestation) []byte {
	copyAtt := att
	copyAtt.Signature = ""
	raw, _ := json.Marshal(copyAtt)
	return raw
}

func (r *NodeRuntime) signPorygonBatchAttestation(result PorygonESCBatchResult) (PorygonESCBatchAttestation, error) {
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return PorygonESCBatchAttestation{}, err
	}
	att := PorygonESCBatchAttestation{NodeID: r.node.NodeID, BlockHash: result.BlockHash, Height: result.Height, ExecutionShardID: result.ExecutionShardID, CommitteeEpochDigest: result.CommitteeEpochDigest, ResultDigest: result.ResultDigest}
	att.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonBatchAttestationBytes(att)))
	return att, nil
}

func (r *NodeRuntime) verifyPorygonBatchAttestation(result PorygonESCBatchResult, att PorygonESCBatchAttestation) error {
	// Batch certificates aggregate attestations from multiple replicas that
	// agreed on the same sender-independent ResultDigest.  The representative
	// entry.Result keeps the SenderNodeID of one replica only, so certificate
	// verification must bind every attestation to the common result identity,
	// not to that representative sender.
	if att.BlockHash != result.BlockHash || att.Height != result.Height || att.ExecutionShardID != result.ExecutionShardID || att.CommitteeEpochDigest != result.CommitteeEpochDigest || att.ResultDigest != result.ResultDigest {
		return fmt.Errorf("porygon batch attestation identity mismatch")
	}
	key, err := r.pbftPublicKey(att.NodeID)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(att.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, porygonBatchAttestationBytes(att), sig) {
		return fmt.Errorf("porygon batch attestation signature invalid for %s", att.NodeID)
	}
	return nil
}

func (r *NodeRuntime) verifyPorygonBatchResultAttestation(result PorygonESCBatchResult, att PorygonESCBatchAttestation) error {
	// A direct batch-result message is stricter than an aggregate certificate:
	// the wire sender, result sender and signing identity must be identical.
	if att.NodeID != result.SenderNodeID {
		return fmt.Errorf("porygon batch result attestation sender mismatch")
	}
	return r.verifyPorygonBatchAttestation(result, att)
}

func porygonBatchCertDigest(cert PorygonESCBatchCertificate) string {
	copyCert := cert
	copyCert.CertificateDigest = ""
	copyCert.Entries = make([]PorygonESCBatchCertificateEntry, len(cert.Entries))
	for i, entry := range cert.Entries {
		copyEntry := entry
		copyEntry.Voters = append([]string(nil), entry.Voters...)
		copyEntry.Attestations = append([]PorygonESCBatchAttestation(nil), entry.Attestations...)
		copyCert.Entries[i] = copyEntry
	}
	sort.Slice(copyCert.Entries, func(i, j int) bool {
		return copyCert.Entries[i].ExecutionShardID < copyCert.Entries[j].ExecutionShardID
	})
	for i := range copyCert.Entries {
		sort.Strings(copyCert.Entries[i].Voters)
		sort.Slice(copyCert.Entries[i].Attestations, func(a, b int) bool {
			return copyCert.Entries[i].Attestations[a].NodeID < copyCert.Entries[i].Attestations[b].NodeID
		})
	}
	raw, _ := json.Marshal(copyCert)
	return stableTextDigest(string(raw))
}

func (r *NodeRuntime) acceptPorygonBatchResult(result PorygonESCBatchResult, att PorygonESCBatchAttestation) error {
	members := r.porygonExecutionRoleMembers(result.Height, result.ExecutionShardID)
	if !containsString(members, result.SenderNodeID) {
		return fmt.Errorf("porygon batch result sender %s not in dynamic ESC %s", result.SenderNodeID, result.ExecutionShardID)
	}
	if result.CommitteeEpochDigest != porygonCommitteeEpochSeed(result.Height, r.node.ShardID) {
		return fmt.Errorf("porygon batch result committee epoch mismatch")
	}
	if result.ResultDigest == "" || result.ResultDigest != porygonESCBatchResultDigest(result) {
		return fmt.Errorf("porygon batch result digest mismatch")
	}
	if err := r.verifyPorygonBatchResultAttestation(result, att); err != nil {
		return err
	}
	state := r.porygonBatchState()
	key := result.BlockHash
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.senderDigest[key] == nil {
		state.senderDigest[key] = map[string]map[string]string{}
	}
	if state.senderDigest[key][result.ExecutionShardID] == nil {
		state.senderDigest[key][result.ExecutionShardID] = map[string]string{}
	}
	if prior := state.senderDigest[key][result.ExecutionShardID][result.SenderNodeID]; prior != "" && prior != result.ResultDigest {
		return fmt.Errorf("porygon batch result equivocation from %s", result.SenderNodeID)
	}
	state.senderDigest[key][result.ExecutionShardID][result.SenderNodeID] = result.ResultDigest
	if state.results[key] == nil {
		state.results[key] = map[string]map[string]*porygonBatchBucket{}
	}
	if state.results[key][result.ExecutionShardID] == nil {
		state.results[key][result.ExecutionShardID] = map[string]*porygonBatchBucket{}
	}
	bucket := state.results[key][result.ExecutionShardID][result.ResultDigest]
	if bucket == nil {
		bucket = &porygonBatchBucket{result: result, voters: map[string]bool{}, attestations: map[string]PorygonESCBatchAttestation{}}
		state.results[key][result.ExecutionShardID][result.ResultDigest] = bucket
	}
	bucket.voters[result.SenderNodeID] = true
	bucket.attestations[att.NodeID] = att
	return nil
}

func (r *NodeRuntime) tryBuildPorygonBatchCertificate(blockHash string, height uint64, requiredShards []string) (PorygonESCBatchCertificate, bool, error) {
	if !r.isCurrentLeader() {
		return PorygonESCBatchCertificate{}, false, fmt.Errorf("porygon batch certificate only OC leader may build")
	}
	state := r.porygonBatchState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if existing, ok := state.certs[blockHash]; ok {
		return existing, true, nil
	}
	cert := PorygonESCBatchCertificate{BlockHash: blockHash, Height: height, CommitteeEpochDigest: porygonCommitteeEpochSeed(height, r.node.ShardID), LeaderNodeID: r.node.NodeID}
	shards := append([]string(nil), requiredShards...)
	sort.Strings(shards)
	for _, sid := range shards {
		members := r.porygonExecutionRoleMembers(height, sid)
		threshold := porygonShardedExecutionResultThreshold(len(members))
		buckets := state.results[blockHash][sid]
		var chosen *porygonBatchBucket
		for _, bucket := range buckets {
			if len(bucket.voters) >= threshold {
				if chosen != nil && chosen.result.ResultDigest != bucket.result.ResultDigest {
					return PorygonESCBatchCertificate{}, false, fmt.Errorf(
						"multiple Porygon batch digests reached Te for %s: digest_a=%s root_a=%s voters_a=%v digest_b=%s root_b=%s voters_b=%v",
						sid, chosen.result.ResultDigest, chosen.result.StateRoot, sortedBoolKeys(chosen.voters),
						bucket.result.ResultDigest, bucket.result.StateRoot, sortedBoolKeys(bucket.voters),
					)
				}
				chosen = bucket
			}
		}
		if chosen == nil {
			return PorygonESCBatchCertificate{}, false, nil
		}
		voters := sortedBoolKeys(chosen.voters)
		atts := make([]PorygonESCBatchAttestation, 0, len(chosen.attestations))
		for _, a := range chosen.attestations {
			atts = append(atts, a)
		}
		sort.Slice(atts, func(i, j int) bool { return atts[i].NodeID < atts[j].NodeID })
		cert.Entries = append(cert.Entries, PorygonESCBatchCertificateEntry{ExecutionShardID: sid, ResultDigest: chosen.result.ResultDigest, Threshold: threshold, Voters: voters, Attestations: atts, Result: chosen.result})
	}
	cert.CertificateDigest = porygonBatchCertDigest(cert)
	state.certs[blockHash] = cert
	return cert, true, nil
}

func (r *NodeRuntime) validatePorygonBatchCertificate(cert PorygonESCBatchCertificate, requiredShards []string) error {
	if cert.BlockHash == "" || cert.Height == 0 || cert.CertificateDigest != porygonBatchCertDigest(cert) || cert.CommitteeEpochDigest != porygonCommitteeEpochSeed(cert.Height, r.node.ShardID) {
		return fmt.Errorf("porygon batch certificate identity/digest mismatch")
	}
	required := map[string]bool{}
	for _, sid := range requiredShards {
		required[sid] = true
	}
	if len(cert.Entries) != len(required) {
		return fmt.Errorf("porygon batch certificate shard coverage mismatch")
	}
	for _, entry := range cert.Entries {
		if !required[entry.ExecutionShardID] || entry.ResultDigest != entry.Result.ResultDigest || entry.ResultDigest != porygonESCBatchResultDigest(entry.Result) {
			return fmt.Errorf("porygon batch certificate entry mismatch")
		}
		members := r.porygonExecutionRoleMembers(cert.Height, entry.ExecutionShardID)
		threshold := porygonShardedExecutionResultThreshold(len(members))
		if entry.Threshold != threshold {
			return fmt.Errorf("porygon sharded ESC result threshold mismatch for %s: got=%d want=%d", entry.ExecutionShardID, entry.Threshold, threshold)
		}
		seenDeferred := map[string]bool{}
		for _, partitionID := range entry.Result.DeferredPartitions {
			if partitionID != entry.ExecutionShardID || seenDeferred[partitionID] {
				return fmt.Errorf("invalid Porygon deferred partition evidence %q for ESC %s", partitionID, entry.ExecutionShardID)
			}
			seenDeferred[partitionID] = true
		}
		if !containsString(members, entry.Result.SenderNodeID) {
			return fmt.Errorf("invalid Porygon batch representative %s", entry.Result.SenderNodeID)
		}
		if len(entry.Voters) < threshold {
			return fmt.Errorf("porygon sharded ESC strict-majority not met for %s", entry.ExecutionShardID)
		}
		declaredVoters := map[string]bool{}
		for _, voter := range entry.Voters {
			if declaredVoters[voter] || !containsString(members, voter) {
				return fmt.Errorf("invalid Porygon batch declared voter %s", voter)
			}
			declaredVoters[voter] = true
		}
		seen := map[string]bool{}
		for _, att := range entry.Attestations {
			if seen[att.NodeID] || !containsString(members, att.NodeID) || !declaredVoters[att.NodeID] {
				return fmt.Errorf("invalid Porygon batch voter %s", att.NodeID)
			}
			seen[att.NodeID] = true
			if err := r.verifyPorygonBatchAttestation(entry.Result, att); err != nil {
				return err
			}
		}
		if len(seen) != len(declaredVoters) {
			return fmt.Errorf("porygon batch voter/attestation set mismatch for %s", entry.ExecutionShardID)
		}
		if !seen[entry.Result.SenderNodeID] {
			return fmt.Errorf("porygon batch representative attestation missing for %s", entry.ExecutionShardID)
		}
		if len(seen) < threshold {
			return fmt.Errorf("porygon sharded ESC authenticated strict-majority not met for %s", entry.ExecutionShardID)
		}
	}
	return nil
}

func (r *NodeRuntime) sendPorygonBatchCertificate(ctx context.Context, nodeID string, cert PorygonESCBatchCertificate) error {
	env, err := p2p.NewEnvelope(porygonESCBatchCertificateMessage, r.node.NodeID, nodeID, r.node.ShardID, cert.Height, r.currentPBFTView(), cert.Height, cert)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, nodeID, env)
}

func (r *NodeRuntime) broadcastPorygonBatchCertificate(ctx context.Context, cert PorygonESCBatchCertificate) {
	for _, nodeID := range r.node.Validators {
		if nodeID == "" || nodeID == r.node.NodeID {
			continue
		}
		_ = r.sendPorygonBatchCertificate(ctx, nodeID, cert)
	}
}

func (r *NodeRuntime) porygonBatchExchange(ctx context.Context, local PorygonESCBatchResult, requiredShards []string) (PorygonESCBatchCertificate, error) {
	local.ExecutionShardID = r.porygonExecutionRoleShardID(local.Height)
	local.SenderNodeID = r.node.NodeID
	local.CommitteeEpochDigest = porygonCommitteeEpochSeed(local.Height, r.node.ShardID)
	required := map[string]bool{}
	for _, sid := range requiredShards {
		if sid != "" {
			required[sid] = true
		}
	}
	requiredShards = sortedBoolKeys(required)
	if required[local.ExecutionShardID] {
		local = porygonSealBatchResult(local)
		att, err := r.signPorygonBatchAttestation(local)
		if err != nil {
			return PorygonESCBatchCertificate{}, err
		}
		leader := r.leaderID(r.node.ShardID)
		if leader == r.node.NodeID {
			if err := r.acceptPorygonBatchResult(local, att); err != nil {
				return PorygonESCBatchCertificate{}, err
			}
		} else {
			wire := PorygonESCBatchResultWire{Result: local, Attestation: att}
			env, err := p2p.NewEnvelope(porygonESCBatchResultMessage, r.node.NodeID, leader, r.node.ShardID, local.Height, r.currentPBFTView(), local.Height, wire)
			if err != nil {
				return PorygonESCBatchCertificate{}, err
			}
			if err := r.sendToNode(ctx, leader, env); err != nil {
				return PorygonESCBatchCertificate{}, err
			}
		}
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	recovery := time.NewTicker(50 * time.Millisecond)
	defer recovery.Stop()
	for {
		state := r.porygonBatchState()
		state.mu.Lock()
		cert, ok := state.certs[local.BlockHash]
		state.mu.Unlock()
		if ok {
			if err := r.validatePorygonBatchCertificate(cert, requiredShards); err != nil {
				return PorygonESCBatchCertificate{}, err
			}
			return cert, nil
		}
		if r.isCurrentLeader() {
			cert, ready, err := r.tryBuildPorygonBatchCertificate(local.BlockHash, local.Height, requiredShards)
			if err != nil {
				return PorygonESCBatchCertificate{}, err
			}
			if ready {
				// The leader must validate the exact certificate it is about to
				// broadcast/consume. This prevents a leader-only commit when peers
				// would reject the aggregate certificate.
				if err := r.validatePorygonBatchCertificate(cert, requiredShards); err != nil {
					return PorygonESCBatchCertificate{}, err
				}
				r.broadcastPorygonBatchCertificate(ctx, cert)
				return cert, nil
			}
		}
		select {
		case <-ctx.Done():
			return PorygonESCBatchCertificate{}, ctx.Err()
		case <-ticker.C:
		case <-recovery.C:
			if !r.isCurrentLeader() {
				leader := r.leaderID(r.node.ShardID)
				req := map[string]any{"block_hash": local.BlockHash, "height": local.Height}
				env, _ := p2p.NewEnvelope(porygonESCBatchCertificateRequestMessage, r.node.NodeID, leader, r.node.ShardID, local.Height, r.currentPBFTView(), local.Height, req)
				_ = r.sendToNode(ctx, leader, env)
			}
		}
	}
}

func (r *NodeRuntime) handlePorygonESCBatchResult(ctx context.Context, msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("Porygon ESC batch result sent to non-leader")
	}
	wire, err := p2p.DecodePayload[PorygonESCBatchResultWire](msg)
	if err != nil {
		return err
	}
	if wire.Result.SenderNodeID != msg.FromNode {
		return fmt.Errorf("Porygon batch envelope sender mismatch")
	}
	return r.acceptPorygonBatchResult(wire.Result, wire.Attestation)
}
func (r *NodeRuntime) handlePorygonESCBatchCertificate(msg p2p.MessageEnvelope) error {
	cert, err := p2p.DecodePayload[PorygonESCBatchCertificate](msg)
	if err != nil {
		return err
	}
	required := make([]string, 0, len(cert.Entries))
	for _, entry := range cert.Entries {
		required = append(required, entry.ExecutionShardID)
	}
	if err := r.validatePorygonBatchCertificate(cert, required); err != nil {
		return err
	}
	// The certificate is self-authenticated by its ESC attestations and may be
	// relayed by a later PBFT leader during recovery. Do not bind certificate
	// validity to the transport sender or original OC leader.
	semanticDigest := porygonBatchCertificateSemanticDigest(cert)
	state := r.porygonBatchState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if existing, ok := state.certs[cert.BlockHash]; ok {
		if porygonBatchCertificateSemanticDigest(existing) != semanticDigest {
			return fmt.Errorf("conflicting Porygon batch certificate semantic result")
		}
		// Different leaders/voter subsets may produce byte-distinct certificates
		// for the same authenticated ESC result. They are protocol-equivalent; keep
		// the first validated certificate instead of treating signatures as state.
		return nil
	}
	state.certs[cert.BlockHash] = cert
	return nil
}
func (r *NodeRuntime) handlePorygonESCBatchCertificateRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
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
	state := r.porygonBatchState()
	state.mu.Lock()
	cert, ok := state.certs[req.BlockHash]
	state.mu.Unlock()
	if !ok {
		return nil
	}
	return r.sendPorygonBatchCertificate(ctx, msg.FromNode, cert)
}
