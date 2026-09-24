package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
)

const (
	porygonESCWaveResultMessage      = "PORYGON_ESC_WAVE_RESULT_V1"
	porygonESCWaveCertificateMessage = "PORYGON_ESC_WAVE_CERTIFICATE_V1"
)

// PorygonWaveTxResult is the business execution output produced by exactly one
// ESC for one transaction. Every global validator later materializes the
// certified deltas without re-running the business transaction.
type PorygonWaveTxResult struct {
	TxID    string            `json:"tx_id"`
	Receipt execution.Receipt `json:"receipt"`
	Delta   execution.TxDelta `json:"delta"`
}

// PorygonESCWaveResult is one ESC replica's result for the subset of a global
// wave owned by its execution shard. Timing is excluded from ResultDigest so
// replicas can agree on semantics despite local wall-clock differences.
type PorygonESCWaveResult struct {
	BlockHash           string                `json:"block_hash"`
	Height              uint64                `json:"height"`
	Wave                int                   `json:"wave"`
	ExecutionShardID    string                `json:"execution_shard_id"`
	SenderNodeID        string                `json:"sender_node_id"`
	Results             []PorygonWaveTxResult `json:"results"`
	ResultDigest        string                `json:"result_digest"`
	BusinessExecutionUS int64                 `json:"business_execution_us"`
}

type PorygonESCWaveCertificateEntry struct {
	ExecutionShardID string                  `json:"execution_shard_id"`
	ResultDigest     string                  `json:"result_digest"`
	Voters           []string                `json:"voters"`
	Attestations     []PorygonESCAttestation `json:"attestations,omitempty"`
	Result           PorygonESCWaveResult    `json:"result"`
}

// PorygonESCWaveCertificate is the MBE adaptation of the OC accepting an ESC
// result only after more than half of that ESC returned the same result.
type PorygonESCWaveCertificate struct {
	BlockHash         string                           `json:"block_hash"`
	Height            uint64                           `json:"height"`
	Wave              int                              `json:"wave"`
	LeaderNodeID      string                           `json:"leader_node_id"`
	Entries           []PorygonESCWaveCertificateEntry `json:"entries"`
	CertificateDigest string                           `json:"certificate_digest"`
}

type PorygonWaveExchangeFunc func(context.Context, PorygonESCWaveResult, []string) (PorygonESCWaveCertificate, error)

func porygonESCWaveResultDigest(result PorygonESCWaveResult) string {
	copy := result
	copy.SenderNodeID = ""
	copy.ResultDigest = ""
	copy.BusinessExecutionUS = 0
	copy.Results = append([]PorygonWaveTxResult(nil), copy.Results...)
	sort.Slice(copy.Results, func(i, j int) bool { return copy.Results[i].TxID < copy.Results[j].TxID })
	payload, _ := json.Marshal(copy)
	return stableTextDigest(string(payload))
}

func sealPorygonESCWaveResult(result PorygonESCWaveResult) PorygonESCWaveResult {
	result.ResultDigest = porygonESCWaveResultDigest(result)
	return result
}

func validatePorygonESCWaveResult(result PorygonESCWaveResult) error {
	if result.BlockHash == "" || result.Height == 0 || result.Wave < 0 {
		return fmt.Errorf("porygon ESC wave result identity is incomplete")
	}
	if strings.TrimSpace(result.ExecutionShardID) == "" || strings.TrimSpace(result.SenderNodeID) == "" {
		return fmt.Errorf("porygon ESC wave result sender/shard is incomplete")
	}
	seen := map[string]bool{}
	for _, item := range result.Results {
		if item.TxID == "" || seen[item.TxID] {
			return fmt.Errorf("porygon ESC wave result has empty/duplicate transaction id")
		}
		seen[item.TxID] = true
		if item.Delta.TxID != item.TxID || item.Receipt.TxID != item.TxID {
			return fmt.Errorf("porygon ESC wave result transaction identity mismatch for %s", item.TxID)
		}
	}
	if result.ResultDigest == "" || result.ResultDigest != porygonESCWaveResultDigest(result) {
		return fmt.Errorf("porygon ESC wave result digest mismatch")
	}
	return nil
}

func porygonESCWaveCertificateDigest(cert PorygonESCWaveCertificate) string {
	type entryDigest struct {
		ExecutionShardID string   `json:"execution_shard_id"`
		ResultDigest     string   `json:"result_digest"`
		Voters           []string `json:"voters"`
	}
	projection := struct {
		BlockHash    string        `json:"block_hash"`
		Height       uint64        `json:"height"`
		Wave         int           `json:"wave"`
		LeaderNodeID string        `json:"leader_node_id"`
		Entries      []entryDigest `json:"entries"`
	}{BlockHash: cert.BlockHash, Height: cert.Height, Wave: cert.Wave, LeaderNodeID: cert.LeaderNodeID}
	for _, entry := range cert.Entries {
		voters := append([]string(nil), entry.Voters...)
		sort.Strings(voters)
		projection.Entries = append(projection.Entries, entryDigest{ExecutionShardID: entry.ExecutionShardID, ResultDigest: entry.ResultDigest, Voters: voters})
	}
	sort.Slice(projection.Entries, func(i, j int) bool {
		return projection.Entries[i].ExecutionShardID < projection.Entries[j].ExecutionShardID
	})
	payload, _ := json.Marshal(projection)
	return stableTextDigest(string(payload))
}

func sealPorygonESCWaveCertificate(cert PorygonESCWaveCertificate) PorygonESCWaveCertificate {
	cert.CertificateDigest = porygonESCWaveCertificateDigest(cert)
	return cert
}

func validatePorygonESCWaveCertificate(cert PorygonESCWaveCertificate) error {
	if cert.BlockHash == "" || cert.Height == 0 || cert.Wave < 0 || cert.LeaderNodeID == "" {
		return fmt.Errorf("porygon ESC certificate identity is incomplete")
	}
	seenShards := map[string]bool{}
	for _, entry := range cert.Entries {
		if entry.ExecutionShardID == "" || seenShards[entry.ExecutionShardID] {
			return fmt.Errorf("porygon ESC certificate has empty/duplicate shard")
		}
		seenShards[entry.ExecutionShardID] = true
		if err := validatePorygonESCWaveResult(entry.Result); err != nil {
			return err
		}
		if entry.Result.BlockHash != cert.BlockHash || entry.Result.Height != cert.Height || entry.Result.Wave != cert.Wave {
			return fmt.Errorf("porygon ESC certificate result block/wave mismatch")
		}
		if entry.Result.ExecutionShardID != entry.ExecutionShardID || entry.ResultDigest != entry.Result.ResultDigest {
			return fmt.Errorf("porygon ESC certificate result identity mismatch")
		}
		if len(entry.Voters) == 0 {
			return fmt.Errorf("porygon ESC certificate has no voters")
		}
	}
	if cert.CertificateDigest == "" || cert.CertificateDigest != porygonESCWaveCertificateDigest(cert) {
		return fmt.Errorf("porygon ESC certificate digest mismatch")
	}
	return nil
}

type porygonESCResultBucket struct {
	Result       PorygonESCWaveResult
	Voters       map[string]bool
	Attestations map[string]PorygonESCAttestation
}

type porygonESCExchangeState struct {
	mu               sync.Mutex
	results          map[string]map[string]map[string]*porygonESCResultBucket
	senderDigest     map[string]map[string]map[string]string
	certificates     map[string]PorygonESCWaveCertificate
	certificateOrder []string
}

var porygonESCExchangeStates sync.Map // map[*NodeRuntime]*porygonESCExchangeState

func (r *NodeRuntime) porygonESCState() *porygonESCExchangeState {
	if value, ok := porygonESCExchangeStates.Load(r); ok {
		return value.(*porygonESCExchangeState)
	}
	created := &porygonESCExchangeState{
		results:          map[string]map[string]map[string]*porygonESCResultBucket{},
		senderDigest:     map[string]map[string]map[string]string{},
		certificates:     map[string]PorygonESCWaveCertificate{},
		certificateOrder: []string{},
	}
	actual, _ := porygonESCExchangeStates.LoadOrStore(r, created)
	return actual.(*porygonESCExchangeState)
}

func porygonESCWaveKey(blockHash string, wave int) string {
	return fmt.Sprintf("%s|%d", blockHash, wave)
}

func (r *NodeRuntime) porygonExecutionShardMembers(executionShardID string) []string {
	out := []string{}
	for _, node := range r.plan.NodeConfigs {
		if effectiveExecutionShardID(node) == executionShardID {
			out = append(out, node.NodeID)
		}
	}
	sort.Strings(out)
	return out
}

// porygonExecutionResultThreshold follows Porygon's Multi-Shard Update
// rule: the OC accepts an ESC execution result only when more than one half
// of that ESC returned the same result. For n members this is floor(n/2)+1
// matching replicas (n=4 => 3).
func porygonExecutionResultThreshold(memberCount int) int {
	if memberCount < 1 {
		return 1
	}
	return memberCount/2 + 1
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) acceptPorygonESCWaveResult(result PorygonESCWaveResult) error {
	return r.acceptPorygonESCWaveResultWithAttestation(result, PorygonESCAttestation{})
}

func (r *NodeRuntime) acceptPorygonESCWaveResultWithAttestation(result PorygonESCWaveResult, attestation PorygonESCAttestation) error {
	if err := validatePorygonESCWaveResult(result); err != nil {
		return err
	}
	members := r.porygonExecutionShardMembers(result.ExecutionShardID)
	if len(members) == 0 || !containsString(members, result.SenderNodeID) {
		return fmt.Errorf("porygon ESC result sender %s is not a member of %s", result.SenderNodeID, result.ExecutionShardID)
	}
	if attestation.NodeID != "" {
		if err := r.verifyPorygonESCAttestation(result, attestation); err != nil {
			return err
		}
	} else if r.pbftAuthenticationRequired() {
		return fmt.Errorf("porygon ESC result from %s is missing authenticated attestation", result.SenderNodeID)
	}
	key := porygonESCWaveKey(result.BlockHash, result.Wave)
	state := r.porygonESCState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.senderDigest[key] == nil {
		state.senderDigest[key] = map[string]map[string]string{}
	}
	if state.senderDigest[key][result.ExecutionShardID] == nil {
		state.senderDigest[key][result.ExecutionShardID] = map[string]string{}
	}
	if prior := state.senderDigest[key][result.ExecutionShardID][result.SenderNodeID]; prior != "" && prior != result.ResultDigest {
		return fmt.Errorf("porygon ESC result equivocation from %s", result.SenderNodeID)
	}
	state.senderDigest[key][result.ExecutionShardID][result.SenderNodeID] = result.ResultDigest
	if state.results[key] == nil {
		state.results[key] = map[string]map[string]*porygonESCResultBucket{}
	}
	if state.results[key][result.ExecutionShardID] == nil {
		state.results[key][result.ExecutionShardID] = map[string]*porygonESCResultBucket{}
	}
	bucket := state.results[key][result.ExecutionShardID][result.ResultDigest]
	if bucket == nil {
		bucket = &porygonESCResultBucket{Result: result, Voters: map[string]bool{}, Attestations: map[string]PorygonESCAttestation{}}
		state.results[key][result.ExecutionShardID][result.ResultDigest] = bucket
	}
	bucket.Voters[result.SenderNodeID] = true
	if attestation.NodeID != "" {
		bucket.Attestations[attestation.NodeID] = attestation
	}
	return nil
}

func (r *NodeRuntime) tryBuildPorygonESCWaveCertificate(blockHash string, height uint64, wave int, requiredShards []string) (PorygonESCWaveCertificate, bool, error) {
	leader := r.leaderID(r.node.ShardID)
	if leader == "" || leader != r.node.NodeID {
		return PorygonESCWaveCertificate{}, false, fmt.Errorf("porygon ESC certificate may only be built by global leader")
	}
	key := porygonESCWaveKey(blockHash, wave)
	state := r.porygonESCState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if existing, ok := state.certificates[key]; ok {
		return existing, true, nil
	}
	cert := PorygonESCWaveCertificate{BlockHash: blockHash, Height: height, Wave: wave, LeaderNodeID: leader}
	shards := append([]string(nil), requiredShards...)
	sort.Strings(shards)
	for _, shardID := range shards {
		members := r.porygonExecutionShardMembers(shardID)
		threshold := porygonExecutionResultThreshold(len(members))
		buckets := state.results[key][shardID]
		var chosen *porygonESCResultBucket
		for _, bucket := range buckets {
			if len(bucket.Voters) >= threshold {
				if chosen != nil && chosen.Result.ResultDigest != bucket.Result.ResultDigest {
					return PorygonESCWaveCertificate{}, false, fmt.Errorf("multiple Porygon ESC result digests reached majority for %s", shardID)
				}
				chosen = bucket
			}
		}
		if chosen == nil {
			return PorygonESCWaveCertificate{}, false, nil
		}
		voters := sortedBoolKeys(chosen.Voters)
		cert.Entries = append(cert.Entries, PorygonESCWaveCertificateEntry{ExecutionShardID: shardID, ResultDigest: chosen.Result.ResultDigest, Voters: voters, Attestations: sortedPorygonESCAttestations(chosen.Attestations), Result: chosen.Result})
	}
	cert = sealPorygonESCWaveCertificate(cert)
	storePorygonESCWaveCertificateLocked(state, key, cert)
	return cert, true, nil
}

func (r *NodeRuntime) validatePorygonCertificateAgainstPlan(cert PorygonESCWaveCertificate, requiredShards []string) error {
	if err := validatePorygonESCWaveCertificate(cert); err != nil {
		return err
	}
	leader := r.leaderID(r.node.ShardID)
	if leader == "" || cert.LeaderNodeID != leader {
		return fmt.Errorf("porygon ESC certificate leader mismatch")
	}
	required := map[string]bool{}
	for _, shardID := range requiredShards {
		required[shardID] = true
	}
	if len(cert.Entries) != len(required) {
		return fmt.Errorf("porygon ESC certificate shard coverage mismatch")
	}
	for _, entry := range cert.Entries {
		if !required[entry.ExecutionShardID] {
			return fmt.Errorf("porygon ESC certificate contains unexpected shard %s", entry.ExecutionShardID)
		}
		members := r.porygonExecutionShardMembers(entry.ExecutionShardID)
		if len(entry.Voters) < porygonExecutionResultThreshold(len(members)) {
			return fmt.Errorf("porygon ESC certificate does not satisfy execution-result threshold for %s", entry.ExecutionShardID)
		}
		if !containsString(entry.Voters, entry.Result.SenderNodeID) {
			return fmt.Errorf("porygon ESC certificate representative sender is not a voter")
		}
		seen := map[string]bool{}
		for _, voter := range entry.Voters {
			if seen[voter] || !containsString(members, voter) {
				return fmt.Errorf("porygon ESC certificate has invalid voter %s", voter)
			}
			seen[voter] = true
		}
		if err := r.verifyPorygonCertificateEntryAttestations(entry); err != nil {
			return err
		}
	}
	return nil
}

func (r *NodeRuntime) storePorygonESCWaveCertificate(cert PorygonESCWaveCertificate) error {
	if err := validatePorygonESCWaveCertificate(cert); err != nil {
		return err
	}
	for _, entry := range cert.Entries {
		if err := r.verifyPorygonCertificateEntryAttestations(entry); err != nil {
			return err
		}
	}
	key := porygonESCWaveKey(cert.BlockHash, cert.Wave)
	state := r.porygonESCState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if existing, ok := state.certificates[key]; ok && existing.CertificateDigest != cert.CertificateDigest {
		return fmt.Errorf("conflicting porygon ESC certificate for %s", key)
	}
	storePorygonESCWaveCertificateLocked(state, key, cert)
	return nil
}

func (r *NodeRuntime) waitPorygonESCWaveCertificate(ctx context.Context, blockHash string, height uint64, wave int, requiredShards []string) (PorygonESCWaveCertificate, error) {
	key := porygonESCWaveKey(blockHash, wave)
	ticker := time.NewTicker(2 * time.Millisecond)
	recoveryTicker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	defer recoveryTicker.Stop()
	for {
		state := r.porygonESCState()
		state.mu.Lock()
		cert, ok := state.certificates[key]
		state.mu.Unlock()
		if ok {
			if err := r.validatePorygonCertificateAgainstPlan(cert, requiredShards); err != nil {
				return PorygonESCWaveCertificate{}, err
			}
			return cert, nil
		}
		select {
		case <-ctx.Done():
			return PorygonESCWaveCertificate{}, ctx.Err()
		case <-ticker.C:
		case <-recoveryTicker.C:
			_ = r.requestPorygonESCWaveCertificate(ctx, blockHash, height, wave)
		}
	}
}

func (r *NodeRuntime) releasePorygonESCWaveState(blockHash string, wave int) {
	key := porygonESCWaveKey(blockHash, wave)
	state := r.porygonESCState()
	state.mu.Lock()
	delete(state.results, key)
	delete(state.senderDigest, key)
	prunePorygonESCWaveCertificatesLocked(state)
	state.mu.Unlock()
}

func (r *NodeRuntime) broadcastPorygonESCWaveCertificate(ctx context.Context, cert PorygonESCWaveCertificate) error {
	targets := make([]string, 0, len(r.node.Validators))
	for _, nodeID := range r.node.Validators {
		if nodeID == "" || nodeID == r.node.NodeID {
			continue
		}
		targets = append(targets, nodeID)
	}
	type sendOutcome struct {
		nodeID string
		err    error
	}
	outcomes := make(chan sendOutcome, len(targets))
	for _, nodeID := range targets {
		nodeID := nodeID
		go func() {
			outcomes <- sendOutcome{nodeID: nodeID, err: r.sendPorygonESCWaveCertificateToNode(ctx, nodeID, cert)}
		}()
	}
	failures := 0
	for range targets {
		if outcome := <-outcomes; outcome.err != nil {
			failures++
		}
	}
	if len(targets) > 0 {
		r.addPorygonRuntimeMetric("porygon_esc_certificate_broadcast_target_count", int64(len(targets)))
	}
	if failures > 0 {
		r.addPorygonRuntimeMetric("porygon_esc_certificate_broadcast_send_failure_count", int64(failures))
	}
	r.addPorygonRuntimeMetric("porygon_esc_certificate_broadcast_success_count", int64(len(targets)-failures))
	return nil
}

func (r *NodeRuntime) porygonWaveExchange(ctx context.Context, local PorygonESCWaveResult, requiredShards []string) (PorygonESCWaveCertificate, error) {
	if r.plugins.BlockExecutor == nil || r.plugins.BlockExecutor.ID() != porygonBlockExecutorID {
		return PorygonESCWaveCertificate{}, fmt.Errorf("porygon ESC exchange invoked for non-Porygon executor")
	}
	defer r.releasePorygonESCWaveState(local.BlockHash, local.Wave)
	local.ExecutionShardID = effectiveExecutionShardID(r.node)
	local.SenderNodeID = r.node.NodeID
	required := map[string]bool{}
	for _, shardID := range requiredShards {
		if shardID != "" {
			required[shardID] = true
		}
	}
	requiredShards = sortedBoolKeys(required)
	if required[local.ExecutionShardID] {
		local = sealPorygonESCWaveResult(local)
		attestation, err := r.signPorygonESCAttestation(local)
		if err != nil {
			return PorygonESCWaveCertificate{}, err
		}
		leader := r.leaderID(r.node.ShardID)
		if leader == "" {
			return PorygonESCWaveCertificate{}, fmt.Errorf("porygon global leader is unavailable")
		}
		if leader == r.node.NodeID {
			if err := r.acceptPorygonESCWaveResultWithAttestation(local, attestation); err != nil {
				return PorygonESCWaveCertificate{}, err
			}
		} else {
			wire := PorygonESCWaveResultWire{Result: local, Attestation: attestation}
			envelope, err := p2p.NewEnvelope(porygonESCWaveResultMessage, r.node.NodeID, leader, r.node.ShardID, local.Height, r.currentPBFTView(), local.Height, wire)
			if err != nil {
				return PorygonESCWaveCertificate{}, err
			}
			if err := r.sendToNode(ctx, leader, envelope); err != nil {
				return PorygonESCWaveCertificate{}, err
			}
		}
	}
	if r.isCurrentLeader() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			cert, ready, err := r.tryBuildPorygonESCWaveCertificate(local.BlockHash, local.Height, local.Wave, requiredShards)
			if err != nil {
				return PorygonESCWaveCertificate{}, err
			}
			if ready {
				if err := r.validatePorygonCertificateAgainstPlan(cert, requiredShards); err != nil {
					return PorygonESCWaveCertificate{}, err
				}
				if err := r.broadcastPorygonESCWaveCertificate(ctx, cert); err != nil {
					return PorygonESCWaveCertificate{}, err
				}
				return cert, nil
			}
			select {
			case <-ctx.Done():
				return PorygonESCWaveCertificate{}, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	return r.waitPorygonESCWaveCertificate(ctx, local.BlockHash, local.Height, local.Wave, requiredShards)
}

func (r *NodeRuntime) handlePorygonESCWaveResult(ctx context.Context, msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("porygon ESC wave result sent to non-leader")
	}
	wire, err := p2p.DecodePayload[PorygonESCWaveResultWire](msg)
	if err != nil {
		return err
	}
	if wire.Result.BlockHash == "" {
		legacy, legacyErr := p2p.DecodePayload[PorygonESCWaveResult](msg)
		if legacyErr != nil {
			return legacyErr
		}
		wire.Result = legacy
	}
	result := wire.Result
	if msg.FromNode != result.SenderNodeID || msg.Height != result.Height {
		return fmt.Errorf("porygon ESC result envelope identity mismatch")
	}
	return r.acceptPorygonESCWaveResultWithAttestation(result, wire.Attestation)
}

func (r *NodeRuntime) handlePorygonESCWaveCertificate(ctx context.Context, msg p2p.MessageEnvelope) error {
	cert, err := p2p.DecodePayload[PorygonESCWaveCertificate](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != cert.LeaderNodeID || msg.Height != cert.Height || msg.FromNode != r.leaderID(r.node.ShardID) {
		return fmt.Errorf("porygon ESC certificate envelope identity mismatch")
	}
	return r.storePorygonESCWaveCertificate(cert)
}
