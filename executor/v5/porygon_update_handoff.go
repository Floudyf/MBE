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
)

const (
	porygonUpdateHandoffRequestMessage = "PORYGON_UPDATE_HANDOFF_REQUEST_V1"
	porygonUpdateHandoffVoteMessage    = "PORYGON_UPDATE_HANDOFF_VOTE_V1"
)

type PorygonUpdateHandoffRequest struct {
	BlockHash       string `json:"block_hash"`
	Height          uint64 `json:"height"`
	Attempt         int    `json:"attempt"`
	ProtocolRound   uint64 `json:"protocol_round"`
	PartitionID     string `json:"partition_id"`
	UpdateDigest    string `json:"update_digest"`
	CommitteeDigest string `json:"committee_digest"`
	RequestDigest   string `json:"request_digest"`
}

type PorygonUpdateHandoffVote struct {
	RequestDigest   string `json:"request_digest"`
	BlockHash       string `json:"block_hash"`
	Height          uint64 `json:"height"`
	Attempt         int    `json:"attempt"`
	ProtocolRound   uint64 `json:"protocol_round"`
	PartitionID     string `json:"partition_id"`
	UpdateDigest    string `json:"update_digest"`
	CommitteeDigest string `json:"committee_digest"`
	NodeID          string `json:"node_id"`
	Signature       string `json:"signature"`
}

type PorygonUpdateHandoffCertificate struct {
	RequestDigest     string                     `json:"request_digest"`
	BlockHash         string                     `json:"block_hash"`
	Height            uint64                     `json:"height"`
	Attempt           int                        `json:"attempt"`
	ProtocolRound     uint64                     `json:"protocol_round"`
	PartitionID       string                     `json:"partition_id"`
	UpdateDigest      string                     `json:"update_digest"`
	CommitteeDigest   string                     `json:"committee_digest"`
	Committee         []string                   `json:"committee"`
	Threshold         int                        `json:"threshold"`
	Votes             []PorygonUpdateHandoffVote `json:"votes"`
	CertificateDigest string                     `json:"certificate_digest"`
}

type porygonUpdateHandoffState struct {
	mu     sync.Mutex
	votes  map[string]map[string]PorygonUpdateHandoffVote
	signal map[string]chan struct{}
}

var porygonUpdateHandoffStates sync.Map // map[*NodeRuntime]*porygonUpdateHandoffState

func (r *NodeRuntime) porygonUpdateHandoffState() *porygonUpdateHandoffState {
	if value, ok := porygonUpdateHandoffStates.Load(r); ok {
		return value.(*porygonUpdateHandoffState)
	}
	created := &porygonUpdateHandoffState{votes: map[string]map[string]PorygonUpdateHandoffVote{}, signal: map[string]chan struct{}{}}
	actual, _ := porygonUpdateHandoffStates.LoadOrStore(r, created)
	return actual.(*porygonUpdateHandoffState)
}

func porygonUpdateProtocolRound(height uint64, attempt int) uint64 {
	if attempt < 1 {
		attempt = 1
	}
	// porygonPipelineForHeight places M at logical slot h+3. A retry is owned
	// by the following ESC round, so attempts advance this protocol round.
	return height + 2 + uint64(attempt)
}

func porygonHandoffRequestDigest(request PorygonUpdateHandoffRequest) string {
	copyRequest := request
	copyRequest.RequestDigest = ""
	return stableJSONDigest(copyRequest)
}

func porygonHandoffVoteBytes(vote PorygonUpdateHandoffVote) []byte {
	copyVote := vote
	copyVote.Signature = ""
	raw, _ := json.Marshal(copyVote)
	return raw
}

func porygonHandoffCertificateDigest(cert PorygonUpdateHandoffCertificate) string {
	copyCert := cert
	copyCert.CertificateDigest = ""
	copyCert.Committee = append([]string(nil), cert.Committee...)
	copyCert.Votes = append([]PorygonUpdateHandoffVote(nil), cert.Votes...)
	sort.Strings(copyCert.Committee)
	sort.Slice(copyCert.Votes, func(i, j int) bool { return copyCert.Votes[i].NodeID < copyCert.Votes[j].NodeID })
	return stableJSONDigest(copyCert)
}

func (r *NodeRuntime) porygonBuildUpdateHandoffRequest(blockHash string, height uint64, attempt int, partitionID, updateDigest string) (PorygonUpdateHandoffRequest, []string, error) {
	round := porygonUpdateProtocolRound(height, attempt)
	members := r.porygonExecutionRoleMembers(round, partitionID)
	if len(members) == 0 {
		return PorygonUpdateHandoffRequest{}, nil, fmt.Errorf("porygon update handoff has empty ESC for %s at round %d", partitionID, round)
	}
	sort.Strings(members)
	request := PorygonUpdateHandoffRequest{
		BlockHash: blockHash, Height: height, Attempt: attempt, ProtocolRound: round,
		PartitionID: partitionID, UpdateDigest: updateDigest, CommitteeDigest: stableJSONDigest(members),
	}
	request.RequestDigest = porygonHandoffRequestDigest(request)
	return request, members, nil
}

func (r *NodeRuntime) signPorygonUpdateHandoffVote(request PorygonUpdateHandoffRequest) (PorygonUpdateHandoffVote, error) {
	members := r.porygonExecutionRoleMembers(request.ProtocolRound, request.PartitionID)
	if !containsString(members, r.node.NodeID) {
		return PorygonUpdateHandoffVote{}, fmt.Errorf("node %s is not Porygon update ESC member for %s round %d", r.node.NodeID, request.PartitionID, request.ProtocolRound)
	}
	if request.RequestDigest != porygonHandoffRequestDigest(request) || request.CommitteeDigest != stableJSONDigest(sortedStringsCopy(members)) {
		return PorygonUpdateHandoffVote{}, fmt.Errorf("porygon update handoff request identity mismatch")
	}
	vote := PorygonUpdateHandoffVote{
		RequestDigest: request.RequestDigest, BlockHash: request.BlockHash, Height: request.Height,
		Attempt: request.Attempt, ProtocolRound: request.ProtocolRound, PartitionID: request.PartitionID,
		UpdateDigest: request.UpdateDigest, CommitteeDigest: request.CommitteeDigest, NodeID: r.node.NodeID,
	}
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return PorygonUpdateHandoffVote{}, err
	}
	vote.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonHandoffVoteBytes(vote)))
	return vote, nil
}

func sortedStringsCopy(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}

func (r *NodeRuntime) verifyPorygonUpdateHandoffVote(vote PorygonUpdateHandoffVote, request PorygonUpdateHandoffRequest, members []string) error {
	if vote.RequestDigest != request.RequestDigest || vote.BlockHash != request.BlockHash || vote.Height != request.Height || vote.Attempt != request.Attempt ||
		vote.ProtocolRound != request.ProtocolRound || vote.PartitionID != request.PartitionID || vote.UpdateDigest != request.UpdateDigest || vote.CommitteeDigest != request.CommitteeDigest {
		return fmt.Errorf("porygon update handoff vote binding mismatch")
	}
	if !containsString(members, vote.NodeID) {
		return fmt.Errorf("porygon update handoff vote signer %s is outside ESC", vote.NodeID)
	}
	key, err := r.pbftPublicKey(vote.NodeID)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(vote.Signature)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, porygonHandoffVoteBytes(vote), sig) {
		return fmt.Errorf("porygon update handoff vote signature invalid")
	}
	return nil
}

func (r *NodeRuntime) acceptPorygonUpdateHandoffVote(vote PorygonUpdateHandoffVote) error {
	request := PorygonUpdateHandoffRequest{
		RequestDigest: vote.RequestDigest, BlockHash: vote.BlockHash, Height: vote.Height, Attempt: vote.Attempt,
		ProtocolRound: vote.ProtocolRound, PartitionID: vote.PartitionID, UpdateDigest: vote.UpdateDigest, CommitteeDigest: vote.CommitteeDigest,
	}
	members := r.porygonExecutionRoleMembers(vote.ProtocolRound, vote.PartitionID)
	if err := r.verifyPorygonUpdateHandoffVote(vote, request, members); err != nil {
		return err
	}
	state := r.porygonUpdateHandoffState()
	state.mu.Lock()
	if state.votes[vote.RequestDigest] == nil {
		state.votes[vote.RequestDigest] = map[string]PorygonUpdateHandoffVote{}
	}
	state.votes[vote.RequestDigest][vote.NodeID] = vote
	signal := state.signal[vote.RequestDigest]
	state.mu.Unlock()
	if signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *NodeRuntime) handlePorygonUpdateHandoffRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonUpdateHandoffRequest](msg)
	if err != nil {
		return err
	}
	if request.RequestDigest != porygonHandoffRequestDigest(request) {
		return fmt.Errorf("porygon update handoff request digest mismatch")
	}
	vote, err := r.signPorygonUpdateHandoffVote(request)
	if err != nil {
		return err
	}
	leader := r.leaderID(r.node.ShardID)
	if leader == r.node.NodeID {
		return r.acceptPorygonUpdateHandoffVote(vote)
	}
	env, err := p2p.NewEnvelope(porygonUpdateHandoffVoteMessage, r.node.NodeID, leader, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, vote)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, leader, env)
}

func (r *NodeRuntime) handlePorygonUpdateHandoffVote(msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("porygon update handoff vote sent to non-leader")
	}
	vote, err := p2p.DecodePayload[PorygonUpdateHandoffVote](msg)
	if err != nil {
		return err
	}
	if vote.NodeID != msg.FromNode {
		return fmt.Errorf("porygon update handoff sender mismatch")
	}
	return r.acceptPorygonUpdateHandoffVote(vote)
}

func (r *NodeRuntime) collectPorygonUpdateHandoffCertificate(ctx context.Context, request PorygonUpdateHandoffRequest, members []string) (PorygonUpdateHandoffCertificate, error) {
	threshold := porygonExecutionThreshold(len(members))
	state := r.porygonUpdateHandoffState()
	state.mu.Lock()
	if state.signal[request.RequestDigest] == nil {
		state.signal[request.RequestDigest] = make(chan struct{}, 1)
	}
	signal := state.signal[request.RequestDigest]
	state.mu.Unlock()

	for _, nodeID := range members {
		if nodeID == r.node.NodeID {
			vote, err := r.signPorygonUpdateHandoffVote(request)
			if err != nil {
				return PorygonUpdateHandoffCertificate{}, err
			}
			if err := r.acceptPorygonUpdateHandoffVote(vote); err != nil {
				return PorygonUpdateHandoffCertificate{}, err
			}
			continue
		}
		env, err := p2p.NewEnvelope(porygonUpdateHandoffRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, request)
		if err != nil {
			return PorygonUpdateHandoffCertificate{}, err
		}
		if err := r.sendToNode(ctx, nodeID, env); err != nil {
			continue
		}
	}

	for {
		state.mu.Lock()
		byNode := state.votes[request.RequestDigest]
		votes := make([]PorygonUpdateHandoffVote, 0, len(byNode))
		for _, vote := range byNode {
			votes = append(votes, vote)
		}
		state.mu.Unlock()
		if len(votes) >= threshold {
			sort.Slice(votes, func(i, j int) bool { return votes[i].NodeID < votes[j].NodeID })
			cert := PorygonUpdateHandoffCertificate{
				RequestDigest: request.RequestDigest, BlockHash: request.BlockHash, Height: request.Height, Attempt: request.Attempt,
				ProtocolRound: request.ProtocolRound, PartitionID: request.PartitionID, UpdateDigest: request.UpdateDigest,
				CommitteeDigest: request.CommitteeDigest, Committee: sortedStringsCopy(members), Threshold: threshold, Votes: votes,
			}
			cert.CertificateDigest = porygonHandoffCertificateDigest(cert)
			r.addPorygonRuntimeMetric("porygon_update_handoff_certificate_count", 1)
			return cert, nil
		}
		select {
		case <-ctx.Done():
			return PorygonUpdateHandoffCertificate{}, ctx.Err()
		case <-signal:
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (r *NodeRuntime) validatePorygonUpdateHandoffCertificate(cert PorygonUpdateHandoffCertificate) error {
	request := PorygonUpdateHandoffRequest{
		RequestDigest: cert.RequestDigest, BlockHash: cert.BlockHash, Height: cert.Height, Attempt: cert.Attempt,
		ProtocolRound: cert.ProtocolRound, PartitionID: cert.PartitionID, UpdateDigest: cert.UpdateDigest, CommitteeDigest: cert.CommitteeDigest,
	}
	if request.RequestDigest != porygonHandoffRequestDigest(request) {
		return fmt.Errorf("porygon update handoff certificate request digest mismatch")
	}
	members := r.porygonExecutionRoleMembers(cert.ProtocolRound, cert.PartitionID)
	members = sortedStringsCopy(members)
	if !sameStringList(members, sortedStringsCopy(cert.Committee)) || cert.CommitteeDigest != stableJSONDigest(members) {
		return fmt.Errorf("porygon update handoff certificate committee mismatch")
	}
	threshold := porygonExecutionThreshold(len(members))
	if cert.Threshold != threshold || len(cert.Votes) < threshold {
		return fmt.Errorf("porygon update handoff certificate threshold not met")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Votes {
		if seen[vote.NodeID] {
			return fmt.Errorf("porygon update handoff duplicate voter %s", vote.NodeID)
		}
		seen[vote.NodeID] = true
		if err := r.verifyPorygonUpdateHandoffVote(vote, request, members); err != nil {
			return err
		}
	}
	if cert.CertificateDigest == "" || cert.CertificateDigest != porygonHandoffCertificateDigest(cert) {
		return fmt.Errorf("porygon update handoff certificate digest mismatch")
	}
	return nil
}
