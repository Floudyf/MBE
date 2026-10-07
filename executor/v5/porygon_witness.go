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
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	porygonWitnessRequestMessage = "PORYGON_WITNESS_REQUEST_V1"
	porygonWitnessVoteMessage    = "PORYGON_WITNESS_VOTE_V1"
)

type PorygonWitnessRequest struct {
	WitnessID       string                 `json:"witness_id"`
	Height          uint64                 `json:"height"`
	OrderingDomain  string                 `json:"ordering_domain"`
	TransactionRoot string                 `json:"transaction_root"`
	AccessRoot      string                 `json:"access_root"`
	FullBodyDigest  string                 `json:"full_body_digest"`
	CommitteeDigest string                 `json:"committee_digest"`
	Transactions    []tx.SignedTransaction `json:"transactions"`
}

type PorygonWitnessVote struct {
	WitnessID       string `json:"witness_id"`
	Height          uint64 `json:"height"`
	OrderingDomain  string `json:"ordering_domain"`
	TransactionRoot string `json:"transaction_root"`
	AccessRoot      string `json:"access_root"`
	FullBodyDigest  string `json:"full_body_digest"`
	CommitteeDigest string `json:"committee_digest"`
	NodeID          string `json:"node_id"`
	Signature       string `json:"signature"`
}

type PorygonWitnessCertificate struct {
	WitnessID         string               `json:"witness_id"`
	Height            uint64               `json:"height"`
	OrderingDomain    string               `json:"ordering_domain"`
	TransactionRoot   string               `json:"transaction_root"`
	AccessRoot        string               `json:"access_root"`
	FullBodyDigest    string               `json:"full_body_digest"`
	CommitteeDigest   string               `json:"committee_digest"`
	CommitteeMembers  []string             `json:"committee_members"`
	Threshold         int                  `json:"threshold"`
	Votes             []PorygonWitnessVote `json:"votes"`
	CertificateDigest string               `json:"certificate_digest"`
}

type porygonWitnessState struct {
	mu     sync.Mutex
	votes  map[string]map[string]PorygonWitnessVote
	signal map[string]chan struct{}
}

var porygonWitnessStates sync.Map // map[*NodeRuntime]*porygonWitnessState

type porygonPrewitnessedBatch struct {
	TargetHeight uint64
	LeaderID     string
	Items        []tx.SignedTransaction
	Certificate  PorygonWitnessCertificate
}

var porygonPrewitnessedBatches sync.Map // map[*mempool.Mempool]porygonPrewitnessedBatch

func (r *NodeRuntime) porygonWitnessState() *porygonWitnessState {
	if value, ok := porygonWitnessStates.Load(r); ok {
		return value.(*porygonWitnessState)
	}
	created := &porygonWitnessState{votes: map[string]map[string]PorygonWitnessVote{}, signal: map[string]chan struct{}{}}
	actual, _ := porygonWitnessStates.LoadOrStore(r, created)
	return actual.(*porygonWitnessState)
}

func porygonWitnessTarget(height uint64, orderingDomain string, items []tx.SignedTransaction) PorygonWitnessRequest {
	txIDs := transactionIDs(items)
	target := PorygonWitnessRequest{
		Height: height, OrderingDomain: orderingDomain,
		TransactionRoot: stableJSONDigest(txIDs),
		AccessRoot:      porygonAccessRoot(items),
		FullBodyDigest:  stableJSONDigest(items),
		Transactions:    append([]tx.SignedTransaction(nil), items...),
	}
	target.WitnessID = stableTextDigest(fmt.Sprintf("%d|%s|%s|%s|%s", height, orderingDomain, target.TransactionRoot, target.AccessRoot, target.FullBodyDigest))
	return target
}

func porygonWitnessCommitteeDigest(members []string) string {
	copyMembers := append([]string(nil), members...)
	sort.Strings(copyMembers)
	return stableJSONDigest(copyMembers)
}

func porygonWitnessVoteSigningBytes(vote PorygonWitnessVote) []byte {
	projection := vote
	projection.Signature = ""
	raw, _ := json.Marshal(projection)
	return raw
}

func porygonWitnessCertificateDigest(cert PorygonWitnessCertificate) string {
	copyCert := cert
	copyCert.CertificateDigest = ""
	copyCert.CommitteeMembers = append([]string(nil), cert.CommitteeMembers...)
	copyCert.Votes = append([]PorygonWitnessVote(nil), cert.Votes...)
	sort.Strings(copyCert.CommitteeMembers)
	sort.Slice(copyCert.Votes, func(i, j int) bool { return copyCert.Votes[i].NodeID < copyCert.Votes[j].NodeID })
	raw, _ := json.Marshal(copyCert)
	return stableTextDigest(string(raw))
}

func (r *NodeRuntime) porygonSignWitnessVote(target PorygonWitnessRequest) (PorygonWitnessVote, error) {
	key, err := r.pbftSigningPrivateKey()
	if err != nil {
		return PorygonWitnessVote{}, err
	}
	vote := PorygonWitnessVote{
		WitnessID: target.WitnessID, Height: target.Height, OrderingDomain: target.OrderingDomain,
		TransactionRoot: target.TransactionRoot, AccessRoot: target.AccessRoot,
		FullBodyDigest: target.FullBodyDigest, CommitteeDigest: target.CommitteeDigest,
		NodeID: r.node.NodeID,
	}
	vote.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, porygonWitnessVoteSigningBytes(vote)))
	return vote, nil
}

func (r *NodeRuntime) porygonVerifyWitnessVote(vote PorygonWitnessVote, members []string) error {
	if vote.NodeID == "" || vote.Signature == "" || !containsString(members, vote.NodeID) {
		return fmt.Errorf("porygon witness vote signer is not a committee member")
	}
	publicKey, err := r.pbftPublicKey(vote.NodeID)
	if err != nil {
		return err
	}
	signature, err := base64.StdEncoding.DecodeString(vote.Signature)
	if err != nil {
		return fmt.Errorf("porygon witness signature encoding: %w", err)
	}
	if !ed25519.Verify(publicKey, porygonWitnessVoteSigningBytes(vote), signature) {
		return fmt.Errorf("porygon witness signature verification failed for %s", vote.NodeID)
	}
	return nil
}

func (r *NodeRuntime) porygonValidateWitnessRequest(request PorygonWitnessRequest) error {
	expected := porygonWitnessTarget(request.Height, request.OrderingDomain, request.Transactions)
	members := porygonWitnessCommittee(r.plan.NodeConfigs, request.Height, request.OrderingDomain, request.FullBodyDigest)
	committeeDigest := porygonWitnessCommitteeDigest(members)
	if request.WitnessID != expected.WitnessID || request.TransactionRoot != expected.TransactionRoot || request.AccessRoot != expected.AccessRoot || request.FullBodyDigest != expected.FullBodyDigest {
		return fmt.Errorf("porygon witness request body commitment mismatch")
	}
	if request.CommitteeDigest != committeeDigest {
		return fmt.Errorf("porygon witness request committee mismatch")
	}
	for _, item := range request.Transactions {
		if err := tx.Verify(item); err != nil {
			return fmt.Errorf("porygon witness transaction verification failed: %w", err)
		}
	}
	return nil
}

func (r *NodeRuntime) handlePorygonWitnessRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonWitnessRequest](msg)
	if err != nil {
		return err
	}
	if err := r.porygonValidateWitnessRequest(request); err != nil {
		return err
	}
	if err := r.porygonStoreTransactionBlockForRole(porygonBuildTransactionBlock(request.Height, request.OrderingDomain, request.Transactions, "")); err != nil {
		return err
	}
	members := porygonWitnessCommittee(r.plan.NodeConfigs, request.Height, request.OrderingDomain, request.FullBodyDigest)
	if !containsString(members, r.node.NodeID) {
		return nil
	}
	vote, err := r.porygonSignWitnessVote(request)
	if err != nil {
		return err
	}
	leader := r.leaderID(r.node.ShardID)
	if leader == "" {
		return fmt.Errorf("porygon witness OC leader unavailable")
	}
	if leader == r.node.NodeID {
		return r.acceptPorygonWitnessVote(vote)
	}
	envelope, err := p2p.NewEnvelope(porygonWitnessVoteMessage, r.node.NodeID, leader, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, vote)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, leader, envelope)
}

func (r *NodeRuntime) acceptPorygonWitnessVote(vote PorygonWitnessVote) error {
	members := porygonWitnessCommittee(r.plan.NodeConfigs, vote.Height, vote.OrderingDomain, vote.FullBodyDigest)
	if vote.CommitteeDigest != porygonWitnessCommitteeDigest(members) {
		return fmt.Errorf("porygon witness vote committee digest mismatch")
	}
	if err := r.porygonVerifyWitnessVote(vote, members); err != nil {
		return err
	}
	state := r.porygonWitnessState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.votes[vote.WitnessID] == nil {
		state.votes[vote.WitnessID] = map[string]PorygonWitnessVote{}
	}
	state.votes[vote.WitnessID][vote.NodeID] = vote
	if signal := state.signal[vote.WitnessID]; signal != nil {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *NodeRuntime) handlePorygonWitnessVote(msg p2p.MessageEnvelope) error {
	if !r.isCurrentLeader() {
		return fmt.Errorf("porygon witness vote sent to non-OC leader")
	}
	vote, err := p2p.DecodePayload[PorygonWitnessVote](msg)
	if err != nil {
		return err
	}
	if msg.FromNode != vote.NodeID {
		return fmt.Errorf("porygon witness vote envelope signer mismatch")
	}
	return r.acceptPorygonWitnessVote(vote)
}

func porygonBlockProducerConfig(plugin BlockProducerPlugin) map[string]any {
	switch typed := plugin.(type) {
	case porygonBlockProducer:
		return typed.config
	case *porygonBlockProducer:
		return typed.config
	default:
		return nil
	}
}

func porygonConfiguredWitnessThreshold(config map[string]any, memberCount int) int {
	threshold := porygonWitnessFaultThreshold(memberCount)
	if value := intValue(config["witness_threshold"]); value > threshold {
		threshold = value
	}
	if threshold > memberCount {
		threshold = memberCount
	}
	return threshold
}

func (r *NodeRuntime) collectPorygonWitnessCertificate(ctx context.Context, target PorygonWitnessRequest) (PorygonWitnessCertificate, error) {
	members := porygonWitnessCommittee(r.plan.NodeConfigs, target.Height, target.OrderingDomain, target.FullBodyDigest)
	if len(members) == 0 {
		return PorygonWitnessCertificate{}, fmt.Errorf("porygon witness committee is empty")
	}
	target.CommitteeDigest = porygonWitnessCommitteeDigest(members)
	threshold := porygonConfiguredWitnessThreshold(porygonBlockProducerConfig(r.plugins.BlockProducer), len(members))

	state := r.porygonWitnessState()
	state.mu.Lock()
	if state.signal[target.WitnessID] == nil {
		state.signal[target.WitnessID] = make(chan struct{}, 1)
	}
	state.mu.Unlock()

	for _, nodeID := range members {
		if nodeID == r.node.NodeID {
			vote, err := r.porygonSignWitnessVote(target)
			if err != nil {
				return PorygonWitnessCertificate{}, err
			}
			if err := r.acceptPorygonWitnessVote(vote); err != nil {
				return PorygonWitnessCertificate{}, err
			}
			continue
		}
		envelope, err := p2p.NewEnvelope(porygonWitnessRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, target.Height, r.currentPBFTView(), target.Height, target)
		if err != nil {
			return PorygonWitnessCertificate{}, err
		}
		if err := r.sendToNode(ctx, nodeID, envelope); err != nil {
			return PorygonWitnessCertificate{}, err
		}
	}

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		state.mu.Lock()
		votesMap := state.votes[target.WitnessID]
		votes := make([]PorygonWitnessVote, 0, len(votesMap))
		for _, vote := range votesMap {
			votes = append(votes, vote)
		}
		state.mu.Unlock()
		if len(votes) >= threshold {
			sort.Slice(votes, func(i, j int) bool { return votes[i].NodeID < votes[j].NodeID })
			cert := PorygonWitnessCertificate{
				WitnessID: target.WitnessID, Height: target.Height, OrderingDomain: target.OrderingDomain,
				TransactionRoot: target.TransactionRoot, AccessRoot: target.AccessRoot, FullBodyDigest: target.FullBodyDigest,
				CommitteeDigest: target.CommitteeDigest, CommitteeMembers: append([]string(nil), members...), Threshold: threshold, Votes: votes,
			}
			cert.CertificateDigest = porygonWitnessCertificateDigest(cert)
			r.addPorygonRuntimeMetric("porygon_witness_certificate_count", 1)
			r.addPorygonRuntimeMetric("porygon_witness_vote_count", int64(len(votes)))
			return cert, nil
		}
		select {
		case <-ctx.Done():
			return PorygonWitnessCertificate{}, ctx.Err()
		case <-timeout.C:
			return PorygonWitnessCertificate{}, fmt.Errorf("porygon witness threshold timeout: got=%d want=%d", len(votes), threshold)
		case <-ticker.C:
		}
	}
}

func (r *NodeRuntime) verifyPorygonWitnessCertificate(cert PorygonWitnessCertificate, items []tx.SignedTransaction) error {
	target := porygonWitnessTarget(cert.Height, cert.OrderingDomain, items)
	members := porygonWitnessCommittee(r.plan.NodeConfigs, cert.Height, cert.OrderingDomain, cert.FullBodyDigest)
	expectedThreshold := porygonConfiguredWitnessThreshold(porygonBlockProducerConfig(r.plugins.BlockProducer), len(members))
	if cert.WitnessID != target.WitnessID || cert.TransactionRoot != target.TransactionRoot || cert.AccessRoot != target.AccessRoot || cert.FullBodyDigest != target.FullBodyDigest {
		return fmt.Errorf("porygon witness certificate transaction block mismatch")
	}
	if cert.CommitteeDigest != porygonWitnessCommitteeDigest(members) || cert.Threshold != expectedThreshold || len(cert.Votes) < expectedThreshold {
		return fmt.Errorf("porygon witness certificate threshold/committee mismatch")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Votes {
		if seen[vote.NodeID] {
			return fmt.Errorf("porygon witness certificate duplicate voter %s", vote.NodeID)
		}
		seen[vote.NodeID] = true
		if vote.WitnessID != cert.WitnessID || vote.Height != cert.Height || vote.TransactionRoot != cert.TransactionRoot || vote.AccessRoot != cert.AccessRoot || vote.FullBodyDigest != cert.FullBodyDigest || vote.CommitteeDigest != cert.CommitteeDigest {
			return fmt.Errorf("porygon witness vote/certificate identity mismatch")
		}
		if err := r.porygonVerifyWitnessVote(vote, members); err != nil {
			return err
		}
	}
	if cert.CertificateDigest == "" || cert.CertificateDigest != porygonWitnessCertificateDigest(cert) {
		return fmt.Errorf("porygon witness certificate digest mismatch")
	}
	return nil
}

func (r *NodeRuntime) verifyPorygonWitnessCertificateRef(cert PorygonWitnessCertificate, ref PorygonTransactionBlockRef) error {
	members := porygonWitnessCommittee(r.plan.NodeConfigs, cert.Height, cert.OrderingDomain, cert.FullBodyDigest)
	expectedThreshold := porygonConfiguredWitnessThreshold(porygonBlockProducerConfig(r.plugins.BlockProducer), len(members))
	if cert.Height != ref.Height || cert.OrderingDomain != ref.OrderingDomain || cert.TransactionRoot != ref.TransactionRoot || cert.AccessRoot != ref.AccessRoot || cert.FullBodyDigest != ref.FullBodyDigest {
		return fmt.Errorf("porygon witness certificate/ref mismatch")
	}
	if cert.CertificateDigest != ref.WitnessCertificateDigest || cert.CommitteeDigest != porygonWitnessCommitteeDigest(members) || cert.Threshold != expectedThreshold || len(cert.Votes) < expectedThreshold {
		return fmt.Errorf("porygon witness certificate/ref threshold mismatch")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Votes {
		if seen[vote.NodeID] {
			return fmt.Errorf("porygon witness certificate duplicate voter %s", vote.NodeID)
		}
		seen[vote.NodeID] = true
		if vote.WitnessID != cert.WitnessID || vote.Height != cert.Height || vote.TransactionRoot != cert.TransactionRoot || vote.AccessRoot != cert.AccessRoot || vote.FullBodyDigest != cert.FullBodyDigest || vote.CommitteeDigest != cert.CommitteeDigest {
			return fmt.Errorf("porygon witness vote/ref identity mismatch")
		}
		if err := r.porygonVerifyWitnessVote(vote, members); err != nil {
			return err
		}
	}
	if cert.CertificateDigest == "" || cert.CertificateDigest != porygonWitnessCertificateDigest(cert) {
		return fmt.Errorf("porygon witness certificate digest mismatch")
	}
	return nil
}

func porygonStorePrewitnessedBatch(pool *mempool.Mempool, batch porygonPrewitnessedBatch) {
	if pool == nil || len(batch.Items) == 0 {
		return
	}
	porygonPrewitnessedBatches.Store(pool, batch)
}

func porygonTakePrewitnessedBatch(pool *mempool.Mempool, targetHeight uint64, leaderID string) (porygonPrewitnessedBatch, bool) {
	if pool == nil {
		return porygonPrewitnessedBatch{}, false
	}
	value, ok := porygonPrewitnessedBatches.LoadAndDelete(pool)
	if !ok {
		return porygonPrewitnessedBatch{}, false
	}
	batch := value.(porygonPrewitnessedBatch)
	if batch.TargetHeight != targetHeight || batch.LeaderID == "" || batch.LeaderID != leaderID || batch.Certificate.Height != targetHeight {
		pool.ReleaseReserved(batch.Items)
		return porygonPrewitnessedBatch{}, false
	}
	return batch, true
}

func (r *NodeRuntime) startPorygonCrossBatchWitness(ctx context.Context, currentHeight uint64) {
	if r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID || !r.isCurrentLeader() || r.pool == nil {
		return
	}
	if _, loaded := porygonPrewitnessInFlight.LoadOrStore(r.pool, true); loaded {
		return
	}
	go func() {
		defer porygonPrewitnessInFlight.Delete(r.pool)
		started := time.Now()
		items := r.pool.ReserveReady(r.blockSize())
		if len(items) == 0 {
			return
		}
		// The next-batch witness has now started while currentHeight is already
		// inside PBFT. This is the concrete wall-clock-overlap evidence consumed
		// by the executor metric; merely enabling the pipeline is not enough.
		porygonCrossBatchWitnessOverlapHeight.Store(r, currentHeight)
		target := porygonWitnessTarget(currentHeight+1, r.node.ShardID, items)
		cert, err := r.collectPorygonWitnessCertificate(ctx, target)
		if err != nil {
			r.pool.ReleaseReserved(items)
			r.addPorygonRuntimeMetric("porygon_cross_batch_witness_failure_count", 1)
			return
		}
		// A witness result belongs only to the exact next height and the leader that
		// initiated it. If PBFT changed view/leader while Witness was in flight, do
		// not strand the reservation in a stale cache.
		if !r.isCurrentLeader() || r.porygonConsensusNextHeight() != target.Height {
			r.pool.ReleaseReserved(items)
			r.addPorygonRuntimeMetric("porygon_cross_batch_witness_stale_release_count", 1)
			return
		}
		porygonStorePrewitnessedBatch(r.pool, porygonPrewitnessedBatch{TargetHeight: target.Height, LeaderID: r.node.NodeID, Items: items, Certificate: cert})
		r.addPorygonRuntimeMetric("porygon_cross_batch_witness_completed_count", 1)
		r.addPorygonRuntimeMetric("porygon_cross_batch_witness_overlap_us", time.Since(started).Microseconds())
	}()
}

var porygonPrewitnessInFlight sync.Map // map[*mempool.Mempool]bool

// porygonCrossBatchWitnessOverlapHeight records the current PBFT height during
// which this runtime actually began witnessing the next batch. It is local
// observability state only; it never enters consensus, routing, or execution
// decisions.
var porygonCrossBatchWitnessOverlapHeight sync.Map // map[*NodeRuntime]uint64

func (r *NodeRuntime) porygonCrossBatchWitnessOverlapObserved(height uint64) bool {
	value, ok := porygonCrossBatchWitnessOverlapHeight.Load(r)
	if !ok {
		return false
	}
	observed, ok := value.(uint64)
	return ok && observed == height
}

func porygonPrewitnessRunning(pool *mempool.Mempool) bool {
	if pool == nil {
		return false
	}
	_, ok := porygonPrewitnessInFlight.Load(pool)
	return ok
}

func (r *NodeRuntime) ensurePorygonWitnessedBlock(ctx context.Context, block realblock.Block) (realblock.Block, error) {
	if r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return block, nil
	}
	if len(block.TxList) == 0 {
		return r.bindPorygonCrossRoundEvidence(block)
	}
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return block, err
	}
	if evidence.WitnessCertificate != nil && evidence.WitnessPolicy == "ec_witness_certificate_v1" {
		if err := r.verifyPorygonWitnessCertificate(*evidence.WitnessCertificate, block.TxList); err == nil {
			changed := false
			if prior, ok := r.porygonLatestGlobalRootEvidence(); ok && prior.Height+1 == block.Height && !prior.RolledBack {
				if evidence.PreviousGlobalStateRoot != prior.GlobalStateRoot || evidence.PreviousMultiShardCertificateDigest != prior.CertificateDigest {
					evidence.PreviousGlobalStateRoot = prior.GlobalStateRoot
					evidence.PreviousMultiShardCertificateDigest = prior.CertificateDigest
					changed = true
				}
			}
			if changed {
				if err := attachProposalEvidence(&block, porygonProposalEvidenceID, evidence); err != nil {
					return block, err
				}
				realblock.AssignHash(&block)
			}
			return r.bindPorygonCrossRoundEvidence(block)
		}
	}
	// Paper data availability boundary: the co-located Storage Role creates and
	// persists the full Transaction Block before EC Witness. PBFT later carries
	// only its compact reference and witness certificate.
	if err := r.porygonStoreTransactionBlockForRole(porygonBuildTransactionBlock(block.Height, block.ShardID, block.TxList, "")); err != nil {
		return block, err
	}
	target := porygonWitnessTarget(block.Height, block.ShardID, block.TxList)
	cert, err := r.collectPorygonWitnessCertificate(ctx, target)
	if err != nil {
		return block, err
	}
	evidence.WitnessPolicy = "ec_witness_certificate_v1"
	evidence.WitnessThreshold = cert.Threshold
	evidence.DataAvailabilityRef = cert.CertificateDigest
	evidence.WitnessCertificate = &cert
	if prior, ok := r.porygonLatestGlobalRootEvidence(); ok && prior.Height+1 == block.Height && !prior.RolledBack {
		evidence.PreviousGlobalStateRoot = prior.GlobalStateRoot
		evidence.PreviousMultiShardCertificateDigest = prior.CertificateDigest
	}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, evidence); err != nil {
		return block, err
	}
	realblock.AssignHash(&block)
	r.addPorygonRuntimeMetric("porygon_witness_threshold_enforced_count", 1)
	return r.bindPorygonCrossRoundEvidence(block)
}

func (r *NodeRuntime) verifyPorygonWitnessEvidence(block realblock.Block) error {
	if r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return nil
	}
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return err
	}
	if porygonCompactProposalTransactions(block) && evidence.CompactProposal != nil && evidence.CompactProposal.Maintenance {
		if len(evidence.CompactProposal.L) != 0 || evidence.WitnessPolicy != "paper_maintenance_no_witness_v1" || evidence.WitnessCertificate != nil {
			return fmt.Errorf("porygon maintenance proposal witness boundary invalid")
		}
		if err := r.verifyPorygonCrossRoundEvidence(block); err != nil {
			return err
		}
		r.addPorygonRuntimeMetric("porygon_maintenance_proposal_verified_count", 1)
		return nil
	}
	if evidence.WitnessPolicy != "ec_witness_certificate_v1" || evidence.WitnessCertificate == nil {
		return fmt.Errorf("porygon ordering requires an EC witness certificate")
	}
	if porygonCompactProposalTransactions(block) {
		if evidence.CompactProposal == nil || len(evidence.CompactProposal.L) != 1 {
			return fmt.Errorf("porygon compact proposal L missing")
		}
		if err := r.verifyPorygonWitnessCertificateRef(*evidence.WitnessCertificate, evidence.CompactProposal.L[0]); err != nil {
			return err
		}
	} else {
		if len(block.TxList) == 0 {
			return fmt.Errorf("porygon non-compact proposal has no transaction body")
		}
		if err := r.verifyPorygonWitnessCertificate(*evidence.WitnessCertificate, block.TxList); err != nil {
			return err
		}
	}
	if err := r.verifyPorygonCrossRoundEvidence(block); err != nil {
		return err
	}
	r.addPorygonRuntimeMetric("porygon_witness_certificate_verified_count", 1)
	return nil
}
