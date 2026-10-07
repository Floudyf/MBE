package v5

import (
	"sync"

	"metaverse-chainlab/executor/realism/tx"
)

type porygonProposalReservation struct {
	Height uint64
	Items  []tx.SignedTransaction
}

type porygonProposalReservationState struct {
	mu     sync.Mutex
	byHash map[string]porygonProposalReservation
}

var porygonProposalReservationStates sync.Map // map[*NodeRuntime]*porygonProposalReservationState

func (r *NodeRuntime) porygonProposalReservationState() *porygonProposalReservationState {
	if value, ok := porygonProposalReservationStates.Load(r); ok {
		return value.(*porygonProposalReservationState)
	}
	created := &porygonProposalReservationState{byHash: map[string]porygonProposalReservation{}}
	actual, _ := porygonProposalReservationStates.LoadOrStore(r, created)
	return actual.(*porygonProposalReservationState)
}

func porygonCloneTransactions(items []tx.SignedTransaction) []tx.SignedTransaction {
	if len(items) == 0 {
		return nil
	}
	return append([]tx.SignedTransaction(nil), items...)
}

// Compact Proposal.L intentionally removes TxList from the PBFT object. Keep
// local mempool reservation ownership in a separate ledger keyed by proposal
// hash so view-change and durable commit never depend on the compact body.
func (r *NodeRuntime) porygonRememberProposalReservation(blockHash string, height uint64, items []tx.SignedTransaction) {
	if r == nil || r.pool == nil || blockHash == "" || len(items) == 0 {
		return
	}
	state := r.porygonProposalReservationState()
	state.mu.Lock()
	state.byHash[blockHash] = porygonProposalReservation{Height: height, Items: porygonCloneTransactions(items)}
	state.mu.Unlock()
	r.addPorygonRuntimeMetric("porygon_proposal_reservation_record_count", 1)
}

func (r *NodeRuntime) porygonTakeProposalReservation(blockHash string) ([]tx.SignedTransaction, bool) {
	if r == nil || blockHash == "" {
		return nil, false
	}
	state := r.porygonProposalReservationState()
	state.mu.Lock()
	entry, ok := state.byHash[blockHash]
	if ok {
		delete(state.byHash, blockHash)
	}
	state.mu.Unlock()
	if !ok {
		return nil, false
	}
	return porygonCloneTransactions(entry.Items), true
}

func (r *NodeRuntime) porygonReleaseProposalReservation(blockHash string, fallback []tx.SignedTransaction) {
	if r == nil || r.pool == nil {
		return
	}
	items, ok := r.porygonTakeProposalReservation(blockHash)
	if !ok {
		items = porygonCloneTransactions(fallback)
	}
	if len(items) == 0 {
		return
	}
	r.pool.ReleaseReserved(items)
	r.addPorygonRuntimeMetric("porygon_proposal_reservation_release_count", 1)
}

func (r *NodeRuntime) porygonCommitProposalReservation(blockHash string, fallback []tx.SignedTransaction) {
	if r == nil || r.pool == nil {
		return
	}
	items, ok := r.porygonTakeProposalReservation(blockHash)
	if !ok {
		items = porygonCloneTransactions(fallback)
	}
	if len(items) == 0 {
		return
	}
	r.pool.CommitReserved(items)
	r.addPorygonRuntimeMetric("porygon_proposal_reservation_commit_count", 1)
}

func (r *NodeRuntime) porygonProposalReservationCount() int {
	if r == nil {
		return 0
	}
	state := r.porygonProposalReservationState()
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.byHash)
}

// porygonReleaseStalePrewitnessedBatch releases locally reserved next-batch
// work whenever PBFT view/leader/height no longer matches the certificate that
// created it. Prewitness state is a local overlap optimization, never consensus.
func (r *NodeRuntime) porygonReleaseStalePrewitnessedBatch() {
	if r == nil || r.pool == nil {
		return
	}
	value, ok := porygonPrewitnessedBatches.Load(r.pool)
	if !ok {
		return
	}
	batch := value.(porygonPrewitnessedBatch)
	if r.isCurrentLeader() && batch.LeaderID == r.node.NodeID && batch.TargetHeight == r.porygonConsensusNextHeight() {
		return
	}
	value, ok = porygonPrewitnessedBatches.LoadAndDelete(r.pool)
	if !ok {
		return
	}
	batch = value.(porygonPrewitnessedBatch)
	if len(batch.Items) > 0 {
		r.pool.ReleaseReserved(batch.Items)
		r.addPorygonRuntimeMetric("porygon_cross_batch_witness_stale_release_count", 1)
	}
}
