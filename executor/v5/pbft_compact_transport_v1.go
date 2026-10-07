package v5

import (
	"context"
	"fmt"
	"strings"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_SHARED_PBFT_COMPACT_V1
//
// The normal V5 transaction lifecycle gossips the complete signed transaction
// to every validator before PBFT. Re-sending the same immutable signed body in
// PRE-PREPARE is redundant. Compact transport keeps the exact PBFT digest,
// signatures, quorum rules and committed block semantics, while sending the
// proposal's ordered TxIDs/TxRoot plus all block-level metadata. A validator
// reconstructs TxList from its own mempool. If gossip missed a body, only the
// missing signed bodies are fetched from the proposal leader before PREPARE.
const (
	pbftCompactTxRequestMessage  = "PBFT_TX_BODY_REQUEST"
	pbftCompactTxResponseMessage = "PBFT_TX_BODY_RESPONSE"
)

type pbftCompactTxRequest struct {
	BlockHash       string   `json:"block_hash"`
	Height          uint64   `json:"height"`
	View            uint64   `json:"view"`
	Sequence        uint64   `json:"sequence"`
	RequesterNodeID string   `json:"requester_node_id"`
	TxIDs           []string `json:"tx_ids"`
}

type pbftCompactTxResponse struct {
	BlockHash    string                 `json:"block_hash"`
	Height       uint64                 `json:"height"`
	View         uint64                 `json:"view"`
	Sequence     uint64                 `json:"sequence"`
	LeaderNodeID string                 `json:"leader_node_id"`
	Transactions []tx.SignedTransaction `json:"transactions"`
}

type pbftCompactPendingProposal struct {
	FromNode string
	Pre      pbft.PrePrepare
	Bodies   map[string]tx.SignedTransaction
}

type pbftCompactRuntimeState struct {
	mu      sync.Mutex
	pending map[string]*pbftCompactPendingProposal
}

var pbftCompactRuntimeStates sync.Map // map[*NodeRuntime]*pbftCompactRuntimeState

func pbftCompactStateFor(r *NodeRuntime) *pbftCompactRuntimeState {
	if r == nil {
		return &pbftCompactRuntimeState{pending: map[string]*pbftCompactPendingProposal{}}
	}
	if value, ok := pbftCompactRuntimeStates.Load(r); ok {
		return value.(*pbftCompactRuntimeState)
	}
	created := &pbftCompactRuntimeState{pending: map[string]*pbftCompactPendingProposal{}}
	actual, _ := pbftCompactRuntimeStates.LoadOrStore(r, created)
	return actual.(*pbftCompactRuntimeState)
}

func cleanupPBFTCompactRuntime(r *NodeRuntime) {
	if r != nil {
		pbftCompactRuntimeStates.Delete(r)
	}
}

func (r *NodeRuntime) addPBFTCompactMetric(name string, delta int64) {
	if r == nil || name == "" || delta == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts[name] += delta
}

func (r *NodeRuntime) compactPBFTPrePrepareForWire(pre pbft.PrePrepare) pbft.PrePrepare {
	if len(pre.Block.TxList) == 0 || len(pre.Block.TxIDs) == 0 {
		return pre
	}
	wire := pre
	wire.Block = pre.Block
	wire.Block.TxIDs = append([]string(nil), pre.Block.TxIDs...)
	wire.Block.TxList = nil
	r.addPBFTCompactMetric("pbft_compact_preprepare_broadcast_count", 1)
	r.addPBFTCompactMetric("pbft_compact_tx_body_elided_count", int64(len(pre.Block.TxList)))
	return wire
}

func validatePBFTCompactBlockIdentity(pre pbft.PrePrepare) error {
	if strings.TrimSpace(pre.BlockHash) == "" || pre.Block.BlockHash != pre.BlockHash {
		return fmt.Errorf("compact proposal block hash identity mismatch")
	}
	if realblock.Hash(pre.Block) != pre.BlockHash {
		return fmt.Errorf("compact proposal block digest mismatch")
	}
	if realblock.TxRoot(pre.Block.TxIDs) != pre.Block.TxRoot {
		return fmt.Errorf("compact proposal transaction root mismatch")
	}
	seen := make(map[string]bool, len(pre.Block.TxIDs))
	for _, id := range pre.Block.TxIDs {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return fmt.Errorf("compact proposal contains blank/duplicate transaction id")
		}
		seen[id] = true
	}
	return nil
}

func (r *NodeRuntime) hydratePBFTCompactPrePrepare(ctx context.Context, fromNode string, pre pbft.PrePrepare) (pbft.PrePrepare, bool, error) {
	if len(pre.Block.TxIDs) == 0 || len(pre.Block.TxList) > 0 {
		return pre, false, nil
	}
	if err := validatePBFTCompactBlockIdentity(pre); err != nil {
		return pbft.PrePrepare{}, false, err
	}
	if fromNode == "" || fromNode != pre.LeaderID {
		return pbft.PrePrepare{}, false, fmt.Errorf("compact proposal source/leader mismatch")
	}

	bodies := map[string]tx.SignedTransaction{}
	if r.pool != nil {
		for id, item := range r.pool.LookupMany(pre.Block.TxIDs) {
			bodies[id] = item
		}
	}
	state := pbftCompactStateFor(r)
	state.mu.Lock()
	if existing := state.pending[pre.BlockHash]; existing != nil {
		for id, item := range existing.Bodies {
			bodies[id] = item
		}
	}
	missing := make([]string, 0)
	for _, id := range pre.Block.TxIDs {
		if _, ok := bodies[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		delete(state.pending, pre.BlockHash)
		state.mu.Unlock()
		full := pre
		full.Block.TxList = make([]tx.SignedTransaction, 0, len(pre.Block.TxIDs))
		for _, id := range pre.Block.TxIDs {
			full.Block.TxList = append(full.Block.TxList, bodies[id])
		}
		r.addPBFTCompactMetric("pbft_compact_preprepare_reconstructed_count", 1)
		return full, false, nil
	}
	state.pending[pre.BlockHash] = &pbftCompactPendingProposal{FromNode: fromNode, Pre: pre, Bodies: bodies}
	state.mu.Unlock()

	request := pbftCompactTxRequest{
		BlockHash: pre.BlockHash, Height: pre.Height, View: pre.View, Sequence: pre.Sequence,
		RequesterNodeID: r.node.NodeID, TxIDs: append([]string(nil), missing...),
	}
	envelope, err := p2p.NewEnvelope(pbftCompactTxRequestMessage, r.node.NodeID, fromNode, r.node.ShardID, pre.Height, pre.View, pre.Sequence, request)
	if err != nil {
		return pbft.PrePrepare{}, false, err
	}
	if err := r.sendToNode(ctx, fromNode, envelope); err != nil {
		return pbft.PrePrepare{}, false, err
	}
	r.addPBFTCompactMetric("pbft_compact_tx_body_request_count", 1)
	r.addPBFTCompactMetric("pbft_compact_missing_tx_requested_count", int64(len(missing)))
	return pbft.PrePrepare{}, true, nil
}

func (r *NodeRuntime) handlePBFTCompactTxRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[pbftCompactTxRequest](msg)
	if err != nil {
		return err
	}
	if msg.FromNode == "" || msg.FromNode != request.RequesterNodeID || !r.pbftState().IsValidator(msg.FromNode) {
		return fmt.Errorf("compact body request requester is not an authenticated shard validator")
	}
	if msg.Height != request.Height || msg.View != request.View || msg.Sequence != request.Sequence || request.Sequence != request.Height {
		return fmt.Errorf("compact body request slot mismatch")
	}
	if r.pbftState().Leader() != r.node.NodeID {
		return fmt.Errorf("compact body request reached non-primary")
	}

	r.mu.Lock()
	proposal, ok := r.proposals[request.BlockHash]
	r.mu.Unlock()
	if !ok || proposal.BlockHash == "" || proposal.BlockHash != request.BlockHash || proposal.Height != request.Height {
		return fmt.Errorf("compact body request references unknown proposal")
	}
	byID := make(map[string]tx.SignedTransaction, len(proposal.TxList))
	for _, item := range proposal.TxList {
		byID[item.TxID] = item
	}
	response := pbftCompactTxResponse{
		BlockHash: request.BlockHash, Height: request.Height, View: request.View, Sequence: request.Sequence,
		LeaderNodeID: r.node.NodeID, Transactions: make([]tx.SignedTransaction, 0, len(request.TxIDs)),
	}
	seen := map[string]bool{}
	for _, id := range request.TxIDs {
		if id == "" || seen[id] {
			return fmt.Errorf("compact body request contains blank/duplicate transaction id")
		}
		seen[id] = true
		item, exists := byID[id]
		if !exists {
			return fmt.Errorf("compact body request transaction %s is not in proposal", id)
		}
		response.Transactions = append(response.Transactions, item)
	}
	envelope, err := p2p.NewEnvelope(pbftCompactTxResponseMessage, r.node.NodeID, request.RequesterNodeID, r.node.ShardID, request.Height, request.View, request.Sequence, response)
	if err != nil {
		return err
	}
	if err := r.sendToNode(ctx, request.RequesterNodeID, envelope); err != nil {
		return err
	}
	r.addPBFTCompactMetric("pbft_compact_tx_body_response_count", 1)
	r.addPBFTCompactMetric("pbft_compact_missing_tx_served_count", int64(len(response.Transactions)))
	return nil
}

func (r *NodeRuntime) handlePBFTCompactTxResponse(ctx context.Context, msg p2p.MessageEnvelope) error {
	response, err := p2p.DecodePayload[pbftCompactTxResponse](msg)
	if err != nil {
		return err
	}
	state := pbftCompactStateFor(r)
	state.mu.Lock()
	pending := state.pending[response.BlockHash]
	if pending == nil {
		state.mu.Unlock()
		return fmt.Errorf("compact body response has no pending proposal")
	}
	pre := pending.Pre
	if msg.FromNode != pre.LeaderID || response.LeaderNodeID != pre.LeaderID || msg.FromNode != response.LeaderNodeID {
		state.mu.Unlock()
		return fmt.Errorf("compact body response source/leader mismatch")
	}
	if msg.Height != pre.Height || msg.View != pre.View || msg.Sequence != pre.Sequence || response.Height != pre.Height || response.View != pre.View || response.Sequence != pre.Sequence {
		state.mu.Unlock()
		return fmt.Errorf("compact body response slot mismatch")
	}
	allowed := make(map[string]bool, len(pre.Block.TxIDs))
	for _, id := range pre.Block.TxIDs {
		allowed[id] = true
	}
	for _, item := range response.Transactions {
		if !allowed[item.TxID] {
			state.mu.Unlock()
			return fmt.Errorf("compact body response contains transaction outside proposal")
		}
		if _, exists := pending.Bodies[item.TxID]; exists {
			continue
		}
		if err := tx.Verify(item); err != nil {
			state.mu.Unlock()
			return fmt.Errorf("compact body response transaction %s failed verification: %w", item.TxID, err)
		}
		pending.Bodies[item.TxID] = item
	}
	state.mu.Unlock()

	hydrated, waiting, err := r.hydratePBFTCompactPrePrepare(ctx, pending.FromNode, pre)
	if err != nil {
		return err
	}
	if waiting {
		return nil
	}
	r.addPBFTCompactMetric("pbft_compact_preprepare_repair_completed_count", 1)
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, pending.FromNode, r.node.NodeID, r.node.ShardID, hydrated.Height, hydrated.View, hydrated.Sequence, hydrated)
	if err != nil {
		return err
	}
	return r.handlePBFTPrePrepare(ctx, envelope)
}
