package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	porygonTxBlockFetchRequestMessage  = "PORYGON_TXBLOCK_FETCH_REQUEST_V1"
	porygonTxBlockFetchResponseMessage = "PORYGON_TXBLOCK_FETCH_RESPONSE_V1"
)

// PorygonTransactionBlock is the paper data-availability object.  Full signed
// transactions live here, outside the compact PBFT proposal body.
type PorygonTransactionBlock struct {
	Version                  string                 `json:"version"`
	TransactionBlockID       string                 `json:"transaction_block_id"`
	Height                   uint64                 `json:"height"`
	OrderingDomain           string                 `json:"ordering_domain"`
	Transactions             []tx.SignedTransaction `json:"transactions"`
	TransactionIDs           []string               `json:"transaction_ids"`
	TransactionRoot          string                 `json:"transaction_root"`
	AccessRoot               string                 `json:"access_root"`
	FullBodyDigest           string                 `json:"full_body_digest"`
	WitnessCertificateDigest string                 `json:"witness_certificate_digest,omitempty"`
}

type PorygonTransactionBlockRef struct {
	TransactionBlockID       string   `json:"transaction_block_id"`
	Height                   uint64   `json:"height"`
	OrderingDomain           string   `json:"ordering_domain"`
	TransactionCount         int      `json:"transaction_count"`
	TransactionRoot          string   `json:"transaction_root"`
	AccessRoot               string   `json:"access_root"`
	FullBodyDigest           string   `json:"full_body_digest"`
	WitnessCertificateDigest string   `json:"witness_certificate_digest"`
	StorageNodeIDs           []string `json:"storage_node_ids,omitempty"`
}

type PorygonTxBlockFetchRequest struct {
	RequestID          string `json:"request_id"`
	TransactionBlockID string `json:"transaction_block_id"`
	FullBodyDigest     string `json:"full_body_digest"`
	Height             uint64 `json:"height"`
}

type PorygonTxBlockFetchResponse struct {
	RequestID string                  `json:"request_id"`
	Block     PorygonTransactionBlock `json:"block"`
	Success   bool                    `json:"success"`
	Error     string                  `json:"error,omitempty"`
}

type porygonTxBlockFetchState struct {
	mu      sync.Mutex
	waiters map[string]chan PorygonTxBlockFetchResponse
	blocks  map[string]PorygonTransactionBlock
}

var porygonTxBlockFetchStates sync.Map // map[*NodeRuntime]*porygonTxBlockFetchState

func porygonBuildTransactionBlock(height uint64, orderingDomain string, items []tx.SignedTransaction, witnessDigest string) PorygonTransactionBlock {
	txIDs := transactionIDs(items)
	body := PorygonTransactionBlock{
		Version: "porygon_transaction_block_v1", Height: height, OrderingDomain: orderingDomain,
		Transactions: append([]tx.SignedTransaction(nil), items...), TransactionIDs: append([]string(nil), txIDs...),
		TransactionRoot: stableJSONDigest(txIDs), AccessRoot: porygonAccessRoot(items), FullBodyDigest: stableJSONDigest(items),
		WitnessCertificateDigest: witnessDigest,
	}
	body.TransactionBlockID = stableTextDigest(fmt.Sprintf("%d|%s|%s|%s|%s", body.Height, body.OrderingDomain, body.TransactionRoot, body.AccessRoot, body.FullBodyDigest))
	return body
}

func porygonTransactionBlockRef(body PorygonTransactionBlock) PorygonTransactionBlockRef {
	return PorygonTransactionBlockRef{
		TransactionBlockID: body.TransactionBlockID, Height: body.Height, OrderingDomain: body.OrderingDomain,
		TransactionCount: len(body.Transactions), TransactionRoot: body.TransactionRoot, AccessRoot: body.AccessRoot,
		FullBodyDigest: body.FullBodyDigest, WitnessCertificateDigest: body.WitnessCertificateDigest,
	}
}

func porygonValidateTransactionBlock(body PorygonTransactionBlock) error {
	if body.Version != "porygon_transaction_block_v1" || body.TransactionBlockID == "" || body.Height == 0 || body.OrderingDomain == "" {
		return fmt.Errorf("invalid Porygon transaction block identity")
	}
	expected := porygonBuildTransactionBlock(body.Height, body.OrderingDomain, body.Transactions, body.WitnessCertificateDigest)
	if body.TransactionBlockID != expected.TransactionBlockID || body.TransactionRoot != expected.TransactionRoot || body.AccessRoot != expected.AccessRoot || body.FullBodyDigest != expected.FullBodyDigest {
		return fmt.Errorf("Porygon transaction block commitment mismatch")
	}
	if len(body.TransactionIDs) != len(expected.TransactionIDs) {
		return fmt.Errorf("Porygon transaction block transaction count mismatch")
	}
	for i := range body.TransactionIDs {
		if body.TransactionIDs[i] != expected.TransactionIDs[i] {
			return fmt.Errorf("Porygon transaction block transaction order mismatch")
		}
		if err := tx.Verify(body.Transactions[i]); err != nil {
			return fmt.Errorf("Porygon transaction block signature verification failed: %w", err)
		}
	}
	return nil
}

func porygonValidateTransactionBlockRef(ref PorygonTransactionBlockRef, body PorygonTransactionBlock) error {
	if err := porygonValidateTransactionBlock(body); err != nil {
		return err
	}
	if ref.TransactionBlockID != body.TransactionBlockID || ref.Height != body.Height || ref.OrderingDomain != body.OrderingDomain || ref.TransactionCount != len(body.Transactions) || ref.TransactionRoot != body.TransactionRoot || ref.AccessRoot != body.AccessRoot || ref.FullBodyDigest != body.FullBodyDigest {
		return fmt.Errorf("Porygon transaction block reference mismatch")
	}
	if ref.WitnessCertificateDigest != "" && body.WitnessCertificateDigest != "" && ref.WitnessCertificateDigest != body.WitnessCertificateDigest {
		return fmt.Errorf("Porygon transaction block witness digest mismatch")
	}
	return nil
}

func (r *NodeRuntime) porygonTransactionBlockPath(id string) string {
	if r == nil || r.node.DataDir == "" || id == "" {
		return ""
	}
	return filepath.Join(r.node.DataDir, "porygon_transaction_blocks", id+".json")
}

func (r *NodeRuntime) porygonCacheTransactionBlock(body PorygonTransactionBlock) error {
	if err := porygonValidateTransactionBlock(body); err != nil {
		return err
	}
	state := r.porygonTxBlockFetchState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if prior, ok := state.blocks[body.TransactionBlockID]; ok && prior.FullBodyDigest != body.FullBodyDigest {
		return fmt.Errorf("conflicting Porygon transaction block %s", body.TransactionBlockID)
	}
	state.blocks[body.TransactionBlockID] = body
	return nil
}

// Only the co-located Storage Role persists full Transaction Blocks.  EC
// Witness/Execution nodes keep them in their per-runtime cache and can discard
// them after the EC lifecycle, preserving the paper's compact validator store.
func (r *NodeRuntime) porygonPersistTransactionBlock(body PorygonTransactionBlock) error {
	if err := r.porygonCacheTransactionBlock(body); err != nil {
		return err
	}
	path := r.porygonTransactionBlockPath(body.TransactionBlockID)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (r *NodeRuntime) porygonLocalTransactionBlock(id string) (PorygonTransactionBlock, bool) {
	state := r.porygonTxBlockFetchState()
	state.mu.Lock()
	if body, ok := state.blocks[id]; ok {
		state.mu.Unlock()
		return body, true
	}
	state.mu.Unlock()
	path := r.porygonTransactionBlockPath(id)
	if path == "" {
		return PorygonTransactionBlock{}, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return PorygonTransactionBlock{}, false
	}
	var body PorygonTransactionBlock
	if json.Unmarshal(raw, &body) != nil || porygonValidateTransactionBlock(body) != nil {
		return PorygonTransactionBlock{}, false
	}
	_ = r.porygonCacheTransactionBlock(body)
	return body, true
}

func (r *NodeRuntime) porygonReleaseTransactionBlockCache(refs []PorygonTransactionBlockRef) {
	state := r.porygonTxBlockFetchState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, ref := range refs {
		delete(state.blocks, ref.TransactionBlockID)
	}
}

func (r *NodeRuntime) porygonTxBlockFetchState() *porygonTxBlockFetchState {
	if value, ok := porygonTxBlockFetchStates.Load(r); ok {
		return value.(*porygonTxBlockFetchState)
	}
	created := &porygonTxBlockFetchState{waiters: map[string]chan PorygonTxBlockFetchResponse{}, blocks: map[string]PorygonTransactionBlock{}}
	actual, _ := porygonTxBlockFetchStates.LoadOrStore(r, created)
	return actual.(*porygonTxBlockFetchState)
}

func (r *NodeRuntime) handlePorygonTxBlockFetchRequest(ctx context.Context, msg p2p.MessageEnvelope) error {
	request, err := p2p.DecodePayload[PorygonTxBlockFetchRequest](msg)
	if err != nil {
		return err
	}
	response := PorygonTxBlockFetchResponse{RequestID: request.RequestID}
	body, ok := r.porygonLocalTransactionBlock(request.TransactionBlockID)
	if !ok || body.FullBodyDigest != request.FullBodyDigest {
		response.Error = "transaction_block_unavailable"
	} else {
		response.Block = body
		response.Success = true
	}
	envelope, err := p2p.NewEnvelope(porygonTxBlockFetchResponseMessage, r.node.NodeID, msg.FromNode, r.node.ShardID, request.Height, r.currentPBFTView(), request.Height, response)
	if err != nil {
		return err
	}
	return r.sendToNode(ctx, msg.FromNode, envelope)
}

func (r *NodeRuntime) handlePorygonTxBlockFetchResponse(msg p2p.MessageEnvelope) error {
	response, err := p2p.DecodePayload[PorygonTxBlockFetchResponse](msg)
	if err != nil {
		return err
	}
	if response.Success {
		if err := porygonValidateTransactionBlock(response.Block); err != nil {
			return err
		}
		if err := r.porygonCacheTransactionBlock(response.Block); err != nil {
			return err
		}
	}
	state := r.porygonTxBlockFetchState()
	state.mu.Lock()
	waiter := state.waiters[response.RequestID]
	state.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- response:
		default:
		}
	}
	return nil
}

func (r *NodeRuntime) porygonFetchTransactionBlock(ctx context.Context, ref PorygonTransactionBlockRef, candidates []string) (PorygonTransactionBlock, error) {
	if body, ok := r.porygonLocalTransactionBlock(ref.TransactionBlockID); ok {
		if err := porygonValidateTransactionBlockRef(ref, body); err == nil {
			return body, nil
		}
	}
	candidates = append(candidates, ref.StorageNodeIDs...)
	candidates = uniqueStrings(candidates)
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return PorygonTransactionBlock{}, fmt.Errorf("Porygon transaction block %s has no availability source", ref.TransactionBlockID)
	}
	state := r.porygonTxBlockFetchState()
	requestID := stableTextDigest(fmt.Sprintf("%s|%s|%d|%d", r.node.NodeID, ref.TransactionBlockID, ref.Height, time.Now().UnixNano()))
	waiter := make(chan PorygonTxBlockFetchResponse, len(candidates))
	state.mu.Lock()
	state.waiters[requestID] = waiter
	state.mu.Unlock()
	defer func() { state.mu.Lock(); delete(state.waiters, requestID); state.mu.Unlock() }()
	request := PorygonTxBlockFetchRequest{RequestID: requestID, TransactionBlockID: ref.TransactionBlockID, FullBodyDigest: ref.FullBodyDigest, Height: ref.Height}
	sent := 0
	for _, nodeID := range candidates {
		if nodeID == r.node.NodeID {
			if body, ok := r.porygonLocalTransactionBlock(ref.TransactionBlockID); ok {
				if err := porygonValidateTransactionBlockRef(ref, body); err == nil {
					return body, nil
				}
			}
			continue
		}
		envelope, err := p2p.NewEnvelope(porygonTxBlockFetchRequestMessage, r.node.NodeID, nodeID, r.node.ShardID, ref.Height, r.currentPBFTView(), ref.Height, request)
		if err != nil {
			continue
		}
		if err := r.sendToNode(ctx, nodeID, envelope); err == nil {
			sent++
		}
	}
	if sent == 0 {
		return PorygonTransactionBlock{}, fmt.Errorf("Porygon transaction block %s could not be requested", ref.TransactionBlockID)
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return PorygonTransactionBlock{}, ctx.Err()
		case <-timer.C:
			return PorygonTransactionBlock{}, fmt.Errorf("Porygon transaction block fetch timeout: %s", ref.TransactionBlockID)
		case response := <-waiter:
			if !response.Success {
				continue
			}
			if err := porygonValidateTransactionBlockRef(ref, response.Block); err != nil {
				continue
			}
			r.addPorygonRuntimeMetric("porygon_transaction_block_fetch_count", 1)
			return response.Block, nil
		}
	}
}
