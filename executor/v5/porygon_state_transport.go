package v5

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

const porygonStateProjectionAccessKindPrefix = "porygon_state_projection:"

func porygonStateProjectionAccessKind(access tx.AccessItem) string {
	return porygonStateProjectionAccessKindForProposal(access, 0, "")
}

func porygonStateProjectionAccessKindForProposal(access tx.AccessItem, stateHeight uint64, stateRoot string) string {
	base := porygonStateProjectionAccessKindPrefix + string(access.Mode)
	// Height zero is the legitimate genesis/baseline Proposal.T anchor.  Root
	// presence, not height>0, distinguishes an anchored Paper2 projection from
	// the legacy unanchored helper.
	if strings.TrimSpace(stateRoot) == "" {
		return base
	}
	return base + "|state_height=" + strconv.FormatUint(stateHeight, 10) + "|state_root=" + stateRoot
}

// Legacy helper retained only so an old installed shared runtime remains
// merge-compatible while Paper2 removes speculative epoch forwarding.
func porygonStateProjectionAccessKindAtEpoch(access tx.AccessItem, epoch uint64) string {
	return porygonStateProjectionAccessKind(access)
}
func porygonStateProjectionEpoch(kind string) (uint64, bool) { return 0, false }

func porygonStateProjectionAnchor(kind string) (uint64, string, bool) {
	if !isPorygonStateProjectionAccessKind(kind) {
		return 0, "", false
	}
	var height uint64
	root := ""
	heightSeen := false
	rootSeen := false
	for _, part := range strings.Split(kind, "|") {
		if strings.HasPrefix(part, "state_height=") {
			value, err := strconv.ParseUint(strings.TrimPrefix(part, "state_height="), 10, 64)
			if err == nil {
				height = value
				heightSeen = true
			}
		}
		if strings.HasPrefix(part, "state_root=") {
			root = strings.TrimSpace(strings.TrimPrefix(part, "state_root="))
			rootSeen = true
		}
	}
	return height, root, heightSeen && rootSeen && root != ""
}

func isPorygonStateProjectionAccessKind(kind string) bool {
	return strings.HasPrefix(kind, porygonStateProjectionAccessKindPrefix)
}

func porygonStateProjectionWitnessDigest(response StateFetchResponse, accessKind string) string {
	proofDigest := ""
	if response.PorygonProof != nil {
		proofDigest = response.PorygonProof.ProofDigest
	}
	return stableTextDigest(strings.Join([]string{response.BlockHash, response.QualifiedKey, response.Value, response.StateRoot, response.HomeShard, response.ExecutionShard, accessKind, proofDigest}, "|"))
}

func (r *NodeRuntime) porygonFetchRemoteState(ctx context.Context, block realblock.Block, item tx.SignedTransaction, access tx.AccessItem, homeShard string) (response StateFetchResponse, latency time.Duration, fetchErr error) {
	targetNode := r.stateAccessLeaderID(homeShard)
	if targetNode == "" {
		return StateFetchResponse{}, 0, fmt.Errorf("porygon remote Storage Role leader missing for %s", homeShard)
	}
	requestID := stableTextDigest(strings.Join([]string{"porygon-state-proof", r.node.NodeID, item.TxID, block.BlockHash, access.Key, homeShard, r.stateAccessPartitionID()}, "|"))
	started := time.Now()
	outcome := "response_received"
	defer func() { r.finishStateFetch(requestID, outcome, started, fetchErr) }()
	proposal, err := porygonProposalFromBlock(block)
	if err != nil {
		return StateFetchResponse{}, time.Since(started), err
	}
	partitionRoot := proposal.TPartitionRoots[homeShard]
	if partitionRoot == "" {
		return StateFetchResponse{}, time.Since(started), fmt.Errorf("porygon Proposal T missing home partition root %s", homeShard)
	}
	accessKind := porygonStateProjectionAccessKindForProposal(access, proposal.TStateHeight, partitionRoot)
	r.beginStateFetch(block, item, access, homeShard, requestID)
	waiter := make(chan StateFetchResponse, 1)
	r.mu.Lock()
	if r.stateFetchWaiters == nil {
		r.stateFetchWaiters = map[string]chan StateFetchResponse{}
	}
	r.stateFetchWaiters[requestID] = waiter
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.stateFetchWaiters, requestID); r.mu.Unlock() }()
	request := StateFetchRequest{RequestID: requestID, TxID: item.TxID, BlockHash: block.BlockHash, Key: access.Key, HomeShard: homeShard, ExecutionShard: r.stateAccessPartitionID(), AccessKind: accessKind}
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, r.currentPBFTView(), block.Height, request)
	if err != nil {
		outcome = "envelope_error"
		fetchErr = err
		return StateFetchResponse{}, time.Since(started), err
	}
	if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
		outcome = "send_error"
		fetchErr = err
		return StateFetchResponse{}, time.Since(started), err
	}
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case response = <-waiter:
		latency = time.Since(started)
		if !response.Success {
			outcome = "remote_error"
			fetchErr = fmt.Errorf("porygon remote state projection failed: %s", response.Error)
			return response, latency, fetchErr
		}
		if response.StateRoot != partitionRoot {
			outcome = "state_root_anchor_error"
			fetchErr = fmt.Errorf("porygon state proof root does not match Proposal T")
			return response, latency, fetchErr
		}
		if response.PorygonProof == nil || !porygonVerifyStateProof(*response.PorygonProof, response.QualifiedKey, response.Value, response.StateRoot) {
			outcome = "proof_verification_error"
			fetchErr = fmt.Errorf("porygon state proof verification failed for %s", access.Key)
			return response, latency, fetchErr
		}
		if response.WitnessDigest != porygonStateProjectionWitnessDigest(response, accessKind) {
			outcome = "witness_digest_error"
			fetchErr = fmt.Errorf("porygon state projection witness digest mismatch for %s", access.Key)
			return response, latency, fetchErr
		}
		r.addRuntimeMetric("porygon_state_proof_verified_count", 1)
		return response, latency, nil
	case <-timer.C:
		outcome = "timeout"
		fetchErr = fmt.Errorf("porygon remote state projection timeout for %s", access.Key)
		return StateFetchResponse{}, time.Since(started), fetchErr
	case <-ctx.Done():
		outcome = "context_done"
		fetchErr = ctx.Err()
		return StateFetchResponse{}, time.Since(started), fetchErr
	}
}
