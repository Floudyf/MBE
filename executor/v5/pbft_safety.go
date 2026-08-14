package v5

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/tx"
)

const PBFTIdentitySchemeEd25519V1 = "ed25519-node-identity-v1"
const PBFTPrivateKeyEnv = "MBE_PBFT_PRIVATE_KEY_B64"

// GeneratePBFTIdentityMaterial creates one independent Ed25519 identity per
// runtime node. Public keys are safe to persist in the run plan. Private keys
// are returned separately so the supervisor can inject exactly one key into
// each child process without serializing any private key into plan/artifact JSON.
func GeneratePBFTIdentityMaterial(nodes []NodePlan) (map[string]string, map[string]string, error) {
	ids := make([]string, 0, len(nodes))
	seen := map[string]bool{}
	for _, node := range nodes {
		id := strings.TrimSpace(node.NodeID)
		if id == "" {
			return nil, nil, fmt.Errorf("pbft identity generation found empty node id")
		}
		if seen[id] {
			return nil, nil, fmt.Errorf("pbft identity generation found duplicate node id %s", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	publicKeys := make(map[string]string, len(ids))
	privateKeys := make(map[string]string, len(ids))
	for _, id := range ids {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("generate pbft identity for %s: %w", id, err)
		}
		publicKeys[id] = pbft.EncodePublicKey(publicKey)
		privateKeys[id] = pbft.EncodePrivateKey(privateKey)
	}
	return publicKeys, privateKeys, nil
}

// ValidatePBFTIdentityPlan validates only public identity material. It also
// rejects key reuse across logical validators, because one private key must not
// be able to represent two distinct PBFT identities.
func ValidatePBFTIdentityPlan(plan Plan) error {
	if plan.PBFTIdentityScheme != PBFTIdentitySchemeEd25519V1 {
		return fmt.Errorf("unsupported pbft identity scheme %q", plan.PBFTIdentityScheme)
	}
	if len(plan.PBFTPublicKeys) == 0 {
		return fmt.Errorf("pbft public key registry is empty")
	}
	seenNode := map[string]bool{}
	nodeByID := map[string]NodePlan{}
	ownerByKey := map[string]string{}
	for _, node := range plan.NodeConfigs {
		nodeID := strings.TrimSpace(node.NodeID)
		if nodeID == "" {
			return fmt.Errorf("pbft plan contains empty node id")
		}
		if nodeID != node.NodeID {
			return fmt.Errorf("pbft node id %q contains surrounding whitespace", node.NodeID)
		}
		if seenNode[nodeID] {
			return fmt.Errorf("pbft plan contains duplicate node id %s", nodeID)
		}
		seenNode[nodeID] = true
		nodeByID[nodeID] = node
		encoded := strings.TrimSpace(plan.PBFTPublicKeys[nodeID])
		if encoded == "" {
			return fmt.Errorf("pbft public key missing for node %s", nodeID)
		}
		publicKey, err := pbft.DecodePublicKey(encoded)
		if err != nil {
			return fmt.Errorf("pbft public key for node %s: %w", nodeID, err)
		}
		canonicalKey := pbft.EncodePublicKey(publicKey)
		if owner := ownerByKey[canonicalKey]; owner != "" && owner != nodeID {
			return fmt.Errorf("pbft public key reused by %s and %s", owner, nodeID)
		}
		ownerByKey[canonicalKey] = nodeID
	}
	for nodeID := range plan.PBFTPublicKeys {
		if !seenNode[strings.TrimSpace(nodeID)] {
			return fmt.Errorf("pbft public key registry contains unknown node %s", nodeID)
		}
	}
	for _, node := range plan.NodeConfigs {
		seenValidator := map[string]bool{}
		for _, validatorRaw := range node.Validators {
			validator := strings.TrimSpace(validatorRaw)
			if validator == "" {
				return fmt.Errorf("pbft validator set for %s contains empty node id", node.NodeID)
			}
			if validator != validatorRaw {
				return fmt.Errorf("pbft validator id %q for %s contains surrounding whitespace", validatorRaw, node.NodeID)
			}
			if seenValidator[validator] {
				return fmt.Errorf("pbft validator set for %s contains duplicate node %s", node.NodeID, validator)
			}
			seenValidator[validator] = true
			if !seenNode[validator] {
				return fmt.Errorf("pbft validator %s is not present in node configs", validator)
			}
			validatorNode := nodeByID[validator]
			if strings.TrimSpace(validatorNode.ShardID) != strings.TrimSpace(node.ShardID) {
				return fmt.Errorf("pbft validator %s belongs to shard %s but is listed for shard %s", validator, validatorNode.ShardID, node.ShardID)
			}
			if strings.TrimSpace(plan.PBFTPublicKeys[validator]) == "" {
				return fmt.Errorf("pbft validator %s has no registered public key", validator)
			}
		}
		if !seenValidator[strings.TrimSpace(node.NodeID)] {
			return fmt.Errorf("pbft node %s is not a member of its own validator set", node.NodeID)
		}
	}
	return nil
}

func (r *NodeRuntime) pbftAuthenticationRequired() bool {
	if r == nil {
		return false
	}
	return strings.TrimSpace(r.plan.PBFTIdentityScheme) != "" || len(r.plan.PBFTPublicKeys) > 0 || strings.TrimSpace(r.plan.PlanID) != ""
}

func (r *NodeRuntime) pbftTestIdentityFallbackAllowed() bool {
	if r == nil {
		return false
	}
	return strings.TrimSpace(r.plan.PlanID) == "" && strings.TrimSpace(r.plan.PBFTIdentityScheme) == "" && len(r.plan.PBFTPublicKeys) == 0
}

func deterministicPBFTTestPrivateKey(nodeID string) ed25519.PrivateKey {
	sum := sha256.Sum256([]byte("mbe-pbft-test-only-v1|" + nodeID))
	return ed25519.NewKeyFromSeed(sum[:])
}

func (r *NodeRuntime) pbftSigningPrivateKey() (ed25519.PrivateKey, error) {
	encoded := strings.TrimSpace(os.Getenv(PBFTPrivateKeyEnv))
	if encoded == "" {
		if r.pbftTestIdentityFallbackAllowed() {
			return deterministicPBFTTestPrivateKey(r.node.NodeID), nil
		}
		return nil, fmt.Errorf("pbft private key is not available for node %s", r.node.NodeID)
	}
	privateKey, err := pbft.DecodePrivateKey(encoded)
	if err != nil {
		return nil, fmt.Errorf("pbft private key for node %s: %w", r.node.NodeID, err)
	}
	publicKey, err := r.pbftPublicKey(r.node.NodeID)
	if err != nil {
		return nil, err
	}
	derived := privateKey.Public().(ed25519.PublicKey)
	if !bytes.Equal(derived, publicKey) {
		return nil, fmt.Errorf("pbft private key does not match registered public key for node %s", r.node.NodeID)
	}
	return privateKey, nil
}

func (r *NodeRuntime) pbftPublicKey(nodeID string) (ed25519.PublicKey, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, fmt.Errorf("pbft public key lookup has empty node id")
	}
	if encoded := strings.TrimSpace(r.plan.PBFTPublicKeys[nodeID]); encoded != "" {
		key, err := pbft.DecodePublicKey(encoded)
		if err != nil {
			return nil, fmt.Errorf("pbft public key for node %s: %w", nodeID, err)
		}
		return key, nil
	}
	if r.pbftTestIdentityFallbackAllowed() {
		return deterministicPBFTTestPrivateKey(nodeID).Public().(ed25519.PublicKey), nil
	}
	return nil, fmt.Errorf("pbft public key missing for node %s", nodeID)
}

func (r *NodeRuntime) signPBFTPrePrepare(msg *pbft.PrePrepare) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignPrePrepare(msg, privateKey)
}

func (r *NodeRuntime) signPBFTPrepare(msg *pbft.Prepare) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignPrepare(msg, privateKey)
}

func (r *NodeRuntime) signPBFTCommit(msg *pbft.Commit) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignCommit(msg, privateKey)
}

func (r *NodeRuntime) signPBFTCheckpoint(msg *pbft.Checkpoint) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignCheckpoint(msg, privateKey)
}

func (r *NodeRuntime) signPBFTViewChange(msg *pbft.ViewChange) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignViewChange(msg, privateKey)
}

func (r *NodeRuntime) signPBFTNewView(msg *pbft.NewView) error {
	if !r.pbftAuthenticationRequired() {
		return nil
	}
	privateKey, err := r.pbftSigningPrivateKey()
	if err != nil {
		return err
	}
	return pbft.SignNewView(msg, privateKey)
}

func (r *NodeRuntime) verifyPBFTPrePrepareAuthentication(msg pbft.PrePrepare) error {
	if msg.Signature == "" && !r.pbftAuthenticationRequired() {
		return nil
	}
	publicKey, err := r.pbftPublicKey(msg.LeaderID)
	if err != nil {
		return err
	}
	return pbft.VerifyPrePrepare(msg, publicKey)
}

func (r *NodeRuntime) verifyPBFTPrepareAuthentication(msg pbft.Prepare) error {
	if msg.Signature == "" && !r.pbftAuthenticationRequired() {
		return nil
	}
	publicKey, err := r.pbftPublicKey(msg.NodeID)
	if err != nil {
		return err
	}
	return pbft.VerifyPrepare(msg, publicKey)
}

func (r *NodeRuntime) verifyPBFTCommitAuthentication(msg pbft.Commit) error {
	if msg.Signature == "" && !r.pbftAuthenticationRequired() {
		return nil
	}
	publicKey, err := r.pbftPublicKey(msg.NodeID)
	if err != nil {
		return err
	}
	return pbft.VerifyCommit(msg, publicKey)
}

func (r *NodeRuntime) verifyPBFTCheckpointAuthentication(msg pbft.Checkpoint) error {
	if msg.Signature == "" && !r.pbftAuthenticationRequired() {
		return nil
	}
	publicKey, err := r.pbftPublicKey(msg.NodeID)
	if err != nil {
		return err
	}
	return pbft.VerifyCheckpoint(msg, publicKey)
}

func (r *NodeRuntime) verifyPreparedCertificateAuthentication(cert *pbft.PreparedCertificate) error {
	if cert == nil {
		return nil
	}
	for _, vote := range cert.Prepares {
		if err := r.verifyPBFTPrepareAuthentication(vote); err != nil {
			return fmt.Errorf("prepared certificate vote %s: %w", vote.NodeID, err)
		}
	}
	return nil
}

func (r *NodeRuntime) verifyCommitCertificateAuthentication(cert pbft.CommitCertificate) error {
	for _, vote := range cert.Commits {
		if err := r.verifyPBFTCommitAuthentication(vote); err != nil {
			return fmt.Errorf("commit certificate vote %s: %w", vote.NodeID, err)
		}
	}
	return nil
}

func (r *NodeRuntime) verifyCheckpointCertificateAuthentication(cert *pbft.CheckpointCertificate) error {
	if cert == nil {
		return nil
	}
	for _, vote := range cert.Checkpoints {
		if err := r.verifyPBFTCheckpointAuthentication(vote); err != nil {
			return fmt.Errorf("checkpoint certificate vote %s: %w", vote.NodeID, err)
		}
	}
	return nil
}

func (r *NodeRuntime) verifyPBFTViewChangeAuthentication(msg pbft.ViewChange) error {
	if msg.Signature != "" || r.pbftAuthenticationRequired() {
		publicKey, err := r.pbftPublicKey(msg.NodeID)
		if err != nil {
			return err
		}
		if err := pbft.VerifyViewChange(msg, publicKey); err != nil {
			return err
		}
	}
	if err := r.verifyPreparedCertificateAuthentication(msg.Prepared); err != nil {
		return err
	}
	if err := r.verifyCheckpointCertificateAuthentication(msg.StableCheckpoint); err != nil {
		return err
	}
	if msg.PreparedBlock != nil {
		if err := r.validateConsensusBlockBody(*msg.PreparedBlock); err != nil {
			return fmt.Errorf("view-change prepared block: %w", err)
		}
	}
	return nil
}

func (r *NodeRuntime) verifyPBFTNewViewAuthentication(msg pbft.NewView) error {
	if msg.Signature != "" || r.pbftAuthenticationRequired() {
		publicKey, err := r.pbftPublicKey(msg.LeaderID)
		if err != nil {
			return err
		}
		if err := pbft.VerifyNewView(msg, publicKey); err != nil {
			return err
		}
	}
	for _, viewChange := range msg.ViewChanges {
		if err := r.verifyPBFTViewChangeAuthentication(viewChange); err != nil {
			return fmt.Errorf("new-view view-change from %s: %w", viewChange.NodeID, err)
		}
	}
	if err := r.verifyPreparedCertificateAuthentication(msg.SelectedPrepared); err != nil {
		return fmt.Errorf("new-view selected prepared certificate: %w", err)
	}
	if msg.SelectedBlock != nil {
		if err := r.validateConsensusBlockBody(*msg.SelectedBlock); err != nil {
			return fmt.Errorf("new-view selected block: %w", err)
		}
	}
	return nil
}

// validateConsensusBlockBody closes the binding between the digest-voted TxIDs
// vector and the concrete transaction bodies that executors later consume.
// Formal/runtime plans are strict: every transaction must recompute its TxID and
// pass its client Ed25519 signature. Empty-plan unit fixtures retain the legacy
// unsigned placeholder allowance so old focused state-machine tests do not turn
// into production authentication bypasses.
func (r *NodeRuntime) validateConsensusBlockBody(block realblock.Block) error {
	if block.BlockHash == "" {
		return fmt.Errorf("consensus block has empty block hash")
	}
	if realblock.Hash(block) != block.BlockHash {
		return fmt.Errorf("consensus block hash mismatch at height %d", block.Height)
	}
	if len(block.TxIDs) != len(block.TxList) {
		return fmt.Errorf("consensus block transaction body count mismatch: tx_ids=%d tx_list=%d", len(block.TxIDs), len(block.TxList))
	}
	if realblock.TxRoot(block.TxIDs) != block.TxRoot {
		return fmt.Errorf("consensus block transaction root mismatch at height %d", block.Height)
	}
	seen := make(map[string]bool, len(block.TxIDs))
	strict := r.pbftAuthenticationRequired()
	for index, item := range block.TxList {
		if strings.TrimSpace(item.TxID) == "" {
			return fmt.Errorf("consensus block transaction %d has empty tx id", index)
		}
		if block.TxIDs[index] != item.TxID {
			return fmt.Errorf("consensus block transaction id/body mismatch at index %d", index)
		}
		if seen[item.TxID] {
			return fmt.Errorf("consensus block contains duplicate transaction id %s", item.TxID)
		}
		seen[item.TxID] = true
		if strict {
			if err := tx.Verify(item); err != nil {
				return fmt.Errorf("consensus block transaction %s failed signature/body verification: %w", item.TxID, err)
			}
		}
	}
	return nil
}
