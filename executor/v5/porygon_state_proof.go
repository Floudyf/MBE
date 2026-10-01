package v5

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// PorygonStateProof is a real inclusion/non-membership proof against the same deterministic
// Merkle-treap commitment used by MBE state.Root.  Porygon keeps this proof
// private to its Storage Role protocol so other methods and the shared state
// API remain untouched.
type PorygonStateProof struct {
	Key             string                  `json:"key"`
	Value           string                  `json:"value"`
	Exists          bool                    `json:"exists"`
	Root            string                  `json:"root"`
	TargetLeftHash  string                  `json:"target_left_hash"`
	TargetRightHash string                  `json:"target_right_hash"`
	Path            []PorygonStateProofStep `json:"path"`
	ProofDigest     string                  `json:"proof_digest"`
}

type PorygonStateProofStep struct {
	AncestorKey   string `json:"ancestor_key"`
	AncestorValue string `json:"ancestor_value"`
	SiblingHash   string `json:"sibling_hash"`
	TargetSide    string `json:"target_side"` // left or right
}

type porygonProofNode struct {
	key      string
	value    string
	priority [32]byte
	left     *porygonProofNode
	right    *porygonProofNode
	hash     [32]byte
}

var porygonEmptyCommitmentHash = sha256.Sum256([]byte("mbe-state-merkle-treap-v2:empty"))

func porygonGenerateStateProof(snapshot map[string]string, key string) (PorygonStateProof, bool) {
	root := porygonBuildProofTree(snapshot)
	if root == nil {
		proof := PorygonStateProof{Key: key, Value: "", Exists: false, Root: hex.EncodeToString(porygonEmptyCommitmentHash[:])}
		proof.ProofDigest = porygonStateProofDigest(proof)
		return proof, true
	}
	current := root
	topDown := make([]PorygonStateProofStep, 0, 16)
	for current != nil && current.key != key {
		if key < current.key {
			topDown = append(topDown, PorygonStateProofStep{
				AncestorKey: current.key, AncestorValue: current.value,
				SiblingHash: porygonProofNodeHashHex(current.right), TargetSide: "left",
			})
			current = current.left
		} else {
			topDown = append(topDown, PorygonStateProofStep{
				AncestorKey: current.key, AncestorValue: current.value,
				SiblingHash: porygonProofNodeHashHex(current.left), TargetSide: "right",
			})
			current = current.right
		}
	}
	// Verification reconstructs from the target (or the empty search child for
	// a missing key) towards the root.
	path := make([]PorygonStateProofStep, len(topDown))
	for i := range topDown {
		path[len(topDown)-1-i] = topDown[i]
	}
	proof := PorygonStateProof{Key: key, Root: hex.EncodeToString(root.hash[:]), Path: path}
	if current != nil {
		proof.Exists = true
		proof.Value = current.value
		proof.TargetLeftHash = porygonProofNodeHashHex(current.left)
		proof.TargetRightHash = porygonProofNodeHashHex(current.right)
	} else {
		proof.Exists = false
		proof.Value = ""
		proof.TargetLeftHash = hex.EncodeToString(porygonEmptyCommitmentHash[:])
		proof.TargetRightHash = hex.EncodeToString(porygonEmptyCommitmentHash[:])
	}
	proof.ProofDigest = porygonStateProofDigest(proof)
	return proof, true
}

func porygonVerifyStateProof(proof PorygonStateProof, expectedKey, expectedValue, expectedRoot string) bool {
	if proof.Key != expectedKey || proof.Root != expectedRoot || proof.ProofDigest == "" {
		return false
	}
	if proof.Exists && proof.Value != expectedValue {
		return false
	}
	if !proof.Exists && expectedValue != "" {
		return false
	}
	if proof.ProofDigest != porygonStateProofDigest(proof) {
		return false
	}
	var current [32]byte
	if proof.Exists {
		left, ok := porygonDecodeProofHash(proof.TargetLeftHash)
		if !ok {
			return false
		}
		right, ok := porygonDecodeProofHash(proof.TargetRightHash)
		if !ok {
			return false
		}
		current = porygonCommitmentHash(left, proof.Key, proof.Value, right)
	} else {
		// A non-membership proof terminates at the empty child reached by the
		// deterministic BST search for expectedKey.
		current = porygonEmptyCommitmentHash
	}
	for _, step := range proof.Path {
		sibling, ok := porygonDecodeProofHash(step.SiblingHash)
		if !ok {
			return false
		}
		switch step.TargetSide {
		case "left":
			if expectedKey >= step.AncestorKey {
				return false
			}
			current = porygonCommitmentHash(current, step.AncestorKey, step.AncestorValue, sibling)
		case "right":
			if expectedKey <= step.AncestorKey {
				return false
			}
			current = porygonCommitmentHash(sibling, step.AncestorKey, step.AncestorValue, current)
		default:
			return false
		}
	}
	return hex.EncodeToString(current[:]) == expectedRoot
}

func porygonStateProofDigest(proof PorygonStateProof) string {
	copy := proof
	copy.ProofDigest = ""
	raw, _ := json.Marshal(copy)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func porygonBuildProofTree(snapshot map[string]string) *porygonProofNode {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var root *porygonProofNode
	for _, key := range keys {
		root = porygonProofSet(root, key, snapshot[key])
	}
	return root
}

func porygonProofSet(node *porygonProofNode, key, value string) *porygonProofNode {
	if node == nil {
		created := &porygonProofNode{key: key, value: value, priority: porygonProofPriority(key)}
		porygonRefreshProofHash(created)
		return created
	}
	if key == node.key {
		node.value = value
		porygonRefreshProofHash(node)
		return node
	}
	if key < node.key {
		node.left = porygonProofSet(node.left, key, value)
		if porygonProofPriorityLess(node.left, node) {
			node = porygonRotateProofRight(node)
		}
	} else {
		node.right = porygonProofSet(node.right, key, value)
		if porygonProofPriorityLess(node.right, node) {
			node = porygonRotateProofLeft(node)
		}
	}
	porygonRefreshProofHash(node)
	return node
}

func porygonProofPriority(key string) [32]byte {
	return sha256.Sum256(append([]byte("mbe-state-merkle-treap-v2:priority\x00"), []byte(key)...))
}

func porygonProofPriorityLess(left, right *porygonProofNode) bool {
	if left == nil {
		return false
	}
	for index := range left.priority {
		if left.priority[index] < right.priority[index] {
			return true
		}
		if left.priority[index] > right.priority[index] {
			return false
		}
	}
	return left.key < right.key
}

func porygonRotateProofRight(root *porygonProofNode) *porygonProofNode {
	next := root.left
	root.left = next.right
	next.right = root
	porygonRefreshProofHash(root)
	porygonRefreshProofHash(next)
	return next
}

func porygonRotateProofLeft(root *porygonProofNode) *porygonProofNode {
	next := root.right
	root.right = next.left
	next.left = root
	porygonRefreshProofHash(root)
	porygonRefreshProofHash(next)
	return next
}

func porygonRefreshProofHash(node *porygonProofNode) {
	if node == nil {
		return
	}
	left := porygonEmptyCommitmentHash
	if node.left != nil {
		left = node.left.hash
	}
	right := porygonEmptyCommitmentHash
	if node.right != nil {
		right = node.right.hash
	}
	node.hash = porygonCommitmentHash(left, node.key, node.value, right)
}

func porygonCommitmentHash(left [32]byte, key, value string, right [32]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("mbe-state-merkle-treap-v2:node\x00"))
	_, _ = h.Write(left[:])
	porygonWriteCommitmentString(h, key)
	porygonWriteCommitmentString(h, value)
	_, _ = h.Write(right[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func porygonWriteCommitmentString(w interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func porygonProofNodeHashHex(node *porygonProofNode) string {
	if node == nil {
		return hex.EncodeToString(porygonEmptyCommitmentHash[:])
	}
	return hex.EncodeToString(node.hash[:])
}

func porygonDecodeProofHash(value string) ([32]byte, bool) {
	var out [32]byte
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(out) {
		return out, false
	}
	copy(out[:], raw)
	return out, true
}
