package state

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
)

const (
	LegacyCommitmentVersion = "mbe_flat_sorted_sha256_v1"
	CommitmentVersion       = "mbe_state_merkle_treap_v2"
)

var emptyCommitmentHash = sha256.Sum256([]byte("mbe-state-merkle-treap-v2:empty"))

// LegacyRoot preserves the original MBE flat-state commitment. It is retained
// only for opening/verifying artifacts persisted before CommitmentVersion.
func LegacyRoot(snapshot map[string]string) string {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(snapshot[key])
		b.WriteString("\n")
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// Root returns the current authenticated-state commitment. The v2 commitment
// is a deterministic Merkle treap: the BST order is the state key and the heap
// priority is derived from SHA-256(key), so the same logical state has the same
// root regardless of insertion order. Full-snapshot construction remains
// available for block/recovery verification; hot transaction materialization
// should create one Commitment and update only touched keys.
func Root(snapshot map[string]string) string {
	return NewCommitment(snapshot).Root()
}

func RootForVersion(snapshot map[string]string, version string) string {
	switch version {
	case "", LegacyCommitmentVersion:
		return LegacyRoot(snapshot)
	case CommitmentVersion:
		return Root(snapshot)
	default:
		return ""
	}
}

type commitmentNode struct {
	key      string
	value    string
	priority [32]byte
	left     *commitmentNode
	right    *commitmentNode
	hash     [32]byte
}

// Commitment incrementally maintains the authenticated state root. Set costs
// expected O(log n), rather than sorting and hashing the entire state after
// every transaction.
type Commitment struct {
	root *commitmentNode
}

func NewCommitment(snapshot map[string]string) *Commitment {
	c := &Commitment{}
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	// Sorting is not required for correctness, but makes construction work and
	// profiling deterministic even if the input Go map iteration order differs.
	sort.Strings(keys)
	for _, key := range keys {
		c.root = commitmentSet(c.root, key, snapshot[key])
	}
	return c
}

// Clone returns an O(1) immutable-root snapshot. Commitment.Set uses path-copy
// updates, so later mutations of either clone cannot alter the other root.
func (c *Commitment) Clone() *Commitment {
	if c == nil {
		return nil
	}
	return &Commitment{root: c.root}
}

// CloneOrBuild reuses an authenticated block-start commitment when the caller
// has proved it corresponds exactly to snapshot. The nil fallback preserves all
// legacy/multi-shard paths that still need full-snapshot reconstruction.
func CloneOrBuild(base *Commitment, snapshot map[string]string) *Commitment {
	if base != nil {
		return base.Clone()
	}
	return NewCommitment(snapshot)
}

func (c *Commitment) Set(key, value string) {
	if c == nil {
		return
	}
	c.root = commitmentSet(c.root, key, value)
}

func (c *Commitment) Apply(updates map[string]string, qualify func(string) string) {
	if c == nil || len(updates) == 0 {
		return
	}
	keys := make([]string, 0, len(updates))
	for key := range updates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		qualified := key
		if qualify != nil {
			qualified = qualify(key)
		}
		c.Set(qualified, updates[key])
	}
}

func (c *Commitment) Root() string {
	if c == nil || c.root == nil {
		return hex.EncodeToString(emptyCommitmentHash[:])
	}
	return hex.EncodeToString(c.root.hash[:])
}

func commitmentSet(node *commitmentNode, key, value string) *commitmentNode {
	if node == nil {
		created := &commitmentNode{key: key, value: value, priority: commitmentPriority(key)}
		refreshCommitmentHash(created)
		return created
	}
	// Every mutation path is copied before modification. Subtrees outside the
	// path remain shared and immutable, which makes Commitment.Clone O(1).
	node = cloneCommitmentNode(node)
	if key == node.key {
		node.value = value
		refreshCommitmentHash(node)
		return node
	}
	if key < node.key {
		node.left = commitmentSet(node.left, key, value)
		if commitmentPriorityLess(node.left, node) {
			node = rotateCommitmentRight(node)
		}
	} else {
		node.right = commitmentSet(node.right, key, value)
		if commitmentPriorityLess(node.right, node) {
			node = rotateCommitmentLeft(node)
		}
	}
	refreshCommitmentHash(node)
	return node
}

func cloneCommitmentNode(node *commitmentNode) *commitmentNode {
	if node == nil {
		return nil
	}
	clone := *node
	return &clone
}

func commitmentPriority(key string) [32]byte {
	return sha256.Sum256(append([]byte("mbe-state-merkle-treap-v2:priority\x00"), []byte(key)...))
}

func commitmentPriorityLess(left, right *commitmentNode) bool {
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

func rotateCommitmentRight(root *commitmentNode) *commitmentNode {
	next := cloneCommitmentNode(root.left)
	root.left = next.right
	next.right = root
	refreshCommitmentHash(root)
	refreshCommitmentHash(next)
	return next
}

func rotateCommitmentLeft(root *commitmentNode) *commitmentNode {
	next := cloneCommitmentNode(root.right)
	root.right = next.left
	next.left = root
	refreshCommitmentHash(root)
	refreshCommitmentHash(next)
	return next
}

func refreshCommitmentHash(node *commitmentNode) {
	if node == nil {
		return
	}
	left := emptyCommitmentHash
	if node.left != nil {
		left = node.left.hash
	}
	right := emptyCommitmentHash
	if node.right != nil {
		right = node.right.hash
	}
	h := sha256.New()
	_, _ = h.Write([]byte("mbe-state-merkle-treap-v2:node\x00"))
	_, _ = h.Write(left[:])
	writeCommitmentString(h, node.key)
	writeCommitmentString(h, node.value)
	_, _ = h.Write(right[:])
	copy(node.hash[:], h.Sum(nil))
}

type commitmentWriter interface {
	Write([]byte) (int, error)
}

func writeCommitmentString(w commitmentWriter, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}
