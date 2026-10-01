package v5

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// porygonSortitionEntry is an MBE adapter for Porygon's verifiable committee
// sortition.  PBFT identities remain unchanged; only the Porygon EC/ESC role
// for a protocol height rotates.  Storage-role identity remains fixed and is
// deliberately not derived from this function.
type porygonSortitionEntry struct {
	NodeID string
	Score  string
}

func porygonCommitteeEpochSeed(height uint64, orderingDomain string) string {
	raw := fmt.Sprintf("porygon-committee-v1|%s|%d", orderingDomain, height)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func porygonSortedValidators(nodes []NodePlan, height uint64, orderingDomain string) []string {
	seed := porygonCommitteeEpochSeed(height, orderingDomain)
	entries := make([]porygonSortitionEntry, 0, len(nodes))
	seen := map[string]bool{}
	for _, node := range nodes {
		if node.NodeID == "" || seen[node.NodeID] {
			continue
		}
		seen[node.NodeID] = true
		sum := sha256.Sum256([]byte(seed + "|" + node.NodeID))
		entries = append(entries, porygonSortitionEntry{NodeID: node.NodeID, Score: hex.EncodeToString(sum[:])})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return entries[i].NodeID < entries[j].NodeID
	})
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.NodeID)
	}
	return out
}

func porygonExecutionShardForNode(nodeID string, nodes []NodePlan, height uint64, orderingDomain string, shardCount int) string {
	if shardCount < 1 || nodeID == "" {
		return ""
	}
	ordered := porygonSortedValidators(nodes, height, orderingDomain)
	for index, candidate := range ordered {
		if candidate == nodeID {
			return fmt.Sprintf("s%d", index%shardCount)
		}
	}
	return ""
}

func porygonExecutionShardMembersAtHeight(nodes []NodePlan, height uint64, orderingDomain, executionShardID string, shardCount int) []string {
	out := []string{}
	for _, nodeID := range porygonSortedValidators(nodes, height, orderingDomain) {
		if porygonExecutionShardForNode(nodeID, nodes, height, orderingDomain, shardCount) == executionShardID {
			out = append(out, nodeID)
		}
	}
	sort.Strings(out)
	return out
}

// Porygon's execution threshold Te is distinct from the majority threshold used
// by Multi-Shard Update.  Under the usual <1/3 Byzantine committee bound, f+1
// matching authenticated results exceeds the maximum malicious-only set.
func porygonExecutionThreshold(memberCount int) int {
	if memberCount < 1 {
		return 1
	}
	f := (memberCount - 1) / 3
	return f + 1
}

// Multi-Shard Update accepts a state-root/update result only after >1/2 of the
// responsible committee replicas agree, matching Porygon's update rule.
func porygonMultiShardUpdateThreshold(memberCount int) int {
	if memberCount < 1 {
		return 1
	}
	return memberCount/2 + 1
}

func porygonWitnessCommittee(nodes []NodePlan, height uint64, orderingDomain, transactionBlockDigest string) []string {
	ordered := porygonSortedValidators(nodes, height, orderingDomain+"|witness|"+transactionBlockDigest)
	if len(ordered) <= 4 {
		return ordered
	}
	// Keep a rotating super-majority-sized EC on small MBE clusters. This is a
	// deployment-scale adapter; membership is deterministic and independently
	// verifiable from the common epoch seed.
	size := (len(ordered)*2 + 2) / 3
	if size < 4 {
		size = 4
	}
	if size > len(ordered) {
		size = len(ordered)
	}
	return append([]string(nil), ordered[:size]...)
}

func porygonWitnessFaultThreshold(memberCount int) int {
	if memberCount < 1 {
		return 1
	}
	f := (memberCount - 1) / 3
	return f + 1
}

func (r *NodeRuntime) porygonExecutionShardCount() int {
	if r == nil {
		return 1
	}
	item, ok := r.pluginSnapshot["block_executor"]
	if ok {
		if value := intValue(item.Config["execution_shard_count"]); value > 0 {
			return value
		}
	}
	item, ok = r.pluginSnapshot["scheduler"]
	if ok {
		if value := intValue(item.Config["execution_shard_count"]); value > 0 {
			return value
		}
	}
	return 1
}

func (r *NodeRuntime) porygonExecutionRoleShardID(height uint64) string {
	if r == nil {
		return ""
	}
	return porygonExecutionShardForNode(r.node.NodeID, r.plan.NodeConfigs, height, r.node.ShardID, r.porygonExecutionShardCount())
}

func (r *NodeRuntime) porygonExecutionRoleMembers(height uint64, shardID string) []string {
	if r == nil {
		return nil
	}
	return porygonExecutionShardMembersAtHeight(r.plan.NodeConfigs, height, r.node.ShardID, shardID, r.porygonExecutionShardCount())
}
