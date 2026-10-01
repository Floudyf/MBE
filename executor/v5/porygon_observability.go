package v5

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"metaverse-chainlab/executor/realism/metrics"
)

func (r *NodeRuntime) writePorygonRemoteStateArtifacts() error {
	if r == nil || r.plugins.BlockExecutor == nil || r.plugins.BlockExecutor.ID() != porygonBlockExecutorID {
		return nil
	}
	r.mu.Lock()
	rows := append([][]string(nil), r.remoteStateRows...)
	r.mu.Unlock()
	header := []string{"timestamp", "node_id", "execution_shard", "height", "block_hash", "tx_id", "state_key", "qualified_home_key", "home_shard", "response_execution_shard", "access_kind", "latency_ms", "witness_digest", "home_state_root", "success", "error", "delta_id", "source_height", "source_block_hash", "update_semantics"}
	return metrics.WriteCSV(filepath.Join(r.node.DataDir, "remote_state_access.csv"), header, rows)
}

func porygonLogicalBusinessStateDigest(snapshot map[string]string) string {
	logical := map[string]string{}
	conflicts := map[string][]string{}
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, physicalKey := range keys {
		logicalKey := physicalKey
		if index := strings.Index(logicalKey, "::"); index >= 0 && index+2 < len(logicalKey) {
			logicalKey = logicalKey[index+2:]
		}
		if strings.HasPrefix(logicalKey, "relay_commit:") || strings.HasPrefix(logicalKey, "protocol:") {
			continue
		}
		value := snapshot[physicalKey]
		if prior, ok := logical[logicalKey]; ok && prior != value {
			conflicts[logicalKey] = append(conflicts[logicalKey], prior, value)
			continue
		}
		logical[logicalKey] = value
	}
	logicalKeys := make([]string, 0, len(logical)+len(conflicts))
	for key := range logical {
		logicalKeys = append(logicalKeys, key)
	}
	for key := range conflicts {
		if _, ok := logical[key]; !ok {
			logicalKeys = append(logicalKeys, key)
		}
	}
	sort.Strings(logicalKeys)
	rows := make([]string, 0, len(logicalKeys))
	for _, key := range logicalKeys {
		if values := conflicts[key]; len(values) > 0 {
			sort.Strings(values)
			rows = append(rows, fmt.Sprintf("%s=<physical_partition_conflict:%s>", key, stableJSONDigest(values)))
			continue
		}
		rows = append(rows, key+"="+logical[key])
	}
	return stableTextDigest(strings.Join(rows, "\n"))
}

func porygonStoragePartitionFootprint(snapshot map[string]string) (int, int64) {
	entries := 0
	var logicalBytes int64
	for key, value := range snapshot {
		entries++
		logicalBytes += int64(len(key) + len(value))
	}
	return entries, logicalBytes
}
