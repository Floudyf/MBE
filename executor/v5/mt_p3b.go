package v5

import (
	"metaverse-chainlab/executor/realism/execution"
	"sort"
)

// mtP3BSparseStateDelta preserves executionStateDelta semantics for an
// execution view built by copying base and only assigning successful WriteSets.
// The set contains physical, shard-qualified keys, not logical AccessList keys.
// No deletions occur in the versioned materialization path.
func mtP3BSparseStateDelta(base, final map[string]string, written map[string]struct{}) []execution.StateUpdate {
	keys := make([]string, 0, len(written))
	for key := range written {
		if base[key] != final[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]execution.StateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, execution.StateUpdate{Key: key, Value: final[key]})
	}
	return out
}
