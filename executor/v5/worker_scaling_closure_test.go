package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/execution"
)

func TestFormalWorkerScalingBlockExecutorsAccept32(t *testing.T) {
	ids := []string{
		cgBlockExecutorID,
		acgBlockExecutorID,
		bsxBlockExecutorID,
		execution.AriaBlockExecutorID,
		groundhogBlockExecutorID,
		execution.BatchSIBlockExecutorID,
	}
	for _, id := range ids {
		plugin, err := BuiltinRegistry().Create("block_executor", id, map[string]any{"worker_count": 32})
		if err != nil {
			t.Fatalf("%s worker_count=32: %v", id, err)
		}
		if plugin == nil || plugin.ID() != id {
			t.Fatalf("%s registry result mismatch: %#v", id, plugin)
		}
	}
}
