package recovery

import (
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/realism/metrics"
)

func TestRecoverSummary(t *testing.T) {
	dir := t.TempDir()
	if err := metrics.WriteJSON(filepath.Join(dir, "commit_summary.json"), map[string]any{"node_id": "n0", "committed_height": 1, "latest_block_hash": "b", "latest_state_root": "s"}); err != nil {
		t.Fatal(err)
	}
	summary, err := Recover(dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.NodeRecovery || summary.CommittedHeight != 1 || summary.ProductionRecovery {
		t.Fatalf("unexpected recovery summary: %+v", summary)
	}
}
