package v5

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An event coordinate is not an observed source block; the loader must require
// an explicit provenance marker and the schema dedicated to this extension.
func TestMVEventClockV1LoaderAndProvenance(t *testing.T) {
	root := t.TempDir()
	clientDir := filepath.Join(root, "client")
	workloadDir := filepath.Join(root, "workload")
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workloadDir, 0755); err != nil {
		t.Fatal(err)
	}
	records := []txalloDynamicBlockRowV222{
		{SchemaVersion: "mbe_txallo_event_clock_v1", MaterializedIndex: 0, RawSourceRowIndex: 20, TransactionID: "t20", BlockNum: 0, EpochCoordinate: 21, GlobalSequence: 21, SenderID: "alice", ReceiverID: "bob"},
		{SchemaVersion: "mbe_txallo_event_clock_v1", MaterializedIndex: 1, RawSourceRowIndex: 21, TransactionID: "t21", BlockNum: 0, EpochCoordinate: 22, GlobalSequence: 22, SenderID: "alice", ReceiverID: "carol"},
	}
	write := func(rows []txalloDynamicBlockRowV222) string {
		t.Helper()
		path := filepath.Join(workloadDir, "txallo_dynamic_blocks.jsonl.gz")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		g := gzip.NewWriter(f)
		for _, row := range rows {
			b, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = g.Write(append(b, '\n')); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	mkplan := func(digest, clock string) WorkloadPlan {
		return WorkloadPlan{ActualTxCount: 2, AuditMetadata: map[string]any{
			"txallo_dynamic_blocks": map[string]any{
				"relative_path": "workload/txallo_dynamic_blocks.jsonl.gz",
				"sha256":        digest, "selected_count": 2,
				"evaluation_anchor_raw_row_index": 20,
				"epoch_clock_source":              clock,
			},
		}}
	}
	hash := write(records)
	index, err := txalloLoadDynamicBlockIndexV222(clientDir, mkplan(hash, "mbe_event_order_index"))
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Blocks) != 2 || index.Blocks[0] != 21 || index.Blocks[1] != 22 || index.AnchorBlock != 21 {
		t.Fatalf("bad event coordinate index: %+v", index)
	}
	if _, err := txalloLoadDynamicBlockIndexV222(clientDir, mkplan(hash, "")); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("unlabeled event clock accepted: %v", err)
	}
	rows := append([]txalloDynamicBlockRowV222(nil), records...)
	rows[1].BlockNum = 22 // A virtual event coordinate cannot masquerade as a chain height.
	hash = write(rows)
	if _, err := txalloLoadDynamicBlockIndexV222(clientDir, mkplan(hash, "mbe_event_order_index")); err == nil {
		t.Fatal("invented chain block accepted in event mode")
	}
}
