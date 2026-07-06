package metrics

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCSVAndJSON(t *testing.T) {
	dir := t.TempDir()
	if err := WriteCSV(filepath.Join(dir, "a", "log.csv"), []string{"a"}, [][]string{{"b"}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSON(filepath.Join(dir, "b", "summary.json"), map[string]any{"runtime_truth": "v4_real_node_foundation"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a", "log.csv")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "b", "summary.json")); err != nil {
		t.Fatal(err)
	}
}
