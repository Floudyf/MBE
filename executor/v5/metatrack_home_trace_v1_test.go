package v5

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetaTrackHomeTraceV1ContextClassification(t *testing.T) {
	base := context.Background()
	if got := mtHomeTraceKindV1(base); got != "unclassified" {
		t.Fatalf("expected default classification got %s", got)
	}
	tagged := mtHomeTagContextV1(base, "critical")
	if got := mtHomeTraceKindV1(tagged); got != "critical" {
		t.Fatalf("missing critical tag got %s", got)
	}
	if got := mtHomeTraceKindV1(base); got != "unclassified" {
		t.Fatalf("tag leaked into parent context %s", got)
	}
	if got := mtHomeTraceKindV1(mtHomeTagContextV1(base, "background")); got != "background" {
		t.Fatalf("missing background tag %s", got)
	}
	parent, cancel := context.WithCancel(context.Background())
	child := mtHomeTagContextV1(parent, "background")
	cancel()
	select {
	case <-child.Done():
	default:
		t.Fatal("background trace context broke original cancellation semantics")
	}
}

func TestMetaTrackHomeTraceV1OnlyCandidate(t *testing.T) {
	root := t.TempDir()
	r := &NodeRuntime{node: NodePlan{DataDir: root, NodeID: "n0", ShardID: "s0"}, pluginSnapshot: map[string]PluginConfig{"scheduler": {PluginID: "fast_first_scheduler"}}}
	t.Cleanup(func() {
		if value, loaded := mtHomeTraceByNodeV1.LoadAndDelete(r); loaded {
			state := value.(*mtHomeTraceWriterV1)
			if state.file != nil {
				_ = state.file.Close()
			}
		}
	})
	r.mtHomeTraceSpanV1("home_apply_batch", "s1", "block", "n4", 1, 3, -1)(nil)
	path := filepath.Join(root, "metatrack_home_trace_v1.jsonl")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Full/noncandidate unexpectedly wrote a trace %v", err)
	}
	r.pluginSnapshot["scheduler"] = PluginConfig{PluginID: "metatrack_unlock_scheduler", Config: map[string]any{"resource_trace_enabled": true}}
	r.mtHomeTraceSpanV1("home_apply_batch", "s1", "block", "n4", 1, 3, -1)(nil)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if strings.Count(text, `"phase"`) != 2 {
		t.Fatalf("span not paired: %s", text)
	}
	if strings.Contains(text, `"queue_depth"`) {
		t.Fatalf("unmeasured queue depth invented: %s", text)
	}
}
