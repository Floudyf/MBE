package v5

import (
	"context"
	"metaverse-chainlab/executor/realism/p2p"
	"testing"
)

func TestMetaTrackResourceLaneV2OnlyCandidateAndTraceDisabledByDefault(t *testing.T) {
	r := &NodeRuntime{pluginSnapshot: map[string]PluginConfig{"scheduler": {PluginID: "fast_first_scheduler"}}}
	if r.mtResourceCandidateV2() {
		t.Fatal("other algorithms must not enable arbitration")
	}
	r.pluginSnapshot["scheduler"] = PluginConfig{PluginID: "metatrack_unlock_scheduler"}
	if !r.mtResourceCandidateV2() {
		t.Fatal("candidate did not enable resource arbitration")
	}
	if r.mtHomeTraceEnabledV1() {
		t.Fatal("expensive JSONL logging must default OFF")
	}
	r.pluginSnapshot["scheduler"] = PluginConfig{PluginID: "metatrack_unlock_scheduler", Config: map[string]any{"resource_trace_enabled": true}}
	if !r.mtHomeTraceEnabledV1() {
		t.Fatal("diagnostic trace opt-in was lost")
	}
}

func TestMetaTrackResourceLaneV2HookFallback(t *testing.T) {
	r := &NodeRuntime{pluginSnapshot: map[string]PluginConfig{"scheduler": {PluginID: "metatrack_unlock_scheduler"}}}
	ran := false
	err := r.mtResourceSendV2(context.Background(), "n4", "critical", p2p.MessageEnvelope{}, func() error { ran = true; return nil })
	if err != nil || !ran {
		t.Fatal("missing original-path fallback without runtime transport", err)
	}
}
