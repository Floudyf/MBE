package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
)

func TestMetaTrackV6565RestoresImmediateJoinForLatestAndCurrent(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]any
	}{
		{name: "latest", cfg: map[string]any{"dependency_closed_consensus": true}},
		{name: "current", cfg: map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &NodeRuntime{pluginSnapshot: map[string]PluginConfig{"block_executor": {Config: tc.cfg}}}
			block := realblock.Block{BlockHash: "b-" + tc.name}
			buffer := &metaTrackVersionPublishBuffer{asyncV640: newMetaTrackAsyncVersionPublisherV640()}
			if err := runtime.deferOrJoinMetaTrackAsyncVersionsV656(context.Background(), block, buffer); err != nil {
				t.Fatal(err)
			}
			if _, ok := metaTrackDeferredFinalJoinsV656.Load(metaTrackDeferredFinalJoinKeyV656(runtime, block)); ok {
				t.Fatal("final join must not be deferred after v6.5.6.5 liveness rollback")
			}
			buffer.asyncV640.mu.Lock()
			closed := buffer.asyncV640.closed
			buffer.asyncV640.mu.Unlock()
			if !closed {
				t.Fatal("v6.4 async publisher was not joined/closed immediately")
			}
			if err := runtime.joinDeferredMetaTrackAsyncVersionsV656(context.Background(), block); err != nil {
				t.Fatal(err)
			}
		})
	}
}
