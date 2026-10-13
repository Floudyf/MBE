package v5

// MBE_MT_HOME_CAUSE_TRACE_V1
// Candidate-only observation of resource-service intervals. No routing, PBFT,
// Home-state ordering, retry, commit, or StateReady decision is altered.
// This instrumented run must NOT be used for performance comparisons.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type mtHomeTraceKindKeyV1 struct{}

func mtHomeTagContextV1(ctx context.Context, kind string) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, mtHomeTraceKindKeyV1{}, kind)
}
func mtHomeTraceKindV1(ctx context.Context) string {
	if ctx == nil {
		return "unclassified"
	}
	s, ok := ctx.Value(mtHomeTraceKindKeyV1{}).(string)
	if !ok || s == "" {
		return "unclassified"
	}
	return s
}

type mtHomeTraceWriterV1 struct {
	mu       sync.Mutex
	file     *os.File
	origin   time.Time
	next     uint64
	lost     uint64
	writeErr error
}

var mtHomeTraceByNodeV1 sync.Map // *NodeRuntime -> *mtHomeTraceWriterV1

func (r *NodeRuntime) mtHomeTraceEnabledV1() bool {
	if r == nil || r.pluginSnapshot == nil {
		return false
	}
	scheduler, ok := r.pluginSnapshot["scheduler"]
	return ok && scheduler.PluginID == "metatrack_unlock_scheduler" && boolFromAny(scheduler.Config["resource_trace_enabled"])
}

func (r *NodeRuntime) mtHomeTraceWriterV1() *mtHomeTraceWriterV1 {
	if existing, ok := mtHomeTraceByNodeV1.Load(r); ok {
		return existing.(*mtHomeTraceWriterV1)
	}
	writer := &mtHomeTraceWriterV1{origin: time.Now()}
	f, err := os.OpenFile(filepath.Join(r.node.DataDir, "metatrack_home_trace_v1.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		writer.writeErr = err
		fmt.Fprintf(os.Stderr, "[MT_HOME_TRACE_V1_IO_ERROR] %v\n", err)
	} else {
		writer.file = f
	}
	actual, loaded := mtHomeTraceByNodeV1.LoadOrStore(r, writer)
	if loaded && writer.file != nil {
		_ = writer.file.Close()
	}
	return actual.(*mtHomeTraceWriterV1)
}

func (r *NodeRuntime) mtHomeTraceSpanV1(kind, home, blockHash, peer string, height uint64, items int, queueDepth int) func(error) {
	if !r.mtHomeTraceEnabledV1() {
		return func(error) {}
	}
	writer := r.mtHomeTraceWriterV1()
	id := atomic.AddUint64(&writer.next, 1)
	began := time.Now()
	write := func(phase string, err error) {
		record := map[string]any{
			"schema_version":    "mbe_mt_home_resource_trace_v1",
			"span_id":           id,
			"timestamp_unix_ns": time.Now().UnixNano(),
			"elapsed_mono_ns":   time.Since(writer.origin).Nanoseconds(),
			"node_id":           r.node.NodeID,
			"execution_shard":   r.node.ShardID,
			"home_shard":        home,
			"block_hash":        blockHash,
			"height":            height,
			"peer":              peer,
			"kind":              kind,
			"phase":             phase,
			"item_count":        items,
			"duration_ns":       time.Since(began).Nanoseconds(),
			"success":           err == nil,
		}
		if queueDepth >= 0 {
			record["queue_depth"] = queueDepth
		}
		if err != nil {
			record["error"] = err.Error()
		}
		raw, marshalErr := json.Marshal(record)
		writer.mu.Lock()
		defer writer.mu.Unlock()
		if marshalErr != nil || writer.file == nil {
			writer.lost++
			return
		}
		raw = append(raw, '\n')
		if _, e := writer.file.Write(raw); e != nil {
			writer.lost++
			if writer.writeErr == nil {
				writer.writeErr = e
				fmt.Fprintf(os.Stderr, "[MT_HOME_TRACE_V1_IO_ERROR] %v\n", e)
			}
		}
	}
	write("begin", nil)
	return func(err error) { write("end", err) }
}
