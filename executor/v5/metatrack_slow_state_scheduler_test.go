package v5

// MBE_MT_WAIT_READY_V1: equal worker parallelism regression test.

import (
	"context"
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
	"strings"
	"testing"
	"time"
)

func TestMTWaitReadyPolicies(t *testing.T) {
	items := independentTransferTxs(4)
	exec := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	for _, tt := range []struct {
		id, policy string
		fifo       bool
	}{
		{metaTrackSlowStateSchedulerID, metaTrackWaitReadyPolicy, false},
		{metaTrackWaitFIFOSchedulerID, metaTrackWaitFIFOControlPolicy, true},
	} {
		t.Run(tt.id, func(t *testing.T) {
			block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: items}
			for _, item := range items {
				block.TxIDs = append(block.TxIDs, item.TxID)
			}
			scheduler := builtinScheduler{makeBasic("scheduler", tt.id, nil)}
			if got := scheduler.Schedule(items, exec).ReadyPriorityPolicy; got != tt.policy {
				t.Fatalf("policy %q want %q", got, tt.policy)
			}
			config := map[string]any{"worker_count": 4, "business_execution_delay_ms": 15}
			blockExec := metaTrackBlockExecutor{makeBasic("block_executor", "metatrack_block_executor", config)}
			result, err := blockExec.ExecuteBlock(context.Background(), BlockExecutionInput{Block: block, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: 4, Execution: exec, Scheduler: scheduler})
			if err != nil {
				t.Fatal(err)
			}
			if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got < 2 || got > 4 {
				t.Fatalf("candidate lost worker parallelism, got %d", got)
			}
			if got, _ := result.ActualMetrics["metatrack_candidate_waiting_ready_enabled"].(bool); !got {
				t.Fatal("candidate not enabled")
			}
			if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); got {
				t.Fatal("candidate should not report physical Fast/Conservative Ready split")
			}
			if got, _ := result.ActualMetrics["metatrack_candidate_fifo_no_bypass"].(bool); got != tt.fifo {
				t.Fatalf("FIFO flag=%v expected=%v", got, tt.fifo)
			}
			if got, _ := result.ActualMetrics["metatrack_single_fifo_hol_enabled"].(bool); got != tt.fifo {
				t.Fatalf("HOL flag=%v expected=%v", got, tt.fifo)
			}
			assertMetaTrackSerialEquivalent(t, result.ExecutionResult, items)
		})
	}
}

// TestMTWaitReadyBypassVsFIFO verifies the actual StateReady event and dispatch
// order on the exact same transactions/workers. No mock Waiting queue is used.
func TestMTWaitReadyBypassVsFIFO(t *testing.T) {
	remote := tx.SignedTransaction{TxID: "remote", AccessList: []tx.AccessItem{{Key: "asset:remote", Mode: tx.AccessRead}}}
	local := tx.SignedTransaction{TxID: "local", AccessList: []tx.AccessItem{{Key: "asset:local", Mode: tx.AccessRead}}}
	items := []tx.SignedTransaction{remote, local}
	block := realblock.Block{Height: 1, ShardID: "s1", TxList: items, TxIDs: []string{"remote", "local"}}
	classification := BatchClassificationResult{
		Decisions:     map[string]ExecutionDecision{"remote": {Track: "fast", Reason: "eligible"}, "local": {Track: "fast", Reason: "eligible"}},
		Dependencies:  map[string][]string{"remote": {}, "local": {}},
		StateWaitKeys: map[string][]string{"remote": {"k:asset:remote"}},
	}
	fetch := func(ctx context.Context, item tx.SignedTransaction, access tx.AccessItem) (RemoteStateReadyEvent, error) {
		select {
		case <-ctx.Done():
			return RemoteStateReadyEvent{}, ctx.Err()
		case <-time.After(35 * time.Millisecond):
		}
		return RemoteStateReadyEvent{TxID: item.TxID, Key: access.Key, ReadinessToken: stateReadinessToken(item, access), Value: "7", HomeShard: "s0", LatencyMS: 35}, nil
	}
	for _, tc := range []struct {
		name, policy string
		bypass       bool
	}{
		{"bypass", metaTrackWaitReadyPolicy, true},
		{"fifo_parallel", metaTrackWaitFIFOControlPolicy, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			schedule := ScheduleResult{Ordered: items, ReadyPriorityPolicy: tc.policy}
			events, metrics, outcomes, _, err := executeMetaTrackScheduleWithFullLocality(ctx, schedule, classification, block,
				map[string]string{"s1::asset:local": "3"}, 2, 0, fetch, nil, nil, false, false, true, false, false, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(outcomes) != 2 {
				t.Fatalf("outcomes=%d", len(outcomes))
			}
			localDispatch, remoteReady := -1, -1
			for i, event := range events {
				if event.TxID == "local" && strings.HasPrefix(event.DecisionReason, "actual_dispatch") {
					localDispatch = i
				}
				if event.TxID == "remote" && strings.HasPrefix(event.DecisionReason, "actual_state_ready:") {
					remoteReady = i
				}
			}
			if localDispatch < 0 || remoteReady < 0 {
				t.Fatalf("missing dispatch/stateReady: %#v", events)
			}
			if (localDispatch < remoteReady) != tc.bypass {
				t.Fatalf("bypass=%v; local dispatch=%d remote StateReady=%d", tc.bypass, localDispatch, remoteReady)
			}
			count := intMetric(t, metrics, "metatrack_candidate_ready_bypass_dispatch_count")
			if tc.bypass && count < 1 {
				t.Fatalf("expected actual bypass count, got %d", count)
			}
			if !tc.bypass && count != 0 {
				t.Fatalf("FIFO bypass count=%d", count)
			}
			if !tc.bypass && metrics["metatrack_single_fifo_ready_blocked_sum_ms"].(float64) <= 0 {
				t.Fatalf("missing FIFO HOL evidence: %#v", metrics)
			}
		})
	}
}
