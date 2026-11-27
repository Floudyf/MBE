package v5

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMetaTrackHomePrefetchReceive(t *testing.T) {
	for _, fast := range []bool{false, true} {
		waiter := make(chan StateFetchResponse, 1)
		waiter <- StateFetchResponse{RequestID: "exact", Versioned: true, StateVersion: 17, Success: true}
		got, err := receiveMetaTrackBatchPrefetch(context.Background(), waiter, fast, time.Second)
		if err != nil || got.RequestID != "exact" || got.StateVersion != 17 {
			t.Fatalf("exact response changed: %+v, %v", got, err)
		}
		if _, err := receiveMetaTrackBatchPrefetch(context.Background(), waiter, fast, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("missing response must time out: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := receiveMetaTrackBatchPrefetch(ctx, waiter, fast, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("missing response must honor cancellation: %v", err)
		}
	}
}
