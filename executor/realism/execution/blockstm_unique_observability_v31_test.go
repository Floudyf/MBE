package execution

import (
	"context"
	"testing"
	"time"
)

func TestV31BlockSTMUniqueAbortAndReexecutionCounts(t *testing.T) {
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "v31-unique-hot", 8, "alice", "bob", 1, "v5_safe"))
	executor := NewBlockSTMExecutor(8)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := executor.ExecuteBlock(ctx, b, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	m := executor.Metrics
	if m.UniqueAbortedTransactionCount < 0 || m.UniqueAbortedTransactionCount > len(b.TxList) || m.UniqueAbortedTransactionCount > m.AbortCount {
		t.Fatalf("invalid unique abort count: %+v", m)
	}
	if m.UniqueReexecutedTransactionCount < 0 || m.UniqueReexecutedTransactionCount > len(b.TxList) || m.UniqueReexecutedTransactionCount > m.ReexecutionCount {
		t.Fatalf("invalid unique reexecution count: %+v", m)
	}
	if m.AbortCount > 0 && m.UniqueAbortedTransactionCount == 0 {
		t.Fatalf("abort events exist without a unique aborted transaction: %+v", m)
	}
	if m.ReexecutionCount > 0 && m.UniqueReexecutedTransactionCount == 0 {
		t.Fatalf("reexecution events exist without a unique reexecuted transaction: %+v", m)
	}
}
