package node

import (
	"context"
	"testing"
	"time"
)

func TestV42FinalSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	summary, artifacts, err := RunV42FinalSmoke(ctx, SmokeOptionsV42{OutDir: t.TempDir(), Nodes: 4, Shards: 1, TxCount: 10, EnableCrossShard: true, EnableFaults: true, FrontendAvailable: true})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.ReadyToCommit || !summary.DeterministicExecution || !summary.PersistentStateDB || !summary.RealCrossShardStateMachine || !summary.RecoverySupported || !summary.FaultInjectionSupported {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.ProductionPBFT || summary.FullByzantineSecurity || summary.ProductionBlockchain {
		t.Fatalf("invalid production claims: %+v", summary)
	}
	if len(artifacts) == 0 {
		t.Fatalf("expected artifacts")
	}
}
