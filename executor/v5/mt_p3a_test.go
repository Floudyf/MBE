package v5

import (
	"context"
	"fmt"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMTP3ALegacyNoTaskUsesOwnedWaveSnapshot(t *testing.T) {
	_, s1, key := statelessVersionAdmissionTestRuntimes(t)
	if !s1.genericStatelessRemoteStateEnabled() {
		t.Fatal("stateless remote access not enabled in fixture")
	}
	view := map[string]string{"s1::local": "old"}
	block := realblock.Block{ShardID: "s1", BlockHash: "p3a-empty", Height: 2}
	out, err := s1.prepareLegacyRemoteStateSnapshot(context.Background(), block, view)
	if err != nil {
		t.Fatal(err)
	}
	out["s1::local"] = "new"
	if view["s1::local"] != "new" {
		t.Fatal("legacy versioned-wave no-task path copied the owned snapshot")
	}
	// A versioned remote read is excluded from legacy unversioned prefetch.
	item := statelessVersionAdmissionItem("versioned", "s1", key, 2, 1)
	block.TxList = []tx.SignedTransaction{item}
	out, err = s1.prepareLegacyRemoteStateSnapshot(context.Background(), block, view)
	if err != nil {
		t.Fatal(err)
	}
	out["s1::local"] = "versioned"
	if view["s1::local"] != "versioned" {
		t.Fatal("legacy prefetch attempted an extra remote task for an exact-version access")
	}
}

func TestMTP3AGenericNoTaskRetainsIndependentCopy(t *testing.T) {
	_, s1, _ := statelessVersionAdmissionTestRuntimes(t)
	view := map[string]string{"s1::local": "old"}
	block := realblock.Block{ShardID: "s1", BlockHash: "p3a-generic", Height: 2}
	out, err := s1.prepareMetaTrackStateSnapshot(context.Background(), block, view)
	if err != nil {
		t.Fatal(err)
	}
	if out["s1::local"] != view["s1::local"] {
		t.Fatal("copy changed values")
	}
	out["s1::local"] = "new"
	if view["s1::local"] != "old" {
		t.Fatal("generic prefetch must still return an independent copy")
	}
}

func TestMTP3ARemoteTaskRetainsOverlayIsolation(t *testing.T) {
	_, s1, remoteKey := statelessVersionAdmissionTestRuntimes(t)
	s1.sendToNodeHook = func(_ context.Context, nodeID string, env p2p.MessageEnvelope) error {
		if nodeID != "n-s0" || env.MessageType != stateFetchRequestMessage {
			return fmt.Errorf("unexpected remote task %s %s", nodeID, env.MessageType)
		}
		req, err := p2p.DecodePayload[StateFetchRequest](env)
		if err != nil {
			return err
		}
		resp := StateFetchResponse{
			RequestID: req.RequestID, TxID: req.TxID, BlockHash: req.BlockHash,
			Key: req.Key, QualifiedKey: req.HomeShard + "::" + req.Key,
			Value: "remote-value", HomeShard: req.HomeShard,
			ExecutionShard: req.ExecutionShard, StateRoot: "test-root", Success: true,
			StateVersion: req.RequiredVersion, Versioned: req.Versioned,
		}
		resp.WitnessDigest = stateFetchWitnessDigest(resp, req.AccessKind)
		s1.handleStateFetchResponse(resp)
		return nil
	}
	block := realblock.Block{
		ShardID: "s1", BlockHash: "p3a-fetch", Height: 2,
		TxList: []tx.SignedTransaction{{TxID: "fetch", AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead}}}},
	}
	view := map[string]string{"s1::local": "old"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := s1.prepareLegacyRemoteStateSnapshot(ctx, block, view)
	if err != nil {
		t.Fatal(err)
	}
	if out["s1::"+remoteKey] != "remote-value" {
		t.Fatal("remote fetch overlay missing")
	}
	if _, ok := view["s1::"+remoteKey]; ok {
		t.Fatal("remote fetch mutated caller's view")
	}
	out["s1::local"] = "new"
	if view["s1::local"] != "old" {
		t.Fatal("remote fetch did not isolate view")
	}
}
