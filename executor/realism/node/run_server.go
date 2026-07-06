package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

type AddressTableFile struct {
	Entries []p2p.Peer `json:"entries"`
}

func RunServer(ctx context.Context, cfg Config) (RuntimeSummaryV41, error) {
	if cfg.NodeID == "" {
		return RuntimeSummaryV41{}, fmt.Errorf("node_id is required")
	}
	if cfg.ShardID == "" {
		return RuntimeSummaryV41{}, fmt.Errorf("shard_id is required")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(".cache", "v4_realism_runs", cfg.NodeID)
	}
	if cfg.RunMode == "" {
		cfg.RunMode = "server"
	}
	if cfg.RunDurationMS <= 0 {
		cfg.RunDurationMS = 1000
	}
	runtime := NewRuntimeV41(cfg)
	if err := runtime.Start(ctx); err != nil {
		return RuntimeSummaryV41{}, err
	}
	defer runtime.Stop()
	if cfg.InputJSONL != "" {
		if err := loadAndGossip(ctx, runtime, cfg.InputJSONL); err != nil {
			return RuntimeSummaryV41{}, err
		}
	}
	if cfg.Role == "leader" {
		time.Sleep(time.Duration(cfg.BlockIntervalMS) * time.Millisecond)
		_, _ = runtime.ProposeBlock(ctx)
	}
	time.Sleep(time.Duration(cfg.RunDurationMS) * time.Millisecond)
	if err := runtime.WriteArtifacts(cfg.DataDir); err != nil {
		return RuntimeSummaryV41{}, err
	}
	return runtime.Summary(), nil
}

func loadAndGossip(ctx context.Context, runtime *RuntimeV41, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open input jsonl: %w", err)
	}
	defer f.Close()
	return tx.ReadJSONL(f, func(item tx.SignedTransaction) error {
		result := runtime.node.Mempool.Admit(item)
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			return errors.New(result.RejectReason)
		}
		return runtime.GossipTx(ctx, item)
	})
}

func ParsePeers(value string, shardID string) []p2p.Peer {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	out := []p2p.Peer{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			continue
		}
		out = append(out, p2p.Peer{NodeID: pieces[0], ShardID: shardID, ListenAddr: pieces[1], Role: "validator"})
	}
	return out
}

func LoadPeersFromAddressTable(path string) ([]p2p.Peer, error) {
	if path == "" {
		return nil, nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read address table: %w", err)
	}
	var table AddressTableFile
	if err := json.Unmarshal(payload, &table); err != nil {
		return nil, fmt.Errorf("decode address table: %w", err)
	}
	return table.Entries, nil
}
