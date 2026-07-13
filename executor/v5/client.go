package v5

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"path/filepath"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func SubmitWorkload(ctx context.Context, plan Plan, outDir string) error {
	if len(plan.NodeConfigs) == 0 {
		return fmt.Errorf("plan contains no nodes")
	}
	plugins, err := InstantiatePlugins(plan.NodeConfigs[0].PluginProfile)
	if err != nil {
		return err
	}
	leaders := map[string]NodePlan{}
	for _, node := range plan.NodeConfigs {
		if node.Leader {
			leaders[node.ShardID] = node
		}
	}
	rows := [][]string{}
	routingRows := [][]string{}
	lifecycleRows := [][]string{}
	connections := map[string]net.Conn{}
	generatedCrossShardCount := 0
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	shards := len(leaders)
	if shards == 0 {
		return fmt.Errorf("plan contains no shard leaders")
	}
	if shards < 2 && plan.WorkloadPlan.CrossShardRatio > 0 {
		return fmt.Errorf("cross_shard_ratio requires at least 2 shards")
	}
	for index := 0; index < plan.WorkloadPlan.TxCount; index++ {
		shardIDs := make([]string, 0, shards)
		for shardIndex := 0; shardIndex < shards; shardIndex++ {
			shardIDs = append(shardIDs, fmt.Sprintf("s%d", shardIndex))
		}
		crossShard := crossShardAt(index, plan.WorkloadPlan.TxCount, plan.WorkloadPlan.CrossShardRatio, plan.WorkloadPlan.Seed)
		workload := plugins.Workload.BuildWorkloadItem(WorkloadInput{Index: index, Shards: shards, Seed: plan.WorkloadPlan.Seed, TimeoutEvery: plan.WorkloadPlan.TimeoutEvery, CrossShard: crossShard})
		route := plugins.Routing.Route(RoutingInput{Index: index, StateKeys: workload.StateKeys, ShardIDs: shardIDs, CrossShard: strings.HasPrefix(workload.Payload, "v5_cross")})
		shardID := route.ShardID
		leader, ok := leaders[shardID]
		if !ok {
			return fmt.Errorf("no leader for %s", shardID)
		}
		// Independent deterministic senders keep admission outcome independent of
		// cross-process gossip ordering; each signed transaction starts at nonce 0.
		sender := fmt.Sprintf("client_%s_%d", shardID, index)
		payload := workload.Payload
		if payload == "v5_cross" {
			generatedCrossShardCount++
			targetIndex := 0
			for candidateIndex, candidate := range shardIDs {
				if candidate == shardID {
					targetIndex = (candidateIndex + 1) % shards
					break
				}
			}
			payload = "v5_cross:" + shardIDs[targetIndex]
		}
		isCrossShard := strings.HasPrefix(payload, "v5_cross:")
		targetShard := ""
		if isCrossShard {
			targetShard = strings.TrimPrefix(payload, "v5_cross:")
		}
		seed := fmt.Sprintf("%d:%s", plan.WorkloadPlan.Seed, shardID)
		stateKeys := append([]string{"shard:" + shardID + ":account"}, workload.StateKeys...)
		generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sender, Receiver: "receiver_" + shardID, StartNonce: 0, Value: 1, StateKeys: stateKeys, Seed: seed})
		if err != nil {
			return err
		}
		item := generated[0]
		item.Payload = payload
		_, privateKey := tx.DeterministicKeyPair(seed + ":" + sender)
		if err := tx.Sign(&item, privateKey); err != nil {
			return err
		}
		envelope, err := p2p.NewEnvelope(p2p.MessageTXGossip, "mbe-client", leader.NodeID, shardID, 0, 0, 0, item)
		if err != nil {
			return err
		}
		start := time.Now()
		err = sendPersistent(ctx, connections, leader.ListenAddr, envelope)
		rows = append(rows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, sender, leader.NodeID, shardID, payload, fmt.Sprint(isCrossShard), shardID, targetShard, fmt.Sprint(err == nil), fmt.Sprint(time.Since(start).Milliseconds()), errorString(err)})
		lifecycleRows = append(lifecycleRows, lifecycleRow(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "submitted", NodeID: "mbe-client", ShardID: shardID, Success: err == nil, Error: errorString(err)}))
		routingRows = append(routingRows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, plugins.Routing.ID(), strings.Join(item.StateKeys, "|"), shardID, fmt.Sprint(strings.HasPrefix(payload, "v5_cross:")), route.Reason})
		if err != nil {
			return err
		}
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, rows); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "client_lifecycle.csv"), []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}, lifecycleRows); err != nil {
		return err
	}
	clientEvents := make([]LifecycleEvent, 0, len(lifecycleRows))
	for _, row := range lifecycleRows {
		clientEvents = append(clientEvents, LifecycleEvent{TxID: row[1], LogicalTxID: row[2], Stage: row[3], NodeID: row[4], ShardID: row[5], Success: row[9] == "true"})
	}
	if err := writeLifecycleJSONL(filepath.Join(outDir, "transaction_lifecycle.jsonl"), clientEvents); err != nil {
		return err
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "routing_decision_log.csv"), []string{"timestamp", "tx_id", "routing_plugin", "access_keys", "assigned_shard", "cross_shard", "source"}, routingRows); err != nil {
		return err
	}
	requestedCrossShardCount := requestedCrossShardCount(plan.WorkloadPlan.TxCount, plan.WorkloadPlan.CrossShardRatio)
	return SaveJSON(filepath.Join(outDir, "client_submission_complete.json"), map[string]any{"submitted_unique_logical_tx_count": len(rows), "submitted_tx_count": len(rows), "rejected_during_submission": 0, "first_submitted_at": rows[0][0], "last_submitted_at": rows[len(rows)-1][0], "submission_finished_at": fmt.Sprint(time.Now().UnixMilli()), "requested_cross_shard_ratio": plan.WorkloadPlan.CrossShardRatio, "requested_cross_shard_count": requestedCrossShardCount, "generated_cross_shard_count": generatedCrossShardCount, "observed_cross_shard_ratio": float64(generatedCrossShardCount) / float64(len(rows))})
}

func crossShardAt(index, total int, ratio float64, seed int) bool {
	if index < 0 || total <= 0 || ratio <= 0 {
		return false
	}
	target := requestedCrossShardCount(total, ratio)
	if target <= 0 {
		return false
	}
	if target >= total {
		return true
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("cross-shard:%d:%d", seed, total)))
	offset := int(binary.BigEndian.Uint64(digest[:8]) % uint64(total))
	step := int(binary.BigEndian.Uint64(digest[8:16])%uint64(total-1)) + 1
	for gcd(step, total) != 1 {
		step++
		if step >= total {
			step = 1
		}
	}
	position := (offset + (index * step)) % total
	return position < target
}

func requestedCrossShardCount(total int, ratio float64) int {
	return int(math.Floor(float64(total)*ratio + 0.5))
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func sendPersistent(ctx context.Context, connections map[string]net.Conn, address string, message p2p.MessageEnvelope) error {
	if conn := connections[address]; conn != nil {
		if err := p2p.Encode(conn, message); err == nil {
			return nil
		}
		_ = conn.Close()
		delete(connections, address)
	}
	var last error
	for attempt := 0; attempt < 32; attempt++ {
		dialer := net.Dialer{Timeout: 2 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err == nil {
			err = p2p.Encode(conn, message)
			if err == nil {
				connections[address] = conn
				return nil
			}
			_ = conn.Close()
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 3 * time.Millisecond):
		}
	}
	return last
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
