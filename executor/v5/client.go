package v5

import (
	"context"
	"fmt"
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
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	shards := len(leaders)
	if shards == 0 {
		return fmt.Errorf("plan contains no shard leaders")
	}
	for index := 0; index < plan.WorkloadPlan.TxCount; index++ {
		shardIDs := make([]string, 0, shards)
		for shardIndex := 0; shardIndex < shards; shardIndex++ {
			shardIDs = append(shardIDs, fmt.Sprintf("s%d", shardIndex))
		}
		workload := plugins.Workload.BuildWorkloadItem(WorkloadInput{Index: index, Shards: shards, Seed: plan.WorkloadPlan.Seed, TimeoutEvery: plan.WorkloadPlan.TimeoutEvery, CrossShard: plan.WorkloadPlan.CrossShardRatio > 0 && index < shards})
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
			targetIndex := 0
			for candidateIndex, candidate := range shardIDs {
				if candidate == shardID {
					targetIndex = (candidateIndex + 1) % shards
					break
				}
			}
			payload = "v5_cross:" + shardIDs[targetIndex]
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
		rows = append(rows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, sender, leader.NodeID, shardID, payload, fmt.Sprint(err == nil), fmt.Sprint(time.Since(start).Milliseconds()), errorString(err)})
		lifecycleRows = append(lifecycleRows, lifecycleRow(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: item.TxID, Stage: "submitted", NodeID: "mbe-client", ShardID: shardID, Success: err == nil, Error: errorString(err)}))
		routingRows = append(routingRows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, plugins.Routing.ID(), strings.Join(item.StateKeys, "|"), shardID, fmt.Sprint(strings.HasPrefix(payload, "v5_cross:")), route.Reason})
		if err != nil {
			return err
		}
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "submitted", "latency_ms", "error"}, rows); err != nil {
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
	return SaveJSON(filepath.Join(outDir, "client_submission_complete.json"), map[string]any{"submitted_unique_logical_tx_count": len(rows), "submitted_tx_count": len(rows), "rejected_during_submission": 0, "first_submitted_at": rows[0][0], "last_submitted_at": rows[len(rows)-1][0], "submission_finished_at": fmt.Sprint(time.Now().UnixMilli())})
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
