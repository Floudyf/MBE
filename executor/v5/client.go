package v5

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func SubmitWorkload(ctx context.Context, plan Plan, outDir string) error {
	leaders := map[string]NodePlan{}
	for _, node := range plan.NodeConfigs {
		if node.Leader {
			leaders[node.ShardID] = node
		}
	}
	rows := [][]string{}
	routingRows := [][]string{}
	nonces := map[string]uint64{}
	shards := len(leaders)
	if shards == 0 {
		return fmt.Errorf("plan contains no shard leaders")
	}
	for index := 0; index < plan.WorkloadPlan.TxCount; index++ {
		shardIndex := index % shards
		if plan.NodeConfigs[0].PluginProfile["routing"].PluginID == "metatrack_coaccess_routing" {
			shardIndex = (index*3 + 1) % shards
		}
		shardID := fmt.Sprintf("s%d", shardIndex)
		leader, ok := leaders[shardID]
		if !ok {
			return fmt.Errorf("no leader for %s", shardID)
		}
		sender := "client_" + shardID
		payload := "v5_safe"
		if index%8 == 2 || index%8 == 3 {
			payload = "v5_commutative"
		}
		if index%8 == 4 {
			payload = "v5_conflict"
		}
		// Seed one valid signed cross-shard transaction per source shard before later
		// local nonces advance. Relay admission remains subject to signature and nonce checks.
		if plan.WorkloadPlan.CrossShardRatio > 0 && index < shards && shards > 1 {
			payload = fmt.Sprintf("v5_cross:s%d", (shardIndex+1)%shards)
		}
		if plan.WorkloadPlan.TimeoutEvery > 0 && (index+1)%plan.WorkloadPlan.TimeoutEvery == 0 {
			payload = "v5_timeout"
		}
		// Keep one deterministic account per source shard so its signed nonces form a valid sequence.
		seed := fmt.Sprintf("%d:%s", plan.WorkloadPlan.Seed, shardID)
		stateKeys := []string{"shard:" + shardID + ":account", "asset:" + strconv.Itoa(index)}
		if strings.Contains(payload, "v5_commutative") {
			stateKeys = []string{"shard:" + shardID + ":account", "coaccess:hot-update"}
		}
		if strings.Contains(payload, "v5_conflict") {
			stateKeys = []string{"shard:" + shardID + ":account", "coaccess:conflict"}
		}
		generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sender, Receiver: "receiver_" + shardID, StartNonce: nonces[sender], Value: 1, StateKeys: stateKeys, Seed: seed})
		if err != nil {
			return err
		}
		nonces[sender]++
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
		err = send(ctx, leader.ListenAddr, envelope)
		rows = append(rows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, sender, leader.NodeID, shardID, payload, fmt.Sprint(err == nil), fmt.Sprint(time.Since(start).Milliseconds()), errorString(err)})
		routingRows = append(routingRows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, plan.NodeConfigs[0].PluginProfile["routing"].PluginID, strings.Join(item.StateKeys, "|"), shardID, fmt.Sprint(strings.HasPrefix(payload, "v5_cross:")), "real_client_routing"})
		if err != nil {
			return err
		}
	}
	if err := metrics.WriteCSV(filepath.Join(outDir, "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "submitted", "latency_ms", "error"}, rows); err != nil {
		return err
	}
	return metrics.WriteCSV(filepath.Join(outDir, "routing_decision_log.csv"), []string{"timestamp", "tx_id", "routing_plugin", "access_keys", "assigned_shard", "cross_shard", "source"}, routingRows)
}

func send(ctx context.Context, address string, message p2p.MessageEnvelope) error {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	return p2p.Encode(conn, message)
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
