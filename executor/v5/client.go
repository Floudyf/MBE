package v5

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	metatrackBatchRows := []map[string]any{}
	accessMatrixRows := [][]string{}
	stateFrequencyRows := [][]string{}
	coaccessRows := [][]string{}
	placementRows := [][]string{}
	transactionPlacementRows := [][]string{}
	dependencyRows := [][]string{}
	remoteStateRows := [][]string{}
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
	shardIDs := make([]string, 0, shards)
	for shardIndex := 0; shardIndex < shards; shardIndex++ {
		shardIDs = append(shardIDs, fmt.Sprintf("s%d", shardIndex))
	}
	iterator, err := plugins.Workload.NewIterator(plan.WorkloadPlan, shards, outDir)
	if err != nil {
		return err
	}
	defer iterator.Close()
	batchSize := plugins.BlockProducer.BlockSize()
	if batchSize < 1 {
		batchSize = 1
	}
	batchIndex := 0
	batch := []WorkloadRecord{}
	submitRecord := func(record WorkloadRecord, route RoutingDecision) error {
		executionShard := route.ShardID
		shardID := workloadIngressShard(record, route)
		leader, ok := leaders[shardID]
		if !ok {
			return fmt.Errorf("no leader for %s", shardID)
		}
		sender := fmt.Sprintf("client_%s_%d", shardID, record.Index)
		payload := record.Payload
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
		targetShard := ""
		if strings.HasPrefix(payload, "v5_cross:") {
			targetShard = strings.TrimPrefix(payload, "v5_cross:")
			if colon := strings.Index(targetShard, ":"); colon >= 0 {
				targetShard = targetShard[:colon]
			}
		}
		isCrossShard := targetShard != "" && targetShard != shardID
		stateKeys := append([]string{"shard:" + shardID + ":account"}, record.StateKeys...)
		var item tx.SignedTransaction
		var err error
		if datasetIterator, ok := iterator.(*CanonicalTraceIterator); ok {
			record.StateKeys = stateKeys
			item, err = datasetIterator.SignedTransaction(record)
			sender = item.Sender
			generatedCrossShardCount = datasetIterator.summary.ActualCrossShardCount
		} else {
			seed := fmt.Sprintf("%d:%s", plan.WorkloadPlan.Seed, shardID)
			accessList := syntheticSignedAccessList(sender, "receiver_"+shardID, record.AccessList)
			generated, _, _, genErr := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sender, Receiver: "receiver_" + shardID, StartNonce: 0, Value: 1, StateKeys: stateKeys, AccessList: accessList, Seed: seed})
			err = genErr
			if err == nil {
				item = generated[0]
				item.Payload = payload
				_, privateKey := tx.DeterministicKeyPair(seed + ":" + sender)
				err = tx.Sign(&item, privateKey)
			}
		}
		if err != nil {
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
		reason := route.Reason
		if executionShard != "" && executionShard != shardID {
			reason = strings.TrimSuffix(reason+";execution_shard="+executionShard, ";")
		}
		routingRows = append(routingRows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, plugins.Routing.ID(), strings.Join(item.StateKeys, "|"), shardID, fmt.Sprint(isCrossShard), reason})
		return err
	}
	submitBatch := func(records []WorkloadRecord) error {
		if len(records) == 0 {
			return nil
		}
		decisions := map[int]RoutingDecision{}
		if planner, ok := plugins.Routing.(BatchRoutingPlugin); ok {
			routingRecords := records
			if datasetIterator, ok := iterator.(*CanonicalTraceIterator); ok {
				routingRecords = make([]WorkloadRecord, 0, len(records))
				for _, record := range records {
					next := record
					next.AccessList = canonicalRuntimeAccessList(datasetIterator.plan, record)
					routingRecords = append(routingRecords, next)
				}
			}
			routePlan := planner.PlanBatch(BatchRoutingInput{BatchIndex: batchIndex, Records: routingRecords, ShardIDs: shardIDs})
			appendMetaTrackArtifacts(routePlan, &metatrackBatchRows, &accessMatrixRows, &stateFrequencyRows, &coaccessRows, &placementRows, &transactionPlacementRows, &dependencyRows, &remoteStateRows)
			for _, placement := range routePlan.TransactionPlacements {
				decisions[placement.TxIndex] = RoutingDecision{ShardID: placement.ExecutionShard, Reason: placement.Reason}
			}
		}
		for _, record := range records {
			route := decisions[record.Index]
			if route.ShardID == "" {
				route = plugins.Routing.Route(RoutingInput{Index: record.Index, StateKeys: record.StateKeys, AccessList: record.AccessList, SourceShard: record.SourceShard, ShardIDs: shardIDs, CrossShard: record.CrossShard})
			}
			if err := submitRecord(record, route); err != nil {
				return err
			}
		}
		batchIndex++
		return nil
	}
	for {
		record, err := iterator.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		batch = append(batch, record)
		if len(batch) >= batchSize {
			if err := submitBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := submitBatch(batch); err != nil {
		return err
	}
	replaySummary := iterator.Summary()
	replaySummary.SubmittedCount = len(rows)
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
	if len(metatrackBatchRows) > 0 {
		if err := writeJSONL(filepath.Join(outDir, "metatrack_batch_plan.jsonl"), metatrackBatchRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "access_matrix_summary.csv"), []string{"batch_index", "logical_id", "tx_index", "state_key", "mode"}, accessMatrixRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "state_frequency.csv"), []string{"batch_index", "state_key", "frequency", "read_count", "write_count"}, stateFrequencyRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "coaccess_matrix_edges.csv"), []string{"batch_index", "left_key", "right_key", "weight"}, coaccessRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "placement_plan.csv"), []string{"batch_index", "state_key", "home_shard", "execution_shard", "frequency", "reason"}, placementRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "transaction_placement.csv"), []string{"batch_index", "logical_id", "tx_index", "home_shard", "execution_shard", "target_shard", "coaccess_group", "remote_access_count", "reason"}, transactionPlacementRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "dependency_graph.csv"), []string{"batch_index", "from_logical_id", "to_logical_id", "state_key", "dependency_type"}, dependencyRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "remote_state_access.csv"), []string{"batch_index", "logical_id", "tx_index", "state_key", "home_shard", "execution_shard", "access_kind", "witness_digest"}, remoteStateRows); err != nil {
			return err
		}
	}
	requestedCrossShardCount := requestedCrossShardCount(plan.WorkloadPlan.TxCount, plan.WorkloadPlan.CrossShardRatio)
	if plan.WorkloadPlan.SourceType == "dataset" {
		requestedCrossShardCount = replaySummary.ExpectedCrossShardCount
	}
	if err := SaveJSON(filepath.Join(outDir, "workload_replay_summary.json"), replaySummary); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(outDir, "workload_identity_mapping_summary.json"), map[string]any{"identity_count": replaySummary.IdentityCount, "mapping_digest": replaySummary.MappingDigest, "nonce_continuity": replaySummary.NonceContinuity, "signature_pass_count": replaySummary.SignaturePassCount, "identity_mapping_version": replaySummary.IdentityMappingVersion}); err != nil {
		return err
	}
	return SaveJSON(filepath.Join(outDir, "client_submission_complete.json"), map[string]any{"submitted_unique_logical_tx_count": len(rows), "submitted_tx_count": len(rows), "rejected_during_submission": 0, "first_submitted_at": rows[0][0], "last_submitted_at": rows[len(rows)-1][0], "submission_finished_at": fmt.Sprint(time.Now().UnixMilli()), "requested_cross_shard_ratio": plan.WorkloadPlan.CrossShardRatio, "requested_cross_shard_count": requestedCrossShardCount, "generated_cross_shard_count": generatedCrossShardCount, "observed_cross_shard_ratio": float64(generatedCrossShardCount) / float64(len(rows))})
}

func workloadIngressShard(record WorkloadRecord, route RoutingDecision) string {
	if record.CrossShard && record.SourceShard != "" {
		return record.SourceShard
	}
	return route.ShardID
}

func syntheticSignedAccessList(sender, receiver string, declared []tx.AccessItem) []tx.AccessItem {
	if isPureCommutativeDeltaAccess(declared) {
		return append([]tx.AccessItem(nil), declared...)
	}
	accessList := tx.DefaultTransferAccessList(sender, receiver)
	accessList = append(accessList, declared...)
	return accessList
}

func isPureCommutativeDeltaAccess(items []tx.AccessItem) bool {
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if item.Mode == tx.AccessRead {
			continue
		}
		if item.Mode != tx.AccessCommutativeDelta {
			return false
		}
	}
	return true
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

func appendMetaTrackArtifacts(plan BatchRoutingPlan, planRows *[]map[string]any, accessRows, frequencyRows, coaccessRows, placementRows, transactionRows, dependencyRows, remoteStateRows *[][]string) {
	*planRows = append(*planRows, map[string]any{
		"batch_index":            plan.BatchIndex,
		"plan_digest":            plan.PlanDigest,
		"transaction_count":      len(plan.TransactionPlacements),
		"state_key_count":        len(plan.StateFrequency),
		"coaccess_edge_count":    len(plan.CoaccessEdges),
		"remote_access_estimate": plan.RemoteAccessEstimate,
		"routing_overhead":       plan.RoutingOverhead,
		"shard_load_before":      plan.ShardLoadBefore,
		"shard_load_after":       plan.ShardLoadAfter,
	})
	for _, row := range plan.AccessMatrix {
		*accessRows = append(*accessRows, []string{fmt.Sprint(plan.BatchIndex), row.LogicalID, fmt.Sprint(row.TxIndex), row.Key, string(row.Mode)})
	}
	for _, row := range plan.StateFrequency {
		*frequencyRows = append(*frequencyRows, []string{fmt.Sprint(plan.BatchIndex), row.Key, fmt.Sprint(row.Frequency), fmt.Sprint(row.ReadCount), fmt.Sprint(row.WriteCount)})
	}
	for _, row := range plan.CoaccessEdges {
		*coaccessRows = append(*coaccessRows, []string{fmt.Sprint(plan.BatchIndex), row.LeftKey, row.RightKey, fmt.Sprint(row.Weight)})
	}
	for _, row := range plan.StatePlacements {
		*placementRows = append(*placementRows, []string{fmt.Sprint(plan.BatchIndex), row.Key, row.HomeShard, row.ExecutionShard, fmt.Sprint(row.Frequency), row.Reason})
	}
	for _, row := range plan.TransactionPlacements {
		*transactionRows = append(*transactionRows, []string{fmt.Sprint(plan.BatchIndex), row.LogicalID, fmt.Sprint(row.TxIndex), row.HomeShard, row.ExecutionShard, row.TargetShard, row.CoaccessGroup, fmt.Sprint(row.RemoteAccessCount), row.Reason})
	}
	placementByKey := map[string]StatePlacement{}
	for _, row := range plan.StatePlacements {
		placementByKey[row.Key] = row
	}
	accessByTx := map[int][]AccessMatrixRow{}
	placementByTx := map[int]TransactionPlacement{}
	for _, row := range plan.AccessMatrix {
		accessByTx[row.TxIndex] = append(accessByTx[row.TxIndex], row)
	}
	for _, row := range plan.TransactionPlacements {
		placementByTx[row.TxIndex] = row
	}
	for left := 0; left < len(plan.TransactionPlacements); left++ {
		from := plan.TransactionPlacements[left]
		for right := left + 1; right < len(plan.TransactionPlacements); right++ {
			to := plan.TransactionPlacements[right]
			for _, dependency := range dependencyEdgesForTransactions(accessByTx[from.TxIndex], accessByTx[to.TxIndex]) {
				*dependencyRows = append(*dependencyRows, []string{fmt.Sprint(plan.BatchIndex), from.LogicalID, to.LogicalID, dependency[0], dependency[1]})
			}
		}
	}
	for txIndex, rows := range accessByTx {
		txPlacement := placementByTx[txIndex]
		for _, row := range rows {
			statePlacement, ok := placementByKey[row.Key]
			if !ok || statePlacement.HomeShard == txPlacement.ExecutionShard {
				continue
			}
			witness := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s:%s", plan.BatchIndex, row.LogicalID, row.Key, statePlacement.HomeShard)))
			*remoteStateRows = append(*remoteStateRows, []string{fmt.Sprint(plan.BatchIndex), row.LogicalID, fmt.Sprint(row.TxIndex), row.Key, statePlacement.HomeShard, txPlacement.ExecutionShard, string(row.Mode), hex.EncodeToString(witness[:])})
		}
	}
}

func dependencyEdgesForTransactions(left, right []AccessMatrixRow) [][2]string {
	edges := [][2]string{}
	for _, l := range left {
		for _, r := range right {
			if l.Key != r.Key {
				continue
			}
			if isWriteMode(l.Mode) && isWriteMode(r.Mode) {
				edges = append(edges, [2]string{l.Key, "write_write"})
			} else if isWriteMode(l.Mode) || isWriteMode(r.Mode) {
				edges = append(edges, [2]string{l.Key, "read_write"})
			}
		}
	}
	return edges
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
