package v5

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

type submissionPacer struct {
	mode             string
	interval         time.Duration
	startedAt        time.Time
	nextOrdinal      uint64
	lateReleaseCount uint64
	maxScheduleLag   time.Duration
}

func newSubmissionPacer(plan WorkloadPlan) (*submissionPacer, error) {
	mode := strings.TrimSpace(plan.ReplayMode)
	if mode == "" {
		mode = "max_throughput"
	}
	switch mode {
	case "max_throughput":
		if plan.TargetSubmissionTPS != 0 {
			return nil, fmt.Errorf("max_throughput replay must not set target_submission_tps")
		}
		return &submissionPacer{mode: mode}, nil
	case "fixed_rate":
		if plan.TargetSubmissionTPS <= 0 {
			return nil, fmt.Errorf("fixed_rate replay requires positive target_submission_tps")
		}
		interval := time.Second / time.Duration(plan.TargetSubmissionTPS)
		if interval <= 0 {
			return nil, fmt.Errorf("target_submission_tps %d exceeds timer resolution", plan.TargetSubmissionTPS)
		}
		return &submissionPacer{mode: mode, interval: interval}, nil
	default:
		return nil, fmt.Errorf("unsupported replay_mode %q", mode)
	}
}

// Wait releases fixed-rate transactions on an absolute offered-load timeline.
// For target N, transaction ordinal i is eligible at start+i/N, so normal
// socket-write cost is absorbed inside the interval instead of being added to
// it. If one release becomes genuinely late, emit that transaction once, record
// the lag, and rebase the following slot to now+interval. Rebasing prevents a
// delayed sender from producing a catch-up burst into the transaction pool.
func (p *submissionPacer) Wait(ctx context.Context) error {
	if p == nil || p.mode != "fixed_rate" {
		return nil
	}
	now := time.Now()
	if p.startedAt.IsZero() {
		p.startedAt = now
		p.nextOrdinal = 1
		return nil
	}
	due := p.startedAt.Add(time.Duration(p.nextOrdinal) * p.interval)
	if delay := time.Until(due); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		p.nextOrdinal++
		return nil
	}
	lag := now.Sub(due)
	p.lateReleaseCount++
	if lag > p.maxScheduleLag {
		p.maxScheduleLag = lag
	}
	// Keep the current late release, but never replay missed slots back-to-back.
	// The next transaction becomes eligible one full interval after this release.
	p.startedAt = now
	p.nextOrdinal = 1
	return nil
}

func SubmitWorkload(ctx context.Context, plan Plan, outDir string) error {
	if len(plan.NodeConfigs) == 0 {
		return fmt.Errorf("plan contains no nodes")
	}
	pacer, err := newSubmissionPacer(plan.WorkloadPlan)
	if err != nil {
		return err
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
	placementScoreRows := [][]string{}
	transactionPlacementRows := [][]string{}
	dependencyRows := [][]string{}
	remoteStateRows := [][]string{}
	txalloPlacementRows := [][]string{}
	var txalloBootstrapEvidence map[string]any
	var txalloBootstrapMapping map[string]string
	txalloCrossByLogical := map[string]bool{}
	var txalloDynamic txalloDynamicAllocationRuntime
	var txalloDynamicEnabled bool
	var txalloFeedReader *txalloEpochFeedReaderV22
	var txalloBlockIndex *txalloDynamicBlockIndexV222
	resolvedAccessRows := []resolvedAccessEntry{}
	connections := map[string]net.Conn{}
	generatedCrossShardCount := 0
	var firstSubmissionTime time.Time
	var lastSubmissionTime time.Time
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	// MBE_PORYGON_UNIFIED_SHARD_V10_20260921: workload sharding follows execution shards, not PBFT leader count.
	executionSeen := map[string]bool{}
	for _, node := range plan.NodeConfigs {
		executionSeen[effectiveExecutionShardID(node)] = true
	}
	shardIDs := make([]string, 0, len(executionSeen))
	for shardID := range executionSeen {
		if shardID != "" {
			shardIDs = append(shardIDs, shardID)
		}
	}
	sort.Strings(shardIDs)
	shards := len(shardIDs)
	if shards == 0 {
		return fmt.Errorf("plan contains no execution shards")
	}
	if shards < 2 && plan.WorkloadPlan.CrossShardRatio > 0 {
		return fmt.Errorf("cross_shard_ratio requires at least 2 shards")
	}
	if bootstrapper, ok := plugins.Sharding.(HistoricalAllocationBootstrapper); ok {
		if err := bootstrapper.BootstrapHistoricalAllocation(ctx, HistoricalAllocationBootstrapInput{Plan: plan.WorkloadPlan, DataDir: outDir, ShardIDs: shardIDs}); err != nil {
			return fmt.Errorf("historical allocation bootstrap: %w", err)
		}
		txalloBootstrapEvidence = bootstrapper.HistoricalAllocationEvidence()
		if mapping, ok := plugins.Sharding.(txalloAccountMappingProvider); ok {
			txalloBootstrapMapping = mapping.TxAlloMappingSnapshot()
		}
	}
	if dynamic, ok := plugins.Sharding.(txalloDynamicAllocationRuntime); ok && dynamic.TxAlloDynamicEnabled() {
		if plan.WorkloadPlan.SourceType != "dataset" {
			return fmt.Errorf("TxAllo dynamic G/A lifecycle requires a dataset source-block sidecar")
		}
		txalloDynamic = dynamic
		txalloDynamicEnabled = true
		txalloFeedReader = newTxAlloEpochFeedReaderV22()
		var blockErr error
		txalloBlockIndex, blockErr = txalloLoadDynamicBlockIndexV222(outDir, plan.WorkloadPlan)
		if blockErr != nil {
			return blockErr
		}
		if err := txalloWaitMappingAcks(ctx, plan.NodeConfigs, txalloDynamic.TxAlloMappingEpoch(), txalloDynamic.TxAlloMappingStateDigest()); err != nil {
			return err
		}
	}
	iterator, err := plugins.Workload.NewIterator(plan.WorkloadPlan, shards, outDir, plugins.Sharding)
	if err != nil {
		return err
	}
	defer iterator.Close()
	streamPartitionInvariantV658 := false
	if cfg := metaTrackRoutingConfig(plugins.Routing); cfg != nil {
		streamPartitionInvariantV658 = boolFromAny(cfg["stream_partition_invariant_v658"])
	}
	batchSize := plugins.BlockProducer.BlockSize()
	if streamPartitionInvariantV658 {
		// Experimental mode is a true stream: one record is routed and signed at
		// a time, while the v6.5.1 routing state continues across calls.  The
		// RouteBatch fields remain evidence envelopes only, never an algorithmic
		// partition boundary.
		batchSize = 1
	} else if provider, ok := plugins.Routing.(routingBatchSizeProvider); ok {
		batchSize = provider.RoutingBatchSize(batchSize)
	}
	if batchSize < 1 {
		batchSize = 1
	}
	batchIndex := 0
	batch := []WorkloadRecord{}
	lastWriterOrdinal := map[string]uint64{}
	statelessDirect := usesStatelessDirectExecution(plugins.Routing)
	// MBE_PORYGON_UNIFIED_SHARD_V16_FINALITY_CAPABILITY_20260921: finality
	// capability is separate from payload/metadata runtime capabilities.
	finalityMode := crossShardFinalityMode(plugins.Routing)
	bindExecutionRouting := routingBindsExecutionMetadata(plugins.Routing)
	bindBatchProjectionMetadata := routingBindsBatchProjectionMetadata(plugins.Routing)
	// MBE_METATRACK_MECHPACK_V2_CLIENT_STREAMING
	// Signed predecessor/global-round truth is shared correctness and therefore
	// remains enabled even for the w/o-consensus-aggregation profile. Whether a
	// client buffers future RouteBatches is a separate concern.
	windowProducerV663, modularWindowV663 := plugins.BlockProducer.(metaTrackConsensusWindowProducer)
	transactionFrontierV656Enabled := isMetaTrackRoutingPlugin(plugins.Routing) && bindBatchProjectionMetadata
	leaderStreamingWindowV2 := transactionFrontierV656Enabled && plugins.BlockProducer != nil && plugins.BlockProducer.ID() == metaTrackNLWindowProducerV669ID
	clientWindowBufferingV6568 := transactionFrontierV656Enabled && modularWindowV663 && !leaderStreamingWindowV2 && !streamPartitionInvariantV658
	consensusPredecessorsV656 := newMetaTrackConsensusPredecessorTrackerV656()
	criticalWidthWindowV6568 := newMetaTrackCriticalWidthWindowPlannerV6568()

	submitRecord := func(record WorkloadRecord, route RoutingDecision) error {
		executionShard := route.ShardID
		if plugins.Routing.ID() == porygonRoutingID && strings.TrimSpace(record.RoutingSourceKey) != "" && len(shardIDs) > 0 {
			// Porygon Single-Shard Execution follows the initiating account/object
			// Storage Role, not the generic full-key workload SourceShard hash.
			executionShard = shardFor(plugins.Sharding, []string{record.RoutingSourceKey}, shardIDs)
			if strings.TrimSpace(executionShard) == "" {
				return fmt.Errorf("Porygon initiating account/object sharding returned no execution shard")
			}
		}
		shardID := workloadIngressShard(record, route, statelessDirect)
		if plugins.Routing.ID() == porygonRoutingID || plugins.Routing.ID() == calvinStatefulRoutingID || plugins.Routing.ID() == calvinStatelessRoutingID || optmeGlobalOrderingEnabled(plugins) {
			// Porygon and Calvin both separate logical execution/state partitions
			// from one physical PBFT ordering domain. Keep route.ShardID as the
			// logical workload/execution shard, but submit to that method's actual
			// consensus-domain leader. Other routing profiles are unchanged.
			for _, node := range plan.NodeConfigs {
				if effectiveExecutionShardID(node) == executionShard {
					shardID = effectiveConsensusDomainID(node)
					break
				}
			}
		}
		leader, ok := leaders[shardID]
		if !ok {
			return fmt.Errorf("no leader for %s", shardID)
		}
		payload := record.Payload
		logicalSourceShard := logicalSourceShardForRecord(record, route, plugins.Sharding, shardIDs)
		sender := fmt.Sprintf("client_%s_%d", logicalSourceShard, record.Index)
		targetShard := record.TargetShard
		if payload == "v5_cross" {
			generatedCrossShardCount++
			targetIndex := 0
			for candidateIndex, candidate := range shardIDs {
				if candidate == logicalSourceShard {
					targetIndex = (candidateIndex + 1) % shards
					break
				}
			}
			targetShard = shardIDs[targetIndex]
			payload = "v5_cross:" + targetShard
		}
		if targetShard == "" && strings.HasPrefix(payload, "v5_cross:") {
			targetShard = strings.TrimPrefix(payload, "v5_cross:")
			if colon := strings.Index(targetShard, ":"); colon >= 0 {
				targetShard = targetShard[:colon]
			}
		}
		isCrossShard := record.CrossShard || (targetShard != "" && targetShard != logicalSourceShard)
		if statelessDirect && isCrossShard {
			payload = statelessPayload(payload)
		}
		stateKeys := append([]string{"shard:" + logicalSourceShard + ":account"}, record.StateKeys...)
		var item tx.SignedTransaction
		var err error
		if plugins.Routing.ID() == porygonRoutingID {
			// Sign only the already-resolved Porygon initiating-account Storage Role.
			record.RoutingEpoch = 1
			if record.RoutingOrdinal == 0 {
				record.RoutingOrdinal = uint64(record.Index + 1)
			}
			record.ExecutionShard = executionShard
			record.RoutingReason = "porygon_initiating_account_route"
			record.RoutePlanDigest = stableTextDigest(fmt.Sprintf("porygon_route_v1|%d|%s|%s", record.Index, record.RoutingSourceKey, executionShard))
		}
		if datasetIterator, ok := iterator.(*CanonicalTraceIterator); ok {
			record.StateKeys = stateKeys
			record.Payload = payload
			item, err = datasetIterator.SignedTransaction(record)
			sender = item.Sender
			generatedCrossShardCount = datasetIterator.summary.ActualCrossShardCount
		} else {
			seed := fmt.Sprintf("%d:%s", plan.WorkloadPlan.Seed, logicalSourceShard)
			receiver := "receiver_" + logicalSourceShard
			accessList := syntheticSignedAccessList(sender, receiver, record.AccessList)
			generated, _, _, genErr := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sender, Receiver: receiver, StartNonce: 0, Value: 1, StateKeys: stateKeys, AccessList: accessList, Seed: seed})
			err = genErr
			if err == nil {
				item = generated[0]
				item.Payload = payload
				if (bindExecutionRouting || plugins.Routing.ID() == porygonRoutingID) && record.RoutePlanDigest != "" {
					routing := tx.ExecutionRoutingMetadata{SenderID: item.Sender, ReceiverID: item.Receiver, RoutingEpoch: record.RoutingEpoch, RoutingOrdinal: record.RoutingOrdinal, ExecutionShard: executionShard, RoutingReason: firstNonEmpty(record.RoutingReason, route.Reason), RoutePlanDigest: record.RoutePlanDigest, RouteBatchSequence: record.RouteBatchSequence, RouteBatchTransactionCount: record.RouteBatchTransactionCount, RouteBatchShardTransactionCount: record.RouteBatchShardTransactionCount, ConsensusExecutionPredecessorOrdinals: append([]uint64(nil), record.ConsensusExecutionPredecessorOrdinals...), ConsensusOrderingPredecessorOrdinals: append([]uint64(nil), record.ConsensusOrderingPredecessorOrdinals...), ConsensusExecutionDepth: record.ConsensusExecutionDepth, ConsensusExecutionRound: record.ConsensusExecutionRound, ConsensusWindowSequence: record.ConsensusWindowSequence, ConsensusWindowStartBatchSequence: record.ConsensusWindowStartBatchSequence, ConsensusWindowEndBatchSequence: record.ConsensusWindowEndBatchSequence, ConsensusWindowRouteBatchCount: record.ConsensusWindowRouteBatchCount, ConsensusWindowTransactionCount: record.ConsensusWindowTransactionCount, ConsensusWindowShardTransactionCount: record.ConsensusWindowShardTransactionCount, ConsensusWindowCriticalPath: record.ConsensusWindowCriticalPath, PredictedRemoteReads: record.PredictedRemoteReads, PredictedRemoteWrites: record.PredictedRemoteWrites, StateVersions: append([]tx.StateVersionDependency(nil), record.StateVersions...), ControlPolicy: record.ControlPolicy, LogicalDomains: append([]string(nil), record.LogicalDomains...), Local: record.Local, Bridge: record.Bridge, FrontierDigest: record.FrontierDigest}
					digest, digestErr := tx.ComputeExecutionRoutingDigest(item, routing)
					if digestErr != nil {
						err = digestErr
					} else {
						routing.RouteEntryDigest = digest
						item.ExecutionRouting = &routing
					}
				}
				_, privateKey := tx.DeterministicKeyPair(seed + ":" + sender)
				if err == nil {
					err = tx.Sign(&item, privateKey)
				}
			}
		}
		if err != nil {
			return err
		}
		envelope, err := p2p.NewEnvelope(p2p.MessageTXGossip, "mbe-client", leader.NodeID, shardID, 0, 0, 0, item)
		if err != nil {
			return err
		}
		if err := pacer.Wait(ctx); err != nil {
			return err
		}
		start := time.Now()
		if firstSubmissionTime.IsZero() {
			firstSubmissionTime = start
		}
		err = sendPersistent(ctx, connections, leader.ListenAddr, envelope)
		lastSubmissionTime = time.Now()
		rows = append(rows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, sender, leader.NodeID, shardID, payload, fmt.Sprint(isCrossShard), logicalSourceShard, targetShard, fmt.Sprint(err == nil), fmt.Sprint(time.Since(start).Milliseconds()), errorString(err)})
		lifecycleRows = append(lifecycleRows, lifecycleRow(LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: item.TxID, LogicalTxID: tx.SemanticID(item), Stage: "submitted", NodeID: "mbe-client", ShardID: shardID, Success: err == nil, Error: errorString(err)}))
		reason := firstNonEmpty(record.RoutingReason, route.Reason)
		if executionShard != "" && executionShard != shardID {
			reason = strings.TrimSuffix(reason+";execution_shard="+executionShard, ";")
		}
		routingRows = append(routingRows, []string{fmt.Sprint(time.Now().UnixMilli()), item.TxID, plugins.Routing.ID(), strings.Join(item.StateKeys, "|"), shardID, fmt.Sprint(isCrossShard), reason})
		resolvedAccessRows = append(resolvedAccessRows, resolvedAccessEntryFromTransaction(record, item, logicalSourceShard, executionShard, reason))
		return err
	}
	submitBatch := func(records []WorkloadRecord) error {
		if len(records) == 0 {
			return nil
		}
		preparedV6568 := make([]metaTrackPreparedRecordV6568, 0, len(records))
		if bindExecutionRouting {
			for index := range records {
				ordinal := uint64(records[index].Index + 1)
				records[index].RoutingOrdinal = ordinal
				// MBE_OPTME_V22_CLIENT_NO_SOURCE_VERSIONS: source/workload order must not
				// become an execution constraint for post-consensus OptME. Calvin already
				// follows the same principle and derives ordering from its consensus plan.
				if plugins.Routing.ID() == calvinStatelessRoutingID || plugins.Routing.ID() == optmeStatelessRoutingID {
					// MBE_CALVIN_CONSENSUS_VERSION_PLAN_V34: Stateless Calvin exact
					// predecessor/producer versions are derived only from the final
					// consensus-bound Calvin block order. Client/source order must not
					// be signed into ExecutionRouting.StateVersions.
					records[index].StateVersions = nil
				} else {
					records[index].StateVersions = stateVersionDependenciesForRecord(records[index], ordinal, lastWriterOrdinal)
				}
			}
		}
		placements := map[int]TransactionPlacement{}
		routePlanDigest := ""
		routeControlPolicy := ""
		routePlanUS := int64(0)
		routeBatchShardCounts := map[string]int{}
		versionLivenessEnabled := false
		versionLivenessIndexed := false
		singleFinalSeal := false
		if len(plan.NodeConfigs) > 0 {
			if config, ok := plan.NodeConfigs[0].PluginProfile["block_executor"]; ok {
				versionLivenessEnabled = boolFromAny(config.Config["version_liveness"])
				versionLivenessIndexed = boolFromAny(config.Config["version_liveness_indexed"])
				singleFinalSeal = boolFromAny(config.Config["single_final_seal"])
			}
		}
		if planner, ok := plugins.Routing.(BatchRoutingPlugin); ok {
			routingRecords := append([]WorkloadRecord(nil), records...)
			if _, ok := iterator.(*CanonicalTraceIterator); ok {
				for index, record := range routingRecords {
					if len(record.AccessList) == 0 || record.AccessListDigest == "" {
						return fmt.Errorf("dataset record %s missing resolved access list before batch planning", record.LogicalID)
					}
					if digest := CanonicalAccessListDigest(record.AccessList); digest != record.AccessListDigest {
						return fmt.Errorf("dataset record %s resolved access digest mismatch before batch planning", record.LogicalID)
					}
					routingRecords[index].AccessList = append([]tx.AccessItem(nil), record.AccessList...)
				}
			}
			deferFinalSeal := bindExecutionRouting && versionLivenessEnabled && singleFinalSeal && isMetaTrackRoutingPlugin(plugins.Routing)
			routePlanStarted := time.Now()
			routePlan := planner.PlanBatch(BatchRoutingInput{BatchIndex: batchIndex, Records: routingRecords, ShardIDs: shardIDs, Sharding: plugins.Sharding, DeferFinalSeal: deferFinalSeal, ExpectedTransactionCount: plan.WorkloadPlan.TxCount})
			routePlanUS = time.Since(routePlanStarted).Microseconds()
			if bindExecutionRouting && versionLivenessEnabled && isMetaTrackRoutingPlugin(plugins.Routing) {
				var finalized BatchRoutingPlan
				var ok bool
				if singleFinalSeal {
					finalized, ok = finalizeMetaTrackSignedBatchPlan(records, routePlan, versionLivenessIndexed)
				} else {
					finalized, ok = resealMetaTrackVersionLivenessPlan(records, routePlan)
				}
				if !ok {
					return fmt.Errorf("metatrack version-liveness final seal requires complete deterministic placement")
				}
				routePlan = finalized
			}
			routePlanDigest = routePlan.PlanDigest
			routeControlPolicy = routePlan.ControlPolicy
			// Preserve the pre-existing generic BatchRouting artifact behavior for
			// stateless_hash_routing. New algorithms must explicitly declare their
			// artifact family so they cannot be mislabeled as MetaTrack evidence.
			artifactFamily := "metatrack"
			if capability, ok := plugins.Routing.(BatchRoutingArtifactCapability); ok {
				artifactFamily = capability.BatchRoutingArtifactFamily()
			}
			if artifactFamily == "metatrack" {
				appendMetaTrackArtifacts(routePlan, &metatrackBatchRows, &accessMatrixRows, &stateFrequencyRows, &coaccessRows, &placementRows, &placementScoreRows, &transactionPlacementRows, &dependencyRows, &remoteStateRows)
				if routePlan.IncrementalRoutingPolicy != "" && len(metatrackBatchRows) > 0 {
					metatrackBatchRows[len(metatrackBatchRows)-1]["incremental_routing_plan_us"] = routePlanUS
				}
			} else if artifactFamily == "txallo" {
				// MBE_TXALLO_DYNAMIC_V222: placement evidence is bound to the mapping
				// snapshot active for this source-time epoch, never to a later epoch.
				if bootstrapper, ok := plugins.Sharding.(HistoricalAllocationBootstrapper); ok {
					txalloBootstrapEvidence = bootstrapper.HistoricalAllocationEvidence()
				}
				if mapping, ok := plugins.Sharding.(txalloAccountMappingProvider); ok {
					txalloBootstrapMapping = mapping.TxAlloMappingSnapshot()
				}
				recordByIndex := map[int]WorkloadRecord{}
				for _, rr := range routingRecords {
					recordByIndex[rr.Index] = rr
				}
				mappingDigest := ""
				if txalloBootstrapEvidence != nil {
					mappingDigest = strings.TrimSpace(fmt.Sprint(txalloBootstrapEvidence["mapping_digest"]))
				}
				for _, placement := range routePlan.TransactionPlacements {
					rr := recordByIndex[placement.TxIndex]
					sender := strings.ToLower(strings.TrimSpace(rr.SenderID))
					receiver := strings.ToLower(strings.TrimSpace(rr.ReceiverID))
					senderSource := "fallback_hash"
					if _, ok := txalloBootstrapMapping[sender]; ok {
						senderSource = "history_mapping"
					}
					receiverSource := "none"
					if receiver != "" {
						receiverSource = "fallback_hash"
						if _, ok := txalloBootstrapMapping[receiver]; ok {
							receiverSource = "history_mapping"
						}
					}
					involved := []string{placement.HomeShard}
					if placement.TargetShard != "" && placement.TargetShard != placement.HomeShard {
						involved = append(involved, placement.TargetShard)
					}
					txalloCrossByLogical[placement.LogicalID] = placement.TargetShard != "" && placement.TargetShard != placement.HomeShard
					sort.Strings(involved)
					txalloPlacementRows = append(txalloPlacementRows, []string{fmt.Sprint(routePlan.BatchIndex), placement.LogicalID, fmt.Sprint(placement.TxIndex), sender, receiver, senderSource, receiverSource, placement.HomeShard, placement.TargetShard, strings.Join(involved, "|"), placement.HomeShard, placement.ExecutionShard, placement.TargetShard, fmt.Sprint(placement.TargetShard != "" && placement.TargetShard != placement.HomeShard), fmt.Sprint(placement.RemoteAccessCount), mappingDigest, placement.Reason, routePlan.PlanDigest})
				}
			}
			for _, placement := range routePlan.TransactionPlacements {
				placements[placement.TxIndex] = placement
				if bindBatchProjectionMetadata {
					routeBatchShardCounts[placement.ExecutionShard]++
				}
			}
		}
		for _, record := range records {
			placement, planned := placements[record.Index]
			route := RoutingDecision{}
			if planned {
				route = RoutingDecision{ShardID: placement.ExecutionShard, Reason: placement.Reason}
				record.RoutingEpoch = placement.RoutingEpoch
				record.ExecutionShard = placement.ExecutionShard
				if routeControlPolicy != "" {
					record.ControlPolicy = routeControlPolicy
				} else if len(placement.LogicalDomains) > 0 || placement.Local || placement.Bridge || placement.FrontierDigest != "" {
					record.ControlPolicy = metaTrackLogicalDomainFrontierPolicy
				}
				record.LogicalDomains = append([]string(nil), placement.LogicalDomains...)
				record.Local = placement.Local
				record.Bridge = placement.Bridge
				record.FrontierDigest = placement.FrontierDigest
				record.RoutingReason = placement.Reason
				if bindExecutionRouting {
					record.RoutePlanDigest = routePlanDigest
				}
				if bindBatchProjectionMetadata {
					record.RouteBatchSequence = uint64(batchIndex + 1)
					record.RouteBatchTransactionCount = len(records)
					record.RouteBatchShardTransactionCount = routeBatchShardCounts[placement.ExecutionShard]
				}
				record.PredictedRemoteReads = placement.PredictedRemoteReads
				record.PredictedRemoteWrites = placement.PredictedRemoteWrites
				if capability, ok := plugins.Routing.(AccountPlacementRoutingCapability); ok {
					record = capability.ApplyAccountPlacement(record, placement)
				}
			}
			if route.ShardID == "" {
				route = plugins.Routing.Route(RoutingInput{Index: record.Index, StateKeys: record.StateKeys, AccessList: record.AccessList, SourceShard: record.SourceShard, ShardIDs: shardIDs, CrossShard: record.CrossShard, Sharding: plugins.Sharding})
			}
			if transactionFrontierV656Enabled {
				senderID := strings.TrimSpace(record.SenderID)
				if senderID == "" {
					logicalSourceShard := logicalSourceShardForRecord(record, route, plugins.Sharding, shardIDs)
					senderID = fmt.Sprintf("client_%s_%d", logicalSourceShard, record.Index)
				}
				if err := consensusPredecessorsV656.annotate(record.ExecutionShard, senderID, &record); err != nil {
					return err
				}
				if record.ControlPolicy == metaTrackDeclaredAccessFrontierPolicy {
					record.FrontierDigest = metaTrackDeclaredAccessFrontierDigestV6567(record, record.ExecutionShard)
				}
			}
			if transactionFrontierV656Enabled {
				preparedV6568 = append(preparedV6568, metaTrackPreparedRecordV6568{Record: record, Route: route})
				continue
			}
			if err := submitRecord(record, route); err != nil {
				return err
			}
		}
		if transactionFrontierV656Enabled {
			if leaderStreamingWindowV2 {
				// Evaluate V669 on the just-completed global RouteBatch, sign only
				// its cumulative-prefix certificate, then submit it immediately.
				streamed, err := criticalWidthWindowV6568.PushBatchAdaptiveNLV669StreamingV2(preparedV6568, plugins.BlockProducer.BlockSize())
				if err != nil {
					return err
				}
				for _, prepared := range streamed {
					if err := submitRecord(prepared.Record, prepared.Route); err != nil {
						return err
					}
				}
			} else if !clientWindowBufferingV6568 {
				// Partition-invariant mode and the formal w/o-consensus profile keep
				// signed predecessor/round metadata but do not wait for a future batch.
				for _, prepared := range preparedV6568 {
					if err := submitRecord(prepared.Record, prepared.Route); err != nil {
						return err
					}
				}
			} else {
				closed, err := windowProducerV663.PushMetaTrackRouteBatch(criticalWidthWindowV6568, preparedV6568, plugins.BlockProducer.BlockSize())
				if err != nil {
					return err
				}
				for _, prepared := range closed {
					if err := submitRecord(prepared.Record, prepared.Route); err != nil {
						return err
					}
				}
			}
		}
		batchIndex++
		return nil
	}
	// MBE_TXALLO_DYNAMIC_V222: block-height epoch barrier. A source epoch is
	// exactly 300 source blocks relative to the first evaluation block. The
	// prior epoch must be terminally committed before its A/G update can affect
	// the next epoch. The final partial epoch is never used as future training.
	txalloEpochBucket := int64(-1)
	txalloEpochRecords := []WorkloadRecord{}
	closeTxAlloSourceEpoch := func(epoch int64, records []WorkloadRecord) error {
		if !txalloDynamicEnabled {
			return nil
		}
		if len(records) > 0 {
			if err := txalloWaitCommittedEpoch(ctx, plan.NodeConfigs, records, txalloCrossByLogical, statelessDirect, txalloFeedReader); err != nil {
				return err
			}
		}
		beforeEpoch := txalloDynamic.TxAlloMappingEpoch()
		if err := txalloDynamic.ApplyCommittedTxAlloEpoch(records, uint64(epoch), statelessDirect); err != nil {
			return err
		}
		txalloBootstrapEvidence = txalloDynamic.HistoricalAllocationEvidence()
		txalloBootstrapMapping = txalloDynamic.TxAlloMappingSnapshot()
		if txalloDynamic.TxAlloMappingEpoch() != beforeEpoch {
			if err := txalloWaitMappingAcks(ctx, plan.NodeConfigs, txalloDynamic.TxAlloMappingEpoch(), txalloDynamic.TxAlloMappingStateDigest()); err != nil {
				return err
			}
		}
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
		if txalloDynamicEnabled {
			blockNum, err := txalloBlockIndex.BlockForRecord(record)
			if err != nil {
				return err
			}
			bucket, err := txalloSourceBlockEpoch(blockNum, txalloBlockIndex.AnchorBlock, txalloDynamic.TxAlloAEpochBlocks())
			if err != nil {
				return err
			}
			if txalloEpochBucket >= 0 && bucket < txalloEpochBucket {
				return fmt.Errorf("TxAllo dynamic source block epoch regressed: %d < %d", bucket, txalloEpochBucket)
			}
			if txalloEpochBucket >= 0 && bucket != txalloEpochBucket {
				if err := submitBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
				for closed := txalloEpochBucket; closed < bucket; closed++ {
					records := []WorkloadRecord(nil)
					if closed == txalloEpochBucket {
						records = txalloEpochRecords
					}
					if err := closeTxAlloSourceEpoch(closed, records); err != nil {
						return err
					}
				}
				txalloEpochRecords = nil
			}
			txalloEpochBucket = bucket
			txalloEpochRecords = append(txalloEpochRecords, record)
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
	// Do not feed the final partial source-block epoch back into TxAllo: no
	// later evaluation transaction is allowed to learn from it.
	if clientWindowBufferingV6568 {
		closed := windowProducerV663.FlushMetaTrackWindow(criticalWidthWindowV6568)
		for _, prepared := range closed {
			if err := submitRecord(prepared.Record, prepared.Route); err != nil {
				return err
			}
		}
	}
	if streamPartitionInvariantV658 {
		if err := SaveJSON(filepath.Join(outDir, "metatrack_partition_invariant_summary.json"), map[string]any{
			"schema_version":            "mbe_metatrack_partition_invariant_v658",
			"policy":                    metaTrackPartitionInvariantPolicyV658,
			"routing_mode":              "single_record_stream_with_persistent_history",
			"route_batch_semantic_role": "evidence_envelope_only",
			"consensus_selection":       "capacity_bounded_max_dependency_closed_frontier",
			"version_liveness_mode":     "conservative_unpruned_publish",
			"routing_unit_size":         1,
			"block_size":                plugins.BlockProducer.BlockSize(),
			"block_interval_ms":         plugins.BlockProducer.Interval().Milliseconds(),
			"route_batch_count":         batchIndex,
		}); err != nil {
			return err
		}
	}
	replaySummary := iterator.Summary()
	replaySummary.SubmittedCount = len(rows)
	replaySummary.ReplayMode = pacer.mode
	replaySummary.TargetSubmissionTPS = plan.WorkloadPlan.TargetSubmissionTPS
	replaySummary.PacingSchedule = "absolute_release_timeline_v1"
	replaySummary.PacingLateReleaseCount = pacer.lateReleaseCount
	replaySummary.PacingMaxScheduleLagMS = pacer.maxScheduleLag.Milliseconds()
	if !firstSubmissionTime.IsZero() && !lastSubmissionTime.IsZero() {
		duration := lastSubmissionTime.Sub(firstSubmissionTime)
		replaySummary.SubmissionDurationMS = duration.Milliseconds()
		if duration > 0 && len(rows) > 1 {
			replaySummary.ObservedSubmissionTPS = float64(len(rows)-1) / duration.Seconds()
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
	if err := writeResolvedAccessArtifacts(outDir, resolvedAccessRows); err != nil {
		return err
	}
	// MBE_OPTME_TXALLO_MULTISHARD_ORACLE_V8: emit the standard state-home
	// evidence consumed by the multi-shard correctness oracle. Stateful methods
	// bind logical keys to the transaction execution namespace; stateless methods
	// bind them to their persistent deterministic state homes.
	if optmeTxAlloNeedsStateHomeEvidence(plugins) {
		evidenceRows := optmeTxAlloStateHomeEvidenceRows(resolvedAccessRows, plugins, shardIDs)
		if len(evidenceRows) == 0 {
			return fmt.Errorf("OptME/TxAllo state-home evidence is empty")
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "placement_plan.csv"), []string{"batch_index", "state_key", "home_state_unit", "home_shard", "execution_shard", "frequency", "reason"}, evidenceRows); err != nil {
			return err
		}
	}
	if txalloBootstrapEvidence != nil {
		if bootstrapper, ok := plugins.Sharding.(HistoricalAllocationBootstrapper); ok {
			txalloBootstrapEvidence = bootstrapper.HistoricalAllocationEvidence()
		}
		if mapping, ok := plugins.Sharding.(txalloAccountMappingProvider); ok {
			txalloBootstrapMapping = mapping.TxAlloMappingSnapshot()
		}
		if err := SaveJSON(filepath.Join(outDir, "txallo_allocation_summary.json"), txalloBootstrapEvidence); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "txallo_account_mapping.csv"), []string{"account", "shard"}, txalloMappingRows(txalloBootstrapMapping)); err != nil {
			return err
		}
	}
	if len(txalloPlacementRows) > 0 {
		if err := metrics.WriteCSV(filepath.Join(outDir, "txallo_transaction_placement.csv"), []string{"batch_index", "logical_id", "tx_index", "sender_account", "receiver_account", "sender_mapping_source", "receiver_mapping_source", "sender_shard", "receiver_shard", "involved_shards", "home_shard", "execution_shard", "target_shard", "txallo_cross_shard", "cross_shard_edge_count", "mapping_digest", "reason", "plan_digest"}, txalloPlacementRows); err != nil {
			return err
		}
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
		if err := metrics.WriteCSV(filepath.Join(outDir, "placement_plan.csv"), []string{"batch_index", "state_key", "home_state_unit", "home_shard", "execution_shard", "frequency", "reason"}, placementRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "placement_score.csv"), []string{"batch_index", "state_key", "candidate_shard", "coaccess_affinity", "admissible", "capacity", "projected_load", "current_state_load", "score"}, placementScoreRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "transaction_placement.csv"), []string{"batch_index", "logical_id", "tx_index", "sender_group_id", "routing_epoch", "home_shard", "execution_shard", "target_shard", "coaccess_group", "predicted_remote_reads", "predicted_remote_writes", "remote_access_count", "majority_coverage", "majority_tie", "queue_load_before", "reason"}, transactionPlacementRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "dependency_graph.csv"), []string{"batch_index", "from_logical_id", "to_logical_id", "state_key", "dependency_type"}, dependencyRows); err != nil {
			return err
		}
		remoteStateHeader := []string{"batch_index", "logical_id", "tx_index", "state_key", "home_state_unit", "home_shard", "execution_shard", "access_kind", "witness_digest"}
		if err := metrics.WriteCSV(filepath.Join(outDir, "predicted_remote_access.csv"), remoteStateHeader, remoteStateRows); err != nil {
			return err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "remote_state_access.csv"), remoteStateHeader, remoteStateRows); err != nil {
			return err
		}
	}
	requestedCrossShardCount := requestedCrossShardCount(plan.WorkloadPlan.TxCount, plan.WorkloadPlan.CrossShardRatio)
	if plan.WorkloadPlan.SourceType == "dataset" {
		requestedCrossShardCount = replaySummary.ExpectedCrossShardCount
	}
	replaySummary.ActualCrossShardCount = generatedCrossShardCount
	if replaySummary.ReadCount > 0 {
		replaySummary.ActualCrossShardRatio = float64(generatedCrossShardCount) / float64(replaySummary.ReadCount)
	}
	if err := SaveJSON(filepath.Join(outDir, "workload_replay_summary.json"), replaySummary); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(outDir, "workload_identity_mapping_summary.json"), map[string]any{"identity_count": replaySummary.IdentityCount, "mapping_digest": replaySummary.MappingDigest, "nonce_continuity": replaySummary.NonceContinuity, "signature_pass_count": replaySummary.SignaturePassCount, "identity_mapping_version": replaySummary.IdentityMappingVersion}); err != nil {
		return err
	}
	return SaveJSON(filepath.Join(outDir, "client_submission_complete.json"), map[string]any{"submitted_unique_logical_tx_count": len(rows), "submitted_tx_count": len(rows), "rejected_during_submission": 0, "first_submitted_at": rows[0][0], "last_submitted_at": rows[len(rows)-1][0], "submission_finished_at": fmt.Sprint(time.Now().UnixMilli()), "replay_mode": replaySummary.ReplayMode, "target_submission_tps": replaySummary.TargetSubmissionTPS, "observed_submission_tps": replaySummary.ObservedSubmissionTPS, "submission_duration_ms": replaySummary.SubmissionDurationMS, "pacing_schedule": replaySummary.PacingSchedule, "pacing_late_release_count": replaySummary.PacingLateReleaseCount, "pacing_max_schedule_lag_ms": replaySummary.PacingMaxScheduleLagMS, "requested_cross_shard_ratio": plan.WorkloadPlan.CrossShardRatio, "requested_cross_shard_count": requestedCrossShardCount, "generated_cross_shard_count": generatedCrossShardCount, "observed_cross_shard_ratio": float64(generatedCrossShardCount) / float64(len(rows)), "cross_shard_execution_mode": finalityMode})
}

func optmeTxAlloNeedsStateHomeEvidence(plugins RuntimePlugins) bool {
	if plugins.BlockExecutor != nil {
		switch plugins.BlockExecutor.ID() {
		case optmeStatefulExecutorID, optmeStatelessExecutorID:
			return true
		}
	}
	return plugins.Sharding != nil && plugins.Sharding.ID() == txalloShardingID
}

func optmeTxAlloStateHomeEvidenceRows(entries []resolvedAccessEntry, plugins RuntimePlugins, shardIDs []string) [][]string {
	type evidence struct {
		key       string
		home      string
		execution string
		reason    string
		count     int
	}
	stateless := false
	if plugins.Routing != nil {
		stateless = plugins.Routing.ID() == optmeStatelessRoutingID || plugins.Routing.ID() == txalloStatelessRoutingID
	}
	byPair := map[string]*evidence{}
	for _, entry := range entries {
		for _, access := range entry.AccessList {
			key := strings.TrimSpace(access.Key)
			if key == "" {
				continue
			}
			home := entry.ExecutionShard
			reason := "execution_shard_local_namespace"
			if stateless {
				home = shardFor(plugins.Sharding, []string{key}, shardIDs)
				reason = "deterministic_persistent_state_home"
			}
			if home == "" {
				continue
			}
			pair := key + "\x00" + home + "\x00" + entry.ExecutionShard
			row := byPair[pair]
			if row == nil {
				row = &evidence{key: key, home: home, execution: entry.ExecutionShard, reason: reason}
				byPair[pair] = row
			}
			row.count++
		}
	}
	keys := make([]string, 0, len(byPair))
	for key := range byPair {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([][]string, 0, len(keys))
	for _, key := range keys {
		row := byPair[key]
		out = append(out, []string{"0", row.key, row.key, row.home, row.execution, fmt.Sprint(row.count), row.reason})
	}
	return out
}

func stateVersionDependenciesForRecord(record WorkloadRecord, ordinal uint64, lastWriter map[string]uint64) []tx.StateVersionDependency {
	type accessSummary struct {
		read  bool
		write bool
	}
	byKey := map[string]accessSummary{}
	for _, access := range normalizedAccessItems(record) {
		if access.Key == "" || !isVersionedStateAccess(access) {
			// Commutative/account state keeps its existing semantics. The exact
			// logical version chain is reserved for non-commutative business
			// state, so a commutative writer can never become an unpublished
			// predecessor token.
			continue
		}
		summary := byKey[access.Key]
		summary.read = summary.read || isReadMode(access.Mode)
		summary.write = summary.write || isWriteMode(access.Mode)
		byKey[access.Key] = summary
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tx.StateVersionDependency, 0, len(keys))
	for _, key := range keys {
		summary := byKey[key]
		dependency := tx.StateVersionDependency{Key: key, RequiredVersion: lastWriter[key]}
		if summary.write {
			dependency.ProducedVersion = ordinal
		}
		out = append(out, dependency)
	}
	for _, key := range keys {
		if byKey[key].write {
			lastWriter[key] = ordinal
		}
	}
	return out
}

// MBE_PORYGON_UNIFIED_SHARD_V10_20260921
func workloadIngressShard(record WorkloadRecord, route RoutingDecision, statelessDirect bool) string {
	if statelessDirect {
		return route.ShardID
	}
	if record.CrossShard && record.SourceShard != "" {
		return record.SourceShard
	}
	return route.ShardID
}

// logicalSourceShardForRecord preserves workload identity independently of
// execution placement. MetaTrack may move a transaction to a different
// execution shard than stateless hash routing, but that placement decision
// must not change synthetic sender/receiver IDs, semantic state keys, or the
// deterministic signing seed. Explicit source-shard evidence wins; otherwise
// the stable state-key partition is the logical source/home identity.
func logicalSourceShardForRecord(record WorkloadRecord, route RoutingDecision, sharding ShardingPlugin, shardIDs []string) string {
	if record.SourceShard != "" {
		return record.SourceShard
	}
	if source := shardFor(sharding, record.StateKeys, shardIDs); source != "" {
		return source
	}
	return route.ShardID
}

func statelessPayload(payload string) string {
	if !strings.HasPrefix(payload, "v5_cross:") {
		if payload == "v5_cross" || payload == "" {
			return "v5_stateless"
		}
		return payload
	}
	remainder := strings.TrimPrefix(payload, "v5_cross:")
	if colon := strings.Index(remainder, ":"); colon >= 0 && colon+1 < len(remainder) {
		return remainder[colon+1:]
	}
	return "v5_stateless"
}

type resolvedAccessEntry struct {
	Index                 int                         `json:"index"`
	LogicalID             string                      `json:"logical_id"`
	TxID                  string                      `json:"tx_id"`
	Sender                string                      `json:"sender"`
	Receiver              string                      `json:"receiver"`
	SourceShard           string                      `json:"source_shard"`
	ExecutionShard        string                      `json:"execution_shard"`
	RoutingReason         string                      `json:"routing_reason"`
	RoutingEpoch          uint64                      `json:"routing_epoch"`
	RoutePlanDigest       string                      `json:"route_plan_digest,omitempty"`
	RouteEntryDigest      string                      `json:"route_entry_digest,omitempty"`
	PredictedRemoteReads  int                         `json:"predicted_remote_reads"`
	PredictedRemoteWrites int                         `json:"predicted_remote_writes"`
	AccessListDigest      string                      `json:"access_list_digest"`
	AccessListSchema      string                      `json:"access_list_schema,omitempty"`
	AccessListSource      string                      `json:"access_list_source,omitempty"`
	AccessList            []tx.AccessItem             `json:"access_list"`
	StateVersions         []tx.StateVersionDependency `json:"state_versions,omitempty"` // MBE_TXALLO_EVIDENCE_V203
}

func resolvedAccessEntryFromTransaction(record WorkloadRecord, item tx.SignedTransaction, sourceShard, executionShard, reason string) resolvedAccessEntry {
	digest := item.AccessListDigest
	if digest == "" {
		digest = CanonicalAccessListDigest(item.AccessList)
	}
	entry := resolvedAccessEntry{Index: record.Index, LogicalID: firstNonEmpty(record.LogicalID, item.TxID), TxID: item.TxID, Sender: item.Sender, Receiver: item.Receiver, SourceShard: sourceShard, ExecutionShard: executionShard, RoutingReason: reason, RoutingEpoch: record.RoutingEpoch, RoutePlanDigest: record.RoutePlanDigest, PredictedRemoteReads: record.PredictedRemoteReads, PredictedRemoteWrites: record.PredictedRemoteWrites, AccessListDigest: digest, AccessListSchema: item.AccessListSchema, AccessListSource: item.AccessListSource, AccessList: append([]tx.AccessItem(nil), item.AccessList...), StateVersions: append([]tx.StateVersionDependency(nil), record.StateVersions...)}
	if item.ExecutionRouting != nil {
		entry.RouteEntryDigest = item.ExecutionRouting.RouteEntryDigest
	}
	return entry
}

func writeResolvedAccessArtifacts(outDir string, rows []resolvedAccessEntry) error {
	path := filepath.Join(outDir, "resolved_access_lists.jsonl.gz")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(file)
	overall := sha256.New()
	for _, row := range rows {
		payload, err := json.Marshal(row)
		if err != nil {
			_ = gz.Close()
			_ = file.Close()
			return err
		}
		if _, err := gz.Write(payload); err != nil {
			_ = gz.Close()
			_ = file.Close()
			return err
		}
		if _, err := gz.Write([]byte("\n")); err != nil {
			_ = gz.Close()
			_ = file.Close()
			return err
		}
		digestPayload, err := json.Marshal(resolvedAccessDigestEntry(row))
		if err != nil {
			_ = gz.Close()
			_ = file.Close()
			return err
		}
		overall.Write(digestPayload)
		overall.Write([]byte("\n"))
	}
	if err := gz.Close(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	digest := hex.EncodeToString(overall.Sum(nil))
	if err := os.WriteFile(filepath.Join(outDir, "access_list_digest.txt"), []byte(digest+"\n"), 0o644); err != nil {
		return err
	}
	return SaveJSON(filepath.Join(outDir, "access_list_summary.json"), resolvedAccessSummary(rows, digest))
}

func resolvedAccessDigestEntry(row resolvedAccessEntry) map[string]any {
	return map[string]any{
		"index":              row.Index,
		"logical_id":         row.LogicalID,
		"access_list_digest": row.AccessListDigest,
		"access_list_schema": row.AccessListSchema,
		"access_list_source": row.AccessListSource,
		"access_list":        row.AccessList,
	}
}

func resolvedAccessSummary(rows []resolvedAccessEntry, digest string) map[string]any {
	modeCounts := map[tx.AccessMode]int{}
	keyCounts := map[string]int{}
	edgeCounts := map[string]int{}
	empty := 0
	duplicates := 0
	legacyAliases := 0
	totalKeys := 0
	minKeys := 0
	maxKeys := 0
	for _, row := range rows {
		keyCount := len(row.AccessList)
		if keyCount == 0 {
			empty++
		}
		if minKeys == 0 || keyCount < minKeys {
			minKeys = keyCount
		}
		if keyCount > maxKeys {
			maxKeys = keyCount
		}
		totalKeys += keyCount
		seen := map[string]bool{}
		uniqueKeys := []string{}
		for _, access := range row.AccessList {
			modeCounts[access.Mode]++
			keyCounts[access.Key]++
			if strings.HasPrefix(access.Key, "account:sender:") || strings.HasPrefix(access.Key, "account:receiver:") || strings.HasPrefix(access.Key, "contract:") {
				legacyAliases++
			}
			if seen[access.Key] {
				duplicates++
				continue
			}
			seen[access.Key] = true
			uniqueKeys = append(uniqueKeys, access.Key)
		}
		sort.Strings(uniqueKeys)
		for left := 0; left < len(uniqueKeys); left++ {
			for right := left + 1; right < len(uniqueKeys); right++ {
				edgeCounts[uniqueKeys[left]+"|"+uniqueKeys[right]]++
			}
		}
	}
	average := 0.0
	if len(rows) > 0 {
		average = float64(totalKeys) / float64(len(rows))
	}
	return map[string]any{
		"transaction_count":            len(rows),
		"access_list_digest":           digest,
		"empty_access_list_count":      empty,
		"duplicate_key_count":          duplicates,
		"legacy_account_alias_count":   legacyAliases,
		"average_keys_per_transaction": average,
		"minimum_keys_per_transaction": minKeys,
		"maximum_keys_per_transaction": maxKeys,
		"unique_state_key_count":       len(keyCounts),
		"read_count":                   modeCounts[tx.AccessRead],
		"write_count":                  modeCounts[tx.AccessWrite],
		"read_write_count":             modeCounts[tx.AccessReadWrite],
		"commutative_delta_count":      modeCounts[tx.AccessCommutativeDelta],
		"top_state_keys":               topCounts(keyCounts, 20),
		"top_coaccess_edges":           topCounts(edgeCounts, 20),
	}
}

func topCounts(counts map[string]int, limit int) []map[string]any {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] == counts[keys[j]] {
			return keys[i] < keys[j]
		}
		return counts[keys[i]] > counts[keys[j]]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{"key": key, "count": counts[key]})
	}
	return out
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

func appendMetaTrackArtifacts(plan BatchRoutingPlan, planRows *[]map[string]any, accessRows, frequencyRows, coaccessRows, placementRows, placementScoreRows, transactionRows, dependencyRows, remoteStateRows *[][]string) {
	*planRows = append(*planRows, map[string]any{
		"batch_index":                                     plan.BatchIndex,
		"plan_digest":                                     plan.PlanDigest,
		"sharding_plugin_id":                              plan.ShardingPluginID,
		"state_storage_unit_count":                        plan.StateStorageUnitCount,
		"placement_policy":                                plan.PlacementPolicy,
		"transaction_policy":                              plan.TransactionPolicy,
		"placement_budget":                                plan.PlacementBudget,
		"placement_min_budget":                            plan.PlacementMinBudget,
		"placement_mu":                                    plan.PlacementMu,
		"placement_capacity":                              plan.PlacementCapacity,
		"placement_total_frequency":                       plan.PlacementTotalFrequency,
		"placement_max_frequency":                         plan.PlacementMaxFrequency,
		"transaction_count":                               len(plan.TransactionPlacements),
		"state_key_count":                                 len(plan.StateFrequency),
		"coaccess_edge_count":                             len(plan.CoaccessEdges),
		"remote_access_estimate":                          plan.RemoteAccessEstimate,
		"predicted_remote_access_count":                   plan.RemoteAccessEstimate,
		"placement_fallback_count":                        plan.PlacementFallbackCount,
		"routing_overhead":                                plan.RoutingOverhead,
		"shard_load_before":                               plan.ShardLoadBefore,
		"shard_load_after":                                plan.ShardLoadAfter,
		"incremental_routing_policy":                      plan.IncrementalRoutingPolicy,
		"incremental_expected_transaction_count":          plan.IncrementalExpectedTransactionCount,
		"incremental_execution_shard_capacity":            plan.IncrementalExecutionShardCapacity,
		"incremental_history_transaction_count_before":    plan.IncrementalHistoryTransactionCountBefore,
		"incremental_history_transaction_count_after":     plan.IncrementalHistoryTransactionCountAfter,
		"incremental_exact_state_edge_count":              plan.IncrementalExactStateEdgeCount,
		"incremental_exact_local_state_edge_count":        plan.IncrementalExactLocalStateEdgeCount,
		"incremental_exact_cross_shard_state_edge_count":  plan.IncrementalExactCrossShardStateEdgeCount,
		"incremental_exact_predecessor_edge_count":        plan.IncrementalExactPredecessorEdgeCount,
		"incremental_exact_local_predecessor_count":       plan.IncrementalExactLocalPredecessorCount,
		"incremental_exact_cross_shard_predecessor_count": plan.IncrementalExactCrossShardPredecessorCount,
		"incremental_coaccess_pair_update_count":          plan.IncrementalCoaccessPairUpdateCount,
		"incremental_execution_shard_switch_count":        plan.IncrementalExecutionShardSwitchCount,
		"incremental_capacity_forced_choice_count":        plan.IncrementalCapacityForcedChoiceCount,
		"incremental_exact_first_choice_count":            plan.IncrementalExactFirstChoiceCount,
		"incremental_coaccess_tiebreak_count":             plan.IncrementalCoaccessTiebreakCount,
		"incremental_remote_tiebreak_count":               plan.IncrementalRemoteTiebreakCount,
		"incremental_load_tiebreak_count":                 plan.IncrementalLoadTiebreakCount,
		"incremental_history_digest_before":               plan.IncrementalHistoryDigestBefore,
		"incremental_history_digest_after":                plan.IncrementalHistoryDigestAfter,
		"incremental_batch_partition_invariant":           plan.IncrementalBatchPartitionInvariant,
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
		*placementRows = append(*placementRows, []string{fmt.Sprint(plan.BatchIndex), row.Key, row.HomeStateUnit, row.HomeShard, row.ExecutionShard, fmt.Sprint(row.Frequency), row.Reason})
	}
	for _, row := range plan.PlacementScores {
		*placementScoreRows = append(*placementScoreRows, []string{fmt.Sprint(plan.BatchIndex), row.Key, row.CandidateShard, fmt.Sprint(row.CoaccessLocalityGain), fmt.Sprint(row.Admissible), fmt.Sprint(row.Capacity), fmt.Sprint(row.ProjectedLoad), fmt.Sprint(row.ShardStateLoadPenalty), fmt.Sprint(row.Score)})
	}
	for _, row := range plan.TransactionPlacements {
		*transactionRows = append(*transactionRows, []string{fmt.Sprint(plan.BatchIndex), row.LogicalID, fmt.Sprint(row.TxIndex), row.SenderGroupID, fmt.Sprint(row.RoutingEpoch), row.HomeShard, row.ExecutionShard, row.TargetShard, row.CoaccessGroup, fmt.Sprint(row.PredictedRemoteReads), fmt.Sprint(row.PredictedRemoteWrites), fmt.Sprint(row.RemoteAccessCount), fmt.Sprint(row.MajorityCoverage), fmt.Sprint(row.MajorityTie), fmt.Sprint(row.QueueLoadBefore), row.Reason})
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
			witness := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s:%s:%s", plan.BatchIndex, row.LogicalID, row.Key, statePlacement.HomeStateUnit, statePlacement.HomeShard)))
			*remoteStateRows = append(*remoteStateRows, []string{fmt.Sprint(plan.BatchIndex), row.LogicalID, fmt.Sprint(row.TxIndex), row.Key, statePlacement.HomeStateUnit, statePlacement.HomeShard, txPlacement.ExecutionShard, string(row.Mode), hex.EncodeToString(witness[:])})
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
