package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackVersionLivenessPolicyV5 = "dependency_graph_version_liveness_v5"

const (
	metaTrackVersionClassLocalTransient   = "local_transient"
	metaTrackVersionClassRemoteLive       = "remote_live"
	metaTrackVersionClassDeadIntermediate = "dead_intermediate"
	metaTrackVersionClassFinalPersistent  = "final_persistent"
)

type metaTrackVersionRef struct {
	RecordIndex int
	DepIndex    int
}

func metaTrackVersionLivenessDigest(dependency tx.StateVersionDependency) string {
	return stableTextDigest(strings.Join([]string{
		dependency.Key,
		fmt.Sprint(dependency.RequiredVersion),
		fmt.Sprint(dependency.ProducedVersion),
		dependency.LivenessClass,
		fmt.Sprint(dependency.BatchFinal),
		fmt.Sprint(dependency.ValueSuccessorCount),
		fmt.Sprint(dependency.LocalValueSuccessorCount),
		fmt.Sprint(dependency.RemoteValueSuccessorCount),
		fmt.Sprint(dependency.LocalOrderingSuccessorCount),
		fmt.Sprint(dependency.RemoteOrderingSuccessorCount),
	}, "|"))
}

func metaTrackValidVersionLivenessClass(value string) bool {
	switch value {
	case metaTrackVersionClassLocalTransient, metaTrackVersionClassRemoteLive, metaTrackVersionClassDeadIntermediate, metaTrackVersionClassFinalPersistent:
		return true
	default:
		return false
	}
}

func validateMetaTrackVersionLivenessDependency(dependency tx.StateVersionDependency) error {
	if dependency.ProducedVersion == 0 {
		if dependency.LivenessClass != "" || dependency.LivenessDigest != "" || dependency.BatchFinal ||
			dependency.ValueSuccessorCount != 0 || dependency.LocalValueSuccessorCount != 0 ||
			dependency.RemoteValueSuccessorCount != 0 || dependency.LocalOrderingSuccessorCount != 0 ||
			dependency.RemoteOrderingSuccessorCount != 0 {
			return fmt.Errorf("non-producer %s carries producer liveness metadata", dependency.Key)
		}
		return nil
	}
	if dependency.Key == "" {
		return fmt.Errorf("produced version %d has empty state key", dependency.ProducedVersion)
	}
	if !metaTrackValidVersionLivenessClass(dependency.LivenessClass) {
		return fmt.Errorf("version %s@%d has invalid liveness class %q", dependency.Key, dependency.ProducedVersion, dependency.LivenessClass)
	}
	if dependency.ValueSuccessorCount < 0 || dependency.LocalValueSuccessorCount < 0 ||
		dependency.RemoteValueSuccessorCount < 0 || dependency.LocalOrderingSuccessorCount < 0 ||
		dependency.RemoteOrderingSuccessorCount < 0 {
		return fmt.Errorf("version %s@%d has negative successor count", dependency.Key, dependency.ProducedVersion)
	}
	if dependency.ValueSuccessorCount != dependency.LocalValueSuccessorCount+dependency.RemoteValueSuccessorCount {
		return fmt.Errorf("version %s@%d value successor count mismatch", dependency.Key, dependency.ProducedVersion)
	}
	localSuccessors := dependency.LocalValueSuccessorCount + dependency.LocalOrderingSuccessorCount
	remoteSuccessors := dependency.RemoteValueSuccessorCount + dependency.RemoteOrderingSuccessorCount
	if dependency.BatchFinal != (dependency.LivenessClass == metaTrackVersionClassFinalPersistent) {
		return fmt.Errorf("version %s@%d batch-final/class mismatch", dependency.Key, dependency.ProducedVersion)
	}
	switch dependency.LivenessClass {
	case metaTrackVersionClassLocalTransient:
		if dependency.BatchFinal || localSuccessors <= 0 || remoteSuccessors != 0 {
			return fmt.Errorf("version %s@%d local-transient successor semantics mismatch", dependency.Key, dependency.ProducedVersion)
		}
	case metaTrackVersionClassRemoteLive:
		if dependency.BatchFinal || remoteSuccessors <= 0 {
			return fmt.Errorf("version %s@%d remote-live successor semantics mismatch", dependency.Key, dependency.ProducedVersion)
		}
	case metaTrackVersionClassDeadIntermediate:
		if dependency.BatchFinal || localSuccessors != 0 || remoteSuccessors != 0 {
			return fmt.Errorf("version %s@%d dead-intermediate successor semantics mismatch", dependency.Key, dependency.ProducedVersion)
		}
	case metaTrackVersionClassFinalPersistent:
		if !dependency.BatchFinal {
			return fmt.Errorf("version %s@%d final-persistent is not batch final", dependency.Key, dependency.ProducedVersion)
		}
	}
	if dependency.LivenessDigest == "" || dependency.LivenessDigest != metaTrackVersionLivenessDigest(dependency) {
		return fmt.Errorf("version %s@%d liveness digest mismatch", dependency.Key, dependency.ProducedVersion)
	}
	return nil
}

func validateMetaTrackVersionLivenessMetadata(item tx.SignedTransaction) error {
	if item.ExecutionRouting == nil {
		return fmt.Errorf("version-liveness transaction missing execution routing")
	}
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if err := validateMetaTrackVersionLivenessDependency(dependency); err != nil {
			return err
		}
		requiredClass := strings.TrimSpace(dependency.RequiredLivenessClass)
		requiredDigest := strings.TrimSpace(dependency.RequiredLivenessDigest)
		if dependency.RequiredVersion == 0 && (requiredClass != "" || requiredDigest != "") {
			return fmt.Errorf("zero required version for %s carries required liveness binding", dependency.Key)
		}
		if (requiredClass == "") != (requiredDigest == "") {
			return fmt.Errorf("required liveness binding for %s@%d is incomplete", dependency.Key, dependency.RequiredVersion)
		}
		if requiredClass != "" && !metaTrackValidVersionLivenessClass(requiredClass) {
			return fmt.Errorf("required liveness binding for %s@%d has invalid class %q", dependency.Key, dependency.RequiredVersion, requiredClass)
		}
		if requiredClass == metaTrackVersionClassDeadIntermediate {
			return fmt.Errorf("required liveness binding for %s@%d points to dead intermediate", dependency.Key, dependency.RequiredVersion)
		}
	}
	return nil
}

// resealMetaTrackVersionLivenessPlan is the only supported client-side v5
// integration point. Placement is computed first; liveness is then derived from
// the complete declared-access/exact-version graph; finally both per-transaction
// frontier digests and the enclosing route-plan digest are recomputed from the
// final StateVersions before any ExecutionRouting metadata is signed.
func resealMetaTrackVersionLivenessPlan(records []WorkloadRecord, plan BatchRoutingPlan) (BatchRoutingPlan, bool) {
	placementByIndex := make(map[int]TransactionPlacement, len(plan.TransactionPlacements))
	for _, placement := range plan.TransactionPlacements {
		placementByIndex[placement.TxIndex] = placement
	}
	for index := range records {
		placement, ok := placementByIndex[records[index].Index]
		if !ok || strings.TrimSpace(placement.ExecutionShard) == "" {
			return BatchRoutingPlan{}, false
		}
		records[index].ExecutionShard = placement.ExecutionShard
	}
	if !annotateMetaTrackVersionLivenessRecords(records) {
		return BatchRoutingPlan{}, false
	}
	applyMetaTrackDeclaredAccessFrontierV2(&plan, records)
	plan.PlanDigest = routingPlanDigest(plan)
	return plan, true
}

// annotateMetaTrackVersionLivenessRecords runs after the routing batch has a
// deterministic execution-shard placement for every transaction, but before
// ExecutionRouting metadata is signed. It therefore uses only declared access,
// exact predecessor versions and routing decisions already available before
// execution; no execution result or completion order is consulted.
func annotateMetaTrackVersionLivenessRecords(records []WorkloadRecord) bool {
	if len(records) == 0 {
		return true
	}
	for _, record := range records {
		if strings.TrimSpace(record.ExecutionShard) == "" {
			return false
		}
	}
	for i := range records {
		for j := range records[i].StateVersions {
			records[i].StateVersions[j].RequiredLivenessClass = ""
			records[i].StateVersions[j].RequiredLivenessDigest = ""
		}
	}
	producers := map[string]metaTrackVersionRef{}
	latestByKey := map[string]uint64{}
	for i := range records {
		for j := range records[i].StateVersions {
			dep := &records[i].StateVersions[j]
			if dep.Key == "" || dep.ProducedVersion == 0 {
				continue
			}
			dep.LivenessClass = ""
			dep.LivenessDigest = ""
			dep.BatchFinal = false
			dep.ValueSuccessorCount = 0
			dep.LocalValueSuccessorCount = 0
			dep.RemoteValueSuccessorCount = 0
			dep.LocalOrderingSuccessorCount = 0
			dep.RemoteOrderingSuccessorCount = 0
			id := fmt.Sprintf("%s\x00%d", dep.Key, dep.ProducedVersion)
			producers[id] = metaTrackVersionRef{RecordIndex: i, DepIndex: j}
			if dep.ProducedVersion > latestByKey[dep.Key] {
				latestByKey[dep.Key] = dep.ProducedVersion
			}
		}
	}
	for consumerIndex := range records {
		consumer := records[consumerIndex]
		for _, dependency := range consumer.StateVersions {
			if dependency.Key == "" || dependency.RequiredVersion == 0 {
				continue
			}
			ref, ok := producers[fmt.Sprintf("%s\x00%d", dependency.Key, dependency.RequiredVersion)]
			if !ok {
				continue
			}
			producerDep := &records[ref.RecordIndex].StateVersions[ref.DepIndex]
			producerShard := records[ref.RecordIndex].ExecutionShard
			consumerShard := consumer.ExecutionShard
			access, accessOK := metaTrackAccessByKeyRecord(consumer, dependency.Key)
			if accessOK && requiresExactStateValue(access) {
				producerDep.ValueSuccessorCount++
				if consumerShard == producerShard {
					producerDep.LocalValueSuccessorCount++
				} else {
					producerDep.RemoteValueSuccessorCount++
				}
			} else if consumerShard == producerShard {
				producerDep.LocalOrderingSuccessorCount++
			} else {
				// A remote blind-write successor normally does not read the predecessor,
				// but if it fails or becomes a logical no-op it must still alias the exact
				// predecessor value. Keep that version globally live until the successor
				// outcome is known.
				producerDep.RemoteOrderingSuccessorCount++
			}
		}
	}
	for i := range records {
		for j := range records[i].StateVersions {
			dep := &records[i].StateVersions[j]
			if dep.Key == "" || dep.ProducedVersion == 0 {
				continue
			}
			dep.BatchFinal = latestByKey[dep.Key] == dep.ProducedVersion
			switch {
			case dep.BatchFinal:
				dep.LivenessClass = metaTrackVersionClassFinalPersistent
			case dep.RemoteValueSuccessorCount > 0 || dep.RemoteOrderingSuccessorCount > 0:
				dep.LivenessClass = metaTrackVersionClassRemoteLive
			case dep.LocalValueSuccessorCount > 0 || dep.LocalOrderingSuccessorCount > 0:
				dep.LivenessClass = metaTrackVersionClassLocalTransient
			default:
				dep.LivenessClass = metaTrackVersionClassDeadIntermediate
			}
			dep.LivenessDigest = metaTrackVersionLivenessDigest(*dep)
		}
	}
	// Bind each successor's required version back to the classified predecessor.
	// This lets the runtime release local transient versions safely even when a
	// blind-write successor completes before the asynchronous predecessor
	// publication goroutine has installed its reference counter.
	for consumerIndex := range records {
		for depIndex := range records[consumerIndex].StateVersions {
			dependency := &records[consumerIndex].StateVersions[depIndex]
			if dependency.Key == "" || dependency.RequiredVersion == 0 {
				continue
			}
			ref, ok := producers[fmt.Sprintf("%s\x00%d", dependency.Key, dependency.RequiredVersion)]
			if !ok {
				continue
			}
			predecessor := records[ref.RecordIndex].StateVersions[ref.DepIndex]
			dependency.RequiredLivenessClass = predecessor.LivenessClass
			dependency.RequiredLivenessDigest = predecessor.LivenessDigest
		}
	}
	return true
}

func metaTrackAccessByKeyRecord(record WorkloadRecord, key string) (tx.AccessItem, bool) {
	for _, access := range record.AccessList {
		if access.Key == key {
			return access, true
		}
	}
	return tx.AccessItem{}, false
}

type metaTrackBufferedVersion struct {
	HomeShard string
	Key       string
	Item      state.StateKV
}

type metaTrackVersionPublishBuffer struct {
	mu                             sync.Mutex
	finals                         map[string]metaTrackBufferedVersion
	localSuccessorCount            map[metaTrackVersionIdentity]int
	blockRemoteValueSuccessorCount map[metaTrackVersionIdentity]int
	asyncV640                      *metaTrackAsyncVersionPublisherV640
}

type metaTrackTransientRefState struct {
	mu    sync.Mutex
	refs  map[string]int
	early map[string]int
}

var metaTrackVersionPublishBuffers sync.Map
var metaTrackTransientRefs sync.Map

func metaTrackVersionBufferKey(r *NodeRuntime, block realblock.Block) string {
	return fmt.Sprintf("%p|%s", r, block.BlockHash)
}

func (r *NodeRuntime) newMetaTrackVersionPublishBuffer(block realblock.Block) *metaTrackVersionPublishBuffer {
	buffer := &metaTrackVersionPublishBuffer{
		finals:                         map[string]metaTrackBufferedVersion{},
		localSuccessorCount:            map[metaTrackVersionIdentity]int{},
		blockRemoteValueSuccessorCount: metaTrackBlockRemoteValueConsumerIndexV640(block),
	}
	if r.metaTrackBlockExecutorFlag("dependency_closed_consensus") && r.metaTrackBlockExecutorFlag("batch_remote_writeback") {
		buffer.asyncV640 = newMetaTrackAsyncVersionPublisherV640()
		r.incrementFullLocalityMetric("metatrack_async_version_writeback_v640_block_count", 1)
	}
	if r.metaTrackBlockExecutorFlag("version_liveness_indexed") {
		producerShard := map[metaTrackVersionIdentity]string{}
		for _, item := range block.TxList {
			if item.ExecutionRouting == nil {
				continue
			}
			for _, dep := range item.ExecutionRouting.StateVersions {
				if dep.Key == "" || dep.ProducedVersion == 0 {
					continue
				}
				id := metaTrackVersionIdentity{Key: dep.Key, Version: dep.ProducedVersion}
				producerShard[id] = item.ExecutionRouting.ExecutionShard
			}
		}
		for _, item := range block.TxList {
			if item.ExecutionRouting == nil {
				continue
			}
			for _, dep := range item.ExecutionRouting.StateVersions {
				if dep.Key == "" || dep.RequiredVersion == 0 {
					continue
				}
				id := metaTrackVersionIdentity{Key: dep.Key, Version: dep.RequiredVersion}
				if producerShard[id] == item.ExecutionRouting.ExecutionShard && producerShard[id] != "" {
					buffer.localSuccessorCount[id]++
				}
			}
		}
	}
	metaTrackVersionPublishBuffers.Store(metaTrackVersionBufferKey(r, block), buffer)
	produced, localTransient, remoteLive, deadIntermediate, finalPersistent := 0, 0, 0, 0, 0
	valueEdges, localOrdering, remoteOrdering := 0, 0, 0
	digests := make([]string, 0)
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.ProducedVersion == 0 {
				continue
			}
			produced++
			valueEdges += dep.ValueSuccessorCount
			localOrdering += dep.LocalOrderingSuccessorCount
			remoteOrdering += dep.RemoteOrderingSuccessorCount
			switch dep.LivenessClass {
			case metaTrackVersionClassLocalTransient:
				localTransient++
			case metaTrackVersionClassRemoteLive:
				remoteLive++
			case metaTrackVersionClassDeadIntermediate:
				deadIntermediate++
			case metaTrackVersionClassFinalPersistent:
				finalPersistent++
			}
			if dep.LivenessDigest != "" {
				digests = append(digests, dep.LivenessDigest)
			}
		}
	}
	sort.Strings(digests)
	r.incrementFullLocalityMetric("metatrack_version_liveness_block_count", 1)
	r.incrementFullLocalityMetric("metatrack_version_produced_count", int64(produced))
	r.incrementFullLocalityMetric("metatrack_version_value_dependency_edge_count", int64(valueEdges))
	r.incrementFullLocalityMetric("metatrack_version_local_ordering_edge_count", int64(localOrdering))
	r.incrementFullLocalityMetric("metatrack_version_remote_ordering_edge_count", int64(remoteOrdering))
	r.incrementFullLocalityMetric("metatrack_version_local_transient_count", int64(localTransient))
	r.incrementFullLocalityMetric("metatrack_version_remote_live_count", int64(remoteLive))
	r.incrementFullLocalityMetric("metatrack_version_dead_intermediate_count", int64(deadIntermediate))
	r.incrementFullLocalityMetric("metatrack_version_final_persistent_count", int64(finalPersistent))
	blockRemoteValueConsumerEdges := 0
	for _, count := range buffer.blockRemoteValueSuccessorCount {
		blockRemoteValueConsumerEdges += count
	}
	r.incrementFullLocalityMetric("metatrack_block_remote_value_consumer_edge_count", int64(blockRemoteValueConsumerEdges))
	if len(digests) > 0 {
		r.emitRuntimeEvent(RuntimeEvent{Type: "MetaTrackVersionLivenessPlan", BlockHash: block.BlockHash, Height: block.Height, Success: true, Attributes: map[string]any{"policy": metaTrackVersionLivenessPolicyV5, "digest": stableTextDigest(strings.Join(digests, "|")), "produced_version_count": produced}})
	}
	return buffer
}

func (r *NodeRuntime) discardMetaTrackVersionLiveness(block realblock.Block) {
	key := metaTrackVersionBufferKey(r, block)
	if raw, ok := metaTrackVersionPublishBuffers.Load(key); ok {
		cancelMetaTrackAsyncVersionsV640(raw.(*metaTrackVersionPublishBuffer))
	}
	metaTrackVersionPublishBuffers.Delete(key)
}

func metaTrackLocalSuccessorCountInBlock(block realblock.Block, producer tx.SignedTransaction, dependency tx.StateVersionDependency) int {
	if producer.ExecutionRouting == nil || dependency.Key == "" || dependency.ProducedVersion == 0 {
		return 0
	}
	count := 0
	for _, candidate := range block.TxList {
		if candidate.ExecutionRouting == nil || candidate.ExecutionRouting.ExecutionShard != producer.ExecutionRouting.ExecutionShard {
			continue
		}
		for _, required := range candidate.ExecutionRouting.StateVersions {
			if required.Key == dependency.Key && required.RequiredVersion == dependency.ProducedVersion {
				count++
				break
			}
		}
	}
	return count
}

func metaTrackLocalTransientIsSameBlock(block realblock.Block, producer tx.SignedTransaction, dependency tx.StateVersionDependency) bool {
	expected := dependency.LocalValueSuccessorCount + dependency.LocalOrderingSuccessorCount
	return expected > 0 && metaTrackLocalSuccessorCountInBlock(block, producer, dependency) == expected
}

func metaTrackLocalTransientIsSameBlockIndexed(buffer *metaTrackVersionPublishBuffer, dependency tx.StateVersionDependency) bool {
	if buffer == nil || dependency.Key == "" || dependency.ProducedVersion == 0 {
		return false
	}
	expected := dependency.LocalValueSuccessorCount + dependency.LocalOrderingSuccessorCount
	if expected <= 0 {
		return false
	}
	return buffer.localSuccessorCount[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}] == expected
}

func metaTrackExactSnapshotValue(snapshot map[string]string, key string) (string, bool) {
	if value, ok := snapshot[key]; ok {
		return value, true
	}
	for candidate, value := range snapshot {
		if stateKeysReferToSameLogicalKey(candidate, key) {
			return value, true
		}
	}
	return "", false
}

func metaTrackTransactionLivenessMetadataComplete(item tx.SignedTransaction) bool {
	if item.ExecutionRouting == nil {
		return false
	}
	for _, dep := range item.ExecutionRouting.StateVersions {
		if dep.ProducedVersion == 0 {
			continue
		}
		if dep.LivenessClass == "" || dep.LivenessDigest == "" {
			return false
		}
	}
	return true
}

func metaTrackTransientRefKey(key string, version uint64) string {
	return fmt.Sprintf("%s\x00%d", key, version)
}

func (r *NodeRuntime) metaTrackTransientRefState() *metaTrackTransientRefState {
	key := fmt.Sprintf("%p", r)
	raw, _ := metaTrackTransientRefs.LoadOrStore(key, &metaTrackTransientRefState{refs: map[string]int{}, early: map[string]int{}})
	return raw.(*metaTrackTransientRefState)
}

func (r *NodeRuntime) registerMetaTrackTransientVersion(dependency tx.StateVersionDependency) {
	refs := dependency.LocalValueSuccessorCount + dependency.LocalOrderingSuccessorCount
	state := r.metaTrackTransientRefState()
	refKey := metaTrackTransientRefKey(dependency.Key, dependency.ProducedVersion)
	state.mu.Lock()
	refs -= state.early[refKey]
	delete(state.early, refKey)
	if refs > 0 {
		state.refs[refKey] = refs
	} else {
		delete(state.refs, refKey)
	}
	state.mu.Unlock()
	if refs <= 0 {
		r.deleteMetaTrackTransientVersion(dependency.Key, dependency.ProducedVersion)
		return
	}
	r.incrementFullLocalityMetric("metatrack_version_transient_ref_registered_count", int64(refs))
}

func (r *NodeRuntime) releaseMetaTrackTransientPredecessors(item tx.SignedTransaction) {
	if item.ExecutionRouting == nil {
		return
	}
	state := r.metaTrackTransientRefState()
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.Key == "" || dependency.RequiredVersion == 0 || dependency.RequiredLivenessClass != metaTrackVersionClassLocalTransient {
			continue
		}
		key := metaTrackTransientRefKey(dependency.Key, dependency.RequiredVersion)
		state.mu.Lock()
		refs, registered := state.refs[key]
		if registered {
			refs--
			if refs <= 0 {
				delete(state.refs, key)
			} else {
				state.refs[key] = refs
			}
		} else {
			state.early[key]++
		}
		state.mu.Unlock()
		if registered && refs <= 0 {
			r.deleteMetaTrackTransientVersion(dependency.Key, dependency.RequiredVersion)
		}
	}
}

func (r *NodeRuntime) deleteMetaTrackTransientVersion(key string, version uint64) {
	deleted := false
	r.mu.Lock()
	if versions := r.stateVersionValues[key]; versions != nil {
		if _, exists := versions[version]; exists {
			delete(versions, version)
			deleted = true
		}
		if len(versions) == 0 {
			delete(r.stateVersionValues, key)
		}
	}
	r.mu.Unlock()
	if deleted {
		r.incrementFullLocalityMetric("metatrack_version_gc_count", 1)
	}
}

func (r *NodeRuntime) metaTrackVersionLivenessPublisher(block realblock.Block) StateVersionPublishFunc {
	buffer := r.newMetaTrackVersionPublishBuffer(block)
	forceHomeExactV660 := metaTrackForceHomeExactAccess(r.plugins.StateAccess) || r.metaTrackBlockExecutorFlag(metaTrackAblationForceHomeExactV660) // legacy flag fallback only
	return func(ctx context.Context, item tx.SignedTransaction, delta execution.TxDelta, exactSnapshot map[string]string) error {
		if item.ExecutionRouting == nil || len(item.ExecutionRouting.StateVersions) == 0 {
			return nil
		}
		if !metaTrackTransactionLivenessMetadataComplete(item) {
			return fmt.Errorf("invalid metatrack version-liveness metadata: producer liveness metadata incomplete")
		}
		if err := validateMetaTrackVersionLivenessMetadata(item); err != nil {
			return fmt.Errorf("invalid metatrack version-liveness metadata: %w", err)
		}
		accessByKey := map[string]tx.AccessItem{}
		for _, access := range item.AccessList {
			if access.Key != "" {
				accessByKey[access.Key] = access
			}
		}
		shardIDs := r.shardIDs()
		closureBackgroundAsync := buffer.asyncV640 != nil
		immediateByHome := map[string][]state.StateKV{}
		immediateUnqualified := map[string]string{}
		immediateClassByKey := map[string]string{}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.ProducedVersion == 0 {
				continue
			}
			access, ok := accessByKey[dependency.Key]
			if !ok || !isVersionedStateAccess(access) {
				continue
			}
			homeShard := r.stateHomeShardForKey(dependency.Key, shardIDs)
			if homeShard == "" {
				return fmt.Errorf("versioned state %s has no persistent home shard", dependency.Key)
			}
			value, wrote := writeSetLogicalValue(delta.WriteSet, dependency.Key)
			orderingNoop := !delta.Success || !wrote
			if orderingNoop {
				if resolved, ready := metaTrackExactSnapshotValue(exactSnapshot, dependency.Key); ready {
					value = resolved
				} else if homeShard == r.node.ShardID {
					resolved, err := r.waitForLocalStateVersion(ctx, dependency.Key, dependency.RequiredVersion)
					if err != nil {
						return fmt.Errorf("resolve local predecessor %s@%d for liveness no-op: %w", dependency.Key, dependency.RequiredVersion, err)
					}
					value = resolved
				} else {
					response, latency, err := r.fetchRemoteState(ctx, block, item, access, homeShard)
					if err != nil {
						return fmt.Errorf("resolve remote predecessor %s@%d for liveness no-op: %w", dependency.Key, dependency.RequiredVersion, err)
					}
					r.recordRemoteStateAccess(block, item, access, response, latency)
					value = response.Value
				}
			}

			// The exact version is always made visible on its execution shard first.
			// Same-block consumers therefore continue to use the existing transaction-
			// level local handoff path and never need to wait for Home durability.
			r.publishStateVersion(dependency.Key, dependency.ProducedVersion, value)
			if homeShard == r.node.ShardID {
				switch dependency.LivenessClass {
				case metaTrackVersionClassLocalTransient:
					r.registerMetaTrackTransientVersion(dependency)
					sameBlock := metaTrackLocalTransientIsSameBlock(block, item, dependency)
					if r.metaTrackBlockExecutorFlag("version_liveness_indexed") {
						sameBlock = metaTrackLocalTransientIsSameBlockIndexed(buffer, dependency)
					}
					if !sameBlock {
						r.incrementFullLocalityMetric("metatrack_version_cross_block_preserved_count", 1)
					}
				case metaTrackVersionClassDeadIntermediate:
					r.deleteMetaTrackTransientVersion(dependency.Key, dependency.ProducedVersion)
				}
				continue
			}

			// Preserve the existing bounded publication-owner rule for network side
			// effects. PBFT replicas do not independently duplicate Home publication.
			if r.node.NodeID != block.ProposerID && !r.isCurrentLeader() {
				continue
			}
			versionItem := state.StateKV{
				Key:             qualifyStateKey(r.node.ShardID, dependency.Key),
				Value:           value,
				TxIDs:           []string{item.TxID},
				ApplyOrigin:     "version_liveness_v5",
				RoutingOrdinal:  dependency.ProducedVersion,
				PreviousVersion: dependency.RequiredVersion,
				ProducedVersion: dependency.ProducedVersion,
				OrderingNoop:    orderingNoop,
			}
			if dependency.LivenessClass == metaTrackVersionClassLocalTransient {
				r.registerMetaTrackTransientVersion(dependency)
			}
			effectiveClass := dependency.LivenessClass
			sameBlock := metaTrackLocalTransientIsSameBlock(block, item, dependency)
			if r.metaTrackBlockExecutorFlag("version_liveness_indexed") {
				sameBlock = metaTrackLocalTransientIsSameBlockIndexed(buffer, dependency)
			}
			if effectiveClass == metaTrackVersionClassLocalTransient && !sameBlock {
				// A route-batch-local successor may land in a later PBFT block. Keep the
				// predecessor globally visible/durable in that case.
				effectiveClass = metaTrackVersionClassRemoteLive
				r.incrementFullLocalityMetric("metatrack_version_cross_block_preserved_count", 1)
			}
			ablationClass := metaTrackAblationHomePublicationClassV660(forceHomeExactV660, dependency.LocalValueSuccessorCount, effectiveClass)
			if ablationClass != effectiveClass {
				r.incrementFullLocalityMetric("metatrack_ablation_force_home_exact_publish_count_v660", 1)
				effectiveClass = ablationClass
			}
			switch effectiveClass {
			case metaTrackVersionClassLocalTransient, metaTrackVersionClassDeadIntermediate:
				if effectiveClass == metaTrackVersionClassDeadIntermediate {
					r.deleteMetaTrackTransientVersion(dependency.Key, dependency.ProducedVersion)
				}
				nodes := len(r.stateAccessNodeIDsForShard(homeShard))
				r.incrementFullLocalityMetric("metatrack_version_home_writeback_elided_count", 1)
				r.incrementFullLocalityMetric("metatrack_version_avoidable_apply_message_count", int64(nodes))
				r.incrementFullLocalityMetric("metatrack_version_avoidable_ack_message_count", int64(nodes))
				continue
			case metaTrackVersionClassFinalPersistent:
				if closureBackgroundAsync {
					identity := metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}
					if buffer.blockRemoteValueSuccessorCount[identity] > 0 {
						immediateByHome[homeShard] = append(immediateByHome[homeShard], versionItem)
						immediateUnqualified[versionItem.Key] = dependency.Key
						immediateClassByKey[versionItem.Key] = effectiveClass
						continue
					}
					if err := r.enqueueMetaTrackAsyncFinalV640(block, buffer, metaTrackAsyncVersionTaskV640{
						HomeShard:  homeShard,
						LogicalKey: dependency.Key,
						Item:       versionItem,
						Delta:      delta,
					}); err != nil {
						return err
					}
					continue
				}
				if dependency.RemoteValueSuccessorCount == 0 && dependency.RemoteOrderingSuccessorCount == 0 && r.metaTrackBlockExecutorFlag("final_version_batch_writeback") {
					buffer.mu.Lock()
					buffer.finals[homeShard+"\x00"+dependency.Key] = metaTrackBufferedVersion{HomeShard: homeShard, Key: dependency.Key, Item: versionItem}
					buffer.mu.Unlock()
					r.incrementFullLocalityMetric("metatrack_version_final_batch_buffered_count", 1)
					continue
				}
			case metaTrackVersionClassRemoteLive:
				if closureBackgroundAsync {
					immediateByHome[homeShard] = append(immediateByHome[homeShard], versionItem)
					immediateUnqualified[versionItem.Key] = dependency.Key
					immediateClassByKey[versionItem.Key] = effectiveClass
					continue
				}
			}

			// Frozen/current MetaTrack keeps the historical synchronous publication path.
			acks, latency, err := r.applyRemoteStateDelta(ctx, block, versionItem, dependency.Key, homeShard, []execution.TxDelta{delta})
			if err != nil {
				return err
			}
			for _, ack := range acks {
				r.recordRemoteStateApply(block, versionItem, dependency.Key, ack, latency)
			}
			if effectiveClass == metaTrackVersionClassRemoteLive {
				r.incrementFullLocalityMetric("metatrack_version_remote_live_immediate_publish_count", 1)
			} else {
				r.incrementFullLocalityMetric("metatrack_version_final_immediate_publish_count", 1)
			}
		}
		if closureBackgroundAsync && len(immediateByHome) > 0 {
			homes := make([]string, 0, len(immediateByHome))
			for home := range immediateByHome {
				homes = append(homes, home)
			}
			sort.Strings(homes)
			for _, home := range homes {
				items := immediateByHome[home]
				sort.SliceStable(items, func(i, j int) bool {
					if items[i].RoutingOrdinal != items[j].RoutingOrdinal {
						return items[i].RoutingOrdinal < items[j].RoutingOrdinal
					}
					return items[i].Key < items[j].Key
				})
				started := time.Now()
				if err := r.applyRemoteStateDeltaBatch(ctx, block, home, items, immediateUnqualified, []execution.TxDelta{delta}); err != nil {
					return err
				}
				r.incrementFullLocalityMetric("metatrack_critical_version_publish_wait_ms", time.Since(started).Milliseconds())
				r.incrementFullLocalityMetric("metatrack_closure_boundary_batch_request_group_count", 1)
				r.incrementFullLocalityMetric("metatrack_closure_boundary_batch_item_count", int64(len(items)))
				for _, versionItem := range items {
					if immediateClassByKey[versionItem.Key] == metaTrackVersionClassRemoteLive {
						r.incrementFullLocalityMetric("metatrack_version_remote_live_immediate_publish_count", 1)
					} else {
						r.incrementFullLocalityMetric("metatrack_closure_boundary_immediate_publish_count", 1)
						r.incrementFullLocalityMetric("metatrack_block_consumer_critical_final_publish_count", 1)
						r.incrementFullLocalityMetric("metatrack_version_final_immediate_publish_count", 1)
					}
				}
			}
		}
		// A predecessor remains live across PBFT block boundaries until this
		// successor has actually completed. Only then may a local transient exact
		// version be reclaimed.
		r.releaseMetaTrackTransientPredecessors(item)
		return nil
	}
}

func (r *NodeRuntime) flushMetaTrackVersionLiveness(ctx context.Context, block realblock.Block, txDeltas []execution.TxDelta) error {
	key := metaTrackVersionBufferKey(r, block)
	raw, ok := metaTrackVersionPublishBuffers.Load(key)
	if !ok {
		return nil
	}
	defer metaTrackVersionPublishBuffers.Delete(key)
	buffer := raw.(*metaTrackVersionPublishBuffer)
	defer cancelMetaTrackAsyncVersionsV640(buffer)
	if err := r.deferOrJoinMetaTrackAsyncVersionsV656(ctx, block, buffer); err != nil {
		return err
	}
	buffer.mu.Lock()
	finals := make([]metaTrackBufferedVersion, 0, len(buffer.finals))
	for _, item := range buffer.finals {
		finals = append(finals, item)
	}
	buffer.mu.Unlock()
	sort.SliceStable(finals, func(i, j int) bool {
		if finals[i].HomeShard != finals[j].HomeShard {
			return finals[i].HomeShard < finals[j].HomeShard
		}
		if finals[i].Item.RoutingOrdinal != finals[j].Item.RoutingOrdinal {
			return finals[i].Item.RoutingOrdinal < finals[j].Item.RoutingOrdinal
		}
		return finals[i].Key < finals[j].Key
	})
	grouped := map[string][]state.StateKV{}
	unqualified := map[string]string{}
	for _, item := range finals {
		grouped[item.HomeShard] = append(grouped[item.HomeShard], item.Item)
		unqualified[item.Item.Key] = item.Key
	}
	homes := make([]string, 0, len(grouped))
	for home := range grouped {
		homes = append(homes, home)
	}
	sort.Strings(homes)
	for _, home := range homes {
		items := grouped[home]
		if len(items) == 0 {
			continue
		}
		if r.metaTrackBlockExecutorFlag("final_version_batch_writeback") {
			if err := r.applyRemoteStateDeltaBatch(ctx, block, home, items, unqualified, txDeltas); err != nil {
				return err
			}
			r.incrementFullLocalityMetric("metatrack_version_final_batch_request_group_count", 1)
			r.incrementFullLocalityMetric("metatrack_version_final_batch_item_count", int64(len(items)))
			continue
		}
		for _, item := range items {
			logicalKey := unqualified[item.Key]
			acks, latency, err := r.applyRemoteStateDelta(ctx, block, item, logicalKey, home, txDeltas)
			if err != nil {
				return err
			}
			for _, ack := range acks {
				r.recordRemoteStateApply(block, item, logicalKey, ack, latency)
			}
		}
	}

	return nil
}
