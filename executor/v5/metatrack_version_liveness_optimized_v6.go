package v5

import (
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

type metaTrackAccessIndex map[string]tx.AccessItem

func metaTrackBuildAccessIndex(record WorkloadRecord) metaTrackAccessIndex {
	index := make(metaTrackAccessIndex, len(record.AccessList))
	for _, access := range record.AccessList {
		if access.Key != "" {
			index[access.Key] = access
		}
	}
	return index
}

// annotateMetaTrackVersionLivenessRecordsIndexed is semantically equivalent to
// the v5 classifier but constructs producer and AccessList indices once. The
// newest profile uses this path so liveness remains O(accesses + dependency
// edges) instead of repeatedly scanning each transaction AccessList.
func annotateMetaTrackVersionLivenessRecordsIndexed(records []WorkloadRecord) bool {
	if len(records) == 0 {
		return true
	}
	accessIndex := make([]metaTrackAccessIndex, len(records))
	for i := range records {
		if strings.TrimSpace(records[i].ExecutionShard) == "" {
			return false
		}
		accessIndex[i] = metaTrackBuildAccessIndex(records[i])
		for j := range records[i].StateVersions {
			records[i].StateVersions[j].RequiredLivenessClass = ""
			records[i].StateVersions[j].RequiredLivenessDigest = ""
		}
	}
	producers := map[metaTrackVersionIdentity]metaTrackVersionRef{}
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
			identity := metaTrackVersionIdentity{Key: dep.Key, Version: dep.ProducedVersion}
			if _, exists := producers[identity]; exists {
				return false
			}
			producers[identity] = metaTrackVersionRef{RecordIndex: i, DepIndex: j}
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
			ref, ok := producers[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}]
			if !ok {
				continue
			}
			producerDep := &records[ref.RecordIndex].StateVersions[ref.DepIndex]
			producerShard := records[ref.RecordIndex].ExecutionShard
			consumerShard := consumer.ExecutionShard
			access, accessOK := accessIndex[consumerIndex][dependency.Key]
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
	for consumerIndex := range records {
		for depIndex := range records[consumerIndex].StateVersions {
			dependency := &records[consumerIndex].StateVersions[depIndex]
			if dependency.Key == "" || dependency.RequiredVersion == 0 {
				continue
			}
			ref, ok := producers[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}]
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

func finalizeMetaTrackSignedBatchPlan(records []WorkloadRecord, plan BatchRoutingPlan, indexedLiveness bool) (BatchRoutingPlan, bool) {
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
	if indexedLiveness {
		if !annotateMetaTrackVersionLivenessRecordsIndexed(records) {
			return BatchRoutingPlan{}, false
		}
	} else if !annotateMetaTrackVersionLivenessRecords(records) {
		return BatchRoutingPlan{}, false
	}
	applyMetaTrackDeclaredAccessFrontierV2(&plan, records)
	plan.PlanDigest = routingPlanDigest(plan)
	return plan, true
}
