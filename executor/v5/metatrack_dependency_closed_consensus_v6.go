package v5

import (
	"fmt"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

const metaTrackDependencyClosedConsensusPolicyV6 = "dependency_closed_projection_prefix_v6"

type metaTrackVersionIdentity struct {
	Key     string
	Version uint64
}

type metaTrackProducedVersionIdentity struct {
	Sequence uint64
	Ordinal  uint64
}

// validateMetaTrackDependencyClosedProjectionBlock proves the safety property
// used by the newest MetaTrack profile before a multi-projection block is
// accepted. Exact-version edges must move strictly forward in the globally
// signed RoutingOrdinal order, and every produced (key, version) identity must
// be unique inside the aggregate block. Combined with complete signed
// projection packing, this rules out future-version back edges and therefore
// prevents an exact-version StateReady cycle inside the aggregate block.
func validateMetaTrackDependencyClosedProjectionBlock(block realblock.Block, requireMetadata bool) ([]metaTrackBatchProjectionIdentity, error) {
	projections, err := validateMetaTrackAggregatedBatchProjections(block, requireMetadata)
	if err != nil {
		return nil, err
	}
	if len(projections) == 0 {
		return projections, nil
	}
	produced := map[metaTrackVersionIdentity]metaTrackProducedVersionIdentity{}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			return nil, fmt.Errorf("metatrack dependency closure transaction %s missing execution routing", txIdentifier(item))
		}
		routing := item.ExecutionRouting
		ordinal := routing.RoutingOrdinal
		if ordinal == 0 {
			return nil, fmt.Errorf("metatrack dependency closure transaction %s has zero routing ordinal", txIdentifier(item))
		}
		for _, dependency := range routing.StateVersions {
			if dependency.ProducedVersion > 0 {
				if dependency.ProducedVersion != ordinal {
					return nil, fmt.Errorf("metatrack dependency closure produced version mismatch: tx=%s key=%s produced=%d ordinal=%d", txIdentifier(item), dependency.Key, dependency.ProducedVersion, ordinal)
				}
				identity := metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}
				if previous, exists := produced[identity]; exists {
					return nil, fmt.Errorf("metatrack dependency closure duplicate producer: key=%s version=%d first_sequence=%d first_ordinal=%d", dependency.Key, dependency.ProducedVersion, previous.Sequence, previous.Ordinal)
				}
				produced[identity] = metaTrackProducedVersionIdentity{Sequence: routing.RouteBatchSequence, Ordinal: ordinal}
			}
			if dependency.RequiredVersion > 0 && dependency.RequiredVersion >= ordinal {
				return nil, fmt.Errorf("metatrack dependency closure future/non-monotonic requirement: tx=%s key=%s required=%d ordinal=%d", txIdentifier(item), dependency.Key, dependency.RequiredVersion, ordinal)
			}
		}
	}
	// If a producer is present locally in the aggregate block, its signed
	// projection and ordinal must precede the consumer. Remote predecessors are
	// allowed: global ordinal monotonicity still prevents a dependency cycle.
	for _, item := range block.TxList {
		routing := item.ExecutionRouting
		for _, dependency := range routing.StateVersions {
			if dependency.RequiredVersion == 0 {
				continue
			}
			producer, exists := produced[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}]
			if !exists {
				continue
			}
			if producer.Ordinal >= routing.RoutingOrdinal || producer.Sequence > routing.RouteBatchSequence {
				return nil, fmt.Errorf("metatrack dependency closure local predecessor order mismatch: tx=%s key=%s required=%d", txIdentifier(item), dependency.Key, dependency.RequiredVersion)
			}
		}
	}
	return projections, nil
}

// selectMetaTrackDependencyClosedPBFTProjections reuses the historical complete
// signed-projection aggregator, but grows only the largest safe prefix. It does
// not split a projection and does not solve a global maximum-set problem.
func selectMetaTrackDependencyClosedPBFTProjections(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	_, _, all, err := selectMetaTrackAggregatedBatchProjections(items, limit, shardID)
	if err != nil {
		return nil, nil, nil, err
	}
	var bestSelected, bestDeferred []tx.SignedTransaction
	var bestProjections []metaTrackBatchProjectionIdentity
	for count := 1; count <= len(all); count++ {
		selected, deferred, projections, selectErr := selectMetaTrackBatchProjectionGroups(items, limit, shardID, count)
		if selectErr != nil {
			if count == 1 {
				return nil, nil, nil, selectErr
			}
			break
		}
		block := realblock.Block{ShardID: shardID, TxList: selected}
		if _, validateErr := validateMetaTrackDependencyClosedProjectionBlock(block, true); validateErr != nil {
			if count == 1 {
				return nil, nil, nil, validateErr
			}
			break
		}
		bestSelected, bestDeferred, bestProjections = selected, deferred, projections
	}
	if len(bestSelected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack dependency closure found no safe complete projection")
	}
	return bestSelected, bestDeferred, bestProjections, nil
}
