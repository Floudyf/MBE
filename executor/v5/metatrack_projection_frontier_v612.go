package v5

import (
	"fmt"
	"sort"

	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_PROJECTION_FRONTIER_V612
//
// The dependency-closed v6 producer already packs complete signed projections
// into one PBFT block. This helper adds only an execution-release frontier:
// cross-projection exact-value dependencies form a DAG and a projection becomes
// runnable only after all predecessor projections in that DAG have completed.
// No threshold, timeout or workload-specific constant participates in the rule.
type metaTrackProjectionFrontierV612Plan struct {
	SequenceByTxID           map[string]uint64
	PredecessorsBySequence   map[uint64][]uint64
	TxIDsBySequence          map[uint64][]string
	ProjectionCount          int
	LayerCount               int
	MaxLayerProjectionWidth  int
	CrossProjectionEdgeCount int
}

type metaTrackProjectionVersionTokenV612 struct {
	Key     string
	Version uint64
}

type metaTrackProjectionProducerV612 struct {
	TxID     string
	Sequence uint64
	Ordinal  uint64
}

func buildMetaTrackProjectionFrontierV612Plan(items []tx.SignedTransaction) (metaTrackProjectionFrontierV612Plan, error) {
	plan := metaTrackProjectionFrontierV612Plan{
		SequenceByTxID:         map[string]uint64{},
		PredecessorsBySequence: map[uint64][]uint64{},
		TxIDsBySequence:        map[uint64][]string{},
	}
	if len(items) == 0 {
		return plan, nil
	}

	sequences := map[uint64]bool{}
	producers := map[metaTrackProjectionVersionTokenV612]metaTrackProjectionProducerV612{}
	ordinalByTxID := map[string]uint64{}
	for _, item := range items {
		if item.ExecutionRouting == nil {
			return plan, fmt.Errorf("transaction %s missing signed execution routing metadata", txIdentifier(item))
		}
		routing := item.ExecutionRouting
		txID := txIdentifier(item)
		if txID == "" || routing.RouteBatchSequence == 0 || routing.RoutingOrdinal == 0 {
			return plan, fmt.Errorf("transaction %s has incomplete projection identity", txID)
		}
		if _, exists := plan.SequenceByTxID[txID]; exists {
			return plan, fmt.Errorf("duplicate transaction identity %s in projection frontier", txID)
		}
		plan.SequenceByTxID[txID] = routing.RouteBatchSequence
		ordinalByTxID[txID] = routing.RoutingOrdinal
		plan.TxIDsBySequence[routing.RouteBatchSequence] = append(plan.TxIDsBySequence[routing.RouteBatchSequence], txID)
		sequences[routing.RouteBatchSequence] = true
		for _, dep := range routing.StateVersions {
			if dep.Key == "" || dep.ProducedVersion == 0 {
				continue
			}
			token := metaTrackProjectionVersionTokenV612{Key: dep.Key, Version: dep.ProducedVersion}
			if previous, exists := producers[token]; exists && previous.TxID != txID {
				return plan, fmt.Errorf("duplicate exact-version producer %s@%d", dep.Key, dep.ProducedVersion)
			}
			producers[token] = metaTrackProjectionProducerV612{TxID: txID, Sequence: routing.RouteBatchSequence, Ordinal: routing.RoutingOrdinal}
		}
	}

	edges := map[uint64]map[uint64]bool{}
	indegree := map[uint64]int{}
	for sequence := range sequences {
		edges[sequence] = map[uint64]bool{}
		indegree[sequence] = 0
	}
	for _, item := range items {
		routing := item.ExecutionRouting
		consumerTxID := txIdentifier(item)
		for _, dep := range routing.StateVersions {
			if dep.Key == "" || dep.RequiredVersion == 0 || !transactionRequiresExactStateValue(item, dep.Key) {
				continue
			}
			producer, exists := producers[metaTrackProjectionVersionTokenV612{Key: dep.Key, Version: dep.RequiredVersion}]
			if !exists || producer.Sequence == routing.RouteBatchSequence {
				continue
			}
			consumerOrdinal := ordinalByTxID[consumerTxID]
			if producer.Ordinal >= consumerOrdinal {
				return plan, fmt.Errorf("exact-version ordinal inversion %s@%d producer=%d consumer=%d", dep.Key, dep.RequiredVersion, producer.Ordinal, consumerOrdinal)
			}
			if producer.Sequence > routing.RouteBatchSequence {
				return plan, fmt.Errorf("projection sequence inversion %s@%d producer_sequence=%d consumer_sequence=%d", dep.Key, dep.RequiredVersion, producer.Sequence, routing.RouteBatchSequence)
			}
			if !edges[producer.Sequence][routing.RouteBatchSequence] {
				edges[producer.Sequence][routing.RouteBatchSequence] = true
				indegree[routing.RouteBatchSequence]++
				plan.CrossProjectionEdgeCount++
			}
		}
	}

	ready := make([]uint64, 0, len(sequences))
	for sequence := range sequences {
		if indegree[sequence] == 0 {
			ready = append(ready, sequence)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	layerBySequence := map[uint64]int{}
	processed := 0
	for len(ready) > 0 {
		sequence := ready[0]
		ready = ready[1:]
		processed++
		layer := 0
		for predecessor := range sequences {
			if edges[predecessor][sequence] && layerBySequence[predecessor]+1 > layer {
				layer = layerBySequence[predecessor] + 1
			}
		}
		layerBySequence[sequence] = layer
		next := make([]uint64, 0, len(edges[sequence]))
		for successor := range edges[sequence] {
			indegree[successor]--
			if indegree[successor] == 0 {
				next = append(next, successor)
			}
		}
		if len(next) > 0 {
			ready = append(ready, next...)
			sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
		}
	}
	if processed != len(sequences) {
		return plan, fmt.Errorf("projection dependency graph contains a cycle")
	}

	layerWidths := map[int]int{}
	for sequence := range sequences {
		preds := make([]uint64, 0)
		for predecessor := range sequences {
			if edges[predecessor][sequence] {
				preds = append(preds, predecessor)
			}
		}
		sort.Slice(preds, func(i, j int) bool { return preds[i] < preds[j] })
		plan.PredecessorsBySequence[sequence] = preds
		layer := layerBySequence[sequence]
		layerWidths[layer]++
		if layer+1 > plan.LayerCount {
			plan.LayerCount = layer + 1
		}
		sort.Strings(plan.TxIDsBySequence[sequence])
	}
	for _, width := range layerWidths {
		if width > plan.MaxLayerProjectionWidth {
			plan.MaxLayerProjectionWidth = width
		}
	}
	plan.ProjectionCount = len(sequences)
	return plan, nil
}

func (p *metaTrackProjectionFrontierV612Plan) transactionReady(txID string, completed map[string]bool) bool {
	if p == nil {
		return true
	}
	sequence, ok := p.SequenceByTxID[txID]
	if !ok {
		return false
	}
	for _, predecessor := range p.PredecessorsBySequence[sequence] {
		for _, predecessorTxID := range p.TxIDsBySequence[predecessor] {
			if !completed[predecessorTxID] {
				return false
			}
		}
	}
	return true
}
