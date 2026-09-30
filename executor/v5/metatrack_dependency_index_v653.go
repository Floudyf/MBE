package v5

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_UNIFIED_DEPENDENCY_INDEX_V653
// MBE_METATRACK_PROJECTION_ORDER_BARRIER_EXTENSION_V655
//
// The newest MetaTrack path already signs every piece of static evidence needed
// to derive execution readiness: declared/scheduling access, exact-version
// dependencies, sender/nonce order, execution placement and RouteBatch identity.
// v6.5.3 indexes that evidence once per node mempool and reuses it across block
// proposals. No runtime StateReady result, execution completion, network delay or
// other future information enters this index.
//
// Execution barriers and ordering-only constraints remain separate. Only exact
// value, RAW and same-sender nonce edges contribute to readiness. WAW/WAR remain
// deterministic materialization/order evidence and never become execution
// barriers here.
type metaTrackIndexedVersionV653 struct {
	ProducerTxID                 string
	Sequence                     uint64
	Ordinal                      uint64
	ExecutionShard               string
	LivenessClass                string
	BatchFinal                   bool
	LocalValueSuccessorCount     int
	RemoteValueSuccessorCount    int
	LocalOrderingSuccessorCount  int
	RemoteOrderingSuccessorCount int
}

type metaTrackIndexedTxV653 struct {
	Item           tx.SignedTransaction
	TxID           string
	Ordinal        uint64
	Sequence       uint64
	ExecutionShard string
	AccessByKey    map[string]tx.AccessItem
	DepthL         int
}

type metaTrackDependencyIndexV653 struct {
	mu sync.Mutex

	itemsByOrdinal map[uint64]tx.SignedTransaction
	txByID         map[string]metaTrackIndexedTxV653
	ordinalOwner   map[uint64]string

	versionProducer map[metaTrackVersionIdentity]string
	versionInfo     map[metaTrackVersionIdentity]metaTrackIndexedVersionV653

	predecessors            map[string]map[string]bool
	children                map[string]map[string]bool
	projectionPred          map[uint64]map[uint64]bool
	projectionChildren      map[uint64]map[uint64]bool
	projectionOrderPred     map[uint64]map[uint64]bool
	projectionOrderChildren map[uint64]map[uint64]bool

	lastWriter         map[string]string
	commutativeWriters map[string][]string
	readers            map[string][]string
	lastSenderTx       map[string]string

	maxOrdinal    uint64
	newTxCount    int64
	reusedTxCount int64
	rebuildCount  int64
}

type metaTrackDependencyIndexStatsV653 struct {
	IndexedTransactionCount     int
	ExecutionEdgeCount          int
	ProjectionEdgeCount         int
	ProjectionOrderingEdgeCount int
	NewTransactionCount         int64
	ReusedTransactionCount      int64
	RebuildCount                int64
}

var metaTrackDependencyIndexCacheV653 = struct {
	sync.Mutex
	byPool map[*mempool.Mempool]*metaTrackDependencyIndexV653
}{byPool: map[*mempool.Mempool]*metaTrackDependencyIndexV653{}}

func newMetaTrackDependencyIndexV653() *metaTrackDependencyIndexV653 {
	index := &metaTrackDependencyIndexV653{itemsByOrdinal: map[uint64]tx.SignedTransaction{}}
	index.resetDerivedLocked()
	return index
}

func metaTrackDependencyIndexForPoolV653(pool *mempool.Mempool) *metaTrackDependencyIndexV653 {
	if pool == nil {
		return newMetaTrackDependencyIndexV653()
	}
	metaTrackDependencyIndexCacheV653.Lock()
	defer metaTrackDependencyIndexCacheV653.Unlock()
	if index := metaTrackDependencyIndexCacheV653.byPool[pool]; index != nil {
		return index
	}
	index := newMetaTrackDependencyIndexV653()
	metaTrackDependencyIndexCacheV653.byPool[pool] = index
	return index
}

func (index *metaTrackDependencyIndexV653) resetDerivedLocked() {
	index.txByID = map[string]metaTrackIndexedTxV653{}
	index.ordinalOwner = map[uint64]string{}
	index.versionProducer = map[metaTrackVersionIdentity]string{}
	index.versionInfo = map[metaTrackVersionIdentity]metaTrackIndexedVersionV653{}
	index.predecessors = map[string]map[string]bool{}
	index.children = map[string]map[string]bool{}
	index.projectionPred = map[uint64]map[uint64]bool{}
	index.projectionChildren = map[uint64]map[uint64]bool{}
	index.projectionOrderPred = map[uint64]map[uint64]bool{}
	index.projectionOrderChildren = map[uint64]map[uint64]bool{}
	index.lastWriter = map[string]string{}
	index.commutativeWriters = map[string][]string{}
	index.readers = map[string][]string{}
	index.lastSenderTx = map[string]string{}
	index.maxOrdinal = 0
}

func metaTrackIndexAccessByKeyV653(item tx.SignedTransaction) map[string]tx.AccessItem {
	out := map[string]tx.AccessItem{}
	for _, access := range classificationAccessItems(item) {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			continue
		}
		out[key] = access
	}
	return out
}

func (index *metaTrackDependencyIndexV653) addExecutionEdgeLocked(from, to string) {
	if from == "" || to == "" || from == to {
		return
	}
	if index.predecessors[to] == nil {
		index.predecessors[to] = map[string]bool{}
	}
	if index.predecessors[to][from] {
		return
	}
	index.predecessors[to][from] = true
	if index.children[from] == nil {
		index.children[from] = map[string]bool{}
	}
	index.children[from][to] = true
	fromTx, fromOK := index.txByID[from]
	toTx, toOK := index.txByID[to]
	if fromOK && toOK && fromTx.Sequence != toTx.Sequence {
		if index.projectionPred[toTx.Sequence] == nil {
			index.projectionPred[toTx.Sequence] = map[uint64]bool{}
		}
		index.projectionPred[toTx.Sequence][fromTx.Sequence] = true
		if index.projectionChildren[fromTx.Sequence] == nil {
			index.projectionChildren[fromTx.Sequence] = map[uint64]bool{}
		}
		index.projectionChildren[fromTx.Sequence][toTx.Sequence] = true
	}
}

func (index *metaTrackDependencyIndexV653) addOrderingOnlyProjectionEdgeLocked(from, to string) {
	if from == "" || to == "" || from == to {
		return
	}
	fromTx, fromOK := index.txByID[from]
	toTx, toOK := index.txByID[to]
	if !fromOK || !toOK || fromTx.Sequence == toTx.Sequence {
		return
	}
	if index.projectionOrderPred[toTx.Sequence] == nil {
		index.projectionOrderPred[toTx.Sequence] = map[uint64]bool{}
	}
	if index.projectionOrderPred[toTx.Sequence][fromTx.Sequence] {
		return
	}
	index.projectionOrderPred[toTx.Sequence][fromTx.Sequence] = true
	if index.projectionOrderChildren[fromTx.Sequence] == nil {
		index.projectionOrderChildren[fromTx.Sequence] = map[uint64]bool{}
	}
	index.projectionOrderChildren[fromTx.Sequence][toTx.Sequence] = true
}

func (index *metaTrackDependencyIndexV653) addItemLocked(item tx.SignedTransaction) error {
	if item.ExecutionRouting == nil {
		return fmt.Errorf("metatrack v6.5.3 dependency index missing execution routing: tx=%s", txIdentifier(item))
	}
	routing := item.ExecutionRouting
	txID := txIdentifier(item)
	if txID == "" {
		return fmt.Errorf("metatrack v6.5.3 dependency index contains empty transaction identity")
	}
	if routing.RoutingOrdinal == 0 || routing.RouteBatchSequence == 0 || strings.TrimSpace(routing.ExecutionShard) == "" {
		return fmt.Errorf("metatrack v6.5.3 dependency index incomplete signed routing metadata: tx=%s", txID)
	}
	if owner := index.ordinalOwner[routing.RoutingOrdinal]; owner != "" && owner != txID {
		return fmt.Errorf("metatrack v6.5.3 duplicate routing ordinal %d: %s/%s", routing.RoutingOrdinal, owner, txID)
	}
	index.ordinalOwner[routing.RoutingOrdinal] = txID
	indexed := metaTrackIndexedTxV653{
		Item: item, TxID: txID, Ordinal: routing.RoutingOrdinal,
		Sequence: routing.RouteBatchSequence, ExecutionShard: routing.ExecutionShard,
		AccessByKey: metaTrackIndexAccessByKeyV653(item), DepthL: 1,
	}
	index.txByID[txID] = indexed
	if index.predecessors[txID] == nil {
		index.predecessors[txID] = map[string]bool{}
	}
	if index.children[txID] == nil {
		index.children[txID] = map[string]bool{}
	}
	if index.projectionPred[routing.RouteBatchSequence] == nil {
		index.projectionPred[routing.RouteBatchSequence] = map[uint64]bool{}
	}
	if index.projectionChildren[routing.RouteBatchSequence] == nil {
		index.projectionChildren[routing.RouteBatchSequence] = map[uint64]bool{}
	}
	if index.projectionOrderPred[routing.RouteBatchSequence] == nil {
		index.projectionOrderPred[routing.RouteBatchSequence] = map[uint64]bool{}
	}
	if index.projectionOrderChildren[routing.RouteBatchSequence] == nil {
		index.projectionOrderChildren[routing.RouteBatchSequence] = map[uint64]bool{}
	}

	// Same-sender predecessor is deterministic signed-order evidence already used
	// by the scheduler. Because the index is fed in RoutingOrdinal order, this is
	// the same previous-sender transaction the execution DAG sees.
	if sender := strings.TrimSpace(item.Sender); sender != "" {
		if previous := index.lastSenderTx[sender]; previous != "" {
			index.addExecutionEdgeLocked(previous, txID)
		}
		index.lastSenderTx[sender] = txID
	}

	// Reuse the scheduler's actual RAW semantics. Blind writes and WAW/WAR are
	// deliberately not added as execution barriers.
	for _, access := range classificationAccessItems(item) {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			continue
		}
		switch access.Mode {
		case tx.AccessCommutativeDelta:
			if previous := index.lastWriter[key]; previous != "" {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			for _, previous := range index.readers[key] {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			index.commutativeWriters[key] = append(index.commutativeWriters[key], txID)
		case tx.AccessRead:
			if previous := index.lastWriter[key]; previous != "" {
				index.addExecutionEdgeLocked(previous, txID)
			}
			for _, previous := range index.commutativeWriters[key] {
				index.addExecutionEdgeLocked(previous, txID)
			}
			index.readers[key] = append(index.readers[key], txID)
		case tx.AccessWrite:
			if previous := index.lastWriter[key]; previous != "" {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			for _, previous := range index.commutativeWriters[key] {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			for _, previous := range index.readers[key] {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			index.readers[key] = nil
			index.commutativeWriters[key] = nil
			index.lastWriter[key] = txID
		case tx.AccessReadWrite:
			if previous := index.lastWriter[key]; previous != "" {
				index.addExecutionEdgeLocked(previous, txID)
			}
			for _, previous := range index.commutativeWriters[key] {
				index.addExecutionEdgeLocked(previous, txID)
			}
			for _, previous := range index.readers[key] {
				index.addOrderingOnlyProjectionEdgeLocked(previous, txID)
			}
			index.readers[key] = nil
			index.commutativeWriters[key] = nil
			index.lastWriter[key] = txID
		}
	}

	// Exact-value producer identity is already signed into StateVersions. Reuse it
	// instead of rebuilding a separate V11/V612 owner map for every proposal.
	for _, dependency := range routing.StateVersions {
		if strings.TrimSpace(dependency.Key) == "" {
			return fmt.Errorf("metatrack v6.5.3 dependency index empty state-version key: tx=%s", txID)
		}
		if dependency.RequiredVersion > 0 {
			if dependency.RequiredVersion >= routing.RoutingOrdinal {
				return fmt.Errorf("metatrack v6.5.3 future/non-monotonic exact requirement: tx=%s key=%s required=%d ordinal=%d", txID, dependency.Key, dependency.RequiredVersion, routing.RoutingOrdinal)
			}
			if transactionRequiresExactStateValue(item, dependency.Key) {
				identity := metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}
				if producerTxID := index.versionProducer[identity]; producerTxID != "" {
					producer := index.txByID[producerTxID]
					if producer.Ordinal >= routing.RoutingOrdinal || producer.Sequence > routing.RouteBatchSequence {
						return fmt.Errorf("metatrack v6.5.3 exact predecessor order mismatch: tx=%s key=%s required=%d", txID, dependency.Key, dependency.RequiredVersion)
					}
					index.addExecutionEdgeLocked(producerTxID, txID)
				}
			}
		}
	}
	for _, dependency := range routing.StateVersions {
		if dependency.ProducedVersion == 0 {
			continue
		}
		if dependency.ProducedVersion != routing.RoutingOrdinal {
			return fmt.Errorf("metatrack v6.5.3 produced version mismatch: tx=%s key=%s produced=%d ordinal=%d", txID, dependency.Key, dependency.ProducedVersion, routing.RoutingOrdinal)
		}
		identity := metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}
		if previous := index.versionProducer[identity]; previous != "" && previous != txID {
			return fmt.Errorf("metatrack v6.5.3 duplicate exact producer: key=%s version=%d first=%s second=%s", dependency.Key, dependency.ProducedVersion, previous, txID)
		}
		index.versionProducer[identity] = txID
		index.versionInfo[identity] = metaTrackIndexedVersionV653{
			ProducerTxID: txID, Sequence: routing.RouteBatchSequence, Ordinal: routing.RoutingOrdinal,
			ExecutionShard: routing.ExecutionShard, LivenessClass: dependency.LivenessClass,
			BatchFinal:                   dependency.BatchFinal,
			LocalValueSuccessorCount:     dependency.LocalValueSuccessorCount,
			RemoteValueSuccessorCount:    dependency.RemoteValueSuccessorCount,
			LocalOrderingSuccessorCount:  dependency.LocalOrderingSuccessorCount,
			RemoteOrderingSuccessorCount: dependency.RemoteOrderingSuccessorCount,
		}
	}

	depth := 1
	for predecessor := range index.predecessors[txID] {
		if candidate := index.txByID[predecessor].DepthL + 1; candidate > depth {
			depth = candidate
		}
	}
	indexed = index.txByID[txID]
	indexed.DepthL = depth
	index.txByID[txID] = indexed
	if routing.RoutingOrdinal > index.maxOrdinal {
		index.maxOrdinal = routing.RoutingOrdinal
	}
	return nil
}

func (index *metaTrackDependencyIndexV653) rebuildLocked() error {
	items := make([]tx.SignedTransaction, 0, len(index.itemsByOrdinal))
	ordinals := make([]uint64, 0, len(index.itemsByOrdinal))
	for ordinal := range index.itemsByOrdinal {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	for _, ordinal := range ordinals {
		items = append(items, index.itemsByOrdinal[ordinal])
	}
	index.resetDerivedLocked()
	for _, item := range items {
		if err := index.addItemLocked(item); err != nil {
			return err
		}
	}
	index.rebuildCount++
	return nil
}

func (index *metaTrackDependencyIndexV653) ensure(items []tx.SignedTransaction) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	newItems := make([]tx.SignedTransaction, 0)
	newOrdinals := make([]uint64, 0)
	needsRebuild := false
	rollbackNew := func() {
		for _, ordinal := range newOrdinals {
			delete(index.itemsByOrdinal, ordinal)
		}
		index.newTxCount -= int64(len(newOrdinals))
		_ = index.rebuildLocked()
		// A rollback rebuild restores prior valid state and is not an observed
		// out-of-order workload rebuild.
		if index.rebuildCount > 0 {
			index.rebuildCount--
		}
	}
	for _, item := range items {
		if item.ExecutionRouting == nil || item.ExecutionRouting.RoutingOrdinal == 0 {
			return fmt.Errorf("metatrack v6.5.3 dependency index cannot ingest unsigned routing metadata")
		}
		txID := txIdentifier(item)
		ordinal := item.ExecutionRouting.RoutingOrdinal
		if existing, ok := index.itemsByOrdinal[ordinal]; ok {
			if txIdentifier(existing) != txID {
				return fmt.Errorf("metatrack v6.5.3 routing ordinal collision %d", ordinal)
			}
			index.reusedTxCount++
			continue
		}
		if _, exists := index.txByID[txID]; exists {
			return fmt.Errorf("metatrack v6.5.3 transaction identity reused at a different ordinal: %s", txID)
		}
		index.itemsByOrdinal[ordinal] = item
		newItems = append(newItems, item)
		newOrdinals = append(newOrdinals, ordinal)
		index.newTxCount++
		if index.maxOrdinal != 0 && ordinal <= index.maxOrdinal {
			needsRebuild = true
		}
	}
	if len(newItems) == 0 {
		return nil
	}
	if needsRebuild {
		if err := index.rebuildLocked(); err != nil {
			rollbackNew()
			return err
		}
		return nil
	}
	sort.Slice(newItems, func(i, j int) bool {
		return newItems[i].ExecutionRouting.RoutingOrdinal < newItems[j].ExecutionRouting.RoutingOrdinal
	})
	for _, item := range newItems {
		if err := index.addItemLocked(item); err != nil {
			rollbackNew()
			return err
		}
	}
	return nil
}

func (index *metaTrackDependencyIndexV653) activeProjectionPredecessors(items []tx.SignedTransaction) (map[uint64][]uint64, error) {
	index.mu.Lock()
	defer index.mu.Unlock()
	activeSequences := map[uint64]bool{}
	for _, item := range items {
		if item.ExecutionRouting == nil {
			return nil, fmt.Errorf("metatrack v6.5.3 active projection missing routing metadata")
		}
		activeSequences[item.ExecutionRouting.RouteBatchSequence] = true
	}
	out := map[uint64][]uint64{}
	for sequence := range activeSequences {
		preds := make([]uint64, 0)
		for predecessor := range index.projectionPred[sequence] {
			if activeSequences[predecessor] {
				preds = append(preds, predecessor)
			}
		}
		sort.Slice(preds, func(i, j int) bool { return preds[i] < preds[j] })
		out[sequence] = preds
	}
	return out, nil
}

func (index *metaTrackDependencyIndexV653) stats() metaTrackDependencyIndexStatsV653 {
	index.mu.Lock()
	defer index.mu.Unlock()
	edges := 0
	for _, preds := range index.predecessors {
		edges += len(preds)
	}
	projectionEdges := 0
	for _, preds := range index.projectionPred {
		projectionEdges += len(preds)
	}
	projectionOrderingEdges := 0
	for _, preds := range index.projectionOrderPred {
		projectionOrderingEdges += len(preds)
	}
	return metaTrackDependencyIndexStatsV653{
		IndexedTransactionCount: len(index.txByID), ExecutionEdgeCount: edges,
		ProjectionEdgeCount: projectionEdges, ProjectionOrderingEdgeCount: projectionOrderingEdges, NewTransactionCount: index.newTxCount,
		ReusedTransactionCount: index.reusedTxCount, RebuildCount: index.rebuildCount,
	}
}

// selectMetaTrackDependencyClosedPBFTProjectionsIndexedV653 is the hot-path
// projection selector. It groups complete signed projections once, updates the
// per-mempool dependency index only for transactions not seen before, and then
// checks the cached projection predecessor sets for the currently active
// candidate. The legacy v6 validator still verifies the final selected block
// exactly once before proposal.
func selectMetaTrackDependencyClosedPBFTProjectionsIndexedV653(items []tx.SignedTransaction, limit int, shardID string, pool *mempool.Mempool) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	candidate, _, projections, err := selectMetaTrackAggregatedBatchProjections(items, limit, shardID)
	if err != nil {
		return nil, nil, nil, err
	}
	index := metaTrackDependencyIndexForPoolV653(pool)
	if err := index.ensure(candidate); err != nil {
		return nil, nil, nil, err
	}
	projectionPredecessors, err := index.activeProjectionPredecessors(candidate)
	if err != nil {
		return nil, nil, nil, err
	}

	// Preserve signed RouteBatch order: admit the longest contiguous prefix from
	// the oldest complete projection while its cached active predecessor count is
	// zero. Later independent projections never jump over a blocked earlier one.
	readySequences := map[uint64]bool{}
	for _, projection := range projections {
		if len(projectionPredecessors[projection.Sequence]) != 0 {
			break
		}
		readySequences[projection.Sequence] = true
	}
	if len(readySequences) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.3 dependency-ready projection prefix is empty")
	}

	selected := make([]tx.SignedTransaction, 0, len(candidate))
	selectedOrdinals := map[uint64]bool{}
	for _, item := range candidate {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.3 projection missing execution routing: tx=%s", txIdentifier(item))
		}
		if !readySequences[item.ExecutionRouting.RouteBatchSequence] {
			continue
		}
		selected = append(selected, item)
		selectedOrdinals[item.ExecutionRouting.RoutingOrdinal] = true
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.3 dependency-ready selector chose no transactions")
	}

	selectedProjections := make([]metaTrackBatchProjectionIdentity, 0, len(readySequences))
	for _, projection := range projections {
		if readySequences[projection.Sequence] {
			selectedProjections = append(selectedProjections, projection)
		}
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if item.ExecutionRouting != nil && selectedOrdinals[item.ExecutionRouting.RoutingOrdinal] {
			continue
		}
		deferred = append(deferred, item)
	}

	if _, err := validateMetaTrackDependencyClosedProjectionBlock(realblock.Block{ShardID: shardID, TxList: selected}, true); err != nil {
		return nil, nil, nil, err
	}
	return selected, deferred, selectedProjections, nil
}
