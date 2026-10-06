package v5

import (
	"fmt"
	"sort"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	statepkg "metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

type porygonPaperTxStatus string

const (
	porygonPaperTxOrdered       porygonPaperTxStatus = "ordered"
	porygonPaperTxITxExecuted   porygonPaperTxStatus = "itx_executed"
	porygonPaperTxCTxPreexec    porygonPaperTxStatus = "ctx_preexecuted"
	porygonPaperTxUpdatePending porygonPaperTxStatus = "update_pending"
	porygonPaperTxUpdateApplied porygonPaperTxStatus = "update_applied"
	porygonPaperTxCommitted     porygonPaperTxStatus = "committed"
	porygonPaperTxAbandoned     porygonPaperTxStatus = "abandoned"
	porygonPaperTxRolledBack    porygonPaperTxStatus = "rolled_back"
)

type PorygonTxRoundLifecycle struct {
	TxID                  string               `json:"tx_id"`
	OriginHeight          uint64               `json:"origin_height"`
	CrossShard            bool                 `json:"cross_shard"`
	Accesses              []tx.AccessItem      `json:"accesses"`
	LockedKeys            []string             `json:"locked_keys"`
	InvolvedShards        []int                `json:"involved_shards"`
	Status                porygonPaperTxStatus `json:"status"`
	WitnessRound          uint64               `json:"witness_round"`
	OrderingRound         uint64               `json:"ordering_round"`
	PreExecutionRound     uint64               `json:"pre_execution_round,omitempty"`
	UpdateProposalRound   uint64               `json:"update_proposal_round,omitempty"`
	UpdateProposalHeight  uint64               `json:"update_proposal_height,omitempty"`
	UpdateExecutionRound  uint64               `json:"update_execution_round,omitempty"`
	UpdateExecutionHeight uint64               `json:"update_execution_height,omitempty"`
	CommitRound           uint64               `json:"commit_round,omitempty"`
	CommitProposalHeight  uint64               `json:"commit_proposal_height,omitempty"`
	UpdateDigest          string               `json:"update_digest,omitempty"`
}

type porygonPaperRuntimeState struct {
	mu                      sync.Mutex
	baselineHeight          uint64
	txs                     map[string]*PorygonTxRoundLifecycle
	proposalUpdates         map[uint64][]PorygonProposalUpdate
	certifiedSnapshots      map[uint64]map[string]string
	certifiedRoots          map[uint64]string
	certifiedPartitionRoots map[uint64]map[string]string
	latestCertifiedHeight   uint64
}

var porygonPaperRuntimeStates sync.Map // map[*NodeRuntime]*porygonPaperRuntimeState

func (r *NodeRuntime) porygonPaperRuntimeState() *porygonPaperRuntimeState {
	if value, ok := porygonPaperRuntimeStates.Load(r); ok {
		return value.(*porygonPaperRuntimeState)
	}
	r.mu.Lock()
	committedHeight := r.committedHeight
	r.mu.Unlock()
	snapshot := r.plugins.StateStorage.Snapshot(r.db)
	localRoot := statepkg.RootOfSnapshot(snapshot)
	partitionRoots := map[string]string{}
	shardCount := r.porygonExecutionShardCount()
	if len(snapshot) == 0 {
		// The formal Porygon runs start from the same empty business state on every
		// storage partition.  Only in that provable case may one local root seed all
		// partition roots before the first ESC certificate exists.
		for shard := 0; shard < shardCount; shard++ {
			partitionRoots[porygonExecutionShardID(shard)] = localRoot
		}
	} else {
		partitionRoots[r.stateAccessPartitionID()] = localRoot
	}
	globalRoot := stableJSONDigest(partitionRoots)
	created := &porygonPaperRuntimeState{
		baselineHeight: committedHeight,
		txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
		certifiedSnapshots:      map[uint64]map[string]string{committedHeight: copyRegistryStringMap(snapshot)},
		certifiedRoots:          map[uint64]string{committedHeight: globalRoot},
		certifiedPartitionRoots: map[uint64]map[string]string{committedHeight: copyRegistryStringMap(partitionRoots)},
		latestCertifiedHeight:   committedHeight,
	}
	actual, _ := porygonPaperRuntimeStates.LoadOrStore(r, created)
	return actual.(*porygonPaperRuntimeState)
}

func porygonPaperPruneCertifiedLocked(state *porygonPaperRuntimeState) {
	for h := range state.certifiedSnapshots {
		if h+8 < state.latestCertifiedHeight {
			delete(state.certifiedSnapshots, h)
			delete(state.certifiedRoots, h)
			delete(state.certifiedPartitionRoots, h)
		}
	}
}

// Durable storage is a materialization witness for an already certified
// Porygon partition root.  It must never create or rewrite consensus truth:
// pipeline execution/Storage-Role certificates own certifiedPartitionRoots and
// certifiedRoots, while the durable DB only proves that this replica materialized
// the root it already accepted.
func (r *NodeRuntime) porygonPaperValidateDurableSnapshot(height uint64, snapshot map[string]string) error {
	state := r.porygonPaperRuntimeState()
	root := statepkg.RootOfSnapshot(snapshot)
	partitionID := r.stateAccessPartitionID()
	state.mu.Lock()
	defer state.mu.Unlock()
	partitionRoots := state.certifiedPartitionRoots[height]
	if partitionRoots == nil {
		return fmt.Errorf("Porygon durable snapshot has no certified partition-root set at height %d", height)
	}
	expected := partitionRoots[partitionID]
	if expected == "" {
		return fmt.Errorf("Porygon durable snapshot has no certified root at height %d partition %s", height, partitionID)
	}
	if root != expected {
		return fmt.Errorf("Porygon durable snapshot/certified partition root mismatch at height %d partition %s", height, partitionID)
	}
	if existing := state.certifiedSnapshots[height]; existing != nil {
		if existingRoot := statepkg.RootOfSnapshot(existing); existingRoot != root {
			return fmt.Errorf("Porygon durable snapshot conflicts with certified snapshot at height %d partition %s", height, partitionID)
		}
	}
	state.certifiedSnapshots[height] = copyRegistryStringMap(snapshot)
	return nil
}

// Concurrent proposal execution may start from an older agreed T root.  Never
// publish that whole historical snapshot as the new canonical T state: doing so
// would erase disjoint writes already certified by another pipeline stage.
// Merge only this execution's deterministic materialized delta onto the latest
// canonical certified snapshot, then certify the merged root.
func (r *NodeRuntime) porygonPaperRecordCertifiedDelta(height uint64, delta []statepkg.StateKV, observedPartitionRoots map[string]string) (map[string]string, string, error) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	baseHeight := state.baselineHeight
	if height > state.baselineHeight {
		baseHeight = height - 1
	}
	base := copyRegistryStringMap(state.certifiedSnapshots[baseHeight])
	if base == nil {
		return nil, "", fmt.Errorf("Porygon canonical base snapshot unavailable at height %d for certification of height %d", baseHeight, height)
	}
	for _, item := range delta {
		if item.Key == "" {
			continue
		}
		base[item.Key] = item.Value
	}
	partitionRoots := copyRegistryStringMap(state.certifiedPartitionRoots[baseHeight])
	if partitionRoots == nil {
		return nil, "", fmt.Errorf("Porygon canonical partition-root base unavailable at height %d", baseHeight)
	}
	for shard, root := range observedPartitionRoots {
		if shard != "" && root != "" {
			partitionRoots[shard] = root
		}
	}
	if len(partitionRoots) < r.porygonExecutionShardCount() {
		return nil, "", fmt.Errorf("Porygon certified partition-root coverage incomplete at height %d", height)
	}
	localPartition := r.stateAccessPartitionID()
	localRoot := statepkg.RootOfSnapshot(base)
	expectedLocalRoot := partitionRoots[localPartition]
	if expectedLocalRoot == "" {
		return nil, "", fmt.Errorf("Porygon certified local partition root missing at height %d partition %s", height, localPartition)
	}
	if localRoot != expectedLocalRoot {
		return nil, "", fmt.Errorf("Porygon local snapshot/certified partition root mismatch at height %d partition %s", height, localPartition)
	}
	globalRoot := stableJSONDigest(partitionRoots)
	if existing := state.certifiedPartitionRoots[height]; existing != nil && stableJSONDigest(existing) != globalRoot {
		return nil, "", fmt.Errorf("Porygon certified state equivocation at height %d", height)
	}
	if existingRoot := state.certifiedRoots[height]; existingRoot != "" && existingRoot != globalRoot {
		return nil, "", fmt.Errorf("Porygon certified global root equivocation at height %d", height)
	}
	state.certifiedSnapshots[height] = copyRegistryStringMap(base)
	state.certifiedPartitionRoots[height] = copyRegistryStringMap(partitionRoots)
	state.certifiedRoots[height] = globalRoot
	if height > state.latestCertifiedHeight {
		state.latestCertifiedHeight = height
	}
	porygonPaperPruneCertifiedLocked(state)
	return copyRegistryStringMap(base), globalRoot, nil
}

// porygonPaperCanonicalPartitionBase separates the execution read anchor T(h-2)
// from canonical materialization.  State certified for height h is folded onto
// the already certified state at h-1, never onto whichever height happened to
// finish most recently on this replica.
func (r *NodeRuntime) porygonPaperCanonicalPartitionBase(height uint64, partitionID string) (uint64, string, bool) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	baseHeight := state.baselineHeight
	if height > state.baselineHeight {
		baseHeight = height - 1
	}
	root := state.certifiedPartitionRoots[baseHeight][partitionID]
	return baseHeight, root, root != ""
}

// Proposal semantic verification is meaningful only after this replica has the
// local execution result that the paper says B_h depends on.  A slow backup is
// not a Byzantine mismatch: it waits for the same PRE-PREPARE retransmission.
func (r *NodeRuntime) porygonPaperProposalValidationReady(proposalHeight uint64) (bool, string) {
	if r == nil || proposalHeight == 0 {
		return false, "invalid proposal height"
	}
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	baseline := state.baselineHeight
	desired := baseline
	if proposalHeight > baseline+2 {
		desired = proposalHeight - 2
	}
	root := state.certifiedRoots[desired]
	coverage := len(state.certifiedPartitionRoots[desired])
	state.mu.Unlock()
	if root == "" || coverage < r.porygonExecutionShardCount() {
		return false, fmt.Sprintf("certified T state not ready at height %d", desired)
	}
	if desired <= baseline {
		return true, ""
	}
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	executedHeight := pipeline.executedHeight
	pipeline.mu.Unlock()
	if executedHeight < desired {
		return false, fmt.Sprintf("execution height %d below required T height %d", executedHeight, desired)
	}
	if _, err := r.porygonPaperCertifiedUpdatesForProposal(proposalHeight); err != nil {
		return false, err.Error()
	}
	return true, ""
}

func (r *NodeRuntime) porygonPaperStateAnchor() (uint64, string, map[string]string) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	h := state.latestCertifiedHeight
	root := state.certifiedRoots[h]
	partitions := copyRegistryStringMap(state.certifiedPartitionRoots[h])
	if len(partitions) < r.porygonExecutionShardCount() {
		return h, "", partitions
	}
	return h, root, partitions
}

// Proposal B_i is built from the T result returned in the previous protocol
// round, which corresponds to execution of B_(i-2).  Never use a newer
// execution result merely because it happened to finish early: that would
// collapse the paper's round structure and change the conflict/commit timing.
func (r *NodeRuntime) porygonPaperStateAnchorForProposal(proposalHeight uint64) (uint64, string, map[string]string) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	desired := state.baselineHeight
	if proposalHeight > state.baselineHeight+2 {
		desired = proposalHeight - 2
	}
	root := state.certifiedRoots[desired]
	partitions := copyRegistryStringMap(state.certifiedPartitionRoots[desired])
	if root == "" || len(partitions) < r.porygonExecutionShardCount() {
		return desired, "", partitions
	}
	return desired, root, partitions
}

func (r *NodeRuntime) porygonPaperPartitionSnapshot(height uint64, partitionID, partitionRoot string) (map[string]string, error) {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	snapshot := copyRegistryStringMap(state.certifiedSnapshots[height])
	expected := state.certifiedPartitionRoots[height][partitionID]
	state.mu.Unlock()
	if snapshot == nil || expected == "" {
		return nil, fmt.Errorf("Porygon agreed T partition snapshot unavailable at height %d partition %s", height, partitionID)
	}
	if expected != partitionRoot || statepkg.RootOfSnapshot(snapshot) != partitionRoot {
		return nil, fmt.Errorf("Porygon agreed T partition root mismatch at height %d partition %s", height, partitionID)
	}
	return snapshot, nil
}

func (r *NodeRuntime) porygonPaperUpdatesForProposal(height uint64) []PorygonProposalUpdate {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	rows := append([]PorygonProposalUpdate(nil), state.proposalUpdates[height]...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].OriginHeight != rows[j].OriginHeight {
			return rows[i].OriginHeight < rows[j].OriginHeight
		}
		left := porygonPaperProposalUpdateOrderIndex(rows[i])
		right := porygonPaperProposalUpdateOrderIndex(rows[j])
		if left != right {
			return left < right
		}
		return rows[i].TxID < rows[j].TxID
	})
	return rows
}

func porygonPaperStateUpdates(delta execution.TxDelta, shardCount int) []PorygonStateUpdate {
	if !delta.Success || len(delta.WriteSet) == 0 {
		return nil
	}
	keys := make([]string, 0, len(delta.WriteSet))
	for key := range delta.WriteSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]PorygonStateUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, PorygonStateUpdate{TxID: delta.TxID, OriginalIndex: delta.OriginalIndex, Key: key, Value: delta.WriteSet[key]})
	}
	return porygonCanonicalStateUpdates(out)
}

func (r *NodeRuntime) porygonPaperRegisterOrdered(block realblock.Block) error {
	plan, err := porygonPipelinePlan(block)
	if err != nil {
		return err
	}
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	orderedHeight := pipeline.orderedHeight
	pipeline.mu.Unlock()
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, assignment := range plan.Assignments {
		if assignment.Abandoned {
			continue
		}
		current := state.txs[assignment.TxID]
		if current != nil {
			continue
		}
		accesses := append([]tx.AccessItem(nil), assignment.Accesses...)
		state.txs[assignment.TxID] = &PorygonTxRoundLifecycle{
			TxID: assignment.TxID, OriginHeight: block.Height, CrossShard: assignment.CrossShard, Accesses: accesses,
			LockedKeys: append([]string(nil), assignment.LockedKeys...), InvolvedShards: append([]int(nil), assignment.InvolvedShards...),
			Status: porygonPaperTxOrdered, WitnessRound: block.Height, OrderingRound: block.Height + 1,
		}
	}
	r.porygonPaperCommitReadyLocked(state, orderedHeight)
	return nil
}

func (r *NodeRuntime) porygonPaperRegisterExecution(block realblock.Block, executed BlockExecutionResult) error {
	plan, err := porygonPipelinePlan(block)
	if err != nil {
		return err
	}
	proposal, err := porygonProposalFromBlock(block)
	if err != nil {
		return err
	}
	deltaByID := map[string]execution.TxDelta{}
	for _, d := range executed.ExecutionResult.TxDeltas {
		deltaByID[d.TxID] = d
	}
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()

	// U_i is applied by this EC before L_i.  Its transaction remains pending
	// until a later PBFT proposal commits the resulting T root.
	for _, update := range proposal.U {
		lifecycle := state.txs[update.TxID]
		if lifecycle == nil {
			continue
		}
		if lifecycle.UpdateProposalHeight == 0 || block.Height != lifecycle.UpdateProposalHeight {
			return fmt.Errorf("Porygon CTx U applied at wrong proposal height for %s: got=%d want=%d", lifecycle.TxID, block.Height, lifecycle.UpdateProposalHeight)
		}
		lifecycle.Status = porygonPaperTxUpdateApplied
		lifecycle.UpdateExecutionRound = lifecycle.WitnessRound + 4
		lifecycle.UpdateExecutionHeight = block.Height
		// Figure 6: a CTx ordered in B_h is pre-executed next round, carried
		// as U in B_(h+2), applied by that proposal's EC, and is not committed
		// until the resulting T root is recorded in B_(h+4).
		lifecycle.CommitProposalHeight = lifecycle.OriginHeight + 4
		lifecycle.CommitRound = lifecycle.WitnessRound + 5
	}

	for _, assignment := range plan.Assignments {
		lifecycle := state.txs[assignment.TxID]
		if lifecycle == nil {
			continue
		}
		if assignment.Abandoned {
			lifecycle.Status = porygonPaperTxAbandoned
			continue
		}
		delta := deltaByID[assignment.TxID]
		if !delta.Success {
			lifecycle.Status = porygonPaperTxAbandoned
			continue
		}
		lifecycle.PreExecutionRound = lifecycle.WitnessRound + 2
		if !assignment.CrossShard {
			lifecycle.Status = porygonPaperTxITxExecuted
			// Figure 6: the subtree returned by execution of B_h is aggregated
			// by the OC in the next protocol round and recorded in B_(h+2).
			lifecycle.CommitProposalHeight = lifecycle.OriginHeight + 2
			lifecycle.CommitRound = lifecycle.WitnessRound + 3
			continue
		}
		updates := porygonPaperStateUpdates(delta, plan.ExecutionShardCount)
		update := PorygonProposalUpdate{TxID: assignment.TxID, OriginHeight: block.Height, Kind: "commit", InvolvedShards: append([]int(nil), assignment.InvolvedShards...), Updates: updates}
		update.UpdateDigest = porygonProposalUpdateDigest(update)
		// Figure 6: S from execution of B_h is verified by the OC in the next
		// round and becomes U in B_(h+2), not in the immediately following
		// proposal.
		targetHeight := block.Height + 2
		state.proposalUpdates[targetHeight] = append(state.proposalUpdates[targetHeight], update)
		lifecycle.Status = porygonPaperTxUpdatePending
		lifecycle.UpdateProposalRound = lifecycle.WitnessRound + 3
		lifecycle.UpdateProposalHeight = targetHeight
		lifecycle.UpdateDigest = update.UpdateDigest
	}
	r.porygonPaperCommitReadyLocked(state, block.Height)
	return nil
}

func (r *NodeRuntime) porygonPaperCommitReadyLocked(state *porygonPaperRuntimeState, orderedHeight uint64) {
	for _, lifecycle := range state.txs {
		if lifecycle.CommitProposalHeight == 0 || lifecycle.CommitProposalHeight > orderedHeight {
			continue
		}
		switch lifecycle.Status {
		case porygonPaperTxITxExecuted, porygonPaperTxUpdateApplied:
			if err := r.porygonPaperRoundInvariant(*lifecycle); err != nil {
				r.addPorygonRuntimeMetric("porygon_round_invariant_failure_count", 1)
				continue
			}
			lifecycle.Status = porygonPaperTxCommitted
			r.addPorygonRuntimeMetric("porygon_transaction_level_commit_count", 1)
		}
	}
}

func (r *NodeRuntime) porygonPaperPendingEvidence() []porygonPendingTransactionEvidence {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	out := []porygonPendingTransactionEvidence{}
	for _, lifecycle := range state.txs {
		switch lifecycle.Status {
		case porygonPaperTxCommitted, porygonPaperTxAbandoned, porygonPaperTxRolledBack:
			continue
		}
		out = append(out, porygonPendingTransactionEvidence{TxID: lifecycle.TxID, Height: lifecycle.OriginHeight, Accesses: append([]tx.AccessItem(nil), lifecycle.Accesses...), LockedKeys: append([]string(nil), lifecycle.LockedKeys...)})
	}
	return porygonCanonicalPendingEvidence(out)
}

// porygonPaperPendingEvidenceForProposal is consensus-time truth.  It must not
// depend on whether one replica happened to finish a background Execution/Commit
// worker earlier than another.  Figure 6 keeps a CTx ordered in B_h protected
// while B_(h+1), B_(h+2) and B_(h+3) are ordered.  B_(h+4) may be proposed only
// after E(B_(h+2)) has completed (porygonPaperProposalRoundBlocked), so the CTx
// has left the conflict-pending window at that deterministic proposal height.
// ITx never enters the cross-round CTx pending set.
func (r *NodeRuntime) porygonPaperPendingEvidenceForProposal(proposalHeight uint64) []porygonPendingTransactionEvidence {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	out := []porygonPendingTransactionEvidence{}
	for _, lifecycle := range state.txs {
		if lifecycle == nil || !lifecycle.CrossShard || lifecycle.OriginHeight == 0 || proposalHeight <= lifecycle.OriginHeight {
			continue
		}
		if proposalHeight >= lifecycle.OriginHeight+4 {
			continue
		}
		out = append(out, porygonPendingTransactionEvidence{
			TxID: lifecycle.TxID, Height: lifecycle.OriginHeight,
			Accesses: append([]tx.AccessItem(nil), lifecycle.Accesses...),
			LockedKeys: append([]string(nil), lifecycle.LockedKeys...),
		})
	}
	return porygonCanonicalPendingEvidence(out)
}

func (r *NodeRuntime) porygonPaperProposalRoundBlocked(nextHeight uint64) bool {
	if r == nil || nextHeight == 0 {
		return false
	}
	// Production calls this hook only for the Porygon block producer.  Unit and
	// recovery paths may install an explicit pipeline state before plugins are
	// wired, so honor that state instead of short-circuiting the Figure-6
	// dependency check.  Non-Porygon runtimes with no explicit Porygon state
	// remain inert.
	if _, explicit := porygonPipelineRuntimes.Load(r); !explicit && !r.porygonPipelineEnabledRuntime() {
		return false
	}
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	if nextHeight <= pipeline.baselineHeight+2 {
		return false
	}
	// Paper Figure 6: B_i carries T_i/U_i derived from the execution result
	// returned in round i-1, which is execution of B_(i-2). Therefore O(B_i)
	// may overlap E(B_(i-1)); only E(B_(i-2)) is a hard dependency.
	return pipeline.executedHeight < nextHeight-2
}

func (r *NodeRuntime) porygonPaperMaintenanceReady(nextHeight uint64) bool {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for height, updates := range state.proposalUpdates {
		if height >= nextHeight && len(updates) > 0 {
			return true
		}
	}
	for _, lifecycle := range state.txs {
		switch lifecycle.Status {
		case porygonPaperTxITxExecuted, porygonPaperTxUpdateApplied:
			if lifecycle.CommitProposalHeight >= nextHeight {
				return true
			}
		case porygonPaperTxUpdatePending:
			return true
		}
	}
	return false
}

func (r *NodeRuntime) porygonPaperRoundInvariant(lifecycle PorygonTxRoundLifecycle) error {
	if lifecycle.WitnessRound == 0 || lifecycle.OrderingRound != lifecycle.WitnessRound+1 {
		return fmt.Errorf("Porygon witness/order round invariant violated for %s", lifecycle.TxID)
	}
	if lifecycle.PreExecutionRound != 0 && lifecycle.PreExecutionRound != lifecycle.WitnessRound+2 {
		return fmt.Errorf("Porygon execution round invariant violated for %s", lifecycle.TxID)
	}
	if lifecycle.CrossShard {
		if lifecycle.UpdateProposalRound != 0 && lifecycle.UpdateProposalRound != lifecycle.WitnessRound+3 {
			return fmt.Errorf("Porygon CTx U proposal round invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.UpdateProposalHeight != 0 && lifecycle.UpdateProposalHeight != lifecycle.OriginHeight+2 {
			return fmt.Errorf("Porygon CTx U proposal-height invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.UpdateExecutionRound != 0 && lifecycle.UpdateExecutionRound != lifecycle.WitnessRound+4 {
			return fmt.Errorf("Porygon CTx U execution round invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.UpdateExecutionHeight != 0 && lifecycle.UpdateExecutionHeight != lifecycle.OriginHeight+2 {
			return fmt.Errorf("Porygon CTx U execution-height invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.CommitRound != 0 && lifecycle.CommitRound != lifecycle.WitnessRound+5 {
			return fmt.Errorf("Porygon CTx six-round commit invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.CommitProposalHeight != 0 && lifecycle.CommitProposalHeight != lifecycle.OriginHeight+4 {
			return fmt.Errorf("Porygon CTx commit-proposal invariant violated for %s", lifecycle.TxID)
		}
	} else {
		if lifecycle.CommitRound != 0 && lifecycle.CommitRound != lifecycle.WitnessRound+3 {
			return fmt.Errorf("Porygon ITx four-round commit invariant violated for %s", lifecycle.TxID)
		}
		if lifecycle.CommitProposalHeight != 0 && lifecycle.CommitProposalHeight != lifecycle.OriginHeight+2 {
			return fmt.Errorf("Porygon ITx commit-proposal invariant violated for %s", lifecycle.TxID)
		}
	}
	return nil
}

func (r *NodeRuntime) porygonPaperLifecycleMetrics() map[string]any {
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	pending, itxCommitted, ctxCommitted := 0, 0, 0
	for _, l := range state.txs {
		if l.Status != porygonPaperTxCommitted && l.Status != porygonPaperTxAbandoned && l.Status != porygonPaperTxRolledBack {
			pending++
		}
		if l.Status == porygonPaperTxCommitted {
			if l.CrossShard {
				ctxCommitted++
			} else {
				itxCommitted++
			}
		}
	}
	return map[string]any{"pending_transaction_count": pending, "itx_committed_count": itxCommitted, "ctx_committed_count": ctxCommitted}
}

func porygonProposalFromBlock(block realblock.Block) (PorygonProposalBody, error) {
	evidence, err := decodePorygonTransactionBlockEvidence(block)
	if err != nil {
		return PorygonProposalBody{}, err
	}
	if evidence.CompactProposal == nil {
		return PorygonProposalBody{}, fmt.Errorf("Porygon L/U/T proposal missing")
	}
	return *evidence.CompactProposal, nil
}
