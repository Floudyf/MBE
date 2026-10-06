package v5

// porygonPaperCertifiedPartitionRootsAtHeight returns the complete canonical
// partition-root view already committed for one Porygon height.  The map is a
// copy so callers cannot mutate consensus truth after certification.
func (r *NodeRuntime) porygonPaperCertifiedPartitionRootsAtHeight(height uint64) map[string]string {
	if r == nil {
		return nil
	}
	state := r.porygonPaperRuntimeState()
	state.mu.Lock()
	defer state.mu.Unlock()
	return copyRegistryStringMap(state.certifiedPartitionRoots[height])
}

// porygonPaperLocalCertifiedRootAtHeight resolves this validator's fixed
// Storage Role root from the complete height-h canonical root set.  A root is
// ready only when the full logical partition set is present; a per-round
// changed-root subset is never sufficient here.
func (r *NodeRuntime) porygonPaperLocalCertifiedRootAtHeight(height uint64) (string, bool) {
	roots := r.porygonPaperCertifiedPartitionRootsAtHeight(height)
	if len(roots) < r.porygonExecutionShardCount() {
		return "", false
	}
	root := roots[r.stateAccessPartitionID()]
	return root, root != ""
}
