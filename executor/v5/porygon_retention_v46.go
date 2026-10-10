package v5

import "metaverse-chainlab/executor/realism/tx"

const porygonProtocolRetentionHeights uint64 = 8

// porygonPruneProtocolCaches keeps the h+2/h+4 truth window required by the
// paper while bounding long-run in-memory evidence. It only prunes entries that
// are safely behind the durable frontier; consensus decisions for live heights
// are untouched.
func (r *NodeRuntime) porygonPruneProtocolCaches(durableHeight uint64) {
	if r == nil || durableHeight <= porygonProtocolRetentionHeights {
		return
	}
	cutoff := durableHeight - porygonProtocolRetentionHeights

	if value, ok := porygonPipelineRuntimes.Load(r); ok {
		state := value.(*porygonPipelineRuntime)
		state.mu.Lock()
		for height, item := range state.blocks {
			if height < cutoff && item != nil && item.Phase == porygonPipelineDurable {
				porygonRollbackMasks.Delete(item.Block.BlockHash)
				delete(state.blocks, height)
			}
		}
		for height := range state.rollbackDigests {
			if height < cutoff {
				delete(state.rollbackDigests, height)
			}
		}
		state.mu.Unlock()
	}

	if value, ok := porygonPaperRuntimeStates.Load(r); ok {
		state := value.(*porygonPaperRuntimeState)
		state.mu.Lock()
		for height := range state.proposalUpdates {
			if height < cutoff {
				delete(state.proposalUpdates, height)
			}
		}
		state.mu.Unlock()
		// Archive terminal evidence before dropping the bounded protocol cache.
		// Observer I/O failure preserves the original rows, never protocol truth.
		r.porygonV57ArchiveAndPruneTerminals(state, cutoff, durableHeight)
	}

	if value, ok := porygonBatchExchangeStates.Load(r); ok {
		state := value.(*porygonBatchExchangeState)
		state.mu.Lock()
		for blockHash, cert := range state.certs {
			if cert.Height < cutoff {
				delete(state.certs, blockHash)
				delete(state.results, blockHash)
				delete(state.senderDigest, blockHash)
			}
		}
		for blockHash, shards := range state.results {
			height := uint64(0)
			for _, buckets := range shards {
				for _, bucket := range buckets {
					if bucket != nil {
						height = bucket.result.Height
						break
					}
				}
				if height != 0 {
					break
				}
			}
			if height != 0 && height < cutoff {
				delete(state.results, blockHash)
				delete(state.senderDigest, blockHash)
			}
		}
		state.mu.Unlock()
	}

	if value, ok := porygonWitnessStates.Load(r); ok {
		state := value.(*porygonWitnessState)
		state.mu.Lock()
		for witnessID, votes := range state.votes {
			height := uint64(0)
			for _, vote := range votes {
				height = vote.Height
				break
			}
			if height != 0 && height < cutoff {
				delete(state.votes, witnessID)
				delete(state.signal, witnessID)
			}
		}
		state.mu.Unlock()
	}

	if value, ok := porygonMultiShardStates.Load(r); ok {
		state := value.(*porygonMultiShardState)
		state.mu.Lock()
		for blockHash, cert := range state.certs {
			if cert.Height < cutoff {
				delete(state.certs, blockHash)
				delete(state.acks, blockHash)
			}
		}
		for blockHash, partitions := range state.acks {
			height := uint64(0)
			for _, voters := range partitions {
				for _, ack := range voters {
					height = ack.Height
					break
				}
				if height != 0 {
					break
				}
			}
			if height != 0 && height < cutoff {
				delete(state.acks, blockHash)
			}
		}
		for height := range state.prepared {
			if height < cutoff {
				delete(state.prepared, height)
			}
		}
		state.mu.Unlock()
	}

	if value, ok := porygonUpdateHandoffStates.Load(r); ok {
		state := value.(*porygonUpdateHandoffState)
		state.mu.Lock()
		for requestDigest, votes := range state.votes {
			height := uint64(0)
			for _, vote := range votes {
				height = vote.Height
				break
			}
			if height != 0 && height < cutoff {
				delete(state.votes, requestDigest)
				delete(state.signal, requestDigest)
			}
		}
		state.mu.Unlock()
	}

	if value, ok := porygonRollbackStates.Load(r); ok {
		state := value.(*porygonRollbackState)
		state.mu.Lock()
		for blockHash, cert := range state.certs {
			if cert.Height < cutoff {
				delete(state.certs, blockHash)
				delete(state.acks, blockHash)
				delete(state.signals, blockHash)
			}
		}
		for blockHash, partitions := range state.acks {
			height := uint64(0)
			for _, voters := range partitions {
				for _, ack := range voters {
					height = ack.Height
					break
				}
				if height != 0 {
					break
				}
			}
			if height != 0 && height < cutoff {
				delete(state.acks, blockHash)
				delete(state.signals, blockHash)
			}
		}
		state.mu.Unlock()
	}

	// A proposal reservation surviving more than the protocol retention window
	// cannot belong to a live prepared/new-view proposal. Release it defensively.
	if value, ok := porygonProposalReservationStates.Load(r); ok && r.pool != nil {
		state := value.(*porygonProposalReservationState)
		stale := [][]tx.SignedTransaction{}
		state.mu.Lock()
		for hash, entry := range state.byHash {
			if entry.Height < cutoff {
				stale = append(stale, porygonCloneTransactions(entry.Items))
				delete(state.byHash, hash)
			}
		}
		state.mu.Unlock()
		for _, items := range stale {
			r.pool.ReleaseReserved(items)
		}
	}

	r.addPorygonRuntimeMetric("porygon_protocol_cache_prune_count", 1)
}
