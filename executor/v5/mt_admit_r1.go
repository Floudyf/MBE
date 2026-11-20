package v5

// MT_ADMIT_R1_V1 caches only the immutable signed dependency projection of
// a transaction across repeated block-candidate attempts. It does not cache
// readiness, candidate membership, producer positions, closure or PBFT results.

import (
	"sync"

	"metaverse-chainlab/executor/realism/tx"
)

const mtAdmissionR1Capacity = 4096

type mtAdmissionR1Requirement struct {
	token   string
	key     string
	version uint64
}

type mtAdmissionR1Metadata struct {
	produced []string
	required []mtAdmissionR1Requirement
}

type mtAdmissionR1Memo struct {
	mu      sync.Mutex
	entries map[string]mtAdmissionR1Metadata
	ring    []string
	next    int
}

// Signed transaction identities are immutable after verification at admission.
// Unsigned/manual test transactions never enter the cross-round cache.
func mtAdmissionR1Identity(item tx.SignedTransaction) string {
	if len(item.TxID) != 64 || item.Signature == "" || item.PublicKey == "" {
		return ""
	}
	return item.TxID + ":" + item.Signature
}

func mtAdmissionR1Derive(item tx.SignedTransaction) mtAdmissionR1Metadata {
	var metadata mtAdmissionR1Metadata
	if item.ExecutionRouting == nil {
		return metadata
	}
	for _, dep := range item.ExecutionRouting.StateVersions {
		if dep.Key == "" {
			continue
		}
		if dep.ProducedVersion != 0 {
			metadata.produced = append(metadata.produced, statelessVersionAdmissionToken(dep.Key, dep.ProducedVersion))
		}
		if dep.RequiredVersion != 0 && transactionRequiresExactStateValue(item, dep.Key) {
			metadata.required = append(metadata.required, mtAdmissionR1Requirement{
				token: statelessVersionAdmissionToken(dep.Key, dep.RequiredVersion),
				key:   dep.Key, version: dep.RequiredVersion,
			})
		}
	}
	return metadata
}

func (memo *mtAdmissionR1Memo) metadata(item tx.SignedTransaction) mtAdmissionR1Metadata {
	identity := mtAdmissionR1Identity(item)
	if identity == "" {
		return mtAdmissionR1Derive(item)
	}
	memo.mu.Lock()
	if record, ok := memo.entries[identity]; ok {
		memo.mu.Unlock()
		return record
	}
	memo.mu.Unlock()
	record := mtAdmissionR1Derive(item)
	memo.mu.Lock()
	defer memo.mu.Unlock()
	if existing, ok := memo.entries[identity]; ok {
		return existing
	}
	if memo.entries == nil {
		memo.entries = make(map[string]mtAdmissionR1Metadata, mtAdmissionR1Capacity)
	}
	if len(memo.ring) == mtAdmissionR1Capacity {
		delete(memo.entries, memo.ring[memo.next])
		memo.ring[memo.next] = identity
		memo.next = (memo.next + 1) % mtAdmissionR1Capacity
	} else {
		memo.ring = append(memo.ring, identity)
	}
	memo.entries[identity] = record
	return record
}

// Reuse applies to the repaired Stateless Hash + Block-STM baseline only.
// Other algorithms still derive metadata from their current candidate.
func (r *NodeRuntime) mtAdmissionR1Metadata(item tx.SignedTransaction) mtAdmissionR1Metadata {
	if r == nil || r.plugins.Routing == nil || r.plugins.BlockExecutor == nil ||
		r.plugins.Routing.ID() != "stateless_hash_routing" ||
		r.plugins.BlockExecutor.ID() != "block_stm_block_executor" {
		return mtAdmissionR1Derive(item)
	}
	return r.mtAdmissionR1.metadata(item)
}
