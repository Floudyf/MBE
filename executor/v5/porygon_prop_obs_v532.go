package v5

import (
	"fmt"
	"strings"
)

const porygonProposalPhaseV532Prefix = "PORYGON_PROPOSAL_PHASE_"

// porygonProposalPhaseV532 is observability-only. It appends a compact phase
// marker to the existing consensus_message_log stream and never participates in
// proposal, PBFT, L/U/T, execution, or recovery decisions.
func (r *NodeRuntime) porygonProposalPhaseV532(phase string, height uint64, blockHash string, success bool, detail string) {
	if r == nil || r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return
	}
	phase = strings.TrimSpace(strings.ToUpper(phase))
	if phase == "" {
		return
	}
	poolLen := 0
	reserved := 0
	if r.pool != nil {
		poolLen = r.pool.Len()
		reserved = r.pool.ReservedCount()
	}
	if height == 0 {
		height = r.porygonConsensusNextHeight()
	}
	detail = strings.NewReplacer("\r", " ", "\n", " ", ",", ";").Replace(strings.TrimSpace(detail))
	from := fmt.Sprintf("view=%d;pool=%d;reserved=%d;next=%d;success=%t;detail=%s", r.currentPBFTView(), poolLen, reserved, r.porygonConsensusNextHeight(), success, detail)
	r.logConsensus(porygonProposalPhaseV532Prefix+phase, from, blockHash, height)
}

func porygonProposalPhaseErrV532(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
