package v5

import (
	"encoding/json"
	"fmt"

	realblock "metaverse-chainlab/executor/realism/block"
)

func attachProposalEvidence(block *realblock.Block, algorithmID string, payload any) error {
	if block == nil {
		return fmt.Errorf("proposal evidence requires block")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode proposal evidence: %w", err)
	}
	block.ProposalEvidence = &realblock.ProposalEvidenceEnvelope{
		AlgorithmID:   algorithmID,
		PayloadDigest: stableJSONDigest(payload),
		Payload:       encoded,
	}
	realblock.AssignHash(block)
	return nil
}
