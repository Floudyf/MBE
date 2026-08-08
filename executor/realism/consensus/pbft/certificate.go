package pbft

import (
	"fmt"
	"sort"

	"metaverse-chainlab/executor/realism/block"
)

func (s *State) validatePreparedCertificateLocked(cert PreparedCertificate) error {
	if cert.BlockHash == "" || cert.Height == 0 || cert.Sequence == 0 {
		return fmt.Errorf("invalid_prepared_certificate_identity")
	}
	if len(cert.Prepares) < s.prepareQuorumLocked() {
		return fmt.Errorf("prepared_certificate_insufficient_votes")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Prepares {
		if !s.isValidatorLocked(vote.NodeID) {
			return fmt.Errorf("prepared_certificate_unknown_validator")
		}
		if seen[vote.NodeID] {
			return fmt.Errorf("prepared_certificate_duplicate_validator")
		}
		if vote.View != cert.View || vote.Sequence != cert.Sequence || vote.Height != cert.Height || vote.BlockHash != cert.BlockHash {
			return fmt.Errorf("prepared_certificate_vote_mismatch")
		}
		seen[vote.NodeID] = true
	}
	return nil
}

func (s *State) validateCommitCertificateLocked(cert CommitCertificate) error {
	if cert.BlockHash == "" || cert.Height == 0 || cert.Sequence == 0 || cert.Sequence != cert.Height {
		return fmt.Errorf("invalid_commit_certificate_identity")
	}
	if len(cert.Commits) < s.commitQuorumLocked() {
		return fmt.Errorf("commit_certificate_insufficient_votes")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Commits {
		if !s.isValidatorLocked(vote.NodeID) {
			return fmt.Errorf("commit_certificate_unknown_validator")
		}
		if seen[vote.NodeID] {
			return fmt.Errorf("commit_certificate_duplicate_validator")
		}
		if vote.View != cert.View || vote.Sequence != cert.Sequence || vote.Height != cert.Height || vote.BlockHash != cert.BlockHash {
			return fmt.Errorf("commit_certificate_vote_mismatch")
		}
		seen[vote.NodeID] = true
	}
	return nil
}

func (s *State) validateCheckpointCertificateLocked(cert CheckpointCertificate) error {
	if cert.Height == 0 || cert.BlockHash == "" || cert.StateRoot == "" {
		return fmt.Errorf("invalid_checkpoint_certificate_identity")
	}
	if len(cert.Checkpoints) < s.commitQuorumLocked() {
		return fmt.Errorf("checkpoint_certificate_insufficient_votes")
	}
	seen := map[string]bool{}
	for _, vote := range cert.Checkpoints {
		if !s.isValidatorLocked(vote.NodeID) {
			return fmt.Errorf("checkpoint_certificate_unknown_validator")
		}
		if seen[vote.NodeID] {
			return fmt.Errorf("checkpoint_certificate_duplicate_validator")
		}
		if vote.Height != cert.Height || vote.BlockHash != cert.BlockHash || vote.StateRoot != cert.StateRoot {
			return fmt.Errorf("checkpoint_certificate_vote_mismatch")
		}
		seen[vote.NodeID] = true
	}
	return nil
}

func sortedPrepares(votes map[string]Prepare) []Prepare {
	ids := make([]string, 0, len(votes))
	for id := range votes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Prepare, 0, len(ids))
	for _, id := range ids {
		out = append(out, votes[id])
	}
	return out
}

func sortedCommits(votes map[string]Commit) []Commit {
	ids := make([]string, 0, len(votes))
	for id := range votes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Commit, 0, len(ids))
	for _, id := range ids {
		out = append(out, votes[id])
	}
	return out
}

func sortedCheckpoints(votes map[string]Checkpoint) []Checkpoint {
	ids := make([]string, 0, len(votes))
	for id := range votes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Checkpoint, 0, len(ids))
	for _, id := range ids {
		out = append(out, votes[id])
	}
	return out
}

func copyPreparedCertificate(cert PreparedCertificate) PreparedCertificate {
	next := cert
	next.Prepares = append([]Prepare(nil), cert.Prepares...)
	return next
}

func copyCommitCertificate(cert CommitCertificate) CommitCertificate {
	next := cert
	next.Commits = append([]Commit(nil), cert.Commits...)
	return next
}

func copyCheckpointCertificate(cert CheckpointCertificate) CheckpointCertificate {
	next := cert
	next.Checkpoints = append([]Checkpoint(nil), cert.Checkpoints...)
	return next
}

func copyBlockPtr(value *block.Block) *block.Block {
	if value == nil {
		return nil
	}
	next := *value
	return &next
}

func copyPreparedPtr(value *PreparedCertificate) *PreparedCertificate {
	if value == nil {
		return nil
	}
	next := copyPreparedCertificate(*value)
	return &next
}

func copyCheckpointPtr(value *CheckpointCertificate) *CheckpointCertificate {
	if value == nil {
		return nil
	}
	next := copyCheckpointCertificate(*value)
	return &next
}

func copyNewView(value NewView) NewView {
	next := value
	next.ViewChanges = make([]ViewChange, 0, len(value.ViewChanges))
	for _, vc := range value.ViewChanges {
		next.ViewChanges = append(next.ViewChanges, copyViewChange(vc))
	}
	next.SelectedPrepared = copyPreparedPtr(value.SelectedPrepared)
	next.SelectedBlock = copyBlockPtr(value.SelectedBlock)
	return next
}
