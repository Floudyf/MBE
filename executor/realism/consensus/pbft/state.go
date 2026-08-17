package pbft

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

type Stage string

const (
	StageIdle        Stage = "idle"
	StagePrePrepared Stage = "pre_prepared"
	StagePrepared    Stage = "prepared"
	StageCommitted   Stage = "committed"
	StageViewChange  Stage = "view_change"
)

const (
	defaultCheckpointInterval uint64 = 20
	defaultWatermarkWindow    uint64 = 200
)

type State struct {
	mu sync.RWMutex

	NodeID          string
	ShardID         string
	ViewID          uint64
	SequenceID      uint64
	Height          uint64
	LeaderID        string
	InitialLeaderID string
	ValidatorSet    []string
	F               int

	PrepareVotes      map[string]map[string]Prepare
	CommitVotes       map[string]map[string]Commit
	EarlyPrepareVotes map[string]map[string]Prepare
	EarlyCommitVotes  map[string]map[string]Commit
	ViewChangeVotes   map[uint64]map[string]ViewChange
	CheckpointVotes   map[string]map[string]Checkpoint

	LockedBlocks       map[string]block.Block
	PrePrepareBySlot   map[string]string
	PrepareBySlotNode  map[string]map[string]string
	CommitBySlotNode   map[string]map[string]string
	PreparedByHeight   map[uint64]PreparedCertificate
	PreparedBlocks     map[uint64]block.Block
	CommittedBlocks    map[uint64]block.Block
	CommitCertificates map[uint64]CommitCertificate

	CommitBroadcasts     map[string]time.Time
	ViewChangeBroadcasts map[uint64]bool
	AcceptedNewViews     map[uint64]NewView

	StableCheckpoint   CheckpointCertificate
	LowWatermark       uint64
	HighWatermark      uint64
	CheckpointInterval uint64
	WatermarkWindow    uint64

	LastProgressTime time.Time
	Stage            Stage
}

type Snapshot struct {
	NodeID                 string               `json:"node_id"`
	ShardID                string               `json:"shard_id"`
	View                   uint64               `json:"view"`
	LeaderID               string               `json:"leader_id"`
	Height                 uint64               `json:"height"`
	Sequence               uint64               `json:"sequence"`
	Stage                  Stage                `json:"stage"`
	PrepareQuorum          int                  `json:"prepare_quorum"`
	CommitQuorum           int                  `json:"commit_quorum"`
	LowWatermark           uint64               `json:"low_watermark"`
	HighWatermark          uint64               `json:"high_watermark"`
	StableCheckpointHeight uint64               `json:"stable_checkpoint_height"`
	Prepared               *PreparedCertificate `json:"prepared,omitempty"`
	CommitCertificateCount int                  `json:"commit_certificate_count"`
	LastProgressAtMS       int64                `json:"last_progress_at_ms"`
}

func NewState(nodeID, shardID, leaderID string, validators []string) *State {
	validatorCopy := append([]string(nil), validators...)
	sort.Strings(validatorCopy)
	if len(validatorCopy) == 0 && nodeID != "" {
		validatorCopy = []string{nodeID}
	}
	if leaderID == "" && len(validatorCopy) > 0 {
		leaderID = validatorCopy[0]
	}
	return &State{
		NodeID:               nodeID,
		ShardID:              shardID,
		LeaderID:             leaderID,
		InitialLeaderID:      leaderID,
		ValidatorSet:         validatorCopy,
		F:                    FaultTolerance(len(validatorCopy)),
		PrepareVotes:         map[string]map[string]Prepare{},
		CommitVotes:          map[string]map[string]Commit{},
		EarlyPrepareVotes:    map[string]map[string]Prepare{},
		EarlyCommitVotes:     map[string]map[string]Commit{},
		ViewChangeVotes:      map[uint64]map[string]ViewChange{},
		CheckpointVotes:      map[string]map[string]Checkpoint{},
		LockedBlocks:         map[string]block.Block{},
		PrePrepareBySlot:     map[string]string{},
		PrepareBySlotNode:    map[string]map[string]string{},
		CommitBySlotNode:     map[string]map[string]string{},
		PreparedByHeight:     map[uint64]PreparedCertificate{},
		PreparedBlocks:       map[uint64]block.Block{},
		CommittedBlocks:      map[uint64]block.Block{},
		CommitCertificates:   map[uint64]CommitCertificate{},
		CommitBroadcasts:     map[string]time.Time{},
		ViewChangeBroadcasts: map[uint64]bool{},
		AcceptedNewViews:     map[uint64]NewView{},
		CheckpointInterval:   defaultCheckpointInterval,
		WatermarkWindow:      defaultWatermarkWindow,
		HighWatermark:        defaultWatermarkWindow,
		LastProgressTime:     time.Now(),
		Stage:                StageIdle,
	}
}

func (s *State) ConfigureCheckpoint(interval, window uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if interval > 0 {
		s.CheckpointInterval = interval
	}
	if window > 0 {
		s.WatermarkWindow = window
	}
	if s.WatermarkWindow < s.CheckpointInterval*2 {
		s.WatermarkWindow = s.CheckpointInterval * 2
	}
	s.HighWatermark = s.LowWatermark + s.WatermarkWindow
}

func (s *State) PrepareQuorum() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prepareQuorumLocked()
}

func (s *State) prepareQuorumLocked() int {
	return Quorum(len(s.ValidatorSet))
}

func (s *State) CommitQuorum() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commitQuorumLocked()
}

func (s *State) commitQuorumLocked() int {
	return Quorum(len(s.ValidatorSet))
}

func (s *State) IsValidator(nodeID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isValidatorLocked(nodeID)
}

func (s *State) isValidatorLocked(nodeID string) bool {
	if nodeID == "" {
		return false
	}
	for _, validator := range s.ValidatorSet {
		if validator == nodeID {
			return true
		}
	}
	return false
}

func (s *State) ValidatePrePrepare(msg PrePrepare) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validatePrePrepareLocked(msg)
}

func (s *State) validatePrePrepareLocked(msg PrePrepare) error {
	if msg.View != s.ViewID {
		return fmt.Errorf("wrong_view")
	}
	expectedLeader := s.leaderForViewLocked(msg.View)
	if msg.LeaderID != expectedLeader {
		return fmt.Errorf("wrong_leader")
	}
	if msg.Sequence == 0 || msg.Height == 0 || msg.Sequence != msg.Height {
		return fmt.Errorf("wrong_sequence")
	}
	if !s.inWatermarksLocked(msg.Height) {
		return fmt.Errorf("outside_watermarks")
	}
	if msg.BlockHash == "" || msg.BlockHash != msg.Block.BlockHash || block.Hash(msg.Block) != msg.BlockHash {
		return fmt.Errorf("wrong_digest")
	}
	if msg.Height != msg.Block.Height {
		return fmt.Errorf("wrong_height")
	}
	slot := slotKey(msg.View, msg.Sequence)
	if locked := s.PrePrepareBySlot[slot]; locked != "" && locked != msg.BlockHash {
		return fmt.Errorf("conflicting_preprepare")
	}
	if prepared, ok := s.PreparedByHeight[msg.Height]; ok && prepared.BlockHash != "" && prepared.BlockHash != msg.BlockHash {
		return fmt.Errorf("prepared_digest_conflict")
	}
	return nil
}

func (s *State) OnPrePrepare(msg PrePrepare) (Prepare, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validatePrePrepareLocked(msg); err != nil {
		return Prepare{}, err
	}
	slot := slotKey(msg.View, msg.Sequence)
	s.PrePrepareBySlot[slot] = msg.BlockHash
	s.Height = msg.Height
	s.SequenceID = msg.Sequence
	s.LockedBlocks[msg.BlockHash] = msg.Block
	s.replayEarlyVotesLocked(slot, msg.BlockHash)
	if s.Stage != StagePrepared && s.Stage != StageCommitted {
		s.Stage = StagePrePrepared
	}
	s.LastProgressTime = time.Now()
	return Prepare{
		View:      msg.View,
		Sequence:  msg.Sequence,
		Height:    msg.Height,
		NodeID:    s.NodeID,
		BlockHash: msg.BlockHash,
	}, nil
}

func (s *State) IsDuplicatePrePrepare(view, sequence uint64, blockHash string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return blockHash != "" && s.PrePrepareBySlot[slotKey(view, sequence)] == blockHash
}

func (s *State) bufferEarlyPrepareLocked(slot string, msg Prepare) error {
	if s.EarlyPrepareVotes[slot] == nil {
		s.EarlyPrepareVotes[slot] = map[string]Prepare{}
	}
	if existing, ok := s.EarlyPrepareVotes[slot][msg.NodeID]; ok && existing.BlockHash != msg.BlockHash {
		return fmt.Errorf("prepare_equivocation")
	}
	s.EarlyPrepareVotes[slot][msg.NodeID] = msg
	return nil
}

func (s *State) bufferEarlyCommitLocked(slot string, msg Commit) error {
	if s.EarlyCommitVotes[slot] == nil {
		s.EarlyCommitVotes[slot] = map[string]Commit{}
	}
	if existing, ok := s.EarlyCommitVotes[slot][msg.NodeID]; ok && existing.BlockHash != msg.BlockHash {
		return fmt.Errorf("commit_equivocation")
	}
	s.EarlyCommitVotes[slot][msg.NodeID] = msg
	return nil
}

func (s *State) replayEarlyVotesLocked(slot, blockHash string) {
	if votes := s.EarlyPrepareVotes[slot]; len(votes) > 0 {
		if s.PrepareVotes[blockHash] == nil {
			s.PrepareVotes[blockHash] = map[string]Prepare{}
		}
		if s.PrepareBySlotNode[slot] == nil {
			s.PrepareBySlotNode[slot] = map[string]string{}
		}
		for nodeID, vote := range votes {
			if vote.BlockHash != blockHash {
				continue
			}
			s.PrepareVotes[blockHash][nodeID] = vote
			s.PrepareBySlotNode[slot][nodeID] = blockHash
		}
		delete(s.EarlyPrepareVotes, slot)
	}
	if votes := s.EarlyCommitVotes[slot]; len(votes) > 0 {
		if s.CommitVotes[blockHash] == nil {
			s.CommitVotes[blockHash] = map[string]Commit{}
		}
		if s.CommitBySlotNode[slot] == nil {
			s.CommitBySlotNode[slot] = map[string]string{}
		}
		for nodeID, vote := range votes {
			if vote.BlockHash != blockHash {
				continue
			}
			s.CommitVotes[blockHash][nodeID] = vote
			s.CommitBySlotNode[slot][nodeID] = blockHash
		}
		delete(s.EarlyCommitVotes, slot)
	}
}

func (s *State) AcceptPrepare(msg Prepare) (bool, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isValidatorLocked(msg.NodeID) {
		return false, 0, fmt.Errorf("prepare_unknown_validator")
	}
	if msg.View != s.ViewID {
		return false, 0, fmt.Errorf("prepare_wrong_view")
	}
	if msg.Sequence == 0 || msg.Height == 0 || msg.Sequence != msg.Height {
		return false, 0, fmt.Errorf("prepare_wrong_sequence")
	}
	if !s.inWatermarksLocked(msg.Height) {
		return false, 0, fmt.Errorf("prepare_outside_watermarks")
	}
	slot := slotKey(msg.View, msg.Sequence)
	locked := s.PrePrepareBySlot[slot]
	if locked == "" {
		if err := s.bufferEarlyPrepareLocked(slot, msg); err != nil {
			return false, 0, err
		}
		return false, 0, nil
	}
	if locked != msg.BlockHash {
		return false, 0, fmt.Errorf("prepare_without_matching_preprepare")
	}
	if s.PrepareBySlotNode[slot] == nil {
		s.PrepareBySlotNode[slot] = map[string]string{}
	}
	if previous := s.PrepareBySlotNode[slot][msg.NodeID]; previous != "" && previous != msg.BlockHash {
		return false, 0, fmt.Errorf("prepare_equivocation")
	}
	s.PrepareBySlotNode[slot][msg.NodeID] = msg.BlockHash
	if s.PrepareVotes[msg.BlockHash] == nil {
		s.PrepareVotes[msg.BlockHash] = map[string]Prepare{}
	}
	s.PrepareVotes[msg.BlockHash][msg.NodeID] = msg
	count := len(s.PrepareVotes[msg.BlockHash])
	if count >= s.prepareQuorumLocked() {
		cert := PreparedCertificate{
			View:      msg.View,
			Sequence:  msg.Sequence,
			Height:    msg.Height,
			BlockHash: msg.BlockHash,
			Prepares:  sortedPrepares(s.PrepareVotes[msg.BlockHash]),
		}
		existing, hasExisting := s.PreparedByHeight[msg.Height]
		if !hasExisting || cert.View >= existing.View {
			s.PreparedByHeight[msg.Height] = cert
			if b := s.LockedBlocks[msg.BlockHash]; b.BlockHash != "" {
				s.PreparedBlocks[msg.Height] = b
			}
		}
		s.Stage = StagePrepared
		s.LastProgressTime = time.Now()
		return true, count, nil
	}
	return false, count, nil
}

// OnPrepare preserves the V4.1 API. V5 uses AcceptPrepare so invalid votes are
// explicit errors instead of being silently counted.
func (s *State) OnPrepare(msg Prepare) (bool, int) {
	reached, count, _ := s.AcceptPrepare(msg)
	return reached, count
}

func (s *State) AcceptCommit(msg Commit) (bool, int, block.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isValidatorLocked(msg.NodeID) {
		return false, 0, block.Block{}, fmt.Errorf("commit_unknown_validator")
	}
	if msg.View != s.ViewID {
		return false, 0, block.Block{}, fmt.Errorf("commit_wrong_view")
	}
	if msg.Sequence == 0 || msg.Height == 0 || msg.Sequence != msg.Height {
		return false, 0, block.Block{}, fmt.Errorf("commit_wrong_sequence")
	}
	if !s.inWatermarksLocked(msg.Height) {
		return false, 0, block.Block{}, fmt.Errorf("commit_outside_watermarks")
	}
	slot := slotKey(msg.View, msg.Sequence)
	locked := s.PrePrepareBySlot[slot]
	if locked == "" {
		if err := s.bufferEarlyCommitLocked(slot, msg); err != nil {
			return false, 0, block.Block{}, err
		}
		return false, 0, block.Block{}, nil
	}
	if locked != msg.BlockHash {
		return false, 0, block.Block{}, fmt.Errorf("commit_without_matching_preprepare")
	}
	if s.CommitBySlotNode[slot] == nil {
		s.CommitBySlotNode[slot] = map[string]string{}
	}
	if previous := s.CommitBySlotNode[slot][msg.NodeID]; previous != "" && previous != msg.BlockHash {
		return false, 0, block.Block{}, fmt.Errorf("commit_equivocation")
	}
	s.CommitBySlotNode[slot][msg.NodeID] = msg.BlockHash
	if s.CommitVotes[msg.BlockHash] == nil {
		s.CommitVotes[msg.BlockHash] = map[string]Commit{}
	}
	s.CommitVotes[msg.BlockHash][msg.NodeID] = msg
	count := len(s.CommitVotes[msg.BlockHash])
	b := s.LockedBlocks[msg.BlockHash]
	prepared, preparedOK := s.PreparedByHeight[msg.Height]
	if count >= s.commitQuorumLocked() && preparedOK && prepared.BlockHash == msg.BlockHash && b.BlockHash != "" {
		cert := CommitCertificate{
			View: msg.View, Sequence: msg.Sequence, Height: msg.Height,
			BlockHash: msg.BlockHash, Commits: sortedCommits(s.CommitVotes[msg.BlockHash]),
		}
		s.CommitCertificates[msg.Height] = cert
		s.Stage = StageCommitted
		s.CommittedBlocks[b.Height] = b
		s.LastProgressTime = time.Now()
		return true, count, b, nil
	}
	return false, count, block.Block{}, nil
}

func (s *State) OnCommit(msg Commit) (bool, int, block.Block) {
	reached, count, b, _ := s.AcceptCommit(msg)
	return reached, count, b
}

func (s *State) ShouldBroadcastCommit(view, sequence uint64, blockHash string, retryInterval time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := slotKey(view, sequence) + "|" + blockHash
	now := time.Now()
	last := s.CommitBroadcasts[key]
	if !last.IsZero() && retryInterval > 0 && now.Sub(last) < retryInterval {
		return false
	}
	s.CommitBroadcasts[key] = now
	return true
}

// MarkCommitBroadcast preserves the older API. It permits the first broadcast
// only; V5 uses ShouldBroadcastCommit so a lost COMMIT can be retransmitted.
func (s *State) MarkCommitBroadcast(view, sequence uint64, blockHash string) bool {
	return s.ShouldBroadcastCommit(view, sequence, blockHash, time.Duration(1<<63-1))
}

func (s *State) PrepareVoteCount(blockHash string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.PrepareVotes[blockHash])
}

func (s *State) CommitVoteCount(blockHash string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.CommitVotes[blockHash])
}

func (s *State) PreparedCertificate(height uint64) (PreparedCertificate, block.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cert, ok := s.PreparedByHeight[height]
	if !ok {
		return PreparedCertificate{}, block.Block{}, false
	}
	return copyPreparedCertificate(cert), s.PreparedBlocks[height], true
}

func (s *State) CommitCertificate(height uint64) (CommitCertificate, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cert, ok := s.CommitCertificates[height]
	if !ok {
		return CommitCertificate{}, false
	}
	return copyCommitCertificate(cert), true
}

func (s *State) ValidateCommitCertificate(cert CommitCertificate) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateCommitCertificateLocked(cert)
}

// AcceptCommitCertificate imports a quorum proof produced by another correct
// replica. It deliberately does not require cert.View == current ViewID: a
// lagging replica may be catching up after the rest of the shard moved views.
func (s *State) AcceptCommitCertificate(cert CommitCertificate, b block.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateCommitCertificateLocked(cert); err != nil {
		return err
	}
	if b.BlockHash == "" || b.Height != cert.Height || b.BlockHash != cert.BlockHash || block.Hash(b) != cert.BlockHash {
		return fmt.Errorf("commit_certificate_block_mismatch")
	}
	if existing, ok := s.CommitCertificates[cert.Height]; ok && existing.BlockHash != cert.BlockHash {
		return fmt.Errorf("commit_certificate_height_conflict")
	}
	s.CommitCertificates[cert.Height] = copyCommitCertificate(cert)
	s.CommittedBlocks[cert.Height] = b
	s.LastProgressTime = time.Now()
	return nil
}

func (s *State) BuildViewChange(newView, height uint64) (ViewChange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newView <= s.ViewID {
		return ViewChange{}, fmt.Errorf("view_change_not_forward")
	}
	leader := s.leaderForViewLocked(newView)
	vc := ViewChange{
		View:     s.ViewID,
		NewView:  newView,
		NodeID:   s.NodeID,
		Height:   height,
		LeaderID: leader,
	}
	if s.StableCheckpoint.Height > 0 {
		cp := copyCheckpointCertificate(s.StableCheckpoint)
		vc.StableCheckpoint = &cp
	}
	var chosen PreparedCertificate
	var chosenBlock block.Block
	for preparedHeight, cert := range s.PreparedByHeight {
		if preparedHeight > height {
			continue
		}
		if chosen.BlockHash == "" || cert.View > chosen.View || (cert.View == chosen.View && cert.Height > chosen.Height) {
			chosen = cert
			chosenBlock = s.PreparedBlocks[preparedHeight]
		}
	}
	if chosen.BlockHash != "" {
		cert := copyPreparedCertificate(chosen)
		vc.Prepared = &cert
		if chosenBlock.BlockHash != "" {
			blockCopy := chosenBlock
			vc.PreparedBlock = &blockCopy
		}
	}
	s.Stage = StageViewChange
	return vc, nil
}

func (s *State) AcceptViewChange(msg ViewChange) (bool, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acceptViewChangeLocked(msg)
}

func (s *State) acceptViewChangeLocked(msg ViewChange) (bool, int, error) {
	if !s.isValidatorLocked(msg.NodeID) {
		return false, 0, fmt.Errorf("view_change_unknown_validator")
	}
	if msg.NewView <= msg.View || msg.NewView <= s.ViewID {
		return false, 0, fmt.Errorf("view_change_not_forward")
	}
	if msg.LeaderID != s.leaderForViewLocked(msg.NewView) {
		return false, 0, fmt.Errorf("view_change_wrong_leader")
	}
	if msg.StableCheckpoint != nil {
		if err := s.validateCheckpointCertificateLocked(*msg.StableCheckpoint); err != nil {
			return false, 0, err
		}
	}
	if msg.Prepared != nil {
		if err := s.validatePreparedCertificateLocked(*msg.Prepared); err != nil {
			return false, 0, err
		}
		if msg.PreparedBlock == nil || msg.PreparedBlock.BlockHash != msg.Prepared.BlockHash || msg.PreparedBlock.Height != msg.Prepared.Height || block.Hash(*msg.PreparedBlock) != msg.Prepared.BlockHash {
			return false, 0, fmt.Errorf("view_change_prepared_block_mismatch")
		}
	}
	if s.ViewChangeVotes[msg.NewView] == nil {
		s.ViewChangeVotes[msg.NewView] = map[string]ViewChange{}
	}
	if existing, ok := s.ViewChangeVotes[msg.NewView][msg.NodeID]; ok {
		if !sameViewChange(existing, msg) {
			return false, len(s.ViewChangeVotes[msg.NewView]), fmt.Errorf("view_change_equivocation")
		}
		return len(s.ViewChangeVotes[msg.NewView]) >= s.commitQuorumLocked(), len(s.ViewChangeVotes[msg.NewView]), nil
	}
	s.ViewChangeVotes[msg.NewView][msg.NodeID] = copyViewChange(msg)
	count := len(s.ViewChangeVotes[msg.NewView])
	s.Stage = StageViewChange
	// Receiving a VIEW-CHANGE message is coordination activity, not evidence
	// that the pending request made progress. Keeping LastProgressTime intact
	// lets other correct replicas reach their own timeout and join the view.
	return count >= s.commitQuorumLocked(), count, nil
}

func (s *State) OnViewChange(msg ViewChange) (bool, int) {
	reached, count, _ := s.AcceptViewChange(msg)
	return reached, count
}

func (s *State) MarkViewChangeBroadcast(newView uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ViewChangeBroadcasts[newView] {
		return false
	}
	s.ViewChangeBroadcasts[newView] = true
	return true
}

func (s *State) HasViewChangeFrom(newView uint64, nodeID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.ViewChangeVotes[newView][nodeID]
	return ok
}

func (s *State) ViewChangeVoteCount(newView uint64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ViewChangeVotes[newView])
}

func (s *State) BuildNewView(newView, height uint64) (NewView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.NodeID != s.leaderForViewLocked(newView) {
		return NewView{}, fmt.Errorf("new_view_not_primary")
	}
	votes := s.ViewChangeVotes[newView]
	if len(votes) < s.commitQuorumLocked() {
		return NewView{}, fmt.Errorf("new_view_insufficient_view_changes")
	}
	ids := make([]string, 0, len(votes))
	for id := range votes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	viewChanges := make([]ViewChange, 0, len(ids))
	var selected PreparedCertificate
	var selectedBlock block.Block
	for _, id := range ids {
		vc := votes[id]
		viewChanges = append(viewChanges, copyViewChange(vc))
		if vc.Prepared == nil {
			continue
		}
		cert := *vc.Prepared
		if selected.BlockHash == "" || cert.View > selected.View || (cert.View == selected.View && cert.Height > selected.Height) {
			selected = cert
			if vc.PreparedBlock != nil {
				selectedBlock = *vc.PreparedBlock
			}
		} else if cert.View == selected.View && cert.Height == selected.Height && cert.BlockHash != selected.BlockHash {
			return NewView{}, fmt.Errorf("new_view_conflicting_highest_prepared")
		}
	}
	nv := NewView{
		View:        newView,
		LeaderID:    s.leaderForViewLocked(newView),
		Height:      height,
		ViewChanges: viewChanges,
	}
	if selected.BlockHash != "" {
		cert := copyPreparedCertificate(selected)
		nv.SelectedPrepared = &cert
		blockCopy := selectedBlock
		nv.SelectedBlock = &blockCopy
	}
	return nv, nil
}

func (s *State) ValidateNewView(msg NewView) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateNewViewLocked(msg)
}

func (s *State) validateNewViewLocked(msg NewView) error {
	if msg.View < s.ViewID {
		return fmt.Errorf("new_view_not_forward")
	}
	if msg.LeaderID != s.leaderForViewLocked(msg.View) {
		return fmt.Errorf("new_view_wrong_leader")
	}
	if len(msg.ViewChanges) < s.commitQuorumLocked() {
		return fmt.Errorf("new_view_insufficient_view_changes")
	}
	seen := map[string]bool{}
	var highest PreparedCertificate
	var highestBlock block.Block
	for _, vc := range msg.ViewChanges {
		if seen[vc.NodeID] {
			return fmt.Errorf("new_view_duplicate_view_change")
		}
		seen[vc.NodeID] = true
		if vc.NewView != msg.View || vc.LeaderID != msg.LeaderID {
			return fmt.Errorf("new_view_view_change_mismatch")
		}
		if !s.isValidatorLocked(vc.NodeID) {
			return fmt.Errorf("new_view_unknown_validator")
		}
		if vc.StableCheckpoint != nil {
			if err := s.validateCheckpointCertificateLocked(*vc.StableCheckpoint); err != nil {
				return err
			}
		}
		if vc.Prepared != nil {
			if err := s.validatePreparedCertificateLocked(*vc.Prepared); err != nil {
				return err
			}
			if vc.PreparedBlock == nil || vc.PreparedBlock.BlockHash != vc.Prepared.BlockHash || vc.PreparedBlock.Height != vc.Prepared.Height || block.Hash(*vc.PreparedBlock) != vc.Prepared.BlockHash {
				return fmt.Errorf("new_view_prepared_block_mismatch")
			}
			cert := *vc.Prepared
			if highest.BlockHash == "" || cert.View > highest.View || (cert.View == highest.View && cert.Height > highest.Height) {
				highest = cert
				highestBlock = *vc.PreparedBlock
			} else if cert.View == highest.View && cert.Height == highest.Height && cert.BlockHash != highest.BlockHash {
				return fmt.Errorf("new_view_conflicting_highest_prepared")
			}
		}
	}
	if highest.BlockHash == "" {
		if msg.SelectedPrepared != nil || msg.SelectedBlock != nil {
			return fmt.Errorf("new_view_unexpected_selected_prepared")
		}
		return nil
	}
	if msg.SelectedPrepared == nil || msg.SelectedBlock == nil {
		return fmt.Errorf("new_view_missing_selected_prepared")
	}
	if msg.SelectedPrepared.View != highest.View ||
		msg.SelectedPrepared.Sequence != highest.Sequence ||
		msg.SelectedPrepared.Height != highest.Height ||
		msg.SelectedPrepared.BlockHash != highest.BlockHash ||
		msg.SelectedBlock.BlockHash != highestBlock.BlockHash ||
		block.Hash(*msg.SelectedBlock) != highestBlock.BlockHash {
		return fmt.Errorf("new_view_wrong_selected_prepared")
	}
	return nil
}

func (s *State) AcceptNewView(msg NewView) (block.Block, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateNewViewLocked(msg); err != nil {
		return block.Block{}, false, err
	}
	s.AcceptedNewViews[msg.View] = copyNewView(msg)
	if msg.View == s.ViewID {
		if msg.SelectedBlock != nil {
			return *msg.SelectedBlock, true, nil
		}
		return block.Block{}, false, nil
	}
	// A new view retains certified safety evidence but starts a fresh voting
	// round. PREPARE/COMMIT votes are view-specific and must never be counted
	// across views, even when NEW-VIEW safely carries the same block hash.
	s.PrepareVotes = map[string]map[string]Prepare{}
	s.CommitVotes = map[string]map[string]Commit{}
	s.EarlyPrepareVotes = map[string]map[string]Prepare{}
	s.EarlyCommitVotes = map[string]map[string]Commit{}
	s.LockedBlocks = map[string]block.Block{}
	s.PrePrepareBySlot = map[string]string{}
	s.PrepareBySlotNode = map[string]map[string]string{}
	s.CommitBySlotNode = map[string]map[string]string{}
	s.CommitBroadcasts = map[string]time.Time{}
	s.PreparedByHeight = map[uint64]PreparedCertificate{}
	s.PreparedBlocks = map[uint64]block.Block{}
	for view := range s.ViewChangeVotes {
		if view < msg.View {
			delete(s.ViewChangeVotes, view)
			delete(s.ViewChangeBroadcasts, view)
		}
	}
	for view := range s.AcceptedNewViews {
		if view < msg.View {
			delete(s.AcceptedNewViews, view)
		}
	}

	s.ViewID = msg.View
	s.LeaderID = msg.LeaderID
	s.Height = msg.Height
	s.SequenceID = msg.Height
	s.Stage = StageIdle
	s.LastProgressTime = time.Now()
	if msg.SelectedPrepared != nil && msg.SelectedBlock != nil {
		cert := copyPreparedCertificate(*msg.SelectedPrepared)
		b := *msg.SelectedBlock
		s.PreparedByHeight[cert.Height] = cert
		s.PreparedBlocks[cert.Height] = b
		s.LockedBlocks[b.BlockHash] = b
		s.PrePrepareBySlot[slotKey(msg.View, cert.Sequence)] = b.BlockHash
		return b, true, nil
	}
	return block.Block{}, false, nil
}

func (s *State) NewViewProof(view uint64) (NewView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nv, ok := s.AcceptedNewViews[view]
	if !ok {
		return NewView{}, false
	}
	return copyNewView(nv), true
}

func (s *State) OnNewView(msg NewView) {
	// Legacy V4.1 compatibility: the older runtime emits a basic NewView
	// without a certificate. V5 never uses this compatibility branch; it calls
	// AcceptNewView and therefore requires the full view-change proof.
	if len(msg.ViewChanges) == 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		if msg.View > s.ViewID && msg.LeaderID == s.leaderForViewLocked(msg.View) {
			s.ViewID = msg.View
			s.LeaderID = msg.LeaderID
			s.Stage = StageIdle
			s.LastProgressTime = time.Now()
		}
		return
	}
	_, _, _ = s.AcceptNewView(msg)
}

func (s *State) NextLeader(newView uint64) string {
	return s.LeaderForView(newView)
}

func (s *State) LeaderForView(view uint64) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderForViewLocked(view)
}

func (s *State) leaderForViewLocked(view uint64) string {
	if len(s.ValidatorSet) == 0 {
		return s.InitialLeaderID
	}
	base := 0
	for i, nodeID := range s.ValidatorSet {
		if nodeID == s.InitialLeaderID {
			base = i
			break
		}
	}
	return s.ValidatorSet[(base+int(view))%len(s.ValidatorSet)]
}

func (s *State) View() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ViewID
}

func (s *State) ViewHeightSequence() (uint64, uint64, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ViewID, s.Height, s.SequenceID
}

func (s *State) Leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LeaderID
}

func (s *State) LastProgress() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastProgressTime
}

func (s *State) TouchProgress() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastProgressTime = time.Now()
}

func (s *State) CommittedBlockByHash(hash string) (block.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, b := range s.CommittedBlocks {
		if b.BlockHash == hash {
			return b, true
		}
	}
	return block.Block{}, false
}

func (s *State) CommittedBlockAt(height uint64) (block.Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.CommittedBlocks[height]
	return b, ok
}

func (s *State) MarkDurableCommit(b block.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.BlockHash == "" || b.Height == 0 {
		return
	}
	s.CommittedBlocks[b.Height] = b
	delete(s.PreparedByHeight, b.Height)
	delete(s.PreparedBlocks, b.Height)
	for slot, hash := range s.PrePrepareBySlot {
		if locked := s.LockedBlocks[hash]; locked.Height <= b.Height {
			delete(s.PrePrepareBySlot, slot)
			delete(s.PrepareBySlotNode, slot)
			delete(s.CommitBySlotNode, slot)
			delete(s.CommitBroadcasts, slot+"|"+hash)
		}
	}
	for hash, locked := range s.LockedBlocks {
		if locked.Height <= b.Height {
			delete(s.LockedBlocks, hash)
			delete(s.PrepareVotes, hash)
			delete(s.CommitVotes, hash)
		}
	}
	s.Height = b.Height
	s.SequenceID = b.Height
	s.Stage = StageIdle
	s.LastProgressTime = time.Now()
}

func (s *State) CheckpointDue(height uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.CheckpointInterval > 0 && height > 0 && height%s.CheckpointInterval == 0
}

func (s *State) BuildCheckpoint(height uint64, blockHash, stateRoot string) Checkpoint {
	return Checkpoint{Height: height, NodeID: s.NodeID, BlockHash: blockHash, StateRoot: stateRoot}
}

func (s *State) AcceptCheckpoint(msg Checkpoint) (bool, int, CheckpointCertificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isValidatorLocked(msg.NodeID) {
		return false, 0, CheckpointCertificate{}, fmt.Errorf("checkpoint_unknown_validator")
	}
	if msg.Height == 0 || msg.BlockHash == "" || msg.StateRoot == "" {
		return false, 0, CheckpointCertificate{}, fmt.Errorf("checkpoint_invalid_identity")
	}
	if local, ok := s.CommittedBlocks[msg.Height]; ok && local.BlockHash != "" && local.BlockHash != msg.BlockHash {
		return false, 0, CheckpointCertificate{}, fmt.Errorf("checkpoint_block_mismatch")
	}
	key := checkpointKey(msg.Height, msg.BlockHash, msg.StateRoot)
	if s.CheckpointVotes[key] == nil {
		s.CheckpointVotes[key] = map[string]Checkpoint{}
	}
	s.CheckpointVotes[key][msg.NodeID] = msg
	count := len(s.CheckpointVotes[key])
	if count < s.commitQuorumLocked() {
		return false, count, CheckpointCertificate{}, nil
	}
	cert := CheckpointCertificate{
		Height:      msg.Height,
		BlockHash:   msg.BlockHash,
		StateRoot:   msg.StateRoot,
		Checkpoints: sortedCheckpoints(s.CheckpointVotes[key]),
	}

	// A quorum checkpoint learned from remote validators is safety evidence, but
	// it is not yet a locally installed checkpoint. Advancing LowWatermark before
	// this replica has durably committed the checkpoint block can strand a lagging
	// replica exactly one block behind the watermark (for example committed=19,
	// remote stable checkpoint=20). Require both the local durable block and this
	// replica's own checkpoint vote for the same block/state digest before moving
	// local watermarks. Remote votes remain buffered so the transition happens as
	// soon as local catch-up commits the checkpoint height and broadcasts its vote.
	localBlock, locallyDurable := s.CommittedBlocks[msg.Height]
	localVote, localVotePresent := s.CheckpointVotes[key][s.NodeID]
	localCheckpointInstalled := locallyDurable &&
		localBlock.BlockHash == msg.BlockHash &&
		localVotePresent &&
		localVote.Height == msg.Height &&
		localVote.BlockHash == msg.BlockHash &&
		localVote.StateRoot == msg.StateRoot
	if !localCheckpointInstalled {
		return false, count, copyCheckpointCertificate(cert), nil
	}

	newlyStable := msg.Height > s.StableCheckpoint.Height
	if newlyStable {
		s.StableCheckpoint = cert
		s.LowWatermark = msg.Height
		s.HighWatermark = s.LowWatermark + s.WatermarkWindow
		s.pruneStableLocked(msg.Height)
	}
	s.LastProgressTime = time.Now()
	return newlyStable, count, copyCheckpointCertificate(cert), nil
}

func (s *State) StableCheckpointCertificate() (CheckpointCertificate, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.StableCheckpoint.Height == 0 {
		return CheckpointCertificate{}, false
	}
	return copyCheckpointCertificate(s.StableCheckpoint), true
}

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var prepared *PreparedCertificate
	if cert, ok := s.PreparedByHeight[s.Height]; ok {
		next := copyPreparedCertificate(cert)
		prepared = &next
	}
	return Snapshot{
		NodeID:                 s.NodeID,
		ShardID:                s.ShardID,
		View:                   s.ViewID,
		LeaderID:               s.LeaderID,
		Height:                 s.Height,
		Sequence:               s.SequenceID,
		Stage:                  s.Stage,
		PrepareQuorum:          s.prepareQuorumLocked(),
		CommitQuorum:           s.commitQuorumLocked(),
		LowWatermark:           s.LowWatermark,
		HighWatermark:          s.HighWatermark,
		StableCheckpointHeight: s.StableCheckpoint.Height,
		Prepared:               prepared,
		CommitCertificateCount: len(s.CommitCertificates),
		LastProgressAtMS:       s.LastProgressTime.UnixMilli(),
	}
}

func (s *State) inWatermarksLocked(height uint64) bool {
	if height <= s.LowWatermark {
		return false
	}
	if s.HighWatermark == 0 {
		return true
	}
	return height <= s.HighWatermark
}

func (s *State) pruneStableLocked(height uint64) {
	// Commit certificates are compact quorum proofs and are the only safe input
	// to V5 live replica catch-up. Keep them for the lifetime of this runtime
	// instead of pruning them at the PBFT low watermark. The prior trailing-window
	// policy made a correct replica permanently unrecoverable after a long pause:
	// durable blocks remained on disk while the proof required to import them had
	// already been discarded. Raw PREPARE/COMMIT vote maps are still pruned below.
	for hash, b := range s.LockedBlocks {
		if b.Height <= height {
			delete(s.LockedBlocks, hash)
			delete(s.PrepareVotes, hash)
			delete(s.CommitVotes, hash)
		}
	}
	for h := range s.PreparedByHeight {
		if h <= height {
			delete(s.PreparedByHeight, h)
			delete(s.PreparedBlocks, h)
		}
	}
	for h := range s.CommittedBlocks {
		if h < height {
			delete(s.CommittedBlocks, h)
		}
	}
	for slot, hash := range s.PrePrepareBySlot {
		b := s.LockedBlocks[hash]
		if b.BlockHash == "" || b.Height <= height {
			delete(s.PrePrepareBySlot, slot)
			delete(s.PrepareBySlotNode, slot)
			delete(s.CommitBySlotNode, slot)
			delete(s.EarlyPrepareVotes, slot)
			delete(s.EarlyCommitVotes, slot)
		}
	}
	for slot, votes := range s.EarlyPrepareVotes {
		for _, vote := range votes {
			if vote.Height <= height {
				delete(s.EarlyPrepareVotes, slot)
			}
			break
		}
	}
	for slot, votes := range s.EarlyCommitVotes {
		for _, vote := range votes {
			if vote.Height <= height {
				delete(s.EarlyCommitVotes, slot)
			}
			break
		}
	}
	for key, votes := range s.CheckpointVotes {
		for _, vote := range votes {
			if vote.Height < height {
				delete(s.CheckpointVotes, key)
			}
			break
		}
	}
	for view := range s.ViewChangeVotes {
		if view < s.ViewID {
			delete(s.ViewChangeVotes, view)
			delete(s.ViewChangeBroadcasts, view)
		}
	}
	for view := range s.AcceptedNewViews {
		if view < s.ViewID {
			delete(s.AcceptedNewViews, view)
		}
	}
}

func slotKey(view, sequence uint64) string {
	return fmt.Sprintf("%020d:%020d", view, sequence)
}

func checkpointKey(height uint64, blockHash, stateRoot string) string {
	return fmt.Sprintf("%020d:%s:%s", height, blockHash, stateRoot)
}

func sameViewChange(a, b ViewChange) bool {
	if a.View != b.View || a.NewView != b.NewView || a.NodeID != b.NodeID || a.Height != b.Height || a.LeaderID != b.LeaderID {
		return false
	}
	if (a.Prepared == nil) != (b.Prepared == nil) || (a.PreparedBlock == nil) != (b.PreparedBlock == nil) {
		return false
	}
	if a.Prepared != nil {
		if a.Prepared.View != b.Prepared.View || a.Prepared.Sequence != b.Prepared.Sequence || a.Prepared.Height != b.Prepared.Height || a.Prepared.BlockHash != b.Prepared.BlockHash {
			return false
		}
	}
	if a.PreparedBlock != nil && a.PreparedBlock.BlockHash != b.PreparedBlock.BlockHash {
		return false
	}
	if (a.StableCheckpoint == nil) != (b.StableCheckpoint == nil) {
		return false
	}
	if a.StableCheckpoint != nil {
		if a.StableCheckpoint.Height != b.StableCheckpoint.Height || a.StableCheckpoint.BlockHash != b.StableCheckpoint.BlockHash || a.StableCheckpoint.StateRoot != b.StableCheckpoint.StateRoot {
			return false
		}
	}
	return true
}

func copyViewChange(value ViewChange) ViewChange {
	next := value
	next.StableCheckpoint = copyCheckpointPtr(value.StableCheckpoint)
	next.Prepared = copyPreparedPtr(value.Prepared)
	next.PreparedBlock = copyBlockPtr(value.PreparedBlock)
	return next
}
