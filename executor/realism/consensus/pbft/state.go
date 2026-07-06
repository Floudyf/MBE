package pbft

import (
	"fmt"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

type Stage string

const (
	StageIdle      Stage = "idle"
	StagePrepared  Stage = "prepared"
	StageCommitted Stage = "committed"
)

type State struct {
	NodeID           string
	ShardID          string
	ViewID           uint64
	SequenceID       uint64
	Height           uint64
	LeaderID         string
	ValidatorSet     []string
	F                int
	PrepareVotes     map[string]map[string]Prepare
	CommitVotes      map[string]map[string]Commit
	ViewChangeVotes  map[uint64]map[string]ViewChange
	LockedBlocks     map[string]block.Block
	CommittedBlocks  map[uint64]block.Block
	LastProgressTime time.Time
	Stage            Stage
}

func NewState(nodeID, shardID, leaderID string, validators []string) *State {
	return &State{
		NodeID:           nodeID,
		ShardID:          shardID,
		LeaderID:         leaderID,
		ValidatorSet:     append([]string(nil), validators...),
		F:                FaultTolerance(len(validators)),
		PrepareVotes:     map[string]map[string]Prepare{},
		CommitVotes:      map[string]map[string]Commit{},
		ViewChangeVotes:  map[uint64]map[string]ViewChange{},
		LockedBlocks:     map[string]block.Block{},
		CommittedBlocks:  map[uint64]block.Block{},
		LastProgressTime: time.Now(),
		Stage:            StageIdle,
	}
}

func (s *State) PrepareQuorum() int {
	return Quorum(len(s.ValidatorSet))
}

func (s *State) CommitQuorum() int {
	return Quorum(len(s.ValidatorSet))
}

func (s *State) ValidatePrePrepare(msg PrePrepare) error {
	if msg.LeaderID != s.LeaderID {
		return fmt.Errorf("wrong_leader")
	}
	if msg.View != s.ViewID {
		return fmt.Errorf("wrong_view")
	}
	if msg.BlockHash == "" || msg.BlockHash != msg.Block.BlockHash {
		return fmt.Errorf("wrong_digest")
	}
	if msg.Height == 0 || msg.Height != msg.Block.Height {
		return fmt.Errorf("wrong_height")
	}
	return nil
}

func (s *State) OnPrePrepare(msg PrePrepare) (Prepare, error) {
	if err := s.ValidatePrePrepare(msg); err != nil {
		return Prepare{}, err
	}
	s.Height = msg.Height
	s.SequenceID = msg.Sequence
	s.LockedBlocks[msg.BlockHash] = msg.Block
	s.LastProgressTime = time.Now()
	return Prepare{View: msg.View, Sequence: msg.Sequence, Height: msg.Height, NodeID: s.NodeID, BlockHash: msg.BlockHash}, nil
}

func (s *State) OnPrepare(msg Prepare) (bool, int) {
	if s.PrepareVotes[msg.BlockHash] == nil {
		s.PrepareVotes[msg.BlockHash] = map[string]Prepare{}
	}
	s.PrepareVotes[msg.BlockHash][msg.NodeID] = msg
	count := len(s.PrepareVotes[msg.BlockHash])
	if count >= s.PrepareQuorum() {
		s.Stage = StagePrepared
		s.LastProgressTime = time.Now()
		return true, count
	}
	return false, count
}

func (s *State) OnCommit(msg Commit) (bool, int, block.Block) {
	if s.CommitVotes[msg.BlockHash] == nil {
		s.CommitVotes[msg.BlockHash] = map[string]Commit{}
	}
	s.CommitVotes[msg.BlockHash][msg.NodeID] = msg
	count := len(s.CommitVotes[msg.BlockHash])
	b := s.LockedBlocks[msg.BlockHash]
	if count >= s.CommitQuorum() && b.BlockHash != "" {
		s.Stage = StageCommitted
		s.CommittedBlocks[b.Height] = b
		s.LastProgressTime = time.Now()
		return true, count, b
	}
	return false, count, block.Block{}
}

func (s *State) OnViewChange(msg ViewChange) (bool, int) {
	if s.ViewChangeVotes[msg.NewView] == nil {
		s.ViewChangeVotes[msg.NewView] = map[string]ViewChange{}
	}
	s.ViewChangeVotes[msg.NewView][msg.NodeID] = msg
	count := len(s.ViewChangeVotes[msg.NewView])
	return count >= Quorum(len(s.ValidatorSet)), count
}

func (s *State) OnNewView(msg NewView) {
	s.ViewID = msg.View
	s.LeaderID = msg.LeaderID
	s.Stage = StageIdle
	s.LastProgressTime = time.Now()
}

func (s *State) NextLeader(newView uint64) string {
	if len(s.ValidatorSet) == 0 {
		return s.LeaderID
	}
	return s.ValidatorSet[int(newView)%len(s.ValidatorSet)]
}
