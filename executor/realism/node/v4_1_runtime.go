package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

type RuntimeV41 struct {
	cfg       Config
	node      *Node
	transport *p2p.Transport
	consensus *pbft.State
	store     *storage.BlockStore
	proposer  *realblock.Proposer
	pbftLogs  *pbft.Logs

	mu              sync.Mutex
	txGossipSeen    bool
	proposed        []realblock.Block
	commitRecords   []storage.CommitRecord
	sentCommit      map[string]bool
	committed       map[string]bool
	committedHeight uint64
	committedHash   string
}

func NewRuntimeV41(cfg Config) *RuntimeV41 {
	if cfg.Role == "" {
		cfg.Role = "validator"
	}
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 10
	}
	if cfg.Consensus == "" {
		cfg.Consensus = "pbft"
	}
	if cfg.LeaderID == "" {
		cfg.LeaderID = cfg.NodeID
	}
	if len(cfg.Validators) == 0 {
		cfg.Validators = []string{cfg.NodeID}
	}
	n := New(cfg)
	n.Stage = "v4_1_network_consensus_commit"
	r := &RuntimeV41{
		cfg:        cfg,
		node:       n,
		consensus:  pbft.NewState(cfg.NodeID, cfg.ShardID, cfg.LeaderID, cfg.Validators),
		store:      storage.NewBlockStore(cfg.DataDir, cfg.NodeID, cfg.ShardID),
		proposer:   realblock.NewProposer(cfg.NodeID, cfg.ShardID),
		pbftLogs:   &pbft.Logs{},
		sentCommit: map[string]bool{},
		committed:  map[string]bool{},
	}
	r.transport = p2p.NewTransport(cfg.NodeID, cfg.ListenAddr, cfg.Peers, r.HandleMessage)
	return r
}

func (r *RuntimeV41) Start(ctx context.Context) error {
	return r.transport.Start(ctx)
}

func (r *RuntimeV41) Stop() error {
	return r.transport.Stop()
}

func (r *RuntimeV41) ListenAddr() string {
	return r.transport.ListenAddr
}

func (r *RuntimeV41) SetPeers(peers []p2p.Peer) {
	r.transport.SetPeers(peers)
}

func (r *RuntimeV41) HandleMessage(ctx context.Context, msg p2p.MessageEnvelope) error {
	switch msg.MessageType {
	case p2p.MessageTXGossip:
		item, err := p2p.DecodePayload[tx.SignedTransaction](msg)
		if err != nil {
			return err
		}
		result := r.node.Mempool.Admit(item)
		r.mu.Lock()
		r.txGossipSeen = true
		r.mu.Unlock()
		if !result.Accepted && result.RejectReason != "duplicate_tx" {
			return errors.New(result.RejectReason)
		}
	case p2p.MessagePBFTPrePrepare:
		pre, err := p2p.DecodePayload[pbft.PrePrepare](msg)
		if err != nil {
			return err
		}
		prepare, err := r.consensus.OnPrePrepare(pre)
		r.pbftLogs.AddMessage(pbft.MessageLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, MessageType: msg.MessageType, FromNode: msg.FromNode, BlockHash: pre.BlockHash, Height: pre.Height, View: pre.View, Sequence: pre.Sequence, Accepted: err == nil, Error: errorText(err)})
		if err != nil {
			return err
		}
		return r.broadcastPrepare(ctx, prepare)
	case p2p.MessagePBFTPrepare:
		prepare, err := p2p.DecodePayload[pbft.Prepare](msg)
		if err != nil {
			return err
		}
		reached, votes := r.consensus.OnPrepare(prepare)
		r.pbftLogs.AddMessage(pbft.MessageLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, MessageType: msg.MessageType, FromNode: msg.FromNode, BlockHash: prepare.BlockHash, Height: prepare.Height, View: prepare.View, Sequence: prepare.Sequence, Accepted: true})
		r.pbftLogs.AddQuorum(pbft.QuorumLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, ShardID: r.cfg.ShardID, QuorumType: "prepare", BlockHash: prepare.BlockHash, Height: prepare.Height, View: prepare.View, Votes: votes, Required: r.consensus.PrepareQuorum(), Reached: reached})
		if reached {
			return r.broadcastCommit(ctx, pbft.Commit{View: prepare.View, Sequence: prepare.Sequence, Height: prepare.Height, NodeID: r.cfg.NodeID, BlockHash: prepare.BlockHash})
		}
	case p2p.MessagePBFTCommit:
		commit, err := p2p.DecodePayload[pbft.Commit](msg)
		if err != nil {
			return err
		}
		reached, votes, b := r.consensus.OnCommit(commit)
		r.pbftLogs.AddMessage(pbft.MessageLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, MessageType: msg.MessageType, FromNode: msg.FromNode, BlockHash: commit.BlockHash, Height: commit.Height, View: commit.View, Sequence: commit.Sequence, Accepted: true})
		r.pbftLogs.AddQuorum(pbft.QuorumLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, ShardID: r.cfg.ShardID, QuorumType: "commit", BlockHash: commit.BlockHash, Height: commit.Height, View: commit.View, Votes: votes, Required: r.consensus.CommitQuorum(), Reached: reached})
		if reached {
			return r.commitBlock(b, votes)
		}
	case p2p.MessagePBFTViewChange:
		viewChange, err := p2p.DecodePayload[pbft.ViewChange](msg)
		if err != nil {
			return err
		}
		reached, votes := r.consensus.OnViewChange(viewChange)
		r.pbftLogs.AddViewChange(pbft.ViewChangeLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, OldView: viewChange.View, NewView: viewChange.NewView, LeaderID: viewChange.LeaderID, Votes: votes, Required: r.consensus.CommitQuorum(), BasicViewChange: true, ProductionViewChangeProof: false, Checkpoint: false, StableLog: false})
		if reached && r.cfg.NodeID == r.consensus.NextLeader(viewChange.NewView) {
			return r.BroadcastNewView(ctx, viewChange.NewView)
		}
	case p2p.MessagePBFTNewView:
		newView, err := p2p.DecodePayload[pbft.NewView](msg)
		if err != nil {
			return err
		}
		r.consensus.OnNewView(newView)
		r.pbftLogs.AddViewChange(pbft.ViewChangeLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, OldView: newView.View - 1, NewView: newView.View, LeaderID: newView.LeaderID, Votes: r.consensus.CommitQuorum(), Required: r.consensus.CommitQuorum(), BasicViewChange: true, ProductionViewChangeProof: false, Checkpoint: false, StableLog: false})
	}
	return nil
}

func (r *RuntimeV41) GossipTx(ctx context.Context, item tx.SignedTransaction) error {
	msg, err := p2p.NewEnvelope(p2p.MessageTXGossip, r.cfg.NodeID, "", r.cfg.ShardID, 0, r.consensus.ViewID, 0, item)
	if err != nil {
		return err
	}
	errs := r.transport.Broadcast(ctx, msg)
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (r *RuntimeV41) ProposeBlock(ctx context.Context) (realblock.Block, error) {
	if r.cfg.NodeID != r.cfg.LeaderID {
		return realblock.Block{}, fmt.Errorf("not_leader")
	}
	b, err := r.proposer.Build(r.node.Mempool, r.cfg.BlockSize, time.Now())
	if err != nil {
		return realblock.Block{}, err
	}
	r.mu.Lock()
	r.proposed = append(r.proposed, b)
	r.mu.Unlock()
	pre := pbft.PrePrepare{View: r.consensus.ViewID, Sequence: b.Height, Height: b.Height, LeaderID: r.cfg.NodeID, BlockHash: b.BlockHash, Block: b}
	prepare, err := r.consensus.OnPrePrepare(pre)
	if err != nil {
		return realblock.Block{}, err
	}
	msg, err := p2p.NewEnvelope(p2p.MessagePBFTPrePrepare, r.cfg.NodeID, "", r.cfg.ShardID, b.Height, pre.View, pre.Sequence, pre)
	if err != nil {
		return realblock.Block{}, err
	}
	r.transport.Broadcast(ctx, msg)
	return b, r.broadcastPrepare(ctx, prepare)
}

func (r *RuntimeV41) broadcastPrepare(ctx context.Context, prepare pbft.Prepare) error {
	reached, votes := r.consensus.OnPrepare(prepare)
	r.pbftLogs.AddQuorum(pbft.QuorumLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, ShardID: r.cfg.ShardID, QuorumType: "prepare", BlockHash: prepare.BlockHash, Height: prepare.Height, View: prepare.View, Votes: votes, Required: r.consensus.PrepareQuorum(), Reached: reached})
	msg, err := p2p.NewEnvelope(p2p.MessagePBFTPrepare, r.cfg.NodeID, "", r.cfg.ShardID, prepare.Height, prepare.View, prepare.Sequence, prepare)
	if err != nil {
		return err
	}
	r.transport.Broadcast(ctx, msg)
	if reached {
		return r.broadcastCommit(ctx, pbft.Commit{View: prepare.View, Sequence: prepare.Sequence, Height: prepare.Height, NodeID: r.cfg.NodeID, BlockHash: prepare.BlockHash})
	}
	return nil
}

func (r *RuntimeV41) broadcastCommit(ctx context.Context, commit pbft.Commit) error {
	r.mu.Lock()
	if r.sentCommit[commit.BlockHash] {
		r.mu.Unlock()
		return nil
	}
	r.sentCommit[commit.BlockHash] = true
	r.mu.Unlock()
	reached, votes, b := r.consensus.OnCommit(commit)
	r.pbftLogs.AddQuorum(pbft.QuorumLogEntry{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, ShardID: r.cfg.ShardID, QuorumType: "commit", BlockHash: commit.BlockHash, Height: commit.Height, View: commit.View, Votes: votes, Required: r.consensus.CommitQuorum(), Reached: reached})
	msg, err := p2p.NewEnvelope(p2p.MessagePBFTCommit, r.cfg.NodeID, "", r.cfg.ShardID, commit.Height, commit.View, commit.Sequence, commit)
	if err != nil {
		return err
	}
	r.transport.Broadcast(ctx, msg)
	if reached {
		return r.commitBlock(b, votes)
	}
	return nil
}

func (r *RuntimeV41) BroadcastViewChange(ctx context.Context, newView uint64) error {
	vc := pbft.ViewChange{View: r.consensus.ViewID, NewView: newView, NodeID: r.cfg.NodeID, Height: r.consensus.Height, LeaderID: r.consensus.NextLeader(newView)}
	r.consensus.OnViewChange(vc)
	msg, err := p2p.NewEnvelope(p2p.MessagePBFTViewChange, r.cfg.NodeID, "", r.cfg.ShardID, r.consensus.Height, r.consensus.ViewID, r.consensus.SequenceID, vc)
	if err != nil {
		return err
	}
	r.transport.Broadcast(ctx, msg)
	return nil
}

func (r *RuntimeV41) BroadcastNewView(ctx context.Context, newView uint64) error {
	nv := pbft.NewView{View: newView, LeaderID: r.consensus.NextLeader(newView), Height: r.consensus.Height}
	r.consensus.OnNewView(nv)
	msg, err := p2p.NewEnvelope(p2p.MessagePBFTNewView, r.cfg.NodeID, "", r.cfg.ShardID, r.consensus.Height, newView, r.consensus.SequenceID, nv)
	if err != nil {
		return err
	}
	r.transport.Broadcast(ctx, msg)
	return nil
}

func (r *RuntimeV41) commitBlock(b realblock.Block, commitVotes int) error {
	if b.BlockHash == "" {
		return nil
	}
	r.mu.Lock()
	if r.committed[b.BlockHash] {
		r.mu.Unlock()
		return nil
	}
	r.committed[b.BlockHash] = true
	r.committedHeight = b.Height
	r.committedHash = b.BlockHash
	record := storage.CommitRecord{Timestamp: time.Now().UnixMilli(), NodeID: r.cfg.NodeID, ShardID: r.cfg.ShardID, Height: b.Height, BlockHash: b.BlockHash, ProposerID: b.ProposerID, TxCount: len(b.TxIDs), PrepareQuorum: true, CommitQuorum: true, Committed: true, StateCommit: false}
	r.commitRecords = append(r.commitRecords, record)
	r.mu.Unlock()
	return r.store.AppendCommitted(b, record)
}

func (r *RuntimeV41) WriteArtifacts(dir string) error {
	if dir == "" {
		dir = r.cfg.DataDir
	}
	if err := r.transport.Log.WriteCSV(filepath.Join(dir, "network_log.csv")); err != nil {
		return err
	}
	if err := r.pbftLogs.WriteMessageCSV(filepath.Join(dir, "pbft_message_log.csv")); err != nil {
		return err
	}
	if err := r.pbftLogs.WriteQuorumCSV(filepath.Join(dir, "quorum_log.csv")); err != nil {
		return err
	}
	if err := r.pbftLogs.WriteViewChangeCSV(filepath.Join(dir, "view_change_log.csv")); err != nil {
		return err
	}
	if err := r.writeProposalCSV(filepath.Join(dir, "block_proposal_log.csv")); err != nil {
		return err
	}
	r.mu.Lock()
	records := append([]storage.CommitRecord(nil), r.commitRecords...)
	r.mu.Unlock()
	if err := storage.WriteCommitCSV(filepath.Join(dir, "block_commit_log.csv"), records); err != nil {
		return err
	}
	return metrics.WriteJSON(filepath.Join(dir, "v4_1_node_runtime_summary.json"), r.Summary())
}

func (r *RuntimeV41) Summary() RuntimeSummaryV41 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RuntimeSummaryV41{
		RuntimeStage:                   "v4_1_network_consensus_commit",
		RuntimeTruth:                   "v4_real_p2p_consensus_commit",
		NodeID:                         r.cfg.NodeID,
		ShardID:                        r.cfg.ShardID,
		ListenAddr:                     r.transport.ListenAddr,
		Role:                           r.cfg.Role,
		RealSignedTx:                   true,
		PerNodeMempool:                 true,
		RealP2P:                        true,
		TxGossip:                       r.txGossipSeen,
		BlockProposer:                  len(r.proposed) > 0,
		PBFTStyleConsensus:             true,
		RealPBFTMessages:               len(r.pbftLogs.Messages) > 0,
		BlockCommit:                    r.committedHeight > 0,
		StateCommit:                    false,
		CrossShardProtocol:             false,
		FrontendRealismMode:            false,
		ProductionPBFT:                 false,
		FullByzantineSecurity:          false,
		BasicViewChange:                true,
		ProductionViewChangeProof:      false,
		Checkpoint:                     false,
		StableLog:                      false,
		NotBlockEmulatorReplacementYet: true,
		CommittedHeight:                r.committedHeight,
		CommittedBlockHash:             r.committedHash,
	}
}

func (r *RuntimeV41) writeProposalCSV(path string) error {
	r.mu.Lock()
	proposed := append([]realblock.Block(nil), r.proposed...)
	r.mu.Unlock()
	rows := [][]string{}
	for _, b := range proposed {
		rows = append(rows, []string{strconv.FormatInt(b.Timestamp, 10), r.cfg.NodeID, b.ShardID, strconv.FormatUint(b.Height, 10), b.BlockHash, b.ProposerID, strconv.Itoa(len(b.TxIDs)), b.TxRoot, strconv.FormatBool(false)})
	}
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "shard_id", "height", "block_hash", "proposer_id", "tx_count", "tx_root", "state_commit"}, rows)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
