package block

import (
	"fmt"
	"time"

	"metaverse-chainlab/executor/realism/mempool"
)

type Proposer struct {
	NodeID       string
	ShardID      string
	PreviousHash string
	NextHeight   uint64
}

func NewProposer(nodeID, shardID string) *Proposer {
	return &Proposer{NodeID: nodeID, ShardID: shardID, PreviousHash: "genesis", NextHeight: 1}
}

func (p *Proposer) Build(pool *mempool.Mempool, limit int, now time.Time) (Block, error) {
	txs := pool.PopReady(limit)
	if len(txs) == 0 {
		return Block{}, fmt.Errorf("empty_mempool")
	}
	txIDs := make([]string, 0, len(txs))
	for _, item := range txs {
		txIDs = append(txIDs, item.TxID)
	}
	b := Block{
		ShardID:            p.ShardID,
		Height:             p.NextHeight,
		PreviousHash:       p.PreviousHash,
		ProposerID:         p.NodeID,
		Timestamp:          now.UnixMilli(),
		TxIDs:              txIDs,
		TxList:             txs,
		StateRootBefore:    "empty",
		StateRootAfter:     "pending_not_executed",
		ReceiptRoot:        "pending_not_executed",
		StateCommit:        false,
		CrossShardProtocol: false,
	}
	AssignHash(&b)
	p.PreviousHash = b.BlockHash
	p.NextHeight++
	return b, nil
}
