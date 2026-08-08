package block

import (
	"fmt"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

// BuildFromReserved builds a candidate from transactions that have already
// been reserved by an algorithm-specific block producer.  It deliberately
// does not touch the mempool, so existing Proposer.Build behavior remains
// unchanged for every other block producer.
func (p *Proposer) BuildFromReserved(items []tx.SignedTransaction, now time.Time) (Block, error) {
	if len(items) == 0 {
		return Block{}, fmt.Errorf("empty_mempool")
	}
	if now.IsZero() {
		now = time.Now()
	}
	txIDs := make([]string, 0, len(items))
	for _, item := range items {
		txIDs = append(txIDs, item.TxID)
	}
	b := Block{
		ShardID:            p.ShardID,
		Height:             p.NextHeight,
		PreviousHash:       p.PreviousHash,
		ProposerID:         p.NodeID,
		Timestamp:          now.UnixMilli(),
		TxIDs:              txIDs,
		TxList:             append([]tx.SignedTransaction(nil), items...),
		StateRootBefore:    "empty",
		StateRootAfter:     "pending_not_executed",
		ReceiptRoot:        "pending_not_executed",
		StateCommit:        false,
		CrossShardProtocol: false,
	}
	AssignHash(&b)
	return b, nil
}
