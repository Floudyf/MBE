package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func asyncV640Tx(id, shard string, access tx.AccessItem, required, produced uint64) tx.SignedTransaction {
	item := tx.SignedTransaction{
		TxID:       id,
		Sender:     "sender-" + id,
		Receiver:   "receiver-" + id,
		AccessList: []tx.AccessItem{access},
	}
	item.ExecutionRouting = &tx.ExecutionRoutingMetadata{
		RoutingOrdinal: produced,
		ExecutionShard: shard,
		StateVersions: []tx.StateVersionDependency{{
			Key:             access.Key,
			RequiredVersion: required,
			ProducedVersion: produced,
		}},
	}
	return item
}

func TestMetaTrackBlockRemoteValueConsumerIndexV640CountsRemoteExactReadersOnly(t *testing.T) {
	producer := asyncV640Tx("producer", "s0", tx.AccessItem{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}, 0, 1)
	localReader := asyncV640Tx("local-reader", "s0", tx.AccessItem{Key: "asset:k", Mode: tx.AccessRead, UpdateSemantics: "validate"}, 1, 2)
	remoteReader := asyncV640Tx("remote-reader", "s1", tx.AccessItem{Key: "asset:k", Mode: tx.AccessRead, UpdateSemantics: "validate"}, 1, 3)
	blindWriter := asyncV640Tx("blind-writer", "s1", tx.AccessItem{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}, 1, 4)
	index := metaTrackBlockRemoteValueConsumerIndexV640(realblock.Block{TxList: []tx.SignedTransaction{producer, localReader, remoteReader, blindWriter}})
	identity := metaTrackVersionIdentity{Key: "asset:k", Version: 1}
	if got := index[identity]; got != 1 {
		t.Fatalf("remote exact-value consumer count=%d want=1", got)
	}
}

func TestMetaTrackBlockRemoteValueConsumerIndexV640IgnoresExternalProducer(t *testing.T) {
	reader := asyncV640Tx("reader", "s1", tx.AccessItem{Key: "asset:external", Mode: tx.AccessRead, UpdateSemantics: "validate"}, 41, 42)
	index := metaTrackBlockRemoteValueConsumerIndexV640(realblock.Block{TxList: []tx.SignedTransaction{reader}})
	if got := index[metaTrackVersionIdentity{Key: "asset:external", Version: 41}]; got != 0 {
		t.Fatalf("external predecessor must not be classified as an in-block remote consumer edge, got=%d", got)
	}
}
