package v5

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func mtAdmissionR1Fixtures(count int) []tx.SignedTransaction {
	items := make([]tx.SignedTransaction, 0, count)
	for i := 0; i < count; i++ {
		item := tx.SignedTransaction{
			TxID: fmt.Sprintf("%064x", i+1), Signature: fmt.Sprintf("signature_%d", i+1), PublicKey: "public-key-fixture",
			Sender: "sender", Receiver: "receiver", AccessListSchema: "mbe_workload_record_v3",
		}
		metadata := &tx.ExecutionRoutingMetadata{RoutingOrdinal: uint64(i + 1), ExecutionShard: "s0"}
		for keyIdx := 0; keyIdx < 3; keyIdx++ {
			key := fmt.Sprintf("meta-r1/keys/%d", (i+keyIdx*19)%157)
			item.AccessList = append(item.AccessList, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
			required := uint64(0)
			if i >= 2 {
				required = uint64(i - 1)
			}
			metadata.StateVersions = append(metadata.StateVersions, tx.StateVersionDependency{Key: key, RequiredVersion: required, ProducedVersion: uint64(i + 1)})
		}
		item.ExecutionRouting = metadata
		items = append(items, item)
	}
	return items
}

// Reference is a literal copy of the dependency extraction in the installed
// MTAdmitP1 implementation, intentionally independent of the proposed memo.
func mtAdmissionR1ReferenceMetadata(item tx.SignedTransaction) mtAdmissionR1Metadata {
	result := mtAdmissionR1Metadata{}
	if item.ExecutionRouting == nil {
		return result
	}
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.Key == "" || dependency.ProducedVersion == 0 {
			continue
		}
		result.produced = append(result.produced, statelessVersionAdmissionToken(dependency.Key, dependency.ProducedVersion))
	}
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.Key == "" || dependency.RequiredVersion == 0 {
			continue
		}
		if !transactionRequiresExactStateValue(item, dependency.Key) {
			continue
		}
		result.required = append(result.required, mtAdmissionR1Requirement{token: statelessVersionAdmissionToken(dependency.Key, dependency.RequiredVersion), key: dependency.Key, version: dependency.RequiredVersion})
	}
	return result
}

func mtAdmissionR1ReferenceCandidate(items []tx.SignedTransaction) ([][]string, map[string]int) {
	required := make([][]string, len(items))
	producerIndex := make(map[string]int)
	for index, item := range items {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.ProducedVersion == 0 {
				continue
			}
			producerIndex[statelessVersionAdmissionToken(dependency.Key, dependency.ProducedVersion)] = index
		}
	}
	for index, item := range items {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.RequiredVersion == 0 {
				continue
			}
			if !transactionRequiresExactStateValue(item, dependency.Key) {
				continue
			}
			required[index] = append(required[index], statelessVersionAdmissionToken(dependency.Key, dependency.RequiredVersion))
		}
	}
	return required, producerIndex
}

func mtAdmissionR1MemoCandidate(items []tx.SignedTransaction, memo *mtAdmissionR1Memo) ([][]string, map[string]int) {
	required := make([][]string, len(items))
	producerIndex := make(map[string]int)
	metadata := make([]mtAdmissionR1Metadata, len(items))
	for index, item := range items {
		if item.ExecutionRouting == nil {
			continue
		}
		metadata[index] = memo.metadata(item)
		for _, token := range metadata[index].produced {
			producerIndex[token] = index
		}
	}
	for index, item := range items {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, req := range metadata[index].required {
			required[index] = append(required[index], req.token)
		}
	}
	return required, producerIndex
}

func TestMTAdmitR1LegacyMetadataDifferential(t *testing.T) {
	items := mtAdmissionR1Fixtures(1100)
	memo := new(mtAdmissionR1Memo)
	for round := 0; round < 5; round++ {
		for _, item := range items {
			want := mtAdmissionR1ReferenceMetadata(item)
			got := memo.metadata(item)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("round=%d tx=%s metadata mismatch want=%#v got=%#v", round, item.TxID, want, got)
			}
		}
	}
	// Blind writes preserve ProducedVersion but do not require a predecessor value.
	blind := items[0]
	blind.Signature = "independent-blind-sig"
	blind.AccessList[0].Mode = tx.AccessWrite
	blind.ExecutionRouting.StateVersions[0].RequiredVersion = 7
	ref := mtAdmissionR1ReferenceMetadata(blind)
	got := memo.metadata(blind)
	if !reflect.DeepEqual(ref, got) {
		t.Fatalf("blind write dependency mismatch: want=%#v got=%#v", ref, got)
	}
}

func TestMTAdmitR1CandidateFIFOAndDynamicReadyDifferential(t *testing.T) {
	all := mtAdmissionR1Fixtures(1800)
	memo := new(mtAdmissionR1Memo)
	rng := rand.New(rand.NewSource(0xA111))
	for round := 0; round < 180; round++ {
		n := 1000 - (round%17)*43
		start := (round * 7) % (len(all) - n)
		candidate := append([]tx.SignedTransaction(nil), all[start:start+n]...)
		if round%9 == 0 && n > 2 {
			candidate[0], candidate[n-1] = candidate[n-1], candidate[0]
		}
		legacyRequired, legacyProducer := mtAdmissionR1ReferenceCandidate(candidate)
		currentRequired, currentProducer := mtAdmissionR1MemoCandidate(candidate, memo)
		if !reflect.DeepEqual(legacyRequired, currentRequired) || !reflect.DeepEqual(legacyProducer, currentProducer) {
			t.Fatalf("round %d dependency/FIFO index changed", round)
		}
		live := map[string]bool{}
		for _, deps := range legacyRequired {
			for _, token := range deps {
				live[token] = rng.Intn(3) == 0
			}
		}
		a := mtAdmissionReadyClosureP1(legacyRequired, legacyProducer, live)
		b := mtAdmissionReadyClosureP1(currentRequired, currentProducer, live)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("round %d closure admitted different tx", round)
		}
	}
}

func TestMTAdmitR1UnsignedBypassAndMissingMetadata(t *testing.T) {
	memo := new(mtAdmissionR1Memo)
	item := mtAdmissionR1Fixtures(1)[0]
	item.Signature = ""
	first := memo.metadata(item)
	item.ExecutionRouting.StateVersions[0].RequiredVersion = 99
	second := memo.metadata(item)
	if reflect.DeepEqual(first, second) {
		t.Fatal("unsigned/manual mutated transaction was cached")
	}
	if len(memo.entries) != 0 {
		t.Fatal("unsigned transaction entered cache")
	}
	item.ExecutionRouting = nil
	if got := memo.metadata(item); len(got.required) != 0 || len(got.produced) != 0 {
		t.Fatal("nil metadata not handled")
	}
}

func TestMTAdmitR1BoundedEviction(t *testing.T) {
	memo := new(mtAdmissionR1Memo)
	items := mtAdmissionR1Fixtures(mtAdmissionR1Capacity + 600)
	first := memo.metadata(items[0])
	_ = first
	for _, item := range items {
		memo.metadata(item)
	}
	if len(memo.entries) != mtAdmissionR1Capacity || len(memo.ring) != mtAdmissionR1Capacity {
		t.Fatalf("unbounded cache size=%d ring=%d", len(memo.entries), len(memo.ring))
	}
	if _, has := memo.entries[mtAdmissionR1Identity(items[0])]; has {
		t.Fatal("oldest entry did not evict")
	}
	again := memo.metadata(items[0])
	if !reflect.DeepEqual(again, mtAdmissionR1ReferenceMetadata(items[0])) {
		t.Fatal("eviction changed deterministic metadata")
	}
}

func TestMTAdmitR1ConcurrentMemo(t *testing.T) {
	memo := new(mtAdmissionR1Memo)
	items := mtAdmissionR1Fixtures(1200)
	var wg sync.WaitGroup
	errors := make(chan string, 16)
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				item := items[(worker*41+i)%len(items)]
				got := memo.metadata(item)
				if !reflect.DeepEqual(got, mtAdmissionR1ReferenceMetadata(item)) {
					errors <- item.TxID
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errors)
	for id := range errors {
		t.Fatalf("concurrent memo corrupted tx=%s", id)
	}
}

var mtAdmissionR1BenchmarkSink int

func BenchmarkMTAdmitR1LegacyRepeated(b *testing.B) {
	items := mtAdmissionR1Fixtures(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		required, producer := mtAdmissionR1ReferenceCandidate(items)
		mtAdmissionR1BenchmarkSink += len(required) + len(producer)
	}
}
func BenchmarkMTAdmitR1MemoRepeated(b *testing.B) {
	items := mtAdmissionR1Fixtures(1000)
	memo := new(mtAdmissionR1Memo)
	mtAdmissionR1MemoCandidate(items, memo)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		required, producer := mtAdmissionR1MemoCandidate(items, memo)
		mtAdmissionR1BenchmarkSink += len(required) + len(producer)
	}
}

func TestMTAdmitR1MethodIsolation(t *testing.T) {
	node := &NodeRuntime{}
	item := mtAdmissionR1Fixtures(1)[0]
	if got := node.mtAdmissionR1Metadata(item); !reflect.DeepEqual(got, mtAdmissionR1ReferenceMetadata(item)) {
		t.Fatal("non-target method changed extraction semantics")
	}
	if len(node.mtAdmissionR1.entries) != 0 {
		t.Fatal("non-target method populated cache")
	}
}
