package v5

import "testing"

func TestV17BusinessStateKeepsPartitionIdentityForStatefulOracle(t *testing.T) {
	left := map[string]string{"s0::asset:a": "10"}
	right := map[string]string{"s1::asset:a": "10"}
	if canonicalBusinessStateDigest(left) == canonicalBusinessStateDigest(right) {
		t.Fatal("stateful persistence digest must preserve shard/partition identity")
	}
}

func TestV17CommitmentsExposeSeparateLogicalAndPartitionKeys(t *testing.T) {
	left := canonicalBusinessStateCommitmentsV17(map[string]string{"s0::asset:a": "10"})
	right := canonicalBusinessStateCommitmentsV17(map[string]string{"s1::asset:a": "10"})
	if len(left) != 1 || len(right) != 1 {
		t.Fatalf("unexpected commitments: %#v %#v", left, right)
	}
	if left[0].LogicalKeyDigest != right[0].LogicalKeyDigest {
		t.Fatal("logical key digest must be placement independent")
	}
	if left[0].PartitionKeyDigest == right[0].PartitionKeyDigest {
		t.Fatal("partition key digest must preserve physical shard namespace")
	}
	if left[0].ValueDigest != right[0].ValueDigest {
		t.Fatal("equal values must have equal value commitments")
	}
}

func TestV17BusinessStateSkipsProtocolMetadata(t *testing.T) {
	base := canonicalBusinessStateDigest(map[string]string{"s0::asset:a": "10"})
	withProtocol := canonicalBusinessStateDigest(map[string]string{
		"s0::asset:a": "10",
		"s0::protocol:internal": "noise",
		"s0::relay_commit:proof": "noise",
	})
	if base != withProtocol {
		t.Fatal("protocol-only metadata must stay outside business-state digest")
	}
}
