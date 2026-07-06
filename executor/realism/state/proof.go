package state

import (
	"crypto/sha256"
	"encoding/hex"
)

type Proof struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Root      string `json:"root"`
	LeafHash  string `json:"leaf_hash"`
	ProofType string `json:"proof_type"`
}

func (db *DB) GenerateProof(key string) Proof {
	value := db.Get(key)
	leaf := sha256.Sum256([]byte(key + "=" + value))
	return Proof{Key: key, Value: value, Root: db.Root(), LeafHash: hex.EncodeToString(leaf[:]), ProofType: "deterministic_hash_tree_mvp"}
}

func VerifyProof(key, value string, proof Proof, root string) bool {
	leaf := sha256.Sum256([]byte(key + "=" + value))
	return proof.Key == key && proof.Value == value && proof.Root == root && proof.LeafHash == hex.EncodeToString(leaf[:])
}
