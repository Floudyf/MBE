package xshard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type Certificate struct {
	TxID              string `json:"tx_id"`
	SourceShard       string `json:"source_shard"`
	TargetShard       string `json:"target_shard"`
	SourceBlockHash   string `json:"source_block_hash"`
	LockEventHash     string `json:"lock_event_hash"`
	StateRoot         string `json:"state_root"`
	CertificateDigest string `json:"certificate_digest"`
	DeterministicMVP  bool   `json:"deterministic_certificate_mvp"`
}

func NewCertificate(txID, sourceShard, targetShard, sourceBlockHash, lockEventHash, stateRoot string) Certificate {
	cert := Certificate{TxID: txID, SourceShard: sourceShard, TargetShard: targetShard, SourceBlockHash: sourceBlockHash, LockEventHash: lockEventHash, StateRoot: stateRoot, DeterministicMVP: true}
	payload, _ := json.Marshal(cert)
	sum := sha256.Sum256(payload)
	cert.CertificateDigest = hex.EncodeToString(sum[:])
	return cert
}
