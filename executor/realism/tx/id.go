package tx

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func CanonicalBytes(t SignedTransaction) ([]byte, error) {
	return json.Marshal(t.core())
}

func ComputeID(t SignedTransaction) (string, error) {
	payload, err := CanonicalBytes(t)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func AssignID(t *SignedTransaction) error {
	id, err := ComputeID(*t)
	if err != nil {
		return err
	}
	t.TxID = id
	return nil
}
