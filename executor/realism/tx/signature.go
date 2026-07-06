package tx

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

func DeterministicKeyPair(seed string) (ed25519.PublicKey, ed25519.PrivateKey) {
	sum := sha256.Sum256([]byte(seed))
	privateKey := ed25519.NewKeyFromSeed(sum[:])
	return privateKey.Public().(ed25519.PublicKey), privateKey
}

func Sign(t *SignedTransaction, privateKey ed25519.PrivateKey) error {
	if len(privateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("%s: bad private key size", ErrInvalidSignature)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	t.PublicKey = base64.StdEncoding.EncodeToString(publicKey)
	if err := AssignID(t); err != nil {
		return err
	}
	message, err := CanonicalBytes(*t)
	if err != nil {
		return err
	}
	t.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, message))
	return nil
}

func Verify(t SignedTransaction) error {
	if err := t.ValidateBasic(); err != nil {
		return err
	}
	publicKey, err := base64.StdEncoding.DecodeString(t.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return errors.New(ErrInvalidSignature)
	}
	signature, err := base64.StdEncoding.DecodeString(t.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New(ErrInvalidSignature)
	}
	message, err := CanonicalBytes(t)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), message, signature) {
		return errors.New(ErrInvalidSignature)
	}
	return nil
}
