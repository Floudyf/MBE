package tx

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

const ErrSenderPublicKeyMismatch = "sender_public_key_mismatch"

func AddressFromPublicKey(publicKey ed25519.PublicKey) string {
	sum := sha256.Sum256(publicKey)
	return "0x" + hex.EncodeToString(sum[:])[:40]
}

func AddressFromPublicKeyText(publicKeyText string) (string, error) {
	publicKey, err := base64Encoding.DecodeString(strings.TrimSpace(publicKeyText))
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return "", errors.New(ErrInvalidSignature)
	}
	return AddressFromPublicKey(ed25519.PublicKey(publicKey)), nil
}

func IsBoundSender(sender, publicKeyText string) bool {
	address, err := AddressFromPublicKeyText(publicKeyText)
	return err == nil && strings.EqualFold(strings.TrimSpace(sender), address)
}
