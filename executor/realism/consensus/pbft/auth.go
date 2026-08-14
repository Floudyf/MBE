package pbft

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const AuthenticationDomain = "mbe-pbft-ed25519-v1"

func EncodePublicKey(key ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key)
}

func EncodePrivateKey(key ed25519.PrivateKey) string {
	return base64.StdEncoding.EncodeToString(key)
}

func DecodePublicKey(encoded string) (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid pbft public key")
	}
	return ed25519.PublicKey(append([]byte(nil), decoded...)), nil
}

func DecodePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid pbft private key")
	}
	return ed25519.PrivateKey(append([]byte(nil), decoded...)), nil
}

type prePrepareAuthenticationPayload struct {
	View      uint64 `json:"view"`
	Sequence  uint64 `json:"sequence"`
	Height    uint64 `json:"height"`
	LeaderID  string `json:"leader_id"`
	BlockHash string `json:"block_hash"`
}

func prePrepareAuthenticationCore(msg PrePrepare) prePrepareAuthenticationPayload {
	return prePrepareAuthenticationPayload{
		View: msg.View, Sequence: msg.Sequence, Height: msg.Height,
		LeaderID: msg.LeaderID, BlockHash: msg.BlockHash,
	}
}

func SignPrePrepare(msg *PrePrepare, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft pre-prepare is nil")
	}
	signature, err := signAuthenticatedPayload("pre_prepare", prePrepareAuthenticationCore(*msg), privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyPrePrepare(msg PrePrepare, publicKey ed25519.PublicKey) error {
	return verifyAuthenticatedPayload("pre_prepare", prePrepareAuthenticationCore(msg), msg.Signature, publicKey)
}

func SignPrepare(msg *Prepare, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft prepare is nil")
	}
	clone := *msg
	clone.Signature = ""
	signature, err := signAuthenticatedPayload("prepare", clone, privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyPrepare(msg Prepare, publicKey ed25519.PublicKey) error {
	clone := msg
	clone.Signature = ""
	return verifyAuthenticatedPayload("prepare", clone, msg.Signature, publicKey)
}

func SignCommit(msg *Commit, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft commit is nil")
	}
	clone := *msg
	clone.Signature = ""
	signature, err := signAuthenticatedPayload("commit", clone, privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyCommit(msg Commit, publicKey ed25519.PublicKey) error {
	clone := msg
	clone.Signature = ""
	return verifyAuthenticatedPayload("commit", clone, msg.Signature, publicKey)
}

func SignCheckpoint(msg *Checkpoint, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft checkpoint is nil")
	}
	clone := *msg
	clone.Signature = ""
	signature, err := signAuthenticatedPayload("checkpoint", clone, privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyCheckpoint(msg Checkpoint, publicKey ed25519.PublicKey) error {
	clone := msg
	clone.Signature = ""
	return verifyAuthenticatedPayload("checkpoint", clone, msg.Signature, publicKey)
}

func SignViewChange(msg *ViewChange, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft view-change is nil")
	}
	clone := *msg
	clone.Signature = ""
	signature, err := signAuthenticatedPayload("view_change", clone, privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyViewChange(msg ViewChange, publicKey ed25519.PublicKey) error {
	clone := msg
	clone.Signature = ""
	return verifyAuthenticatedPayload("view_change", clone, msg.Signature, publicKey)
}

func SignNewView(msg *NewView, privateKey ed25519.PrivateKey) error {
	if msg == nil {
		return fmt.Errorf("pbft new-view is nil")
	}
	clone := *msg
	clone.Signature = ""
	signature, err := signAuthenticatedPayload("new_view", clone, privateKey)
	if err != nil {
		return err
	}
	msg.Signature = signature
	return nil
}

func VerifyNewView(msg NewView, publicKey ed25519.PublicKey) error {
	clone := msg
	clone.Signature = ""
	return verifyAuthenticatedPayload("new_view", clone, msg.Signature, publicKey)
}

func signAuthenticatedPayload(kind string, payload any, privateKey ed25519.PrivateKey) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("invalid pbft private key size")
	}
	message, err := authenticatedPayloadBytes(kind, payload)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, message)), nil
}

func verifyAuthenticatedPayload(kind string, payload any, encodedSignature string, publicKey ed25519.PublicKey) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid pbft public key size")
	}
	if encodedSignature == "" {
		return fmt.Errorf("missing pbft %s signature", kind)
	}
	signature, err := base64.StdEncoding.DecodeString(encodedSignature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("invalid pbft %s signature encoding", kind)
	}
	message, err := authenticatedPayloadBytes(kind, payload)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, message, signature) {
		return fmt.Errorf("invalid pbft %s signature", kind)
	}
	return nil
}

func authenticatedPayloadBytes(kind string, payload any) ([]byte, error) {
	payloadRaw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal pbft %s payload: %w", kind, err)
	}
	envelope := struct {
		Domain  string          `json:"domain"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{
		Domain:  AuthenticationDomain,
		Kind:    kind,
		Payload: payloadRaw,
	}
	message, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal pbft %s authenticated envelope: %w", kind, err)
	}
	return message, nil
}
