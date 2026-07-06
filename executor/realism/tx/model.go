package tx

import (
	"errors"
	"strings"
)

const (
	ErrInvalidSignature = "invalid_signature"
	ErrEmptySender      = "empty_sender"
	ErrEmptyReceiver    = "empty_receiver"
	ErrInvalidNonce     = "invalid_nonce"
	ErrInvalidValue     = "invalid_value"
	ErrMalformedTx      = "malformed_tx"
	ErrInvalidTxID      = "invalid_tx_id"
)

// SignedTransaction is the V4.0 transaction format admitted by real node mempools.
type SignedTransaction struct {
	TxID          string   `json:"tx_id"`
	Sender        string   `json:"sender"`
	Receiver      string   `json:"receiver"`
	Nonce         uint64   `json:"nonce"`
	Value         int64    `json:"value"`
	StateKeys     []string `json:"state_keys"`
	Payload       string   `json:"payload"`
	Timestamp     int64    `json:"timestamp"`
	Signature     string   `json:"signature"`
	PublicKey     string   `json:"public_key"`
	SourceKind    string   `json:"source_kind,omitempty"`
	TraceSourceID string   `json:"trace_source_id,omitempty"`
}

type coreFields struct {
	Sender        string   `json:"sender"`
	Receiver      string   `json:"receiver"`
	Nonce         uint64   `json:"nonce"`
	Value         int64    `json:"value"`
	StateKeys     []string `json:"state_keys"`
	Payload       string   `json:"payload"`
	Timestamp     int64    `json:"timestamp"`
	PublicKey     string   `json:"public_key"`
	SourceKind    string   `json:"source_kind,omitempty"`
	TraceSourceID string   `json:"trace_source_id,omitempty"`
}

func (t SignedTransaction) core() coreFields {
	return coreFields{
		Sender:        t.Sender,
		Receiver:      t.Receiver,
		Nonce:         t.Nonce,
		Value:         t.Value,
		StateKeys:     append([]string(nil), t.StateKeys...),
		Payload:       t.Payload,
		Timestamp:     t.Timestamp,
		PublicKey:     t.PublicKey,
		SourceKind:    t.SourceKind,
		TraceSourceID: t.TraceSourceID,
	}
}

func (t SignedTransaction) ValidateBasic() error {
	if strings.TrimSpace(t.Sender) == "" {
		return errors.New(ErrEmptySender)
	}
	if strings.TrimSpace(t.Receiver) == "" {
		return errors.New(ErrEmptyReceiver)
	}
	if t.Value <= 0 {
		return errors.New(ErrInvalidValue)
	}
	if len(t.StateKeys) == 0 {
		return errors.New(ErrMalformedTx)
	}
	for _, key := range t.StateKeys {
		if strings.TrimSpace(key) == "" {
			return errors.New(ErrMalformedTx)
		}
	}
	if strings.TrimSpace(t.PublicKey) == "" || strings.TrimSpace(t.Signature) == "" {
		return errors.New(ErrInvalidSignature)
	}
	expected, err := ComputeID(t)
	if err != nil {
		return err
	}
	if t.TxID != expected {
		return errors.New(ErrInvalidTxID)
	}
	return nil
}
