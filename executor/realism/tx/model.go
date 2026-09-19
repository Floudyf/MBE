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
	TxID                   string                    `json:"tx_id"`
	LogicalTxID            string                    `json:"logical_tx_id,omitempty"`
	Sender                 string                    `json:"sender"`
	Receiver               string                    `json:"receiver"`
	Nonce                  uint64                    `json:"nonce"`
	Value                  int64                     `json:"value"`
	StateKeys              []string                  `json:"state_keys"`
	AccessList             []AccessItem              `json:"access_list,omitempty"`
	AccessListDigest       string                    `json:"access_list_digest,omitempty"`
	AccessListSchema       string                    `json:"access_list_schema,omitempty"`
	AccessListSource       string                    `json:"access_list_source,omitempty"`
	SchedulingAccessList   []AccessItem              `json:"scheduling_access_list,omitempty"`
	SchedulingAccessDigest string                    `json:"scheduling_access_digest,omitempty"`
	SchedulingAccessSchema string                    `json:"scheduling_access_schema,omitempty"`
	SchedulingAccessSource string                    `json:"scheduling_access_source,omitempty"`
	Payload                string                    `json:"payload"`
	Timestamp              int64                     `json:"timestamp"`
	Signature              string                    `json:"signature"`
	PublicKey              string                    `json:"public_key"`
	SourceKind             string                    `json:"source_kind,omitempty"`
	TraceSourceID          string                    `json:"trace_source_id,omitempty"`
	ExecutionRouting       *ExecutionRoutingMetadata `json:"execution_routing,omitempty"`
}

type AccessMode string

const (
	AccessRead             AccessMode = "read"
	AccessWrite            AccessMode = "write"
	AccessReadWrite        AccessMode = "read_write"
	AccessCommutativeDelta AccessMode = "commutative_delta"
	AccessUnknown          AccessMode = "unknown"
)

type AccessItem struct {
	Key             string     `json:"key"`
	Mode            AccessMode `json:"mode"`
	UpdateSemantics string     `json:"update_semantics"`
	Delta           int64      `json:"delta,omitempty"`
}

type coreFields struct {
	LogicalTxID            string                    `json:"logical_tx_id,omitempty"`
	Sender                 string                    `json:"sender"`
	Receiver               string                    `json:"receiver"`
	Nonce                  uint64                    `json:"nonce"`
	Value                  int64                     `json:"value"`
	StateKeys              []string                  `json:"state_keys"`
	AccessList             []AccessItem              `json:"access_list,omitempty"`
	AccessListDigest       string                    `json:"access_list_digest,omitempty"`
	AccessListSchema       string                    `json:"access_list_schema,omitempty"`
	AccessListSource       string                    `json:"access_list_source,omitempty"`
	SchedulingAccessList   []AccessItem              `json:"scheduling_access_list,omitempty"`
	SchedulingAccessDigest string                    `json:"scheduling_access_digest,omitempty"`
	SchedulingAccessSchema string                    `json:"scheduling_access_schema,omitempty"`
	SchedulingAccessSource string                    `json:"scheduling_access_source,omitempty"`
	Payload                string                    `json:"payload"`
	Timestamp              int64                     `json:"timestamp"`
	PublicKey              string                    `json:"public_key"`
	SourceKind             string                    `json:"source_kind,omitempty"`
	TraceSourceID          string                    `json:"trace_source_id,omitempty"`
	ExecutionRouting       *ExecutionRoutingMetadata `json:"execution_routing,omitempty"`
}

func (t SignedTransaction) core() coreFields {
	return coreFields{
		LogicalTxID:            t.LogicalTxID,
		Sender:                 t.Sender,
		Receiver:               t.Receiver,
		Nonce:                  t.Nonce,
		Value:                  t.Value,
		StateKeys:              append([]string(nil), t.StateKeys...),
		AccessList:             append([]AccessItem(nil), t.AccessList...),
		AccessListDigest:       t.AccessListDigest,
		AccessListSchema:       t.AccessListSchema,
		AccessListSource:       t.AccessListSource,
		SchedulingAccessList:   append([]AccessItem(nil), t.SchedulingAccessList...),
		SchedulingAccessDigest: t.SchedulingAccessDigest,
		SchedulingAccessSchema: t.SchedulingAccessSchema,
		SchedulingAccessSource: t.SchedulingAccessSource,
		Payload:                t.Payload,
		Timestamp:              t.Timestamp,
		PublicKey:              t.PublicKey,
		SourceKind:             t.SourceKind,
		TraceSourceID:          t.TraceSourceID,
		ExecutionRouting:       cloneExecutionRouting(t.ExecutionRouting),
	}
}

func cloneExecutionRouting(input *ExecutionRoutingMetadata) *ExecutionRoutingMetadata {
	if input == nil {
		return nil
	}
	copyValue := *input
	copyValue.StateVersions = append([]StateVersionDependency(nil), input.StateVersions...)
	copyValue.LogicalDomains = append([]string(nil), input.LogicalDomains...)
	return &copyValue
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
	for _, item := range t.AccessList {
		if strings.TrimSpace(item.Key) == "" {
			return errors.New(ErrMalformedTx)
		}
		switch item.Mode {
		case AccessRead, AccessWrite, AccessReadWrite, AccessCommutativeDelta:
		default:
			return errors.New(ErrMalformedTx)
		}
	}
	for _, item := range t.SchedulingAccessList {
		if strings.TrimSpace(item.Key) == "" || strings.TrimSpace(item.UpdateSemantics) == "" {
			return errors.New(ErrMalformedTx)
		}
		switch item.Mode {
		case AccessRead, AccessWrite, AccessReadWrite, AccessCommutativeDelta, AccessUnknown:
		default:
			return errors.New(ErrMalformedTx)
		}
	}
	if err := ValidateExecutionRouting(t); err != nil {
		return err
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
