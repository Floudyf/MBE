package tx

import (
	"fmt"
	"strings"
)

type GenerateOptions struct {
	Count       int
	Sender      string
	Receiver    string
	StartNonce  uint64
	Value       int64
	StateKeys   []string
	Seed        string
	SourceKind  string
	StartTimeMS int64
}

func Generate(opts GenerateOptions) ([]SignedTransaction, string, string, error) {
	if opts.Count < 0 {
		return nil, "", "", fmt.Errorf("%s: negative count", ErrMalformedTx)
	}
	if opts.SourceKind == "" {
		opts.SourceKind = "mbe_client_generate"
	}
	publicKey, privateKey := DeterministicKeyPair(opts.Seed + ":" + opts.Sender)
	sender := opts.Sender
	if sender == "" || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(sender)), "0x") {
		sender = AddressFromPublicKey(publicKey)
	}
	receiver := opts.Receiver
	if receiver == "" {
		receiver = "0x" + strings.Repeat("0", 40)
	}
	publicKeyText := encodeKey(publicKey)
	privateKeyText := encodeKey(privateKey.Seed())
	txs := make([]SignedTransaction, 0, opts.Count)
	for i := 0; i < opts.Count; i++ {
		nonce := opts.StartNonce + uint64(i)
		stateKeys := append([]string(nil), opts.StateKeys...)
		if len(stateKeys) == 0 {
			stateKeys = DefaultStateKeys(sender, receiver)
		}
		item := SignedTransaction{
			Sender:     sender,
			Receiver:   receiver,
			Nonce:      nonce,
			Value:      opts.Value,
			StateKeys:  stateKeys,
			Payload:    fmt.Sprintf("mbe-client:%s:%s:%d", sender, receiver, nonce),
			Timestamp:  opts.StartTimeMS + int64(i),
			SourceKind: opts.SourceKind,
		}
		if err := Sign(&item, privateKey); err != nil {
			return nil, "", "", err
		}
		txs = append(txs, item)
	}
	return txs, publicKeyText, privateKeyText, nil
}

func DefaultStateKeys(sender, receiver string) []string {
	return []string{"acct:" + strings.TrimSpace(sender), "acct:" + strings.TrimSpace(receiver)}
}

func encodeKey(raw []byte) string {
	return base64Encoding.EncodeToString(raw)
}
