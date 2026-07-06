package account

import "fmt"

const (
	ReasonStaleNonce              = "stale_nonce"
	ReasonFutureNonceNotSupported = "future_nonce_not_supported"
)

type NonceManager struct {
	next map[string]uint64
}

func NewNonceManager() *NonceManager {
	return &NonceManager{next: map[string]uint64{}}
}

func (m *NonceManager) Expected(sender string) uint64 {
	return m.next[sender]
}

func (m *NonceManager) Validate(sender string, nonce uint64) error {
	expected := m.Expected(sender)
	if nonce < expected {
		return fmt.Errorf("%s: expected %d got %d", ReasonStaleNonce, expected, nonce)
	}
	if nonce > expected {
		return fmt.Errorf("%s: expected %d got %d", ReasonFutureNonceNotSupported, expected, nonce)
	}
	return nil
}

func (m *NonceManager) Accept(sender string, nonce uint64) error {
	if err := m.Validate(sender, nonce); err != nil {
		return err
	}
	m.next[sender] = nonce + 1
	return nil
}

func (m *NonceManager) Snapshot() map[string]uint64 {
	out := make(map[string]uint64, len(m.next))
	for k, v := range m.next {
		out[k] = v
	}
	return out
}
