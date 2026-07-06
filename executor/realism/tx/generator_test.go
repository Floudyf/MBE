package tx

import "testing"

func TestGeneratorProducesAdmissibleTxs(t *testing.T) {
	txs, pub, priv, err := Generate(GenerateOptions{
		Count:      3,
		Sender:     "alice",
		Receiver:   "bob",
		StartNonce: 0,
		Value:      1,
		Seed:       "42",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 3 || pub == "" || priv == "" {
		t.Fatalf("unexpected generator output")
	}
	for _, item := range txs {
		if err := Verify(item); err != nil {
			t.Fatalf("generated tx did not verify: %v", err)
		}
	}
}
