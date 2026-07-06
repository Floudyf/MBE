package account

type Account struct {
	Sender    string `json:"sender"`
	NextNonce uint64 `json:"next_nonce"`
}
