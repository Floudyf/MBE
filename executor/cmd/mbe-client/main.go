package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/metrics"
	"metaverse-chainlab/executor/realism/tx"
)

func main() {
	mode := flag.String("mode", "generate", "generate")
	out := flag.String("out", "", "signed transaction JSONL output")
	count := flag.Int("count", 10, "transaction count")
	sender := flag.String("sender", "alice", "sender account")
	receiver := flag.String("receiver", "bob", "receiver account")
	startNonce := flag.Uint64("start-nonce", 0, "start nonce")
	value := flag.Int64("value", 1, "transaction value")
	stateKeys := flag.String("state-keys", "", "pipe-separated state keys")
	seed := flag.String("seed", "1", "deterministic key seed")
	privateKeyOut := flag.String("private-key-out", "", "private key seed output")
	publicKeyOut := flag.String("public-key-out", "", "public key output")
	clientLogOut := flag.String("client-log-out", "", "client_tx_log.csv output")
	flag.Parse()

	if *mode != "generate" {
		fatalf("unsupported mode %q; V4.0 client only supports generate and does not implement RPC submit", *mode)
	}
	if *out == "" {
		fatalf("--out is required")
	}
	keys := splitKeys(*stateKeys)
	txs, publicKey, privateKeySeed, err := tx.Generate(tx.GenerateOptions{
		Count:      *count,
		Sender:     *sender,
		Receiver:   *receiver,
		StartNonce: *startNonce,
		Value:      *value,
		StateKeys:  keys,
		Seed:       *seed,
	})
	if err != nil {
		fatalf("%v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatalf("create output directory: %v", err)
	}
	f, err := os.Create(*out)
	if err != nil {
		fatalf("create output jsonl: %v", err)
	}
	if err := tx.WriteJSONL(f, txs); err != nil {
		f.Close()
		fatalf("%v", err)
	}
	if err := f.Close(); err != nil {
		fatalf("close output jsonl: %v", err)
	}
	if *publicKeyOut != "" {
		if err := os.WriteFile(*publicKeyOut, []byte(publicKey+"\n"), 0o644); err != nil {
			fatalf("write public key: %v", err)
		}
	}
	if *privateKeyOut != "" {
		if err := os.WriteFile(*privateKeyOut, []byte(privateKeySeed+"\n"), 0o600); err != nil {
			fatalf("write private key seed: %v", err)
		}
	}
	if *clientLogOut == "" {
		*clientLogOut = filepath.Join(filepath.Dir(*out), "client_tx_log.csv")
	}
	if err := writeClientLog(*clientLogOut, txs); err != nil {
		fatalf("%v", err)
	}
	fmt.Printf("generated %d signed transactions: %s\n", len(txs), *out)
}

func writeClientLog(path string, txs []tx.SignedTransaction) error {
	rows := make([][]string, 0, len(txs))
	for _, item := range txs {
		rows = append(rows, []string{
			strconv.FormatInt(item.Timestamp, 10),
			item.TxID,
			item.Sender,
			item.Receiver,
			strconv.FormatUint(item.Nonce, 10),
			strconv.FormatInt(item.Value, 10),
			item.SourceKind,
			"false",
			"false",
		})
	}
	return metrics.WriteCSV(path, []string{"timestamp", "tx_id", "sender", "receiver", "nonce", "value", "source_kind", "rpc_submit", "real_p2p"}, rows)
}

func splitKeys(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, "|")
	out := []string{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
