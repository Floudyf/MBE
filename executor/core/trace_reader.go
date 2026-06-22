package core

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
)

// StreamTransactions decompresses and decodes one JSONL record at a time.
func StreamTransactions(path string) (<-chan Transaction, <-chan error) {
	txs := make(chan Transaction)
	errs := make(chan error, 1)
	go func() {
		defer close(txs)
		defer close(errs)
		file, err := os.Open(path)
		if err != nil {
			errs <- err
			return
		}
		defer file.Close()
		gz, err := gzip.NewReader(file)
		if err != nil {
			errs <- err
			return
		}
		defer gz.Close()
		scanner := bufio.NewScanner(gz)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
				errs <- fmt.Errorf("trace line %d: invalid JSON: %w", line, err)
				return
			}
			for _, field := range []string{"tx_id", "tx_type", "timestamp", "chain_id", "contract", "function", "args", "read_set", "write_set", "access_list", "commutative", "update_type", "status", "chain_latency_ms"} {
				value, ok := raw[field]
				if !ok || string(value) == "null" {
					errs <- fmt.Errorf("trace line %d: missing required field %q", line, field)
					return
				}
			}
			var tx Transaction
			if err := json.Unmarshal(scanner.Bytes(), &tx); err != nil {
				errs <- fmt.Errorf("trace line %d: invalid transaction: %w", line, err)
				return
			}
			txs <- tx
		}
		if err := scanner.Err(); err != nil {
			errs <- fmt.Errorf("read trace: %w", err)
		}
	}()
	return txs, errs
}
