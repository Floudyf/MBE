package tx

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

func WriteJSONL(w io.Writer, txs []SignedTransaction) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, item := range txs {
		if err := enc.Encode(item); err != nil {
			return fmt.Errorf("write signed tx jsonl: %w", err)
		}
	}
	return bw.Flush()
}

func ReadJSONL(r io.Reader, handle func(SignedTransaction) error) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var item SignedTransaction
		if err := json.Unmarshal(scanner.Bytes(), &item); err != nil {
			return fmt.Errorf("malformed_tx line %d: %w", line, err)
		}
		if err := handle(item); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read signed tx jsonl: %w", err)
	}
	return nil
}
