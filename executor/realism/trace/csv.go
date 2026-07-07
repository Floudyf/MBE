package trace

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

func readCSV(opts ImportOptions) ([]tx.SignedTransaction, [][]string, int, error) {
	f, err := os.Open(opts.InputCSV)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("open csv trace: %w", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read csv trace: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil, 0, nil
	}
	header := map[string]int{}
	for i, name := range rows[0] {
		header[strings.ToLower(strings.TrimSpace(name))] = i
	}
	nonces := map[string]uint64{}
	out := []tx.SignedTransaction{}
	logRows := [][]string{}
	rejected := 0
	for i, row := range rows[1:] {
		rowNo := i + 2
		sender := first(row, header, "sender", "from")
		receiver := first(row, header, "receiver", "to")
		if sender == "" || receiver == "" {
			rejected++
			logRows = append(logRows, []string{strconv.Itoa(rowNo), "false", tx.ErrMalformedTx, "", sender, receiver, "0"})
			continue
		}
		nonce, ok := parseUint(first(row, header, "nonce"))
		if !ok {
			nonce = nonces[sender]
		}
		nonces[sender] = nonce + 1
		value, ok := parseInt(first(row, header, "value"))
		if !ok || value <= 0 {
			value = 1
		}
		timestamp, _ := parseInt(first(row, header, "timestamp"))
		stateKeys := parseStateKeys(first(row, header, "state_keys"))
		if len(stateKeys) == 0 {
			stateKeys = tx.DefaultStateKeys(sender, receiver)
		}
		generated, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: sender, Receiver: receiver, StartNonce: nonce, Value: value, StateKeys: stateKeys, Seed: opts.Seed, SourceKind: "real_trace_to_signed_tx", StartTimeMS: timestamp})
		if err != nil {
			rejected++
			logRows = append(logRows, []string{strconv.Itoa(rowNo), "false", err.Error(), "", sender, receiver, strconv.FormatUint(nonce, 10)})
			continue
		}
		item := generated[0]
		item.TraceSourceID = fmt.Sprintf("%s:%d", opts.SourceFormat, rowNo)
		if payload := first(row, header, "payload"); payload != "" {
			item.Payload = payload
		}
		_, privateKey := tx.DeterministicKeyPair(opts.Seed + ":" + sender)
		if err := tx.Sign(&item, privateKey); err != nil {
			rejected++
			logRows = append(logRows, []string{strconv.Itoa(rowNo), "false", err.Error(), "", sender, receiver, strconv.FormatUint(nonce, 10)})
			continue
		}
		out = append(out, item)
		logRows = append(logRows, []string{strconv.Itoa(rowNo), "true", "", item.TxID, item.Sender, item.Receiver, strconv.FormatUint(nonce, 10)})
	}
	return out, logRows, rejected, nil
}

func first(row []string, header map[string]int, names ...string) string {
	for _, name := range names {
		if idx, ok := header[name]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
	}
	return ""
}

func parseUint(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	out, err := strconv.ParseUint(value, 10, 64)
	return out, err == nil
}

func parseInt(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	out, err := strconv.ParseInt(value, 10, 64)
	return out, err == nil
}

func parseStateKeys(value string) []string {
	if value == "" {
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
