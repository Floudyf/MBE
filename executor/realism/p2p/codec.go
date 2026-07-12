package p2p

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

func Encode(w io.Writer, msg MessageEnvelope) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode p2p message: %w", err)
	}
	if _, err := w.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write p2p message: %w", err)
	}
	return nil
}

func Decode(r io.Reader) (MessageEnvelope, int, error) {
	return DecodeReader(bufio.NewReader(r))
}

func DecodeReader(reader *bufio.Reader) (MessageEnvelope, int, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return MessageEnvelope{}, len(line), fmt.Errorf("read p2p message: %w", err)
	}
	var msg MessageEnvelope
	if err := json.Unmarshal(line, &msg); err != nil {
		return MessageEnvelope{}, len(line), fmt.Errorf("decode p2p message: %w", err)
	}
	return msg, len(line), nil
}
