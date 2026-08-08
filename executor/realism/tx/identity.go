package tx

import "strings"

// SemanticID returns the stable workload-level identity of a transaction.
// LogicalTxID is assigned before method-specific execution routing is signed,
// so the same logical operation retains one semantic identity even when its
// physical TxID differs across execution methods. Legacy transactions fall
// back to TraceSourceID and then the signed physical TxID.
func SemanticID(item SignedTransaction) string {
	if value := strings.TrimSpace(item.LogicalTxID); value != "" {
		return value
	}
	if value := strings.TrimSpace(item.TraceSourceID); value != "" {
		return value
	}
	return item.TxID
}
