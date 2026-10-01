package v5

import (
	"sort"
	"strconv"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

// porygonStateOwnerIdentity maps a logical state key to the account/object
// identity that owns that state in Porygon's storage sharding model.  The MBE
// canonical workloads use hierarchical state keys (for example
// contract/table/account or contract/table/asset-id).  Hashing the entire field
// key would scatter fields of one account/object across unrelated shards and
// artificially force almost every wide transaction to be cross-shard.
//
// The last non-empty path component is the stable object/account identity for
// the canonical schemas used by MBE.  ':' is included for EVM/account-style
// keys such as balance:<address> and nonce:<address>.  This function depends
// only on the signed access key and therefore introduces no future-information
// or MetaTrack placement signal.
func porygonStateOwnerIdentity(key string) string {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if normalized == "" {
		return ""
	}
	parts := strings.FieldsFunc(normalized, func(r rune) bool {
		switch r {
		case '/', ':', '|', '#':
			return true
		default:
			return false
		}
	})
	for index := len(parts) - 1; index >= 0; index-- {
		if value := strings.TrimSpace(parts[index]); value != "" {
			return value
		}
	}
	return normalized
}

func porygonStateShard(key string, count int) int {
	if count < 1 {
		return 0
	}
	owner := porygonStateOwnerIdentity(key)
	if owner == "" {
		owner = strings.ToLower(strings.TrimSpace(key))
	}
	return stableKey([]string{owner}) % count
}

// porygonAccountShard chooses the Single-Shard Execution destination from the
// initiating account/object locality visible in the signed AccessList.  When a
// canonical account address is present directly in the keys (e.g. balance and
// nonce workloads), it wins.  For schema-projected workloads where the runtime
// sender is a deterministic MBE address but access keys retain source account
// names, the most frequently represented owner is the deterministic initiating
// account proxy.  Ties are stable and lexical.
func porygonAccountShard(item tx.SignedTransaction, count int) int {
	if count < 1 {
		return 0
	}
	// Canonical routing_source_key is resolved before submission. Porygon signs
	// only that route result with a Porygon-specific reason so Single-Shard
	// Execution follows the initiating account/object without AccessList guessing.
	if routing := item.ExecutionRouting; routing != nil && routing.RoutingReason == "porygon_initiating_account_route" {
		value := strings.TrimSpace(routing.ExecutionShard)
		if strings.HasPrefix(value, "s") {
			if index, err := strconv.Atoi(strings.TrimPrefix(value, "s")); err == nil && index >= 0 && index < count {
				return index
			}
		}
	}
	sender := strings.ToLower(strings.TrimSpace(item.Sender))
	ownerCounts := map[string]int{}
	for _, access := range porygonCanonicalAccesses(item) {
		owner := porygonStateOwnerIdentity(access.Key)
		if owner == "" {
			continue
		}
		// Canonical legacy/dataset adapters explicitly tag the routing source
		// state when that role is available in the signed AccessList. Prefer
		// that initiating-account/object evidence over any frequency fallback.
		semantic := strings.ToLower(strings.TrimSpace(access.UpdateSemantics))
		if semantic == "routing_source_state" || strings.Contains(semantic, "routing_source") {
			return stableKey([]string{owner}) % count
		}
		ownerCounts[owner]++
	}
	if sender != "" && ownerCounts[sender] > 0 {
		return stableKey([]string{sender}) % count
	}
	if len(ownerCounts) == 0 {
		identity := sender
		if identity == "" {
			identity = strings.ToLower(strings.TrimSpace(item.LogicalTxID))
		}
		if identity == "" {
			identity = strings.ToLower(strings.TrimSpace(item.TxID))
		}
		return stableKey([]string{identity}) % count
	}
	owners := make([]string, 0, len(ownerCounts))
	for owner := range ownerCounts {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	selected := owners[0]
	selectedCount := ownerCounts[selected]
	for _, owner := range owners[1:] {
		if ownerCounts[owner] > selectedCount {
			selected = owner
			selectedCount = ownerCounts[owner]
		}
	}
	return stableKey([]string{selected}) % count
}
