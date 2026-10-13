package v5

import (
	"fmt"
	"strconv"

	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// Porygon-only typed updates: Storage Role root voting, canonical snapshots,
// proposal-carried U and the durable WAL must apply identical state semantics.
// Normal set and rollback updates remain absolute; a signed commutative access
// is an increment against the certified predecessor state, not a last-writer set.
const porygonDeltaSemanticV1 = "commutative_delta"
const porygonInvalidSemanticV1 = "porygon_invalid_mixed_semantics"

func porygonCheckedAddV1(a, b int64) (int64, error) {
	const max = int64(^uint64(0) >> 1)
	const min = -max - 1
	if (b > 0 && a > max-b) || (b < 0 && a < min-b) {
		return 0, fmt.Errorf("Porygon signed counter overflow: current=%d delta=%d", a, b)
	}
	return a + b, nil
}

func porygonTypedUpdateV1(item PorygonStateUpdate, accesses []tx.AccessItem) PorygonStateUpdate {
	var seenDelta, seenOther bool
	var delta int64
	for _, access := range accesses {
		if access.Key != item.Key || !isWriteMode(access.Mode) {
			continue
		}
		if access.Mode != tx.AccessCommutativeDelta {
			seenOther = true
			continue
		}
		seenDelta = true
		next, err := porygonCheckedAddV1(delta, access.Delta)
		if err != nil {
			item.UpdateSemantics = porygonInvalidSemanticV1
			return item
		}
		delta = next
	}
	if seenDelta && seenOther {
		item.UpdateSemantics = porygonInvalidSemanticV1
	} else if seenDelta {
		item.UpdateSemantics = porygonDeltaSemanticV1
		item.Delta = delta
	}
	return item
}

// Apply one already-certified update to a detached map. No shared state-store
// behavior changes: this is Porygon's validation/reference transition.
func porygonApplyCanonicalKVV1(values map[string]string, item state.StateKV) error {
	if item.OrderingNoop {
		return nil
	}
	switch item.UpdateSemantics {
	case "", "set":
		values[item.Key] = item.Value
		return nil
	case porygonDeltaSemanticV1:
		current := int64(0)
		if old, ok := values[item.Key]; ok && old != "" {
			parsed, err := strconv.ParseInt(old, 10, 64)
			if err != nil {
				return fmt.Errorf("Porygon delta base is not integer for %s: %w", item.Key, err)
			}
			current = parsed
		} else if item.HasInitialValue {
			current = item.InitialValue
		}
		next, err := porygonCheckedAddV1(current, item.Delta)
		if err != nil {
			return fmt.Errorf("Porygon delta %s: %w", item.Key, err)
		}
		values[item.Key] = strconv.FormatInt(next, 10)
		return nil
	default:
		return fmt.Errorf("Porygon unknown update semantics %q for %s", item.UpdateSemantics, item.Key)
	}
}

func porygonStateUpdateKVV1(item PorygonStateUpdate, partitionID string) state.StateKV {
	return state.StateKV{
		Key:   qualifyStateKey(partitionID, item.Key),
		Value: item.Value, UpdateSemantics: item.UpdateSemantics, Delta: item.Delta,
	}
}

// The common annotator must not reinterpret an already certified Porygon
// absolute update as an increment. It may still attach TxIDs for attribution.
func porygonPreserveCertifiedDeltaV1(annotated, certified []state.StateKV) ([]state.StateKV, error) {
	original := make(map[string]state.StateKV, len(certified))
	for _, item := range certified {
		if _, exists := original[item.Key]; exists {
			return nil, fmt.Errorf("Porygon repeated certified materialization key %s", item.Key)
		}
		original[item.Key] = item
	}
	out := make([]state.StateKV, len(annotated))
	for i, item := range annotated {
		reference, found := original[item.Key]
		if !found {
			return nil, fmt.Errorf("Porygon annotated key not in certified set: %s", item.Key)
		}
		item.Value = reference.Value
		item.UpdateSemantics = reference.UpdateSemantics
		item.Delta = reference.Delta
		item.HasInitialValue = reference.HasInitialValue
		item.InitialValue = reference.InitialValue
		item.OrderingNoop = reference.OrderingNoop
		out[i] = item
	}
	if len(out) != len(certified) {
		return nil, fmt.Errorf("Porygon certified delta cardinality mismatch")
	}
	return out, nil
}
