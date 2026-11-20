package v5

import (
	"fmt"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

// mtWaveViewV1 constructs a per-Wave read projection only for the built-in
// Block-STM delta-only execution path. It does not affect PBFT, Ready, the
// full/correctness Block-STM path, exact-version publication, or the final
// consensus-order state-root materialization.
//
// The only source of transactional reads in delta-only Block-STM is
// SerialExecutor.executeTxWithoutStateRoot: declared accesses, account
// balance/nonces in transfer mode, and relay-commit semantics. Include all
// of those and state keys as a conservative superset. A projection is never
// used outside performance + oracle-off.
func mtWaveViewV1(working map[string]string, wave []tx.SignedTransaction, shardID string, plugin BlockExecutorPlugin) map[string]string {
	enabled := false
	switch p := plugin.(type) {
	case blockSTMBlockExecutor:
		enabled = fmt.Sprint(p.config["execution_mode"]) == "performance" && fmt.Sprint(p.config["oracle_mode"]) == "off"
	case *blockSTMBlockExecutor:
		enabled = p != nil && fmt.Sprint(p.config["execution_mode"]) == "performance" && fmt.Sprint(p.config["oracle_mode"]) == "off"
	}
	if !enabled || len(wave) == 0 {
		return copyStringMap(working)
	}

	required := make(map[string]bool, len(wave)*12)
	add := func(key string) {
		if key != "" {
			required[key] = true
		}
	}
	for _, item := range wave {
		for _, access := range item.AccessList {
			switch access.Mode {
			case tx.AccessRead, tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
				add(access.Key)
			default:
				// Future or unknown access semantics: preserve the original full view.
				return copyStringMap(working)
			}
		}
		for _, key := range item.StateKeys {
			add(key)
		}
		// Implicit reads in the legacy transfer fallback, regardless of which
		// signed access-list schema the transaction currently uses.
		add("balance:" + item.Sender)
		add("nonce:" + item.Sender)
		add("balance:" + item.Receiver)
		add("nonce:" + item.Receiver)
		add("relay_commit:" + item.TxID)
	}
	if len(required) >= len(working) {
		return copyStringMap(working)
	}
	view := make(map[string]string, len(required)*2)
	for key := range required {
		qualified := qualifyStateKey(shardID, key)
		vq, hasQualified := working[qualified]
		vr, hasRaw := working[key]
		// logicalSnapshot iterates maps in nondeterministic order when a
		// qualified/unqualified key alias disagrees; never change that case.
		if qualified != key && hasQualified && hasRaw && vq != vr {
			return copyStringMap(working)
		}
		if hasQualified {
			view[qualified] = vq
		}
		if hasRaw {
			view[key] = vr
		}
		// Historical, already qualified state keys are preserved verbatim.
		if strings.Contains(key, "::") {
			if val, ok := working[key]; ok {
				view[key] = val
			}
		}
	}
	return view
}
