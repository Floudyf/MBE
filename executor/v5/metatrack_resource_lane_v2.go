package v5

// MBE_MT_RESOURCE_LANE_V2
// Only the dynamic MetaTrack candidate opts into peer-TCP-write arbitration.
// The original send closure remains the source-of-truth fallback (and is used
// with sendToNodeHook, e.g. by runtime tests). Neither request/ACK lifetimes nor
// state transition ordering, StateReady, PBFT, or other methods are altered.

import (
	"context"
	"metaverse-chainlab/executor/realism/p2p"
)

func (r *NodeRuntime) mtResourceCandidateV2() bool {
	if r == nil || r.pluginSnapshot == nil {
		return false
	}
	selected, ok := r.pluginSnapshot["scheduler"]
	return ok && selected.PluginID == "metatrack_unlock_scheduler"
}

func (r *NodeRuntime) mtResourceSendV2(ctx context.Context, peer, kind string, msg p2p.MessageEnvelope, original func() error) error {
	if !r.mtResourceCandidateV2() || (kind != "critical" && kind != "background") ||
		r.transport == nil || r.sendToNodeHook != nil {
		return original()
	}
	// The p2p implementation admits THIS message at outboundConn.mu rather
	// than holding a new application lock during dialing/retry/ACK waiting.
	return r.transport.SendStateAccessClassV2(ctx, peer, msg, kind)
}
