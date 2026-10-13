package p2p

// MBE_MT_RESOURCE_LANE_V2
// Optional state-access write-admission policy. A Transport without a tagged
// SendStateAccessClassV2 is byte-for-byte equivalent in service order to the
// original SendStateAccess transport. The actual Encode continues to use the
// existing outboundConn.mu, so the policy does not reduce in-flight RPC count.

import (
	"context"
	"sync"
)

const stateAccessMaxCriticalBurstV2 = 3 // bounded liveness guarantee

type stateAccessClassKeyV2 struct{}

type stateAccessWaiterV2 struct {
	notify  chan struct{}
	granted bool // protected by stateAccessWriteAdmissionV2.mu
}

type stateAccessWriteAdmissionV2 struct {
	mu         sync.Mutex
	occupied   bool
	critical   []*stateAccessWaiterV2
	background []*stateAccessWaiterV2
	streak     int
}

func (g *stateAccessWriteAdmissionV2) enter(ctx context.Context, high bool) (func(), bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	g.mu.Lock()
	if !g.occupied && len(g.critical) == 0 && len(g.background) == 0 {
		g.occupied = true
		if high {
			g.streak++
		} else {
			g.streak = 0
		}
		g.mu.Unlock()
		return g.leave, false, nil
	}
	item := &stateAccessWaiterV2{notify: make(chan struct{})}
	if high {
		g.critical = append(g.critical, item)
	} else {
		g.background = append(g.background, item)
	}
	g.mu.Unlock()
	select {
	case <-item.notify:
		if err := ctx.Err(); err != nil {
			g.leave()
			return nil, true, err
		}
		return g.leave, true, nil
	case <-ctx.Done():
		g.mu.Lock()
		selected := item.granted
		if !selected {
			queue := &g.background
			if high {
				queue = &g.critical
			}
			for index, v := range *queue {
				if v == item {
					*queue = append((*queue)[:index], (*queue)[index+1:]...)
					break
				}
			}
		}
		g.mu.Unlock()
		if selected {
			g.leave()
		}
		return nil, true, ctx.Err()
	}
}

func (g *stateAccessWriteAdmissionV2) leave() {
	g.mu.Lock()
	var next *stateAccessWaiterV2
	if len(g.critical) > 0 && (len(g.background) == 0 || g.streak < stateAccessMaxCriticalBurstV2) {
		next = g.critical[0]
		g.critical[0] = nil
		g.critical = g.critical[1:]
		g.streak++
	} else if len(g.background) > 0 {
		next = g.background[0]
		g.background[0] = nil
		g.background = g.background[1:]
		g.streak = 0
	}
	if next == nil {
		g.occupied = false
	} else {
		next.granted = true
		close(next.notify)
	}
	g.mu.Unlock()
}

func (t *Transport) SendStateAccessClassV2(ctx context.Context, peerID string, msg MessageEnvelope, class string) error {
	if class != "critical" && class != "background" {
		return t.SendStateAccess(ctx, peerID, msg)
	}
	return t.send(context.WithValue(ctx, stateAccessClassKeyV2{}, class), peerID, msg, t.stateAccessOutbound, "state_access_send")
}
