package v5

// MBE_MT_HOME_ACK_V2
// A Home-shard inbound TCP reader must never wait synchronously for a
// state-access ACK write on a channel that may carry more incoming requests.
// Each requester has a FIFO sender lane with bounded pending work. The ACK
// still reflects Home's existing per-item validation / pending-delta enqueue;
// durable state application remains consensus-bound and unchanged.

import (
	"context"
	"fmt"
	"sync"

	"metaverse-chainlab/executor/realism/p2p"
)

const metaTrackHomeAckQueueLimitV2 = 4096

type metaTrackHomeAckTaskV2 struct {
	ctx      context.Context
	peer     string
	envelope p2p.MessageEnvelope
}

type metaTrackHomeAckLaneV2 struct {
	jobs     []metaTrackHomeAckTaskV2
	draining bool
}

type metaTrackHomeAckDispatcherV2 struct {
	mu    sync.Mutex
	lanes map[string]*metaTrackHomeAckLaneV2
	send  func(context.Context, string, p2p.MessageEnvelope) error
	sent  func(error)
}

func newMetaTrackHomeAckDispatcherV2(
	send func(context.Context, string, p2p.MessageEnvelope) error,
	sent func(error),
) *metaTrackHomeAckDispatcherV2 {
	return &metaTrackHomeAckDispatcherV2{
		lanes: make(map[string]*metaTrackHomeAckLaneV2),
		send:  send,
		sent:  sent,
	}
}

// enqueue returns as soon as the immutable ACK envelope has been queued.
// The receiver remains free to read other requests / incoming ACKs.
// Overflow fails visibly; the sender's existing bounded retry owns recovery.
func (d *metaTrackHomeAckDispatcherV2) enqueue(ctx context.Context, peer string, envelope p2p.MessageEnvelope) error {
	if d == nil || d.send == nil || ctx == nil || peer == "" {
		return fmt.Errorf("metatrack Home ACK dispatcher invalid arguments")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	lane := d.lanes[peer]
	if lane == nil {
		lane = &metaTrackHomeAckLaneV2{}
		d.lanes[peer] = lane
	}
	if len(lane.jobs) >= metaTrackHomeAckQueueLimitV2 {
		d.mu.Unlock()
		return fmt.Errorf("metatrack Home ACK queue full for %s; sender must retry", peer)
	}
	lane.jobs = append(lane.jobs, metaTrackHomeAckTaskV2{ctx: ctx, peer: peer, envelope: envelope})
	if !lane.draining {
		lane.draining = true
		go d.drain(peer, lane)
	}
	d.mu.Unlock()
	return nil
}

func (d *metaTrackHomeAckDispatcherV2) drain(peer string, lane *metaTrackHomeAckLaneV2) {
	for {
		d.mu.Lock()
		if len(lane.jobs) == 0 {
			delete(d.lanes, peer)
			lane.draining = false
			d.mu.Unlock()
			return
		}
		task := lane.jobs[0]
		lane.jobs[0] = metaTrackHomeAckTaskV2{}
		lane.jobs = lane.jobs[1:]
		d.mu.Unlock()
		err := task.ctx.Err()
		if err == nil {
			err = d.send(task.ctx, task.peer, task.envelope)
		}
		if d.sent != nil {
			d.sent(err)
		}
	}
}

var metaTrackHomeAckDispatchersV2 sync.Map // key: *NodeRuntime

func (r *NodeRuntime) enqueueMetaTrackHomeBatchAckV2(ctx context.Context, requester string, envelope p2p.MessageEnvelope) error {
	newDispatcher := newMetaTrackHomeAckDispatcherV2(
		func(sendCtx context.Context, peer string, msg p2p.MessageEnvelope) error {
			return r.sendStateAccessToNode(sendCtx, peer, msg)
		},
		func(err error) {
			if err != nil {
				r.incrementFullLocalityMetric("metatrack_home_batch_ack_async_send_failure_count", 1)
			} else {
				r.incrementFullLocalityMetric("metatrack_home_batch_ack_async_send_success_count", 1)
			}
		},
	)
	actual, _ := metaTrackHomeAckDispatchersV2.LoadOrStore(r, newDispatcher)
	if err := actual.(*metaTrackHomeAckDispatcherV2).enqueue(ctx, requester, envelope); err != nil {
		r.incrementFullLocalityMetric("metatrack_home_batch_ack_async_enqueue_failure_count", 1)
		return err
	}
	r.incrementFullLocalityMetric("metatrack_home_batch_ack_async_enqueued_count", 1)
	return nil
}
