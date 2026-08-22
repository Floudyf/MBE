package p2p

import "testing"

func TestCatchupLaneIsIndependentFromConsensusAndStateAccessLanes(t *testing.T) {
	tr := NewTransport("n0", "127.0.0.1:0", nil, nil)
	if tr.outbound == nil || tr.stateAccessOutbound == nil || tr.catchupOutbound == nil {
		t.Fatal("transport lanes must all be initialized")
	}
	tr.catchupOutbound["n1"] = &outboundConn{}
	if tr.outbound["n1"] != nil || tr.stateAccessOutbound["n1"] != nil {
		t.Fatal("catch-up lane aliases another outbound lane")
	}
}
