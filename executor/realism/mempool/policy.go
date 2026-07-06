package mempool

import "time"

type Policy struct {
	Capacity int
	TTL      time.Duration
}

func DefaultPolicy() Policy {
	return Policy{
		Capacity: 10000,
		TTL:      10 * time.Minute,
	}
}
