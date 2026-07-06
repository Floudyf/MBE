package p2p

import (
	"context"
	"sync"
)

type Router struct {
	mu       sync.Mutex
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{handlers: map[string]Handler{}}
}

func (r *Router) Handle(messageType string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[messageType] = handler
}

func (r *Router) Dispatch(ctx context.Context, msg MessageEnvelope) error {
	r.mu.Lock()
	handler := r.handlers[msg.MessageType]
	r.mu.Unlock()
	if handler == nil {
		return nil
	}
	return handler(ctx, msg)
}
