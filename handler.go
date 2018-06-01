package blip

import "sync"

// A function that handles an incoming BLIP request and optionally sends a response.
// A handler is called on a new goroutine so it can take as long as it needs to.
// For example, if it has to send a synchronous network request before it can construct
// a response, that's fine.
type Handler func(request *Message)

// Concurrent-safe storage of the map of profile -> handler
type HandlerForProfile struct {
	lock     sync.RWMutex       // Lock to protect concurrent access to handler
	handlers map[string]Handler // Handler function for a request Profile

}

func NewHandlerForProfile() *HandlerForProfile {
	return &HandlerForProfile{
		handlers: make(map[string]Handler),
	}
}

func (h *HandlerForProfile) GetHandler(profile string) Handler {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.handlers[profile]
}

func (h *HandlerForProfile) SetHandler(profile string, handler Handler) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.handlers[profile] = handler
}

func (h *HandlerForProfile) DeleteHandler(profile string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.handlers, profile)
}
