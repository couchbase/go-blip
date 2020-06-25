package blip

import "expvar"

// Expvar instrumentation
var (
	goblipExpvar = expvar.NewMap("goblip")
)

func incrReceiverGoroutines() {
	goblipExpvar.Add("goroutines_receiver", 1)
}

func decrReceiverGoroutines() {
	goblipExpvar.Add("goroutines_receiver", -1)
}

func incrAsyncReadGoroutines() {
	goblipExpvar.Add("goroutines_async_read", 1)
}

func decrAsyncReadGoroutines() {
	goblipExpvar.Add("goroutines_async_read", -1)
}

func incrNextFrameToSendGoroutines() {
	goblipExpvar.Add("goroutines_next_frame_to_send", 1)
}

func decrNextFrameToSendGoroutines() {
	goblipExpvar.Add("goroutines_next_frame_to_send", -1)
}

func incrParseLoopGoroutines() {
	goblipExpvar.Add("goroutines_parse_loop", 1)
}

func decrParseLoopGoroutines() {
	goblipExpvar.Add("goroutines_parse_loop", -1)
}

func incrSenderGoroutines() {
	goblipExpvar.Add("goroutines_sender", 1)
}

func decrSenderGoroutines() {
	goblipExpvar.Add("goroutines_sender", -1)
}

func incrSenderPingCount() {
	goblipExpvar.Add("sender_ping_count", 1)
}

func incrSenderPingGoroutines() {
	goblipExpvar.Add("goroutines_sender_ping", 1)
}

func decrSenderPingGoroutines() {
	goblipExpvar.Add("goroutines_sender_ping", -1)
}
