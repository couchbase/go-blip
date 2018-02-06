package blip

import "expvar"

// Expvar instrumentation
var (
	numGoroutines = expvar.NewMap("goblip")
)

func incrReceiverGoroutines() {
	numGoroutines.Add("goroutines_receiver", 1)
}

func decrReceiverGoroutines() {
	numGoroutines.Add("goroutines_receiver", -1)
}

func incrAsyncReadGoroutines() {
	numGoroutines.Add("goroutines_async_read", 1)
}

func decrAsyncReadGoroutines() {
	numGoroutines.Add("goroutines_async_read", -1)
}

func incrNextFrameToSendGoroutines() {
	numGoroutines.Add("goroutines_next_frame_to_send", 1)
}

func decrNextFrameToSendGoroutines() {
	numGoroutines.Add("goroutines_next_frame_to_send", -1)
}

func incrParseLoopGoroutines() {
	numGoroutines.Add("goroutines_parse_loop", 1)
}

func decrParseLoopGoroutines() {
	numGoroutines.Add("goroutines_parse_loop", -1)
}

func incrSenderGoroutines() {
	numGoroutines.Add("goroutines_sender", 1)
}

func decrSenderGoroutines() {
	numGoroutines.Add("goroutines_sender", -1)
}
