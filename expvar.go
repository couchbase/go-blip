/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
