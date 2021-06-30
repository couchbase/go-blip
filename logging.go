/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import "log"

// Different "types" of logging events that BLIP can call the pluggable logger callback function with.
type LogEventType int

const (
	LogGeneral LogEventType = iota // Normal log
	LogMessage                     // BLIP Message
	LogFrame                       // BLIP Frame
)

// Log callback function
type LogFn func(LogEventType, string, ...interface{})

// Wrap log.Printf to log message.  Discards LogEventType parameter, which matches behavior before that was added.
func logPrintfWrapper() LogFn {
	return func(_ LogEventType, fmt string, args ...interface{}) {
		log.Printf(fmt, args...)
	}
}
