// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package blip

import (
	"sync"
)

//////// REQUEST HANDLER FUNCTIONS

// A function that handles an incoming BLIP request and optionally sends a response.
// The request is not considered handled until this function returns.
//
// After the handler returns, any response you've added to the message (by calling
// `request.Response()“) will be sent, unless the message has the NoReply flag.
// If the message needs a response but none was created, a default empty response will be sent.
// If the handler panics, an error response is sent instead.
type SynchronousHandler = func(request *Message)

// A function that asynchronously handles an incoming BLIP request and optionally sends a response.
// The handler function may return immediately without handling the request.
// The request is not considered handled until the `onComplete` callback is called,
// from any goroutine.
//
// The callback MUST be called eventually, even if the request has the NoReply flag;
// the caller of this handler may be using the callback to track and limit the number of concurrent
// handlers, so failing to call the callback could eventually block delivery of requests.
//
// The function is allowed to call `onComplete` before it returns, i.e. run synchronously,
// but it should still try to return as quickly as possible to avoid blocking upstream code.
type AsyncHandler = func(request *Message, onComplete RequestCompletedCallback)

type Handler = SynchronousHandler

type RequestCompletedCallback = func()

// Utility SynchronousHandler function that responds to a Message with a 404 error.
func Unhandled(request *Message) {
	//log.Printf("Warning: Unhandled BLIP message: %v (Profile=%q)", request, request.Profile())
	request.Response().SetError(BLIPErrorDomain, NotFoundCode, "No handler for BLIP request")
}

// Utility AsyncHandler function that responds to a Message with a 404 error.
func UnhandledAsync(request *Message, onComplete RequestCompletedCallback) {
	defer onComplete()
	Unhandled(request)
}

// A utility that wraps a SynchronousHandler function in an AsyncHandler function.
// When the returned AsyncHandler is called, it will asynchronously run the wrapped handler
// *on a new goroutine* and then call the completion routine.
func AsAsyncHandler(handler SynchronousHandler) AsyncHandler {
	return func(request *Message, onComplete RequestCompletedCallback) {
		go func() {
			defer onComplete()
			handler(request)
		}()
	}
}

// AsyncHandler function that uses the Context's old handlerForProfile and defaultHandler fields.
// Used as the RequestHandler when the client hasn't set one.
// For compatibility reasons it does not copy the HandlerForProfile or DefaultHandler,
// rather it accesses the ones in the Context on every call (without any locking!)
// This is because there are tests that change those properties while the connection is open.
func (context *Context) compatibilityHandler(request *Message, onComplete RequestCompletedCallback) {
	profile := request.Properties[ProfileProperty]
	handler := context.HandlerForProfile[profile]
	if handler == nil {
		handler = context.DefaultHandler
		if handler == nil {
			handler = Unhandled
		}
	}
	// Old handlers ran on individual goroutines:
	go func() {
		defer onComplete()
		handler(request)
	}()
}

//////// DISPATCHER INTERFACE:

// An interface that provides an AsyncHandler function for incoming requests.
// For any Dispatcher instance `d`, `d.Dispatch` without parentheses is an AsyncHandler.
type Dispatcher interface {
	Dispatch(request *Message, onComplete RequestCompletedCallback)
}

//////// BY-PROFILE DISPATCHER

// A Dispatcher implementation that routes messages to handlers based on the Profile property.
type ByProfileDispatcher struct {
	mutex          sync.Mutex
	profiles       map[string]AsyncHandler // Handler for each Profile
	defaultHandler AsyncHandler            // Fallback handler
}

// Sets the AsyncHandler function for a specific Profile property value.
// A nil value removes the current handler.
// This method is thread-safe and may be called while the connection is active.
func (d *ByProfileDispatcher) SetHandler(profile string, handler AsyncHandler) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if handler != nil {
		if d.profiles == nil {
			d.profiles = map[string]AsyncHandler{}
		}
		d.profiles[profile] = handler
	} else {
		delete(d.profiles, profile)
	}
}

// Sets the default handler, when no Profile matches.
// If this is not set, the default default handler responds synchronously with a BLIP/404 error.
// This method is thread-safe and may be called while the connection is active.
func (d *ByProfileDispatcher) SetDefaultHandler(handler AsyncHandler) {
	d.mutex.Lock()
	d.defaultHandler = handler
	d.mutex.Unlock()
}

func (d *ByProfileDispatcher) Dispatch(request *Message, onComplete RequestCompletedCallback) {
	profile := request.Properties[ProfileProperty]
	d.mutex.Lock()
	handler := d.profiles[profile]
	if handler == nil {
		handler = d.defaultHandler
		if handler == nil {
			handler = UnhandledAsync
		}
	}
	d.mutex.Unlock()

	handler(request, onComplete)
}

//////// THROTTLING DISPATCHER

// A Dispatcher implementation that forwards to a given AsyncHandler, but only allows a certain
// number of concurrent calls.
// Excess requests will be queued and later dispatched in the order received.
type ThrottlingDispatcher struct {
	Handler        AsyncHandler
	MaxConcurrency int
	mutex          sync.Mutex
	concurrency    int
	pending        []savedDispatch
}

type savedDispatch struct {
	request    *Message
	onComplete RequestCompletedCallback
}

// Initializes a ThrottlingDispatcher.
func (d *ThrottlingDispatcher) Init(maxConcurrency int, handler AsyncHandler) {
	d.Handler = handler
	d.MaxConcurrency = maxConcurrency
}

func (d *ThrottlingDispatcher) Dispatch(request *Message, onComplete RequestCompletedCallback) {
	d.mutex.Lock()
	if d.concurrency >= d.MaxConcurrency {
		// Too many running; add this one to the queue:
		d.pending = append(d.pending, savedDispatch{request: request, onComplete: onComplete})
		d.mutex.Unlock()
	} else {
		// Dispatch it now:
		d.concurrency++
		d.mutex.Unlock()
		d.dispatchNow(request, onComplete)
	}
}

func (d *ThrottlingDispatcher) dispatchNow(request *Message, onComplete RequestCompletedCallback) {
	d.Handler(request, func() {
		// When the handler finishes, decrement the concurrency count and start a queued request:
		var next savedDispatch
		d.mutex.Lock()
		if len(d.pending) > 0 {
			next = d.pending[0]
			d.pending = d.pending[1:]
		} else {
			d.concurrency--
		}

		d.mutex.Unlock()

		onComplete()

		if next.request != nil {
			d.dispatchNow(next.request, next.onComplete)
		}
	})
}
