package blip

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/net/websocket"
)

// A function that handles an incoming BLIP request and optionally sends a response.
// A handler is called on a new goroutine so it can take as long as it needs to.
// For example, if it has to send a synchronous network request before it can construct
// a response, that's fine.
type Handler func(request *Message)

// Utility function that responds to a Message with a 404 error.
func Unhandled(request *Message) {
	request.Response().SetError(BLIPErrorDomain, 404, "No handler for BLIP request")
}

// Defines how incoming requests are dispatched to handler functions.
type Context struct {

	// The WebSocket subprotocol that this blip context is constrained to.  Eg: BLIP_3+CBMobile_2
	// Client request must indicate that it supports this protocol, else WebSocket handshake will fail.
	WebSocketSubProtocol string

	HandlerForProfile map[string]Handler // Handler function for a request Profile
	DefaultHandler    Handler            // Handler for all otherwise unhandled requests
	FatalErrorHandler func(error)        // Called when connection has a fatal error
	MaxSendQueueCount int                // Max # of messages being sent at once (if >0)
	Logger            LogFn              // Logging callback; defaults to log.Printf
	LogMessages       bool               // If true, will log about messages
	LogFrames         bool               // If true, will log about frames (very verbose)

	OnExitCallback func() // OnExitCallback callback invoked when the underlying connection closes and the receive loop exits.

	WebsocketPingInterval time.Duration // Time between sending ping frames (if >0)

	// An identifier that uniquely defines the context.  NOTE: Random Number Generator not seeded by go-blip.
	ID string
}

// Defines a logging interface for use within the blip codebase.  Implemented by Context.
// Any code that needs to take a Context just for logging purposes should take a Logger instead.
type LogContext interface {
	log(fmt string, params ...interface{})
	logMessage(fmt string, params ...interface{})
	logFrame(fmt string, params ...interface{})
}

//////// SETUP:

// Creates a new Context with an empty dispatch table.
func NewContext(appProtocolId string) *Context {
	return NewContextCustomID(fmt.Sprintf("%x", rand.Int31()), appProtocolId)
}

// Creates a new Context with a custom ID, which can be helpful to differentiate logs between other blip contexts
// in the same process. The AppProtocolId ensures that this client will only connect to peers that have agreed
// upon the same application layer level usage of BLIP.  For example "CBMobile_2" is the AppProtocolId for the
// Couchbase Mobile replication protocol.
func NewContextCustomID(id string, appProtocolId string) *Context {
	return &Context{
		HandlerForProfile:    map[string]Handler{},
		Logger:               logPrintfWrapper(),
		ID:                   id,
		WebSocketSubProtocol: NewWebSocketSubProtocol(appProtocolId),
	}
}

func (context *Context) start(ws *websocket.Conn) *Sender {
	r := newReceiver(context, ws)
	r.sender = newSender(context, ws, r)
	r.sender.start()
	return r.sender
}

// Opens a BLIP connection to a host.
func (context *Context) Dial(url string, origin string) (*Sender, error) {
	config, err := websocket.NewConfig(url, origin)
	if err != nil {
		return nil, err
	}
	return context.DialConfig(config)
}

// Opens a BLIP connection to a host given a websocket.Config, which allows
// the caller to specify Authorization header.
func (context *Context) DialConfig(config *websocket.Config) (*Sender, error) {
	config.Protocol = []string{context.WebSocketSubProtocol}
	ws, err := websocket.DialConfig(config)
	if err != nil {
		return nil, err
	}
	sender := context.start(ws)
	go func() {

		// If the receiveLoop terminates, stop the sender as well
		defer sender.Stop()

		// Update Expvar stats for number of outstanding goroutines
		incrReceiverGoroutines()
		defer decrReceiverGoroutines()

		err := sender.receiver.receiveLoop()
		if err != nil {
			context.log("BLIP/Websocket receiveLoop exited: %v", err)
			if context.OnExitCallback != nil {
				context.OnExitCallback()
			}
		}
	}()
	return sender, nil
}

type WSHandshake func(*websocket.Config, *http.Request) error

// Creates a WebSocket handshake handler
func (context *Context) WebSocketHandshake() WSHandshake {
	return func(config *websocket.Config, rq *http.Request) error {
		protocolHeader := rq.Header.Get("Sec-WebSocket-Protocol")
		if !includesProtocol(protocolHeader, context.WebSocketSubProtocol) {
			context.log("Error: Client doesn't support WS protocol %s, only %s", context.WebSocketSubProtocol, protocolHeader)
			return &websocket.ProtocolError{"I only speak " + context.WebSocketSubProtocol + " protocol"}
		}
		config.Protocol = []string{context.WebSocketSubProtocol}
		return nil
	}
}

// Creates a WebSocket connection handler that dispatches BLIP messages to the Context.
func (context *Context) WebSocketHandler() websocket.Handler {
	return func(ws *websocket.Conn) {
		context.log("Start BLIP/Websocket handler")
		sender := context.start(ws)
		err := sender.receiver.receiveLoop()
		sender.Stop()
		if err != nil && err != io.EOF {
			context.log("BLIP/Websocket Handler exited: %v", err)
			if context.FatalErrorHandler != nil {
				context.FatalErrorHandler(err)
			}
		}
	}
}

// Creates an HTTP handler that accepts WebSocket connections and dispatches BLIP messages
// to the Context.
func (context *Context) WebSocketServer() *websocket.Server {
	return &websocket.Server{
		Handshake: context.WebSocketHandshake(),
		Handler:   context.WebSocketHandler(),
	}
}

//////// DISPATCHING MESSAGES:

func (context *Context) dispatchRequest(request *Message, sender *Sender) {
	defer func() {
		// On return/panic, send the response:
		response := request.Response()
		if panicked := recover(); panicked != nil {
			stack := debug.Stack()
			context.log("PANIC handling BLIP request %v: %v:\n%s", request, panicked, stack)
			if response != nil {
				response.SetError(BLIPErrorDomain, 500, fmt.Sprintf("Panic: %v", panicked))
			}
		}
		if response != nil {
			sender.send(response)
		}
	}()

	context.logMessage("Incoming BLIP Request: %s", request)
	handler := context.HandlerForProfile[request.Properties["Profile"]]
	if handler == nil {
		handler = context.DefaultHandler
		if handler == nil {
			handler = Unhandled
		}
	}
	handler(request)
}

func (context *Context) dispatchResponse(response *Message) {
	defer func() {
		// On return/panic, log a warning:
		if panicked := recover(); panicked != nil {
			stack := debug.Stack()
			context.log("PANIC handling BLIP response %v: %v:\n%s", response, panicked, stack)
		}
	}()

	context.logMessage("Incoming BLIP Response: %s", response)
	//panic("UNIMPLEMENTED") //TODO
}

//////// LOGGING:

func (context *Context) log(format string, params ...interface{}) {
	context.Logger(LogGeneral, format, params...)
}

func (context *Context) logMessage(format string, params ...interface{}) {
	if context.LogMessages {
		context.Logger(LogMessage, format, params...)
	}
}

func (context *Context) logFrame(format string, params ...interface{}) {
	if context.LogFrames {
		context.Logger(LogFrame, format, params...)
	}
}

func includesProtocol(header string, protocol string) bool {
	for _, item := range strings.Split(header, ",") {
		if strings.TrimSpace(item) == protocol {
			return true
		}
	}
	return false
}

//  Copyright (c) 2013 Jens Alfke. Copyright (c) 2015-2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
