/*
Copyright 2013-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	gocontext "context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"nhooyr.io/websocket"
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

	// The WebSocket subprotocols that this blip context is constrained to.  Eg: BLIP_3+CBMobile_2
	// Client request must indicate that it supports one of these protocols, else WebSocket handshake will fail.
	SupportedSubProtocols []string

	// The currently used WebSocket subprotocol by the client, set on a successful handshake.
	activeSubProtocol string

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
func NewContext(appProtocolIds ...string) (*Context, error) {
	return NewContextCustomID(fmt.Sprintf("%x", rand.Int31()), appProtocolIds...)
}

// Creates a new Context with a custom ID, which can be helpful to differentiate logs between other blip contexts
// in the same process. The AppProtocolId ensures that this client will only connect to peers that have agreed
// upon the same application layer level usage of BLIP.  For example "CBMobile_2" is the AppProtocolId for the
// Couchbase Mobile replication protocol.
func NewContextCustomID(id string, appProtocolIds ...string) (*Context, error) {
	if len(appProtocolIds) == 0 {
		return nil, fmt.Errorf("provided protocolIds cannot be empty")
	}

	return &Context{
		HandlerForProfile:     map[string]Handler{},
		Logger:                logPrintfWrapper(),
		ID:                    id,
		SupportedSubProtocols: formatWebSocketSubProtocols(appProtocolIds...),
	}, nil
}

func (context *Context) start(ws *websocket.Conn) *Sender {
	r := newReceiver(context, ws)
	r.sender = newSender(context, ws, r)
	r.sender.start()
	return r.sender
}

// Opens a BLIP connection to a host.
func (context *Context) Dial(url string) (*Sender, error) {
	return context.DialConfig(&DialOptions{
		URL: url,
	})
}

// DialOptions is used by DialConfig to oepn a BLIP connection.
type DialOptions struct {
	URL        string
	HTTPClient *http.Client
	HTTPHeader http.Header
}

// Opens a BLIP connection to a host given a DialOptions, which allows the caller to specify a custom HTTP client and headers.
func (context *Context) DialConfig(opts *DialOptions) (*Sender, error) {

	var (
		ws                  *websocket.Conn
		err                 error
		selectedSubProtocol string
	)

	wsDialOpts := websocket.DialOptions{CompressionMode: websocket.CompressionDisabled}

	if opts != nil {
		wsDialOpts.HTTPClient = opts.HTTPClient
		wsDialOpts.HTTPHeader = opts.HTTPHeader
	}

	// Try to dial with each SupportedSubProtocols
	// The first one that successfully dials will be the one we'll use, otherwise we'll error.
	for _, subProtocol := range context.SupportedSubProtocols {
		wsDialOpts.Subprotocols = []string{subProtocol}
		ws, _, err = websocket.Dial(gocontext.TODO(), opts.URL, &wsDialOpts)
		if err != nil {
			continue
		}

		selectedSubProtocol = subProtocol
		break
	}

	if selectedSubProtocol == "" {
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
	context.activeSubProtocol = extractAppProtocolId(selectedSubProtocol)
	return sender, nil
}

// ActiveSubprotocol returns the currently used WebSocket subprotocol for the Context, set after a successful handshake in
// the case of a host or a successful Dial in the case of a client.
func (context *Context) ActiveSubprotocol() string {
	return context.activeSubProtocol
}

type blipWebsocketServer struct {
	blipCtx *Context
}

// Creates an HTTP handler that accepts WebSocket connections and dispatches BLIP messages
// to the Context.
func (context *Context) WebSocketServer() http.Handler {
	return &blipWebsocketServer{blipCtx: context}
}

func (bwss *blipWebsocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := bwss.handshake(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bwss.handle(ws)
}

func (bwss *blipWebsocketServer) handshake(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	protocolHeader := r.Header.Get("Sec-WebSocket-Protocol")
	protocol, found := includesProtocol(protocolHeader, bwss.blipCtx.SupportedSubProtocols)
	if !found {
		stringSeperatedProtocols := strings.Join(bwss.blipCtx.SupportedSubProtocols, ",")
		bwss.blipCtx.log("Error: Client doesn't support any of WS protocols: %s only %s", stringSeperatedProtocols, protocolHeader)
		return nil, fmt.Errorf("I only speak %s protocols", stringSeperatedProtocols)
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{protocol},
		// InsecureSkipVerify controls whether Origins are checked or not.
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		bwss.blipCtx.FatalErrorHandler(err)
	}

	bwss.blipCtx.activeSubProtocol = extractAppProtocolId(protocol)
	return ws, nil
}

func (bwss *blipWebsocketServer) handle(ws *websocket.Conn) {
	bwss.blipCtx.log("Start BLIP/Websocket handler")
	sender := bwss.blipCtx.start(ws)
	err := sender.receiver.receiveLoop()
	sender.Stop()
	if err != nil && err != io.EOF {
		bwss.blipCtx.log("BLIP/Websocket Handler exited: %v", err)
		if bwss.blipCtx.FatalErrorHandler != nil {
			bwss.blipCtx.FatalErrorHandler(err)
		}
	}
	ws.Close(websocket.StatusNormalClosure, "")
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

func includesProtocol(header string, protocols []string) (string, bool) {
	for _, item := range strings.Split(header, ",") {
		for _, protocol := range protocols {
			if strings.TrimSpace(item) == protocol {
				return protocol, true
			}
		}
	}
	return "", false
}
