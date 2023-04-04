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
	"strings"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"
)

// Defines how incoming requests are dispatched to handler functions.
type Context struct {

	// The WebSocket subprotocols that this blip context is constrained to.  Eg: BLIP_3+CBMobile_2
	// Client request must indicate that it supports one of these protocols, else WebSocket handshake will fail.
	SupportedSubProtocols []string

	// The currently used WebSocket subprotocol by the client, set on a successful handshake.
	activeSubProtocol string

	RequestHandler      AsyncHandler        // Callback that handles incoming requests
	FatalErrorHandler   FatalErrorHandler   // Called when connection has a fatal error
	HandlerPanicHandler HandlerPanicHandler // Called when a profile handler panics
	MaxSendQueueCount   int                 // Max # of messages being sent at once (if >0)
	MaxDispatchedBytes  int                 // Max total size of incoming requests being dispatched (if >0)
	Logger              LogFn               // Logging callback; defaults to log.Printf
	LogMessages         bool                // If true, will log about messages
	LogFrames           bool                // If true, will log about frames (very verbose)

	OnExitCallback func() // OnExitCallback callback invoked when the underlying connection closes and the receive loop exits.

	WebsocketPingInterval time.Duration // Time between sending ping frames (if >0)

	// An identifier that uniquely defines the context.  NOTE: Random Number Generator not seeded by go-blip.
	ID string

	HandlerForProfile map[string]Handler // deprecated; use RequestHandler & ByProfileDispatcher
	DefaultHandler    Handler            // deprecated; use RequestHandler & ByProfileDispatcher

	bytesSent     atomic.Uint64 // Number of bytes sent
	bytesReceived atomic.Uint64 // Number of bytes received
}

// A function called when a Handler function panics.
type HandlerPanicHandler func(request, response *Message, err interface{})

// A function called when the connection closes due to a fatal protocol error.
type FatalErrorHandler func(error)

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
	if context.RequestHandler == nil {
		// Compatibility mode: If the app hasn't set a RequestHandler, set one that uses the old
		// handlerForProfile and defaultHandler.
		context.RequestHandler = context.compatibilityHandler
	} else if len(context.HandlerForProfile) > 0 || context.DefaultHandler != nil {
		panic("blip.Context cannot have both a RequestHandler and legacy handlerForProfile or defaultHandler")
	}
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

// GetBytesSent returns the number of bytes sent since start of the context.
func (context *Context) GetBytesSent() uint64 {
	return context.bytesSent.Load()
}

// GetBytesReceived returns the number of bytes received since start of the context.
func (context *Context) GetBytesReceived() uint64 {
	return context.bytesReceived.Load()
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
		// defer context.dispatcher.stop()

		// Update Expvar stats for number of outstanding goroutines
		incrReceiverGoroutines()
		defer decrReceiverGoroutines()

		err := sender.receiver.receiveLoop()
		if err != nil {
			if isCloseError(err) {
				// lower log level for close
				context.logFrame("BLIP/Websocket receiveLoop exited: %v", err)
			} else {
				context.log("BLIP/Websocket receiveLoop exited with error: %v", err)
			}
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
	// bwss.blipCtx.dispatcher.stop()
	if err != nil && !isCloseError(err) {
		bwss.blipCtx.log("BLIP/Websocket Handler exited with error: %v", err)
		if bwss.blipCtx.FatalErrorHandler != nil {
			bwss.blipCtx.FatalErrorHandler(err)
		}
	}
	ws.Close(websocket.StatusNormalClosure, "")
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

//////// UTILITIES:

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

// isCloseError returns true if the given error is expected on a websocket close (i.e. io.EOF, WS 1000, 1001, 1005, ...)
func isCloseError(err error) bool {
	if err == io.EOF {
		// net package library returned EOF for close (it had no support for close handshakes)
		return true
	}
	// The following status codes are mostly expected for clients closing a connection,
	// either cleanly (1000, 1001) or abruptly (1005)... Not much cause for concern.
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure,
		websocket.StatusGoingAway,
		websocket.StatusNoStatusRcvd:
		return true
	}

	return false
}
