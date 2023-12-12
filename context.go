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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime/debug"
	"strings"
	"sync/atomic"
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

	HandlerForProfile   map[string]Handler                                // Handler function for a request Profile
	DefaultHandler      Handler                                           // Handler for all otherwise unhandled requests
	FatalErrorHandler   func(error)                                       // Called when connection has a fatal error
	HandlerPanicHandler func(request, response *Message, err interface{}) // Called when a profile handler panics
	MaxSendQueueCount   int                                               // Max # of messages being sent at once (if >0)
	Logger              LogFn                                             // Logging callback; defaults to log.Printf
	LogMessages         bool                                              // If true, will log about messages
	LogFrames           bool                                              // If true, will log about frames (very verbose)

	OnExitCallback func() // OnExitCallback callback invoked when the underlying connection closes and the receive loop exits.

	WebsocketPingInterval time.Duration // Time between sending ping frames (if >0)

	// An identifier that uniquely defines the context.  NOTE: Random Number Generator not seeded by go-blip.
	ID string

	bytesSent     atomic.Uint64 // Number of bytes sent
	bytesReceived atomic.Uint64 // Number of bytes received
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

func (blipCtx *Context) start(ws *websocket.Conn) *Sender {
	r := newReceiver(blipCtx, ws)
	r.sender = newSender(blipCtx, ws, r)
	r.sender.start()
	return r.sender
}

// Opens a BLIP connection to a host.
func (blipCtx *Context) Dial(url string) (*Sender, error) {
	return blipCtx.DialConfig(&DialOptions{
		URL: url,
	})
}

// GetBytesSent returns the number of bytes sent since start of the context.
func (blipCtx *Context) GetBytesSent() uint64 {
	return blipCtx.bytesSent.Load()
}

// GetBytesReceived returns the number of bytes received since start of the context.
func (blipCtx *Context) GetBytesReceived() uint64 {
	return blipCtx.bytesReceived.Load()
}

// DialOptions is used by DialConfig to oepn a BLIP connection.
type DialOptions struct {
	URL        string
	HTTPClient *http.Client
	HTTPHeader http.Header
}

// Opens a BLIP connection to a host given a DialOptions, which allows the caller to specify a custom HTTP client and headers.
func (blipCtx *Context) DialConfig(opts *DialOptions) (*Sender, error) {

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
	for _, subProtocol := range blipCtx.SupportedSubProtocols {
		wsDialOpts.Subprotocols = []string{subProtocol}
		ws, _, err = websocket.Dial(context.TODO(), opts.URL, &wsDialOpts)
		if err != nil {
			continue
		}

		selectedSubProtocol = subProtocol
		break
	}

	if selectedSubProtocol == "" {
		return nil, err
	}

	sender := blipCtx.start(ws)
	go func() {

		// If the receiveLoop terminates, stop the sender as well
		defer sender.Stop()

		// Update Expvar stats for number of outstanding goroutines
		incrReceiverGoroutines()
		defer decrReceiverGoroutines()

		err := sender.receiver.receiveLoop()
		if err != nil {
			if isCloseError(err) {
				// lower log level for close
				blipCtx.logFrame("BLIP/Websocket receiveLoop exited: %v", err)
			} else {
				blipCtx.log("BLIP/Websocket receiveLoop exited with error: %v", err)
			}
			if blipCtx.OnExitCallback != nil {
				blipCtx.OnExitCallback()
			}
		}
	}()
	blipCtx.activeSubProtocol = extractAppProtocolId(selectedSubProtocol)
	return sender, nil
}

// ActiveSubprotocol returns the currently used WebSocket subprotocol for the Context, set after a successful handshake in
// the case of a host or a successful Dial in the case of a client.
func (blipCtx *Context) ActiveSubprotocol() string {
	return blipCtx.activeSubProtocol
}

type BlipWebsocketServer struct {
	blipCtx               *Context
	PostHandshakeCallback func(err error)
}

var _ http.Handler = &BlipWebsocketServer{}

// Creates an HTTP handler that accepts WebSocket connections and dispatches BLIP messages
// to the Context.
func (blipCtx *Context) WebSocketServer() *BlipWebsocketServer {
	return &BlipWebsocketServer{blipCtx: blipCtx}
}

func (bwss *BlipWebsocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := bwss.handshake(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bwss.handle(ws)
}

func (bwss *BlipWebsocketServer) handshake(w http.ResponseWriter, r *http.Request) (conn *websocket.Conn, err error) {
	if bwss.PostHandshakeCallback != nil {
		defer func() {
			bwss.PostHandshakeCallback(err)
		}()
	}

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
		return nil, err
	}

	bwss.blipCtx.activeSubProtocol = extractAppProtocolId(protocol)
	return ws, nil
}

func (bwss *BlipWebsocketServer) handle(ws *websocket.Conn) {
	bwss.blipCtx.log("Start BLIP/Websocket handler")
	sender := bwss.blipCtx.start(ws)
	err := sender.receiver.receiveLoop()
	sender.Stop()
	if err != nil && !isCloseError(err) {
		bwss.blipCtx.log("BLIP/Websocket Handler exited with error: %v", err)
		if bwss.blipCtx.FatalErrorHandler != nil {
			bwss.blipCtx.FatalErrorHandler(err)
		}
	}
	ws.Close(websocket.StatusNormalClosure, "")
}

//////// DISPATCHING MESSAGES:

func (blipCtx *Context) dispatchRequest(request *Message, sender *Sender) {
	defer func() {
		// On return/panic, send the response:
		response := request.Response()
		if panicked := recover(); panicked != nil {
			if blipCtx.HandlerPanicHandler != nil {
				blipCtx.HandlerPanicHandler(request, response, panicked)
			} else {
				stack := debug.Stack()
				blipCtx.log("PANIC handling BLIP request %v: %v:\n%s", request, panicked, stack)
				if response != nil {
					response.SetError(BLIPErrorDomain, 500, fmt.Sprintf("Panic: %v", panicked))
				}
			}
		}
		if response != nil {
			sender.send(response)
		}
	}()

	blipCtx.logMessage("Incoming BLIP Request: %s", request)
	handler := blipCtx.HandlerForProfile[request.Properties["Profile"]]
	if handler == nil {
		handler = blipCtx.DefaultHandler
		if handler == nil {
			handler = Unhandled
		}
	}
	handler(request)
}

func (blipCtx *Context) dispatchResponse(response *Message) {
	defer func() {
		// On return/panic, log a warning:
		if panicked := recover(); panicked != nil {
			stack := debug.Stack()
			blipCtx.log("PANIC handling BLIP response %v: %v:\n%s", response, panicked, stack)
		}
	}()

	blipCtx.logMessage("Incoming BLIP Response: %s", response)
	//panic("UNIMPLEMENTED") //TODO
}

//////// LOGGING:

func (blipCtx *Context) log(format string, params ...interface{}) {
	blipCtx.Logger(LogGeneral, format, params...)
}

func (blipCtx *Context) logMessage(format string, params ...interface{}) {
	if blipCtx.LogMessages {
		blipCtx.Logger(LogMessage, format, params...)
	}
}

func (blipCtx *Context) logFrame(format string, params ...interface{}) {
	if blipCtx.LogFrames {
		blipCtx.Logger(LogFrame, format, params...)
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

// isCloseError returns true if the given error is expected on a websocket close (i.e. io.EOF, WS 1000, 1001, 1005, ...)
func isCloseError(err error) bool {
	if errors.Is(err, io.EOF) {
		// - x/net/websocket returned EOF for close (it had no support for close handshakes or wrapped errors)
		// - nhooyr/websocket occasionally wraps EOFs with other errors (e.g. "failed to get reader: failed to read frame header: EOF")
		return true
	}

	// The following status codes are expected for clients closing a connection,
	// either cleanly (1000, 1001) or abruptly (1005)...
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure,
		websocket.StatusGoingAway,
		websocket.StatusNoStatusRcvd:
		return true
	}

	return false
}
