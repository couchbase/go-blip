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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"
)

const checksumLength = 4

type msgStreamerMap map[MessageNumber]*msgReceiver

// The receiving side of a BLIP connection.
// Handles receiving WebSocket messages as frames and assembling them into BLIP messages.
type receiver struct {
	context             *Context        // My owning BLIP Context
	conn                *websocket.Conn // The WebSocket connection
	channel             chan []byte     // WebSocket messages waiting to be processed
	numRequestsReceived MessageNumber   // The number of REQ messages I've received
	sender              *Sender         // My Context's Sender
	frameBuffer         bytes.Buffer    // Used to stream an incoming frame's data
	frameDecoder        *decompressor   // Decompresses compressed frame bodies
	parseError          chan error      // Fatal error generated by frame parser
	activeGoroutines    int32           // goroutine counter for safe teardown

	pendingMutex             sync.Mutex     // For thread-safe access to the fields below
	pendingRequests          msgStreamerMap // Unfinished REQ messages being assembled
	pendingResponses         msgStreamerMap // Unfinished RES messages being assembled
	maxPendingResponseNumber MessageNumber  // Largest RES # I've seen

	dispatchMutex          sync.Mutex // For thread-safe access to the fields below
	dispatchCond           sync.Cond  // Used when receiver stops reading
	maxDispatchedBytes     int        // above this value, receiver stops reading the WebSocket
	dispatchedBytes        int        // Size of dispatched but unhandled incoming requests
	dispatchedMessageCount int        // Number of dispatched but unhandled incoming requests
}

func newReceiver(context *Context, conn *websocket.Conn) *receiver {
	rcvr := &receiver{
		conn:               conn,
		context:            context,
		channel:            make(chan []byte, 10),
		parseError:         make(chan error, 1),
		frameDecoder:       getDecompressor(context),
		pendingRequests:    msgStreamerMap{},
		pendingResponses:   msgStreamerMap{},
		maxDispatchedBytes: context.MaxDispatchedBytes,
	}
	rcvr.dispatchCond = sync.Cond{L: &rcvr.dispatchMutex}
	return rcvr
}

func (r *receiver) receiveLoop() error {
	defer atomic.AddInt32(&r.activeGoroutines, -1)
	atomic.AddInt32(&r.activeGoroutines, 1)
	go r.parseLoop()

	defer close(r.channel)

	for {
		// Receive the next raw WebSocket frame:
		msgType, frame, err := r.conn.Read(context.TODO())
		if err != nil {
			if isCloseError(err) {
				// lower log level for close
				r.context.logFrame("receiveLoop stopped: %v", err)
			} else if parseErr := errorFromChannel(r.parseError); parseErr != nil {
				err = parseErr
			} else {
				r.context.log("Error: receiveLoop exiting with WebSocket error: %v", err)
			}

			return err
		}

		switch msgType {
		case websocket.MessageBinary:
			r.channel <- frame
		default:
			r.context.log("Warning: received WebSocket message of type %v", msgType)
		}
	}
}

func (r *receiver) parseLoop() {
	defer func() { // Panic handler:
		atomic.AddInt32(&r.activeGoroutines, -1)
		if p := recover(); p != nil {
			r.context.log("PANIC in BLIP parseLoop: %v\n%s", p, debug.Stack())
			err, _ := p.(error)
			if err == nil {
				err = fmt.Errorf("panic: %v", p)
			}
			r.fatalError(err)
		}
	}()

	// Update Expvar stats for number of outstanding goroutines
	incrParseLoopGoroutines()
	defer decrParseLoopGoroutines()

	atomic.AddInt32(&r.activeGoroutines, 1)

	for frame := range r.channel {
		r.context.bytesReceived.Add(uint64(len(frame)))
		if err := r.handleIncomingFrame(frame); err != nil {
			r.fatalError(err)
			break
		}
	}

	r.context.logFrame("parseLoop stopped")
	returnDecompressor(r.frameDecoder)
	r.frameDecoder = nil
}

func (r *receiver) fatalError(err error) {
	r.context.log("Error: parseLoop closing socket due to error: %v", err)
	r.parseError <- err
	r.stop()
}

func (r *receiver) stop() {
	r.closePendingResponses()

	r.conn.Close(websocket.StatusNormalClosure, "")

	waitForZeroActiveGoroutines(r.context, &r.activeGoroutines)
}

func (r *receiver) closePendingResponses() {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()

	for _, msgStreamer := range r.pendingResponses {
		msgStreamer.cancelIncoming()
	}
}

func (r *receiver) handleIncomingFrame(frame []byte) error {
	// Parse BLIP header:
	if len(frame) < 2 {
		return fmt.Errorf("illegally short frame")
	}
	r.frameBuffer.Reset()
	r.frameBuffer.Write(frame)
	n, err := binary.ReadUvarint(&r.frameBuffer)
	if err != nil {
		return err
	}
	requestNumber := MessageNumber(n)
	n, err = binary.ReadUvarint(&r.frameBuffer)
	if err != nil {
		return err
	}
	flags := frameFlags(n)
	msgType := flags.messageType()

	if msgType.isAck() {
		// ACKs are parsed specially. They don't go through the codec nor contain a checksum:
		body := r.frameBuffer.Bytes()
		bytesReceived, n := binary.Uvarint(body)
		if n > 0 {
			r.sender.receivedAck(requestNumber, msgType.ackSourceType(), bytesReceived)
		} else {
			r.context.log("Error reading ACK frame: %x", body)
		}
		return nil

	} else {
		// Regular frames have a checksum:
		bufferedFrame := r.frameBuffer.Bytes()
		frameSize := len(bufferedFrame)
		if len(frame) < checksumLength {
			return fmt.Errorf("illegally short frame")
		}
		checksumSlice := bufferedFrame[len(bufferedFrame)-checksumLength:]
		checksum := binary.BigEndian.Uint32(checksumSlice)
		r.frameBuffer.Truncate(r.frameBuffer.Len() - checksumLength)

		if r.context.LogFrames {
			r.context.logFrame("received frame: %s (flags=%8b, length=%d)",
				frameString(requestNumber, flags), flags, r.frameBuffer.Len())
		}

		// Read/decompress the body of the frame:
		var body []byte
		if flags&kCompressed != 0 {
			body, err = r.frameDecoder.decompress(r.frameBuffer.Bytes(), checksum)
		} else {
			body, err = r.frameDecoder.passthrough(r.frameBuffer.Bytes(), &checksum)
		}
		if err != nil {
			r.context.log("Error receiving frame %s: %v. Raw frame = <%x>",
				frameString(requestNumber, flags), err, frame)
			return err
		}

		return r.processIncomingFrame(requestNumber, flags, body, frameSize)
	}
}

func (r *receiver) processIncomingFrame(requestNumber MessageNumber, flags frameFlags, frame []byte, frameSize int) error {
	// Look up or create the writer stream for this message:
	complete := (flags & kMoreComing) == 0
	var msgStream *msgReceiver
	var err error
	switch flags.messageType() {
	case RequestType:
		msgStream, err = r.getPendingRequest(requestNumber, flags, complete)
	case ResponseType, ErrorType:
		msgStream, err = r.getPendingResponse(requestNumber, flags, complete)
	case AckRequestType, AckResponseType:
		break
	default:
		r.context.log("Warning: Ignoring incoming message type, with flags 0x%x", flags)
	}
	if msgStream == nil {
		return err
	}

	// Write the decoded frame body to the stream:
	state, err := msgStream.addIncomingBytes(frame, complete)
	if err != nil {
		return err
	}

	if !complete {
		// Not complete yet; send an ACK message every `kAckInterval` bytes:
		//FIX: This isn't the right place to do this, because this goroutine doesn't block even
		// if the client can't read the message fast enough.
		msgStream.maybeSendAck(frameSize)
	}

	// Dispatch at first or last frame:
	if request := msgStream.inResponseTo; request != nil {
		if state.atStart {
			// Dispatch response to its request message as soon as properties are available:
			r.context.logMessage("Received response %s", msgStream.Message)
			request.responseAvailable(msgStream.Message) // Response to outgoing request
		}
	} else {
		if /*state.atStart ||*/ state.atEnd {
			// Dispatch request to the dispatcher:
			r.dispatch(msgStream.Message)
		}
	}
	return err
}

func (r *receiver) getPendingRequest(requestNumber MessageNumber, flags frameFlags, complete bool) (msgStream *msgReceiver, err error) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	msgStream = r.pendingRequests[requestNumber]
	if msgStream != nil {
		if complete {
			delete(r.pendingRequests, requestNumber)
		}
	} else if requestNumber == r.numRequestsReceived+1 {
		r.numRequestsReceived++
		msgStream = newIncomingMessage(r.sender, requestNumber, flags)
		if !complete {
			r.pendingRequests[requestNumber] = msgStream
		}
	} else {
		return nil, fmt.Errorf("bad incoming request number %d", requestNumber)
	}
	return msgStream, nil
}

func (r *receiver) getPendingResponse(requestNumber MessageNumber, flags frameFlags, complete bool) (msgStream *msgReceiver, err error) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	msgStream = r.pendingResponses[requestNumber]
	if msgStream != nil {
		if msgStream.bytesWritten == 0 {
			msgStream.flags = flags // set flags based on 1st frame of response
		}
		if complete {
			delete(r.pendingResponses, requestNumber)
		}
	} else if requestNumber <= r.maxPendingResponseNumber {
		// sent a request that wasn't expecting a response for?
		r.context.log("Warning: Unexpected response frame to my msg #%d", requestNumber) // benign
	} else {
		// processing a response frame with a message number higher than any requests I've sent
		err = fmt.Errorf("bogus message number %d in response.  Expected to be less than max pending response number (%d)", requestNumber, r.maxPendingResponseNumber)
	}
	return
}

func (r *receiver) awaitResponse(response *Message) {
	// pendingResponses is accessed from both the receiveLoop goroutine and the sender's goroutine,
	// so it needs synchronization.
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	number := response.number
	r.pendingResponses[number] = &msgReceiver{
		Message: response,
	}
	if number > r.maxPendingResponseNumber {
		r.maxPendingResponseNumber = number
	}
}

func (r *receiver) backlog() (pendingRequest, pendingResponses int) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	return len(r.pendingRequests), len(r.pendingResponses)
}

//////// REQUEST DISPATCHING & FLOW CONTROL

func (r *receiver) dispatch(request *Message) {
	sender := r.sender
	requestSize := request.bodySize()
	onComplete := func() {
		response := request.Response()
		if panicked := recover(); panicked != nil {
			if handler := sender.context.HandlerPanicHandler; handler != nil {
				handler(request, response, panicked)
			} else {
				stack := debug.Stack()
				sender.context.log("PANIC handling BLIP request %v: %v:\n%s", request, panicked, stack)
				if response != nil {
					// (It is generally considered bad security to reveal internal state like error
					// messages or stack dumps in a network response.)
					response.SetError(BLIPErrorDomain, HandlerFailedCode, "Internal Error")
				}
			}
		}

		r.subDispatchedBytes(requestSize)

		if response != nil {
			sender.send(response)
		}
	}

	r.addDispatchedBytes(requestSize)
	r.context.RequestHandler(request, onComplete)
	r.waitOnDispatchedBytes()
}

func (r *receiver) addDispatchedBytes(n int) {
	if r.maxDispatchedBytes > 0 {
		r.dispatchMutex.Lock()
		r.dispatchedBytes += n
		r.dispatchedMessageCount++
		r.dispatchMutex.Unlock()
	}
}

func (r *receiver) subDispatchedBytes(n int) {
	if r.maxDispatchedBytes > 0 {
		r.dispatchMutex.Lock()
		prevBytes := r.dispatchedBytes
		r.dispatchedBytes = prevBytes - n
		r.dispatchedMessageCount--
		if prevBytes > r.maxDispatchedBytes && r.dispatchedBytes <= r.maxDispatchedBytes {
			r.dispatchCond.Signal()
		}
		r.dispatchMutex.Unlock()
	}
}

func (r *receiver) waitOnDispatchedBytes() {
	if r.maxDispatchedBytes > 0 {
		r.dispatchMutex.Lock()
		if r.dispatchedBytes > r.maxDispatchedBytes {
			start := time.Now()
			r.context.log("WebSocket receiver paused (%d requests being handled, %d bytes)", r.dispatchedMessageCount, r.dispatchedBytes)
			for r.dispatchedBytes > r.maxDispatchedBytes {
				r.dispatchCond.Wait()
			}
			r.context.log("...WebSocket receiver resuming after %v (now %d requests being handled, %d bytes)", time.Since(start), r.dispatchedMessageCount, r.dispatchedBytes)
		}
		r.dispatchMutex.Unlock()
	}
}
