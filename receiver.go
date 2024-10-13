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
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"nhooyr.io/websocket"
)

const checksumLength = 4

type msgStreamer struct {
	message      *Message
	writer       io.WriteCloser
	bytesWritten uint64
}

type msgStreamerMap map[MessageNumber]*msgStreamer

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
}

func newReceiver(context *Context, conn *websocket.Conn) *receiver {
	return &receiver{
		conn:             conn,
		context:          context,
		channel:          make(chan []byte, 10),
		parseError:       make(chan error, 1),
		frameDecoder:     getDecompressor(context),
		pendingRequests:  msgStreamerMap{},
		pendingResponses: msgStreamerMap{},
	}
}

func (r *receiver) receiveLoop() error {
	defer atomic.AddInt32(&r.activeGoroutines, -1)
	atomic.AddInt32(&r.activeGoroutines, 1)
	go r.parseLoop()

	defer close(r.channel)

	for {
		// Receive the next raw WebSocket frame:
		_, frame, err := r.conn.Read(r.context.GetCancelCtx())
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

		r.channel <- frame
	}
}

func (r *receiver) parseLoop() {
	defer func() { // Panic handler:
		atomic.AddInt32(&r.activeGoroutines, -1)
		if p := recover(); p != nil {
			log.Printf("PANIC in BLIP parseLoop: %v\n%s", p, debug.Stack())
			err, _ := p.(error)
			if err == nil {
				err = fmt.Errorf("Panic: %v", p)
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

	// There can be goroutines spawned by message.asyncRead() that are blocked waiting to
	// read off their end of an io.Pipe, and if the peer abruptly closes a connection which causes
	// the sender to stop(), the other side of that io.Pipe must be closed to avoid the goroutine's
	// call to unblock on the read() call.  This loops through any io.Pipewriters in pendingResponses and
	// close them, unblocking the readers and letting the message.asyncRead() goroutines proceed.
	for _, msgStreamer := range r.pendingResponses {
		err := msgStreamer.writer.Close()
		if err != nil {
			r.context.logMessage("Warning: error closing msgStreamer writer in pending responses while stopping receiver: %v", err)
		}
	}
}

func (r *receiver) handleIncomingFrame(frame []byte) error {
	// Parse BLIP header:
	if len(frame) < 2 {
		return fmt.Errorf("Illegally short frame")
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
			return fmt.Errorf("Illegally short frame")
		}
		checksumSlice := bufferedFrame[len(bufferedFrame)-checksumLength : len(bufferedFrame)]
		checksum := binary.BigEndian.Uint32(checksumSlice)
		r.frameBuffer.Truncate(r.frameBuffer.Len() - checksumLength)

		if r.context.LogFrames {
			r.context.logFrame("Received frame: %s (flags=%8b, length=%d)",
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

		return r.processFrame(requestNumber, flags, body, frameSize)
	}
}

func (r *receiver) processFrame(requestNumber MessageNumber, flags frameFlags, frame []byte, frameSize int) error {
	// Look up or create the writer stream for this message:
	complete := (flags & kMoreComing) == 0
	var msgStream *msgStreamer
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

	// Write the decoded frame body to the stream:
	if msgStream != nil {
		if _, err := writeFull(frame, msgStream.writer); err != nil {
			return err
		} else if complete {
			if err = msgStream.writer.Close(); err != nil {
				r.context.log("Warning: message writer closed with error %v", err)
			}
		} else {
			//FIX: This isn't the right place to do this, because this goroutine doesn't block even
			// if the client can't read the message fast enough. The right place to send the ACK is
			// in the goroutine that's running msgStream.writer. (Somehow...)
			oldWritten := msgStream.bytesWritten
			msgStream.bytesWritten += uint64(frameSize)
			if oldWritten > 0 && (oldWritten/kAckInterval) < (msgStream.bytesWritten/kAckInterval) {
				r.sender.sendAck(requestNumber, flags.messageType(), msgStream.bytesWritten)
			}
		}
	}
	return err
}

func (r *receiver) getPendingRequest(requestNumber MessageNumber, flags frameFlags, complete bool) (msgStream *msgStreamer, err error) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	msgStream = r.pendingRequests[requestNumber]
	if msgStream != nil {
		if complete {
			delete(r.pendingRequests, requestNumber)
		}
	} else if requestNumber == r.numRequestsReceived+1 {
		r.numRequestsReceived++
		request := newIncomingMessage(r.sender, requestNumber, flags, nil)
		atomic.AddInt32(&r.activeGoroutines, 1)
		msgStream = &msgStreamer{
			message: request,
			writer: request.asyncRead(func(err error) {
				r.context.dispatchRequest(request, r.sender)
				atomic.AddInt32(&r.activeGoroutines, -1)
			}),
		}
		if !complete {
			r.pendingRequests[requestNumber] = msgStream
		}
	} else {
		return nil, fmt.Errorf("Bad incoming request number %d", requestNumber)
	}
	return msgStream, nil
}

func (r *receiver) getPendingResponse(requestNumber MessageNumber, flags frameFlags, complete bool) (msgStream *msgStreamer, err error) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	msgStream = r.pendingResponses[requestNumber]
	if msgStream != nil {
		if msgStream.bytesWritten == 0 {
			msgStream.message.flags = flags // set flags based on 1st frame of response
		}
		if complete {
			delete(r.pendingResponses, requestNumber)
		}
	} else if requestNumber <= r.maxPendingResponseNumber {
		// sent a request that wasn't expecting a response for?
		r.context.log("Warning: Unexpected response frame to my msg #%d", requestNumber) // benign
	} else {
		// processing a response frame with a message number higher than any requests I've sent
		err = fmt.Errorf("Bogus message number %d in response.  Expected to be less than max pending response number (%d)", requestNumber, r.maxPendingResponseNumber)
	}
	return
}

// pendingResponses is accessed from both the receiveLoop goroutine and the sender's goroutine,
// so it needs synchronization.
func (r *receiver) awaitResponse(request *Message, writer io.WriteCloser) {
	r.pendingMutex.Lock()
	defer r.pendingMutex.Unlock()
	number := request.number
	r.pendingResponses[number] = &msgStreamer{
		message: request,
		writer:  writer,
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

// Why isn't this in the io package already, when ReadFull is?
func writeFull(buf []byte, writer io.Writer) (nWritten int, err error) {
	for len(buf) > 0 {
		var n int
		n, err = writer.Write(buf)
		if err != nil {
			break
		}
		nWritten += n
		buf = buf[n:]
	}
	return
}
