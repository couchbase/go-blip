package blip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"golang.org/x/net/websocket"
)

const checksumLength = 4
const deflateTrailerLength = 4
const deflateTrailer = "\x00\x00\xff\xff"

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
	frameDecoder        *decompressor   // Decompresses compressed frames from frameBuffer
	parseError          error           // Fatal error generated by frame parser

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
		pendingRequests:  msgStreamerMap{},
		pendingResponses: msgStreamerMap{},
	}
}

func (r *receiver) receiveLoop() error {
	go r.parseLoop()
	for {
		// Receive the next raw WebSocket frame:
		var frame []byte
		if err := websocket.Message.Receive(r.conn, &frame); err != nil {
			if err == io.EOF {
				r.context.logFrame("receiveLoop stopped")
			} else if r.parseError != nil {
				r.context.log("Error: receiveLoop exiting due to parse error: %v", r.parseError)
				err = r.parseError
			} else {
				r.context.log("Error: receiveLoop exiting with WebSocket error: %v", err)
			}
			close(r.channel)
			return err
		}
		r.channel <- frame
	}
	return nil
}

func (r *receiver) parseLoop() {
	r.frameDecoder = getDecompressor(&r.frameBuffer)
	for frame := range r.channel {
		if err := r.handleIncomingFrame(frame); err != nil {
			r.context.log("Error: parseLoop exiting with BLIP error: %v", err)
			r.parseError = err
			r.conn.Close()
			//TODO: Should set a WebSocket close code/msg, but websocket.Conn has no API for that
			break
		}
	}
	r.context.logFrame("parseLoop stopped")
	returnDecompressor(r.frameDecoder)
	r.frameDecoder = nil
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

	compressed := false
	var checksum uint32 = 0
	isACK := msgType.isAck()
	if !isACK {
		// Read checksum (except for ACK messages which don't have one, nor any compression)
		bufferedFrame := r.frameBuffer.Bytes()
		checksumSlice := bufferedFrame[len(bufferedFrame)-checksumLength : len(bufferedFrame)]
		checksum = binary.BigEndian.Uint32(checksumSlice)
		compressed = flags&kCompressed != 0
		if compressed {
			// Replace the checksum with the implicit 00 00 FF FF deflate trailer:
			copy(checksumSlice, deflateTrailer)
		} else {
			// Don't let frameDecoder read checksum
			r.frameBuffer.Truncate(r.frameBuffer.Len() - checksumLength)
		}
	}

	if r.context.LogFrames {
		r.context.logFrame("Received frame: %s (flags=%8b, length=%d)",
			frameString(requestNumber, flags), flags, r.frameBuffer.Len())
	}

	r.frameDecoder.enableCompression(compressed)
	frame, err = r.frameDecoder.readAll()
	if err != nil {
		r.context.log("Error decompressing frame %s: %v", frameString(requestNumber, flags), err)
		return err
	}

	if isACK {
		bytesReceived, n := binary.Uvarint(frame)
		if n > 0 {
			r.sender.receivedAck(requestNumber, msgType.ackSourceType(), bytesReceived)
		} else {
			r.context.log("Error reading ACK frame")
		}
		return nil
	}

	curChecksum := r.frameDecoder.getChecksum()
	if curChecksum != checksum {
		return fmt.Errorf("Frame %s has invalid checksum %x; should be %x",
			frameString(requestNumber, flags), curChecksum, checksum)
	}

	// Look up or create the writer stream for this message:
	complete := (flags & kMoreComing) == 0
	var msgStream *msgStreamer
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
		if frameSize, err := writeFull(frame, msgStream.writer); err != nil {
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
				r.sender.sendAck(requestNumber, msgType, msgStream.bytesWritten)
			}
		}
	}
	return nil
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
		msgStream = &msgStreamer{
			message: request,
			writer: request.asyncRead(func(err error) {
				r.context.dispatchRequest(request, r.sender)
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
		r.context.log("Warning: Unexpected response frame to my msg #%d", requestNumber) // benign
	} else {
		err = fmt.Errorf("Bogus message number %d in response", requestNumber)
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

//  Copyright (c) 2013 Jens Alfke. Copyright (c) 2015-2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
