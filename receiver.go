package blip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"

	"golang.org/x/net/websocket"
)

type receiver struct {
	context                  *Context
	conn                     *websocket.Conn
	channel                  chan []byte
	numRequestsReceived      MessageNumber
	pendingRequests          map[MessageNumber]io.WriteCloser
	pendingResponses         map[MessageNumber]io.WriteCloser
	maxPendingResponseNumber MessageNumber
	sender                   *Sender
	mutex                    sync.Mutex
}

func newReceiver(context *Context, conn *websocket.Conn) *receiver {
	return &receiver{
		conn:             conn,
		context:          context,
		channel:          make(chan []byte, 10),
		pendingRequests:  map[MessageNumber]io.WriteCloser{},
		pendingResponses: map[MessageNumber]io.WriteCloser{},
	}
}

func (r *receiver) receiveLoop() error {
	go r.parseLoop()
	for {
		// Receive the next raw WebSocket frame:
		var frame []byte
		if err := websocket.Message.Receive(r.conn, &frame); err != nil {
			if err != io.EOF {
				log.Printf("receiveLoop exiting with WebSocket error: %v", err)
			}
			return err
		}
		r.channel <- frame
	}
	return nil
}

func (r *receiver) parseLoop() {
	for {
		frame := <-r.channel
		if err := r.handleIncomingFrame(frame); err != nil {
			log.Printf("parseLoop exiting with BLIP error: %v", err)
			return
		}
	}
}

func (r *receiver) handleIncomingFrame(frame []byte) error {
	// Parse BLIP header:
	if len(frame) < 2 {
		return fmt.Errorf("Illegally short frame")
	}
	reader := bytes.NewBuffer(frame)
	n, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	requestNumber := MessageNumber(n)
	n, err = binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	flags := frameFlags(n)

	frame = reader.Bytes()
	r.context.logFrame("Received frame: #%3d, flags=%10b, length=%d", requestNumber, flags, len(frame))
	complete := (flags & kMoreComing) == 0

	// Look up or create the writer stream for this message:
	var writer io.WriteCloser
	switch MessageType(flags & kTypeMask) {
	case RequestType:
		{
			writer = r.pendingRequests[requestNumber]
			if writer != nil {
				if complete {
					delete(r.pendingRequests, requestNumber)
				}
			} else if requestNumber == r.numRequestsReceived+1 {
				r.numRequestsReceived++
				request := newIncomingMessage(r.sender, requestNumber, flags, nil)
				writer = request.asyncRead(func(err error) {
					r.context.dispatchRequest(request, r.sender)
				})
				if !complete {
					r.pendingRequests[requestNumber] = writer
				}
			} else {
				return fmt.Errorf("Bad incoming request number %d", requestNumber)
			}
		}
	case ResponseType, ErrorType:
		{
			var err error
			writer, err = r.getPendingResponse(requestNumber, complete)
			if err != nil {
				return err
			}
		}
	default:
		log.Printf("BLIP: Ignoring incoming message type with flags 0x%x", flags)
		return nil
	}

	if _, err := writeFull(frame, writer); err != nil {
		return err
	}
	if complete {
		writer.Close()
	}
	return nil
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

// pendingResponses is accessed from both the receiveLoop goroutine and the sender's goroutine,
// so it needs synchronization.
func (r *receiver) awaitResponse(number MessageNumber, writer io.WriteCloser) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.pendingResponses[number] = writer
	if number > r.maxPendingResponseNumber {
		r.maxPendingResponseNumber = number
	}
}

func (r *receiver) getPendingResponse(requestNumber MessageNumber, remove bool) (writer io.WriteCloser, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	writer = r.pendingResponses[requestNumber]
	if writer != nil {
		if remove {
			delete(r.pendingResponses, requestNumber)
		}
	} else if requestNumber <= r.maxPendingResponseNumber {
		log.Printf("BLIP: Unexpected response frame to my msg #%d", requestNumber) // benign
	} else {
		err = fmt.Errorf("Bogus message number %d in response", requestNumber)
	}
	return
}

func (r *receiver) backlog() (pendingRequest, pendingResponses int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.pendingRequests), len(r.pendingResponses)
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
