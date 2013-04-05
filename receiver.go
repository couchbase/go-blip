package blip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"code.google.com/p/go.net/websocket"
)

type receiver struct {
	context                  *Context
	conn                     *websocket.Conn
	channel                  chan []byte
	numRequestsReceived      MessageNumber
	pendingRequests          map[MessageNumber]*Message
	pendingResponses         map[MessageNumber]*Message
	maxPendingResponseNumber MessageNumber
	sender                   *Sender
	mutex                    sync.Mutex
}

func newReceiver(context *Context, conn *websocket.Conn) *receiver {
	return &receiver{
		conn:             conn,
		context:          context,
		channel:          make(chan []byte, 10),
		pendingRequests:  map[MessageNumber]*Message{},
		pendingResponses: map[MessageNumber]*Message{},
	}
}

func (r *receiver) receiveLoop() error {
	go r.parseLoop()
	for {
		// Receive the next raw WebSocket frame:
		var frame []byte
		if err := websocket.Message.Receive(r.conn, &frame); err != nil {
			log.Printf("ReceiveLoop exiting with WebSocket error: %v", err)
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
			log.Printf("ReceiveLoop exiting with BLIP error: %v", err)
			return
		}
	}
}

func (r *receiver) handleIncomingFrame(frame []byte) error {
	// Parse BLIP header:
	if len(frame) < kFrameHeaderSize {
		return fmt.Errorf("Illegally short frame")
	}
	reader := bytes.NewReader(frame)
	var requestNumber MessageNumber
	var flags frameFlags
	binary.Read(reader, binary.BigEndian, &requestNumber)
	binary.Read(reader, binary.BigEndian, &flags)
	frame = frame[kFrameHeaderSize:]
	r.context.logFrame("Received frame: #%3d, flags=%10b, length=%d", requestNumber, flags, len(frame))

	complete := (flags & kMoreComing) == 0
	switch MessageType(flags & kTypeMask) {
	case RequestType:
		{
			request := r.pendingRequests[requestNumber]
			if request != nil {
				if complete {
					delete(r.pendingRequests, requestNumber)
				}
			} else if requestNumber == r.numRequestsReceived+1 {
				request = newIncomingMessage(requestNumber, flags)
				if !complete {
					r.pendingRequests[requestNumber] = request
				}
				r.numRequestsReceived++
			} else {
				return fmt.Errorf("Bad incoming request number %d", requestNumber)
			}

			if err := request.receivedFrame(frame, flags); err != nil {
				return err
			}
			if request.complete {
				// Asynchronously dispatch the request to the app's handler
				go r.context.dispatchRequest(request, r.sender)
			}
		}
	case ResponseType, ErrorType:
		{
			response, err := r.getPendingResponse(requestNumber, complete)
			if err != nil {
				return err
			}
			if response != nil {
				if err := response.receivedFrame(frame, flags); err != nil {
					return err
				}
				// Notify the original request that its response is available:
				if response.complete && response.inResponseTo != nil {
					go response.inResponseTo.responseComplete(response)
				}
			}
		}
	default:
		log.Printf("BLIP: Ignoring incoming message type with flags 0x%x", flags)
	}
	return nil
}

// pendingResponses is accessed from both the receiveLoop goroutine and the sender's goroutine,
// so it needs synchronization.
func (r *receiver) awaitResponse(response *Message) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.pendingResponses[response.number] = response
	if response.number > r.maxPendingResponseNumber {
		r.maxPendingResponseNumber = response.number
	}
}

func (r *receiver) getPendingResponse(requestNumber MessageNumber, remove bool) (response *Message, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	response = r.pendingResponses[requestNumber]
	if response != nil {
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
