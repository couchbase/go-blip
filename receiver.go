package blip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"code.google.com/p/go.net/websocket"
)

type Receiver struct {
	context             *Context
	numRequestsReceived uint32
	pendingRequests     map[uint32]*Message
	numRequestsSent     uint32
	pendingResponses    map[uint32]*Message
	sender              *Sender
}

func NewReceiver(context *Context) *Receiver {
	return &Receiver{
		context:          context,
		pendingRequests:  map[uint32]*Message{},
		pendingResponses: map[uint32]*Message{},
	}
}

func (r *Receiver) ReceiveLoop(ws *websocket.Conn) error {
	for {
		// Receive the next raw WebSocket frame:
		var frame []byte
		if err := websocket.Message.Receive(ws, &frame); err != nil {
			return err
		}
		if err := r.handleIncomingFrame(frame); err != nil {
			return err
		}
	}
	return nil
}

func (r *Receiver) handleIncomingFrame(frame []byte) error {
	// Parse BLIP header:
	if len(frame) < 6 {
		return fmt.Errorf("Illegally short frame")
	}
	reader := bytes.NewReader(frame)
	var requestNumber uint32
	var flags frameFlags
	binary.Read(reader, binary.BigEndian, &requestNumber)
	binary.Read(reader, binary.BigEndian, &flags)
	frame = frame[6:]
	log.Printf("Received frame: #%3d, flags=%10b, length=%d", requestNumber, flags, len(frame))

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
				request = &Message{
					flags:  flags | kMoreComing,
					number: requestNumber,
				}
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
	case ResponseType:
	case ErrorType:
		{
			response := r.pendingResponses[requestNumber]
			if response != nil {
				if complete {
					delete(r.pendingResponses, requestNumber)
				}
				if err := response.receivedFrame(frame, flags); err != nil {
					return err
				}
				if response.complete {
					// Asynchronously dispatch the response to the app's handler
					go r.context.dispatchResponse(response)
				}

			} else if requestNumber <= r.numRequestsSent {
				log.Printf("?? Unexpected response frame to my msg #%d", requestNumber) // benign
			} else {
				return fmt.Errorf("Bogus message number %d in response", requestNumber)
			}
		}
	default:
		log.Printf("Ignoring incoming message type with flags 0x%x", flags)
	}
	return nil
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
