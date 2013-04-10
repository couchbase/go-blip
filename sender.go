package blip

import (
	"bytes"
	"encoding/binary"

	"code.google.com/p/go.net/websocket"
)

// Size of frame to send by default. This is arbitrary.
const kDefaultFrameSize = 4096
const kBigFrameSize = 4 * kDefaultFrameSize

// The sending side of a BLIP connection. Used to send requests and to close the connection.
type Sender struct {
	context         *Context
	conn            *websocket.Conn
	receiver        *receiver
	queue           *messageQueue
	numRequestsSent MessageNumber
}

func newSender(context *Context, conn *websocket.Conn, receiver *receiver) *Sender {
	return &Sender{
		context:  context,
		conn:     conn,
		receiver: receiver,
		queue:    newMessageQueue(context),
	}
}

// Sends a new outgoing request to be delivered asynchronously.
// Returns false if the message can't be queued because the Sender has stopped.
func (sender *Sender) Send(msg *Message) bool {
	if msg.Type() != RequestType {
		panic("Don't send responses using Sender.Send")
	} else if !msg.Outgoing {
		panic("Can't send an incoming message")
	}
	return sender.send(msg)
}

// Posts a request or response to be delivered asynchronously.
// Returns false if the message can't be queued because the Sender has stopped.
func (sender *Sender) send(msg *Message) bool {
	if msg.encoder != nil {
		panic("Message is already enqueued")
	}

	if !sender.queue.push(msg) {
		return false
	}

	if msg.Type() == RequestType && !msg.NoReply() {
		response := msg.createResponse()
		writer := response.asyncRead(func(err error) {
			msg.responseComplete(response)
		})
		sender.receiver.awaitResponse(response.number, writer)
	}
	return true
}

// Returns statistics about the number of incoming and outgoing messages queued.
func (sender *Sender) Backlog() (incomingRequests, incomingResponses, outgoingRequests, outgoingResponses int) {
	incomingRequests, incomingResponses = sender.receiver.backlog()
	outgoingRequests, outgoingResponses = sender.queue.backlog()
	return
}

// Stops the sender's goroutine.
func (sender *Sender) Stop() {
	sender.queue.stop()
}

func (sender *Sender) Close() {
	sender.Stop()
	sender.conn.Close()
}

// Spawns a goroutine that will write frames to the connection until Stop() is called.
func (sender *Sender) start() {
	sender.conn.PayloadType = websocket.BinaryFrame
	go (func() {
		sender.context.log("Sender starting...")
		buffer := bytes.NewBuffer(make([]byte, 0, kBigFrameSize))
		for {
			msg := sender.queue.pop()
			if msg == nil {
				break
			}
			// As an optimization, allow message to send a big frame unless there's a higher-priority
			// message right behind it:
			maxSize := kBigFrameSize
			if !msg.Urgent() && sender.queue.nextMessageIsUrgent() {
				maxSize = kDefaultFrameSize
			}

			body, flags := msg.nextFrameToSend(maxSize - kFrameHeaderSize)
			
			sender.context.logFrame("Sending frame: %v (flags=%8b, size=%5d", msg, flags, len(body))
			binary.Write(buffer, binary.BigEndian, msg.number)
			binary.Write(buffer, binary.BigEndian, flags)
			buffer.Write(body)
			sender.conn.Write(buffer.Bytes())
			buffer.Reset()

			if (flags & kMoreComing) != 0 {
				if len(body) == 0 {
					panic("empty frame should not have moreComing")
				}
				sender.queue.push(msg) // requeue it so it can send its next frame later
			}
		}
		sender.context.log("Sender stopped")
	})()
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
