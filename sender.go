package blip

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"

	"code.google.com/p/go.net/websocket"
)

// Size of frame to send by default. This is arbitrary.
const kDefaultFrameSize = 4096

// The sending side of a BLIP connection. Used to send requests and to close the connection.
type Sender struct {
	conn            *websocket.Conn
	receiver        *receiver
	queue           []*Message
	cond            *sync.Cond
	numRequestsSent uint32
}

func newSender(conn *websocket.Conn, receiver *receiver) *Sender {
	return &Sender{
		conn:     conn,
		receiver: receiver,
		queue:    []*Message{},
		cond:     sync.NewCond(&sync.Mutex{}),
	}
}

func (sender *Sender) pop() *Message {
	sender.cond.L.Lock()
	defer sender.cond.L.Unlock()
	for len(sender.queue) == 0 && sender.queue != nil {
		sender.cond.Wait()
	}

	if sender.queue == nil {
		return nil
	}

	msg := sender.queue[0]
	sender.queue = sender.queue[1:]
	return msg
}

func (sender *Sender) _push(msg *Message, new bool) bool { // requires lock
	if !msg.Outgoing {
		panic("Not an outgoing message")
	}
	if sender.queue == nil {
		return false
	}
	log.Printf("Push %v", msg)

	index := 0
	n := len(sender.queue)
	if msg.Urgent() && n > 1 {
		// High-priority gets queued after the last existing high-priority message,
		// leaving one regular-priority message in between if possible.
		for index = n - 1; index > 0; index-- {
			if sender.queue[index].Urgent() {
				index += 2

				break
			} else if new && sender.queue[index].encoded == nil {
				// But have to keep message starts in order
				index += 1
				break
			}
		}
		if index == 0 {
			index = 1
		} else if index > n {
			index = n
		}
	} else {
		// Regular priority goes at the end of the queue:
		index = n
	}

	// Insert msg at index:
	sender.queue = append(sender.queue, nil)
	copy(sender.queue[index+1:n+1], sender.queue[index:n])
	sender.queue[index] = msg

	if len(sender.queue) == 1 {
		sender.cond.Signal()
	}
	return true
}

func (sender *Sender) requeue(msg *Message) bool {
	sender.cond.L.Lock()
	defer sender.cond.L.Unlock()

	return sender._push(msg, false)
}

// Posts a message to be delivered asynchronously.
// Returns false if the message can't be queued because the sender has stopped.
func (sender *Sender) send(msg *Message) bool {
	if msg.encoded != nil {
		panic("Message is already enqueued")
	}

	sender.cond.L.Lock()
	defer sender.cond.L.Unlock()

	if msg.Type() == RequestType {
		sender.numRequestsSent++
		msg.number = sender.numRequestsSent
		if !msg.NoReply() {
			sender.receiver.awaitResponse(msg.Response())
		}
	}
	log.Printf("Sending message: %s", msg)
	return sender._push(msg, true)
}

// Sends a new outgoing request.
// Returns false if the message can't be queued because the Sender has stopped.
func (sender *Sender) Send(msg *Message) bool {
	if msg.Type() != RequestType {
		panic("Don't send responses using Sender.Send")
	}
	return sender.send(msg)
}

// Stops the sender's goroutine.
func (sender *Sender) Stop() {
	sender.cond.L.Lock()
	defer sender.cond.L.Unlock()

	sender.queue = nil
	sender.cond.Broadcast()
}

func (sender *Sender) Close() {
	sender.Stop()
	sender.conn.Close()
}

// Spawns a goroutine that will write frames to the connection until Stop() is called.
func (sender *Sender) start() {
	sender.conn.PayloadType = websocket.BinaryFrame
	go (func() {
		log.Printf("Sender starting...")
		for {
			msg := sender.pop()
			if msg == nil {
				break
			}
			frameSize := kDefaultFrameSize
			// As an optimization, allow message to send a big frame unless there's a higher-priority
			// message right behind it:
			if msg.Urgent() || len(sender.queue) == 0 || !sender.queue[0].Urgent() {
				frameSize *= 4
			}

			body, flags := msg.nextFrameToSend(frameSize)
			log.Printf("Sending frame: %v (flags=%8b, size=%5d", msg, flags, len(body))
			var frame bytes.Buffer
			binary.Write(&frame, binary.BigEndian, msg.number)
			binary.Write(&frame, binary.BigEndian, flags)
			frame.Write(body)
			sender.conn.Write(frame.Bytes())

			if (flags & kMoreComing) != 0 {
				sender.requeue(msg) // requeue it so it can send its next frame later
			}
		}
		log.Printf("Sender stopped")
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
