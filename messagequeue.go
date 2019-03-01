package blip

import (
	"sync"
)

const kInitialQueueCapacity = 10

// A queue of outgoing messages. Used by Sender to schedule which frames to send.
type messageQueue struct {
	logContext      LogContext
	maxCount        int
	queue           []*Message
	numRequestsSent MessageNumber
	cond            *sync.Cond
}

func newMessageQueue(logContext LogContext, maxCount int) *messageQueue {
	return &messageQueue{
		logContext: logContext,
		queue:      make([]*Message, 0, kInitialQueueCapacity),
		cond:       sync.NewCond(&sync.Mutex{}),
		maxCount:   maxCount,
	}
}

func (q *messageQueue) _push(msg *Message, new bool) bool { // requires lock
	if !msg.Outgoing {
		panic("Not an outgoing message")
	}

	if q.isStopped() {
		return false
	}
	q.logContext.logFrame("Push %v", msg)

	index := 0
	n := len(q.queue)
	if msg.Urgent() && n > 1 {
		// High-priority gets queued after the last existing high-priority message,
		// leaving one regular-priority message in between if possible.
		for index = n - 1; index >= 0; index-- {
			if q.queue[index].Urgent() {
				index += 2
				break
			} else if new && q.queue[index].encoder == nil {
				// But have to keep message starts in order
				index += 1
				break
			}
		}
		if index <= 0 {
			index = 1
		} else if index > n {
			index = n
		}
	} else {
		// Regular priority goes at the end of the queue:
		index = n
	}

	// Insert msg at index:
	q.queue = append(q.queue, nil)
	copy(q.queue[index+1:n+1], q.queue[index:n])
	q.queue[index] = msg

	if len(q.queue) == 1 {
		q.cond.Signal() // It's non-empty now, so unblock a waiting pop()
	}
	return true
}

// Push an item into the queue
func (q *messageQueue) push(msg *Message) bool {
	return q.pushWithCallback(msg, nil)
}

// Push an item into the queue, also providing a callback function that will be invoked
// after the number is assigned to the message, but before pushing into the queue.
func (q *messageQueue) pushWithCallback(msg *Message, prepushCallback MessageCallback) bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	// Exit early if we know the queue has already been stopped
	if q.isStopped() {
		return false
	}

	isNew := msg.number == 0
	if isNew {
		// When adding a new message, block till the queue is under its maxCount:
		for q.maxCount > 0 && len(q.queue) >= q.maxCount && q.queue != nil {
			q.cond.Wait()
		}

		if msg.Type() != RequestType {
			panic("Response has no number")
		}
		q.numRequestsSent++
		msg.number = q.numRequestsSent
		q.logContext.logMessage("Queued %s", msg)
	}

	if prepushCallback != nil {
		prepushCallback(msg)
	}

	return q._push(msg, isNew)
}

func (q *messageQueue) _maybePop(actuallyPop bool) *Message {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 && q.queue != nil {
		q.cond.Wait()
	}

	if q.queue == nil {
		return nil
	}

	msg := q.queue[0]
	if actuallyPop {
		q.queue = q.queue[1:]

		if len(q.queue) == q.maxCount-1 {
			q.cond.Signal()
		}
	}
	return msg
}

func (q *messageQueue) first() *Message { return q._maybePop(false) }
func (q *messageQueue) pop() *Message   { return q._maybePop(true) }

func (q *messageQueue) find(msgNo MessageNumber, msgType MessageType) *Message {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for _, message := range q.queue {
		if message.number == msgNo && message.Type() == msgType {
			return message
		}
	}
	return nil
}

// Stops the sender's goroutine.
func (q *messageQueue) stop() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()


	// Iterate over messages and call close on every message's readcloser, since it's possible that
	// a goroutine may be blocked on the reader, thus causing a resource leak.  Added during SG #3268
	// diagnosis, but does not fix any reproducible issues.
	for _, message := range q.queue {
		if message.reader == nil {
			continue
		}
		err := message.reader.Close()
		if err != nil {
			q.logContext.logMessage("Warning: messageQueue encountered error closing message reader while stopping. Error: %v", err)
		}
	}

	q.queue = nil
	q.cond.Broadcast()


}

func (q *messageQueue) isStopped() bool {
	return q.queue == nil
}

func (q *messageQueue) nextMessageIsUrgent() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue) > 0 && q.queue[0].Urgent()
}

func (q *messageQueue) length() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue)
}

// Returns statistics about the number of incoming and outgoing messages queued.
func (q *messageQueue) backlog() (outgoingRequests, outgoingResponses int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for _, message := range q.queue {
		if message.Type() == RequestType {
			outgoingRequests++
		}
	}
	outgoingResponses = len(q.queue) - outgoingRequests
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
