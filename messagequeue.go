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
	"sync"
)

const kInitialQueueCapacity = 10

// A queue of outgoing messages. Used by Sender to schedule which frames to send.
type messageQueue struct {
	logContext      LogContext
	maxCount        int
	queue           []*msgSender
	numRequestsSent MessageNumber
	cond            *sync.Cond
	/* (instrumentation for perf testing)
	totalSize        int
	highestCount     int
	highestTotalSize int
	*/
}

func newMessageQueue(logContext LogContext, maxCount int) *messageQueue {
	return &messageQueue{
		logContext: logContext,
		queue:      make([]*msgSender, 0, kInitialQueueCapacity),
		cond:       sync.NewCond(&sync.Mutex{}),
		maxCount:   maxCount,
	}
}

func (q *messageQueue) _push(msg *msgSender, new bool) bool { // requires lock
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
			} else if new && !q.queue[index].inProgress {
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

	/* (instrumentation for perf testing)
	q.totalSize += len(msg.body)
	if n+1 > q.highestCount {
		q.highestCount = n + 1
		q.logContext.log("messageQueue total size = %d (%d messages)", q.totalSize, n+1)
	}
	if q.totalSize > q.highestTotalSize {
		q.highestTotalSize = q.totalSize
		q.logContext.log("messageQueue total size = %d (%d messages)", q.totalSize, n+1)
	}
	*/
	if len(q.queue) == 1 {
		q.cond.Signal() // It's non-empty now, so unblock a waiting pop()
	}

	return true
}

// Push an item into the queue
func (q *messageQueue) push(msg *msgSender) bool {
	return q.pushWithCallback(msg, nil)
}

// Push an item into the queue, also providing a callback function that will be invoked
// after the number is assigned to the message, but before pushing into the queue.
func (q *messageQueue) pushWithCallback(msg *msgSender, prepushCallback MessageCallback) bool {
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
		prepushCallback(msg.Message)
	}

	return q._push(msg, isNew)
}

func (q *messageQueue) _maybePop(actuallyPop bool) *msgSender {
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
		/* (instrumentation for perf testing)
		q.totalSize -= len(msg.body)
		*/
		if len(q.queue) == q.maxCount-1 {
			q.cond.Signal()
		}
	}
	return msg
}

func (q *messageQueue) first() *msgSender { return q._maybePop(false) }
func (q *messageQueue) pop() *msgSender   { return q._maybePop(true) }

func (q *messageQueue) find(msgNo MessageNumber, msgType MessageType) *msgSender {
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

	for _, message := range q.queue {
		message.cancelOutgoing()
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
