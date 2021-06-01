/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	"bytes"
	"io/ioutil"
	"log"
	"sync"
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestMessagePushPop(t *testing.T) {

	// Create a message queue
	maxSendQueueCount := 5
	mq := newMessageQueue(&TestLogContext{silent: true}, maxSendQueueCount)

	// Push a non-urgent message into the queue
	for i := 0; i < 2; i++ {
		msg := NewRequest()
		pushed := mq.push(msg)
		assert.True(t, pushed)
		assert.False(t, mq.nextMessageIsUrgent())
		if i == 0 {
			assert.True(t, mq.first().SerialNumber() == msg.SerialNumber())
		}
	}

	// Push an urgent message into the queue
	urgentMsg := NewRequest()
	urgentMsg.SetUrgent(true)
	pushed := mq.push(urgentMsg)
	assert.True(t, pushed)

	// Since none of the normal messages have had any frames sent
	// per https://github.com/couchbaselabs/BLIP-Cpp/blob/master/docs/BLIP%20Protocol.md#32-sending-messages,
	// the urgent message should _not_ skip to the head of the queue, and so the first message popped from the
	// queue should be the first nomrmal message that was added to the queue
	assert.True(t, mq.first().SerialNumber() == MessageNumber(1))

	// Try to find the urgent message by it's serial number
	foundMsg := mq.find(urgentMsg.SerialNumber(), RequestType)
	assert.True(t, foundMsg != nil)
	assert.True(t, foundMsg.Urgent())

	// Pop the normal messages from the queue
	for i := 0; i < 2; i++ {
		mq.pop()
	}

	// Now the next message should be the urgent message
	assert.True(t, mq.nextMessageIsUrgent())

	// Pop the urgent message from the queue
	mq.pop()

	// Assert that it's empty at this point
	assert.True(t, mq.length() == 0)

}

func TestConcurrentAccess(t *testing.T) {

	// Create a message queue
	maxSendQueueCount := 5
	mq := newMessageQueue(&TestLogContext{silent: true}, maxSendQueueCount)

	// Fill it up to capacity w/ normal messages
	for i := 0; i < maxSendQueueCount; i++ {
		msg := NewRequest()
		pushed := mq.push(msg)
		assert.True(t, pushed)
		assert.False(t, mq.nextMessageIsUrgent())
	}

	doneWg := sync.WaitGroup{}
	doneWg.Add(2)
	pusher := func() {
		for i := 0; i < 100; i++ {
			msg := NewRequest()
			pushed := mq.push(msg)
			assert.True(t, pushed)
		}
		doneWg.Done()
	}
	go pusher()

	popper := func() {
		for i := 0; i < 100; i++ {
			mq.pop()
		}
		doneWg.Done()
	}
	go popper()

	doneWg.Wait()

	// Empty the queue
	for i := 0; i < maxSendQueueCount; i++ {
		mq.pop()
	}

	// Assert that it's empty at this point
	assert.True(t, mq.length() == 0)

}

// From https://github.com/couchbaselabs/BLIP-Cpp/blob/master/docs/BLIP%20Protocol.md#32-sending-messages:
//
// An urgent message is placed after the last other urgent message in the queue.
//
// If there are one or more normal messages after that one, the message is inserted after the first normal
// message (this prevents normal messages from being starved and never reaching the head of the queue.)
// Or if there are no urgent messages in the queue, the message is placed after the first normal message.
// If there are no messages at all, then there's only one place to put the message, of course.
//
// When a newly-ready urgent message is being added to the queue for the first time (in step 1 above),
// it has the additional restriction that it must go after any other message that has not yet had any
// of its frames sent. (This is so that messages are begun in sequential order; otherwise the first
// frame of urgent message number 10 might be sent before the first frame of regular message number 8,
// for example.)
//
//
// Time  Queue (head is far right)
// ----  -------------------------
// T1: [n1]
// T2: [n2] [n1]
// T3: [n3] [n2] [n1]
// T4: [n4] [n3] [n2] [n1]
// T5: [n5] [n4] [n3] [n2] [n1]
// T5: [n5] [n4] [n3] [n2] [u6] [n1]
// T6: [n5] [n4] [n3] [n2] [u6]
// T7: [n5] [n4] [n3] [u7] [n2] [u6]

func TestUrgentMessageOrdering(t *testing.T) { // Test passes, but some assertions commented

	// Create a message queue
	maxSendQueueCount := 25
	mq := newMessageQueue(&TestLogContext{silent: true}, maxSendQueueCount)

	// Add normal messages that are "in-progress" since they have a non-nil msg.encoder
	for i := 0; i < 5; i++ {
		msg := NewRequest()
		pushed := mq.push(msg)
		assert.True(t, pushed)
		assert.False(t, mq.nextMessageIsUrgent())

		// set the msg.encoder to something so that the next urgent message will go to the head of the line
		msg.encoder = ioutil.NopCloser(&bytes.Buffer{})

	}

	// The head of the queue should be a normal message with serial number 1 (n1)
	assert.True(t, mq.first().SerialNumber() == MessageNumber(1))

	// Push an urgent message into the queue
	urgentMsg := NewRequest()
	urgentMsg.SetUrgent(true)
	pushed := mq.push(urgentMsg)
	assert.True(t, pushed)

	// T5: [n5] [n4] [n3] [n2] [u6] [n1]
	// Since there are normal messages in the queue, the urgent message should be inserted _after_ the
	// the first normal message (to prevent normal messages from being starved and never reaching the head of the queue),
	// so head of the queue should _still_ be a normal message with serial number 1 (n1)
	assert.True(t, mq.first().SerialNumber() == MessageNumber(1))
	assert.False(t, mq.first().Urgent())

	// Pop the normal message from the head of the queue
	mq.pop()

	// T6: [n5] [n4] [n3] [n2] [u6]
	// Since all the normal messages have had frames sent (faked, via non-nil msg.encoder), then the
	// urgent message should have skipped to the head of the line
	// assert.True(t, mq.nextMessageIsUrgent())
	headOfLine := mq.first()
	assert.True(t, headOfLine.Urgent())

	// Push another urgent message
	// T7: [n5] [n4] [n3] [u7] [n2] [u6]
	anotherUrgentMsg := NewRequest()
	anotherUrgentMsg.SetUrgent(true)
	pushed = mq.push(anotherUrgentMsg)
	assert.True(t, pushed)

	// The head of the queue should be the first urgent message
	headOfLine = mq.pop()
	assert.True(t, headOfLine.Urgent())
	assert.True(t, headOfLine.SerialNumber() == urgentMsg.SerialNumber())

	// Followed by a normal message, since the second urgent message should have been placed _after_ a normal message
	headOfLine = mq.pop()
	assert.False(t, headOfLine.Urgent())

	// Followed by the less urgent message
	headOfLine = mq.pop()
	assert.True(t, headOfLine.Urgent())
	assert.True(t, headOfLine.SerialNumber() == anotherUrgentMsg.SerialNumber())

	// Followed by 3 normal messages
	headOfLine = mq.pop()
	assert.False(t, headOfLine.Urgent())
	headOfLine = mq.pop()
	assert.False(t, headOfLine.Urgent())
	headOfLine = mq.pop()
	assert.False(t, headOfLine.Urgent())

	// Now the queue should be empty
	assert.True(t, mq.length() == 0)
}

type TestLogContext struct {
	silent bool
	count  int
}

func (tlc *TestLogContext) log(fmt string, params ...interface{}) {
	if !tlc.silent {
		log.Printf(fmt, params...)
	}
	tlc.count++
}

func (tlc *TestLogContext) logMessage(fmt string, params ...interface{}) {
	if !tlc.silent {
		log.Printf(fmt, params...)
	}
	tlc.count++
}

func (tlc *TestLogContext) logFrame(fmt string, params ...interface{}) {
	if !tlc.silent {
		log.Printf(fmt, params...)
	}
	tlc.count++
}
