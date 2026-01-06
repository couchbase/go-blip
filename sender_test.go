/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStopSenderClearsAllMessageQueues(t *testing.T) {
	blipContextEchoServer, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

	server := blipContextEchoServer.WebSocketServer()
	http.Handle("/TestStopSenderClearsAllMessageQueues", server)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Error(http.Serve(listener, nil))
	}()

	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestStopSenderClearsAllMessageQueues", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket: %v", err)
	}

	// Add some messages to the queue + icebox queue
	for i := 0; i < 10; i++ {
		msg := NewRequest()
		msgProp := Properties{
			"id": fmt.Sprint(i),
		}
		msg.Properties = msgProp
		sender.queue.push(msg)
	}
	for i := 10; i < 15; i++ {
		msg := NewRequest()
		msgProp := Properties{
			"id": fmt.Sprint(i),
		}
		msg.Properties = msgProp
		sender.icebox[msgKey{msgNo: MessageNumber(i)}] = msg
	}

	// close sender
	sender.Close()

	// assert both icebox and queue is empty
	assert.Equal(t, 0, len(sender.queue.queue))
	assert.Equal(t, 0, len(sender.icebox))

}

func TestSenderIceboxPanicAfterClosure(t *testing.T) {

	blipContextEchoServer, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

	server := blipContextEchoServer.WebSocketServer()
	http.Handle("/TestSenderIceboxPanicAfterClosure", server)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Error(http.Serve(listener, nil))
	}()

	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestSenderIceboxPanicAfterClosure", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket: %v", err)
	}

	// close sender
	sender.Close()

	// try requeueing messages after closure
	for i := 1; i < 20; i++ {
		msg := NewRequest()
		msgProp := Properties{
			"id": fmt.Sprint(i),
		}
		msg.Properties = msgProp
		sender.requeue(msg, 500000)
	}
}
