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
		msgProp := Properties{
			"id": fmt.Sprint(i),
		}
		sender.queue.push(&Message{Properties: msgProp, Outgoing: true})
	}
	for i := 10; i < 15; i++ {
		msgProp := Properties{
			"id": fmt.Sprint(i),
		}
		msg := &Message{Properties: msgProp}
		sender.icebox[msgKey{msgNo: MessageNumber(i)}] = msg
	}

	// close sender
	sender.Close()

	// assert both icebox and queue is empty
	assert.Equal(t, 0, len(sender.queue.queue))
	assert.Equal(t, 0, len(sender.icebox))

}
