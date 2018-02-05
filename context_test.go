package blip

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"golang.org/x/net/websocket"
)

// This was added in reaction to https://github.com/couchbase/sync_gateway/issues/3268 to either
// confirm or deny erroneous behavior w.r.t sockets being abruptly closed.  The main question attempted
// to be answered is:
//
// - If one side abruptly closes a connection, will the other side receive an error or be "stuck" indefinitely
//
// Test:
//
// - Start two blip contexts: an echo server and an echo client
// - The echo server is configured to respond to incoming echo requests and return responses, with the twist
//       that it abruptly terminates websocket before returning from callback
// - The echo client tries to read the response after sending the request
// - Expected: the echo client should receive some sort of error when trying to read the response, since the server abruptly terminated the connection
// - Actual: the echo client blocks indefinitely trying to read the response
//
func TestAbruptlyCloseConnectionBehavior(t *testing.T) {

	blipContextEchoServer := NewContext()

	receivedRequests := sync.WaitGroup{}

	// ----------------- Setup Echo Server that abruptly terminates socket -------------------------

	// Create a blip profile handler to respond to echo requests and then abruptly close the socket
	dispatchEcho := func(request *Message) {
		defer receivedRequests.Done()
		body, err := request.Body()
		if err != nil {
			log.Printf("ERROR reading body of %s: %s", request, err)
			return
		}
		if request.Properties["Content-Type"] != "application/octet-stream" {
			panic(fmt.Sprintf("Incorrect properties: %#x", request.Properties))
		}
		if response := request.Response(); response != nil {
			response.SetBody(body)
			response.Properties["Content-Type"] = request.Properties["Content-Type"]
		}

		// Try closing the connection to simulate behavior seen in SG #3268
		request.Sender.conn.Close()

	}

	// Blip setup
	blipContextEchoServer.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContextEchoServer.LogMessages = true
	blipContextEchoServer.LogFrames = true

	// Websocket Server
	server := blipContextEchoServer.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		defer func() {
			conn.Close() // in case it wasn't closed already
		}()
		defaultHandler(conn)
	}

	// HTTP Handler wrapping websocket server
	http.Handle("/blip", defaultHandler)
	go func() {
		log.Fatal(http.ListenAndServe(":12345", nil))
	}()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient := NewContext()
	sender, err := blipContextEchoClient.Dial("ws://localhost:12345/blip", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	// Create echo request
	echoRequest := NewRequest()
	echoRequest.SetProfile("BLIPTest/EchoData")
	echoRequest.Properties["Content-Type"] = "application/octet-stream"
	echoRequest.SetBody([]byte("hello"))
	receivedRequests.Add(1)

	// Send echo request
	sent := sender.Send(echoRequest)
	assert.True(t, sent)

	// Wait until the echo server profile handler was invoked and completely finished (and thus abruptly closed socket)
	receivedRequests.Wait()

	// Read the echo response
	response := echoRequest.Response()   // <-- blocks indefinitely here.
	responseBody, err := response.Body()

	// Assertions about echo response (these might need to be altered, maybe what's expected in this scenario is actually an error)
	assert.True(t, err == nil)
	assert.Equals(t, responseBody, []byte("hello"))

}
