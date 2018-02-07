package blip

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"net/http"

	"github.com/couchbaselabs/go.assert"
	"golang.org/x/net/websocket"
	"time"
)

// Reproduce https://github.com/couchbase/sync_gateway/issues/3221
func TestHighMessageCountPingPong(t *testing.T) {

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

	}

	// Blip setup
	blipContextEchoServer.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContextEchoServer.LogMessages = false
	blipContextEchoServer.LogFrames = false

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
		log.Fatal(http.ListenAndServe(":12345", nil)) // TODO: use dynamic port
	}()

	// ----------------- Setup Echo Client ----------------------------------------

	time.Sleep(time.Second)

	blipContextEchoClient := NewContext()
	sender, err := blipContextEchoClient.Dial("ws://localhost:12345/blip", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	for i := 0; i < 150000; i++ {

		// Create echo request
		echoRequest := NewRequest()
		echoRequest.SetProfile("BLIPTest/EchoData")
		echoRequest.Properties["Content-Type"] = "application/octet-stream"
		echoRequest.SetBody([]byte("hello"))
		receivedRequests.Add(1)

		// Send echo request
		sent := sender.Send(echoRequest)
		assert.True(t, sent)

		// Read the echo response
		response := echoRequest.Response()
		responseBody, err := response.Body()
		// log.Printf("responseBody: %v, err: %v", responseBody, err)
		assert.True(t, err == nil)
		assert.Equals(t, string(responseBody), "hello")

	}

	// Wait until the echo server profile handler was invoked and completely finished (and thus abruptly closed socket)
	receivedRequests.Wait()




}
