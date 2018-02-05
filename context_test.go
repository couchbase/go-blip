package blip

import (
	"fmt"
	"log"
	"math/rand"
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
// - Start two blip contexts
// - Start ping-pong message between them
// - One side abruptly closes connection
// - Verify the other side is not stuck indefinitely
//
func TestAbruptlyCloseConnectionBehavior(t *testing.T) {

	blipContext := NewContext()

	receivedRequests := sync.WaitGroup{}
	beforeRespondedReqeust := sync.WaitGroup{}

	dispatchEcho := func(request *Message) {
		defer receivedRequests.Done()
		beforeRespondedReqeust.Done()
		body, err := request.Body()
		if err != nil {
			log.Printf("ERROR reading body of %s: %s", request, err)
			return
		}
		for i, b := range body {
			if b != byte(i%256) {
				panic(fmt.Sprintf("Incorrect body: %x", body))
			}
		}
		if request.Properties["Content-Type"] != "application/octet-stream" {
			panic(fmt.Sprintf("Incorrect properties: %#x", request.Properties))
		}
		if response := request.Response(); response != nil {
			response.SetBody(body)
			response.Properties["Content-Type"] = request.Properties["Content-Type"]
		}

		// Try closing the connection
		request.Sender.conn.Close()
	}

	blipContext.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContext.LogMessages = true
	blipContext.LogFrames = true

	// Server
	server := blipContext.WebSocketServer()
	defaultHandler := server.Handler
	server.Handler = func(conn *websocket.Conn) {
		defer func() {
			conn.Close() // in case it wasn't closed already
		}()
		defaultHandler(conn)
	}

	http.Handle("/blip", defaultHandler)

	go func() {
		log.Fatal(http.ListenAndServe(":12345", nil))
	}()

	context := NewContext()
	sender, err := context.Dial("ws://localhost:12345/blip", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	request := NewRequest()
	request.SetProfile("BLIPTest/EchoData")
	request.Properties["Content-Type"] = "application/octet-stream"
	body := make([]byte, rand.Intn(1024))
	for i := 0; i < len(body); i++ {
		body[i] = byte(i % 256)
	}
	request.SetBody(body)
	receivedRequests.Add(1)
	beforeRespondedReqeust.Add(1)
	sent := sender.Send(request)
	assert.True(t, sent)

	// close connection from sender side after handler received request, but before responded
	//beforeRespondedReqeust.Wait()
	//sender.conn.Close()

	log.Printf("sent request")

	receivedRequests.Wait()

	log.Printf("reading response")
	response := request.Response()
	log.Printf("reading response body")
	responseBody, err := response.Body()
	log.Printf("read response + body")

	assert.True(t, err == nil)
	log.Printf("responseBody: %s", responseBody)

}

//func dispatchEcho(request *Message) {
//	body, err := request.Body()
//	if err != nil {
//		log.Printf("ERROR reading body of %s: %s", request, err)
//		return
//	}
//	for i, b := range body {
//		if b != byte(i%256) {
//			panic(fmt.Sprintf("Incorrect body: %x", body))
//		}
//	}
//	if request.Properties["Content-Type"] != "application/octet-stream" {
//		panic(fmt.Sprintf("Incorrect properties: %#x", request.Properties))
//	}
//	if response := request.Response(); response != nil {
//		response.SetBody(body)
//		response.Properties["Content-Type"] = request.Properties["Content-Type"]
//	}
//}
