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
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Round trip a high number of messages over a loopback websocket
// It was originally an attempt to repro SG issue https://github.com/couchbase/sync_gateway/issues/3221 isolated
// to go-blip.  This test was not able to repro that particular error, but is useful in it's own right as a
// regression test or starting point for other test cases that require two peers running over an actual websocket,
// aka a "functional test".
func TestEchoRoundTrip(t *testing.T) {

	blipContextEchoServer, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

	receivedRequests := sync.WaitGroup{}

	// ----------------- Setup Echo Server  -------------------------

	// Create a blip profile handler to respond to echo requests
	dispatchEcho := func(request *Message) {
		body, err := request.Body()
		if err != nil {
			log.Printf("ERROR reading body of %s: %s", request, err)
			return
		}
		if request.Properties["Content-Type"] != "application/octet-stream" {
			t.Fatalf("Incorrect properties: %#x", request.Properties)
		}
		if response := request.Response(); response != nil {
			response.SetBody(body)
			response.Properties["Content-Type"] = request.Properties["Content-Type"]
		}

	}

	// Blip setup
	blipContextEchoServer.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContextEchoServer.LogMessages = true
	blipContextEchoServer.LogFrames = true

	// Websocket Server
	listener := startTestListener(t, blipContextEchoServer)
	defer listener.Close()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

	sender := startTestClient(t, blipContextEchoClient, listener)
	defer sender.Close()

	numRequests := 100
	receivedRequests.Add(numRequests * 50)

	for i := 0; i < numRequests; i++ {

		// Create echo request
		echoRequest := NewRequest()
		echoRequest.SetProfile("BLIPTest/EchoData")
		echoRequest.Properties["Content-Type"] = "application/octet-stream"
		echoRequest.SetBody([]byte("hello"))

		// Send echo request
		sent := sender.Send(echoRequest)
		assert.True(t, sent)

		// Have multiple readers simultaneously trying to fetch multiple responses for each message and assert on the body.
		// reliably triggers races on m.Response and response.Body that have been seen only rarely in Sync Gateway testing.
		for i := 0; i < 5; i++ {
			go func() {
				for j := 0; j < 10; j++ {
					go func(m *Message) {
						response := m.Response()
						defer receivedRequests.Done()
						if response == nil {
							t.Errorf("unexpected nil message response")
							return
						}
						responseBody, err := response.Body()
						assert.NoError(t, err)
						//log.Printf("Got response: %s", responseBody)
						assert.Equal(t, "hello", string(responseBody))
					}(echoRequest)
				}
			}()
		}

	}

	// Wait until all requests were sent and responded to
	receivedRequests.Wait()
	log.Printf("*** Closing connections ***")
}

// TestSenderPing ensures a client configured with a WebsocketPingInterval sends ping frames on an otherwise idle connection.
func TestSenderPing(t *testing.T) {

	// server
	serverCtx, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	listener := startTestListener(t, serverCtx)
	defer listener.Close()

	// client
	clientCtx, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	clientCtx.LogMessages = true
	clientCtx.LogFrames = true
	clientCtx.WebsocketPingInterval = time.Millisecond * 10

	// client hasn't connected yet, stats are uninitialized
	assert.Equal(t, int64(0), expvarToInt64(goblipExpvar.Get("sender_ping_count")))
	assert.Equal(t, int64(0), expvarToInt64(goblipExpvar.Get("goroutines_sender_ping")))

	sender := startTestClient(t, clientCtx, listener)

	time.Sleep(time.Millisecond * 50)

	// an active ping goroutine with at least 1 ping sent for the above sleep
	assert.Equal(t, int64(1), expvarToInt64(goblipExpvar.Get("goroutines_sender_ping")))
	assert.True(t, expvarToInt64(goblipExpvar.Get("sender_ping_count")) > 0)

	sender.Close()

	// ensure the sender's ping goroutine has exited
	assert.Equal(t, int64(0), expvarToInt64(goblipExpvar.Get("goroutines_sender_ping")))
}

func expvarToInt64(v expvar.Var) int64 {
	if v == nil {
		return 0
	}
	i, _ := strconv.ParseInt(v.String(), 10, 64)
	return i
}

func BenchmarkMessage(b *testing.B) {

	blipServer, err := NewContext(defaultContextOptions)
	require.NoError(b, err)

	handleMessage := func(request *Message) {
		_, err := request.Body()
		if err != nil {
			b.Fatalf("Failed to read body: %v", err)
		}
		response := request.Response()
		response.SetBody([]byte("[]"))
	}

	profileName := "Profile"

	// Blip setup
	blipServer.HandlerForProfile[profileName] = handleMessage

	// Websocket Server
	server := blipServer.WebSocketServer()

	// HTTP Handler wrapping websocket server
	mux := http.NewServeMux()
	mux.Handle("/blip", server)
	listener, err := net.Listen("tcp", ":0")
	require.NoError(b, err)
	go func() {
		require.NoError(b, http.Serve(listener, mux))
	}()

	blipContextClient, err := NewContext(defaultContextOptions)
	require.NoError(b, err)
	blipContextClient.LogMessages = true
	blipContextClient.LogFrames = true
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/blip", port)
	sender, err := blipContextClient.Dial(destUrl)
	require.NoError(b, err)

	b.ResetTimer()
	for range b.N {
		request := NewRequest()
		request.SetProfile(profileName)
		request.SetBody([]byte("hello"))

		if sender.Send(request) != true {
			b.Fatalf("Failed to send message")
		}
		// block to read request
		_ = request.Response()

	}
}
