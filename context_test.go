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
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
)

// The application protocol id of the BLIP websocket subprotocol used in go-blip unit tests
const BlipTestAppProtocolId = "GoBlipUnitTests"

var defaultContextOptions = ContextOptions{ProtocolIds: []string{BlipTestAppProtocolId}}

// This was added in reaction to https://github.com/couchbase/sync_gateway/issues/3268 to either
// confirm or deny erroneous behavior w.r.t sockets being abruptly closed.  The main question attempted
// to be answered is:
//
// - If server side abruptly closes a connection, will the client side receive an error or be "stuck" indefinitely
//
// Test:
//
//   - Start two blip contexts: an echo server and an echo client
//   - The echo server is configured to respond to incoming echo requests and return responses, with the twist
//     that it abruptly terminates websocket before returning from callback
//   - The echo client tries to read the response after sending the request
//   - Expected: the echo client should receive some sort of error when trying to read the response, since the server abruptly terminated the connection
//   - Actual: the echo client blocks indefinitely trying to read the response
func TestServerAbruptlyCloseConnectionBehavior(t *testing.T) {

	blipContextEchoServer, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

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
			t.Fatalf("Incorrect properties: %#x", request.Properties)
		}
		if response := request.Response(); response != nil {
			response.SetBody(body)
			response.Properties["Content-Type"] = request.Properties["Content-Type"]
		}

		// Try closing the connection to simulate behavior seen in SG #3268
		_ = request.Sender.conn.Close(websocket.StatusNoStatusRcvd, "")

	}

	// Blip setup
	blipContextEchoServer.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContextEchoServer.LogMessages = true
	blipContextEchoServer.LogFrames = true

	// Websocket Server
	server := blipContextEchoServer.WebSocketServer()

	// HTTP Handler wrapping websocket server
	http.Handle("/TestServerAbruptlyCloseConnectionBehavior", server)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Error(http.Serve(listener, nil))
	}()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestServerAbruptlyCloseConnectionBehavior", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket: %v", err)
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
	err = WaitWithTimeout(&receivedRequests, time.Second*60)
	if err != nil {
		t.Fatal(err)
	}

	// Read the echo response
	response := echoRequest.Response() // <--- SG #3268 was causing this to block indefinitely
	responseBody, err := response.Body()

	// Assertions about echo response (these might need to be altered, maybe what's expected in this scenario is actually an error)
	assert.True(t, err == nil)
	assert.True(t, len(responseBody) == 0)

	// TODO: add more assertions about the response.  I'm not seeing any errors, or any
	// TODO: way to differentiate this response with a normal response other than having an empty body

}

/*
This was added in reaction to https://github.com/couchbase/sync_gateway/issues/3268 to either
confirm or deny erroneous behavior w.r.t sockets being abruptly closed.  The main question attempted
to be answered is:

- If the client side abruptly closes a connection during a pending server request that requires a response,
will the server side goroutine trying to read the response be blocked indefinitely?

The initial Echo Request and Echo Response might be unnecessary -- it was used to provide a way for the server
to issue an outbound request to a client.  Is there a simpler way?

The test does the following steps:

┌─────────────────────────────┐                                  ┌─────────────────────────────┐
│    blipContextEchoClient    │                                  │    blipContextEchoServer    │
└──────────────┬──────────────┘                                  └──────────────┬──────────────┘

	│                                                                │
	├──────────────────────────────Dial──────────────────────────────▶
	│                                                                │
	│                               Echo                             │
	├─────────────────────────────Request────────────────────────────▶
	│                                                                │
	│                              Echo                              │
	◀────────────────────────────Response────────────────────────────┤
	│                                                                │
	│                               Echo                             │
	◀─────────────────────────────Amplify ───────────────────────────┤
	│                             Request                            │
	│                                                                │
	│                              Close                             │
	├────────────────────────────Connection──────────────────────────▶
	│                                                                │
	│                                                                │
	│                        Echo Response Never                     │
	├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─Happens─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶
	│                          Shouldn't Block                       │
	│                                                                │
	│                                                                │
	│                                                                │
	▼                                                                ▼
*/
func TestClientAbruptlyCloseConnectionBehavior(t *testing.T) {

	blipContextEchoServer, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}

	receivedEchoRequest := sync.WaitGroup{}
	echoAmplifyRoundTripComplete := sync.WaitGroup{}

	// ----------------- Setup Echo Server that abruptly terminates socket -------------------------

	// This "amplifies" the echo by sending an additional outbound request to the client
	// in response to the originally received echo
	sendEchoAmplify := func(clientSender *Sender) {

		defer echoAmplifyRoundTripComplete.Done()

		echoAmplifyRequest := NewRequest()
		echoAmplifyRequest.SetProfile("BLIPTest/EchoAmplifyData")
		echoAmplifyRequest.Properties["Content-Type"] = "application/octet-stream"
		echoAmplifyRequest.SetBody([]byte("hello"))
		sent := clientSender.Send(echoAmplifyRequest)
		assert.True(t, sent)
		echoAmplifyResponse := echoAmplifyRequest.Response() // <--- SG #3268 was causing this to block indefinitely
		echoAmplifyResponseBody, _ := echoAmplifyResponse.Body()
		assert.True(t, len(echoAmplifyResponseBody) == 0)

		// TODO: add more assertions about the response.  I'm not seeing any errors, or any
		// TODO: way to differentiate this response with a normal response other than having an empty body

	}

	// Create a blip profile handler to respond to echo requests and then abruptly close the socket
	dispatchEcho := func(request *Message) {
		defer receivedEchoRequest.Done()
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

		echoAmplifyRoundTripComplete.Add(1)
		go sendEchoAmplify(request.Sender)

	}

	// Blip setup
	blipContextEchoServer.HandlerForProfile["BLIPTest/EchoData"] = dispatchEcho
	blipContextEchoServer.LogMessages = true
	blipContextEchoServer.LogFrames = true

	// Websocket Server
	server := blipContextEchoServer.WebSocketServer()

	// HTTP Handler wrapping websocket server
	http.Handle("/TestClientAbruptlyCloseConnectionBehavior", server)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		t.Error(http.Serve(listener, nil))
	}()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestClientAbruptlyCloseConnectionBehavior", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket: %v", err)
	}

	// Handle EchoAmplifyData that should be initiated by server in response to getting incoming echo requests
	dispatchEchoAmplify := func(request *Message) {
		_, err := request.Body()
		if err != nil {
			log.Printf("ERROR reading body of %s: %s", request, err)
			return
		}
		// Abruptly close the websocket connection before sending a response
		request.Sender.Close()

	}
	blipContextEchoClient.HandlerForProfile["BLIPTest/EchoAmplifyData"] = dispatchEchoAmplify

	// Create echo request
	echoRequest := NewRequest()
	echoRequest.SetProfile("BLIPTest/EchoData")
	echoRequest.Properties["Content-Type"] = "application/octet-stream"
	echoRequest.SetBody([]byte("hello"))
	receivedEchoRequest.Add(1)

	// Send echo request
	sent := sender.Send(echoRequest)
	assert.True(t, sent)

	// Wait until the echo server profile handler was invoked and completely finished (and thus abruptly closed socket)
	err = WaitWithTimeout(&receivedEchoRequest, time.Second*60)
	if err != nil {
		t.Fatal(err)
	}

	// Read the echo response
	response := echoRequest.Response()
	responseBody, err := response.Body()

	// Assertions about echo response (these might need to be altered, maybe what's expected in this scenario is actually an error)
	assert.True(t, err == nil)
	assert.Equal(t, "hello", string(responseBody))

	// Wait until the amplify request was received by client (from server), and that the server read the response
	err = WaitWithTimeout(&echoAmplifyRoundTripComplete, time.Second*60)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIncludesProtocol(t *testing.T) {

	headersWithExpectedResponses := []struct {
		Header             string
		ExpectedResponse   bool
		MatchedSubprotocol string
	}{
		{
			Header:           BlipTestAppProtocolId,
			ExpectedResponse: true,
		},
		{
			Header:           BlipTestAppProtocolId + ",SomeOtherWebsocketSubprotocol",
			ExpectedResponse: true,
		},
		{
			Header:           "SomeOtherWebsocketSubprotocol," + BlipTestAppProtocolId,
			ExpectedResponse: true,
		},
		{
			Header:           "SomeOtherWebsocketSubprotocol",
			ExpectedResponse: false,
		},
	}

	for _, headerWithExpectedResponse := range headersWithExpectedResponses {
		_, matched := includesProtocol(headerWithExpectedResponse.Header, []string{BlipTestAppProtocolId})
		assert.Equal(t, headerWithExpectedResponse.ExpectedResponse, matched)
	}

}

func TestUnsupportedSubProtocol(t *testing.T) {
	testCases := []struct {
		Name                 string
		ServerProtocols      []string
		ClientProtocol       []string
		ActiveServerProtocol string
		ExpectError          bool
	}{
		{
			Name:                 "Unsupported",
			ServerProtocols:      []string{"V2"},
			ClientProtocol:       []string{"V1"},
			ActiveServerProtocol: "",
			ExpectError:          true,
		},
		{
			Name:                 "SupportedOne",
			ServerProtocols:      []string{"V1"},
			ClientProtocol:       []string{"V1"},
			ActiveServerProtocol: "V1",
			ExpectError:          false,
		},
		{
			Name:                 "SupportedTwo",
			ServerProtocols:      []string{"V1", "V2"},
			ClientProtocol:       []string{"V1"},
			ActiveServerProtocol: "V1",
			ExpectError:          false,
		},
		{
			Name:                 "ClientAndServerSupportsTwoV1First",
			ServerProtocols:      []string{"V1", "V2"},
			ClientProtocol:       []string{"V1", "V2"},
			ActiveServerProtocol: "V1",
			ExpectError:          false,
		},
		{
			Name:                 "ClientAndServerSupportsTwoV2First",
			ServerProtocols:      []string{"V1", "V2"},
			ClientProtocol:       []string{"V2", "V1"},
			ActiveServerProtocol: "V2",
			ExpectError:          false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			serverCtx, err := NewContext(ContextOptions{ProtocolIds: testCase.ServerProtocols})
			if err != nil {
				t.Fatal(err)
			}
			serverCtx.LogMessages = true
			serverCtx.LogFrames = true

			server := serverCtx.WebSocketServer()

			mux := http.NewServeMux()
			mux.Handle("/someBlip", server)
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				panic(err)
			}

			go func() {
				err := http.Serve(listener, mux)
				if err != nil {
					panic(err)
				}
			}()

			// Client
			client, err := NewContext(ContextOptions{ProtocolIds: testCase.ClientProtocol})
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			destUrl := fmt.Sprintf("ws://localhost:%d/someBlip", port)

			s, err := client.Dial(destUrl)
			if testCase.ExpectError {
				assert.True(t, err != nil)
			} else {
				assert.Equal(t, nil, err)
				s.Close()
			}

			if testCase.ActiveServerProtocol != "" {
				assert.Equal(t, testCase.ActiveServerProtocol, serverCtx.ActiveSubprotocol())
				assert.Equal(t, serverCtx.ActiveSubprotocol(), client.ActiveSubprotocol())
			}
		})
	}
}

func TestHandshake(t *testing.T) {
	serverCtx, err := NewContext(ContextOptions{ProtocolIds: []string{"ServerProtocol"}})
	require.NoError(t, err)
	serverCtx.LogMessages = true
	serverCtx.LogFrames = true

	server := serverCtx.WebSocketServer()
	wg := sync.WaitGroup{}
	assertHandlerError(t, server, &wg)
	mux := http.NewServeMux()
	mux.Handle("/someBlip", server)
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, listener.Close())
	}()

	go func() {
		// error will happen when listener closes in defer
		_ = http.Serve(listener, mux)
	}()

	// Client
	client, err := NewContext(ContextOptions{ProtocolIds: []string{"ClientProtocol"}})
	require.NoError(t, err)

	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/someBlip", port)

	_, err = client.Dial(destUrl)
	require.Error(t, err)
	assertHandlerError(t, server, &wg)
}

func TestOrigin(t *testing.T) {
	protocol := "Protocol1"
	testCases := []struct {
		serverOrigin  []string
		requestOrigin *string
		hasError      bool
	}{
		{
			serverOrigin:  []string{"example.com"},
			requestOrigin: StringPtr("https://example.com"),
		},
		{
			serverOrigin:  []string{"foobar.com", "example.com"},
			requestOrigin: StringPtr("https://example.com"),
		},
		{
			serverOrigin:  []string{"*"},
			requestOrigin: StringPtr("https://example.com"),
		},
		{
			serverOrigin:  []string{"*"},
			requestOrigin: StringPtr("ws://example.com"),
		},
		{
			serverOrigin:  []string{"*"},
			requestOrigin: StringPtr("wss://example.com"),
		},
		{
			serverOrigin:  []string{"example.com"},
			requestOrigin: StringPtr("wss://example.org"),
			hasError:      true,
		},
		{
			serverOrigin:  []string{""},
			requestOrigin: StringPtr("wss://example.org"),
			hasError:      true,
		},
		{
			serverOrigin:  []string{"example.com"},
			requestOrigin: nil,
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			listener, err := net.Listen("tcp", ":0")
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, listener.Close())
			}()

			serverCtx, err := NewContext(ContextOptions{
				ProtocolIds: []string{protocol},
				Origin:      test.serverOrigin,
			})
			require.NoError(t, err)
			serverCtx.LogMessages = true
			serverCtx.LogFrames = true

			server := serverCtx.WebSocketServer()
			wg := sync.WaitGroup{}
			if test.hasError {
				assertHandlerError(t, server, &wg)
			} else {
				assertHandlerNoError(t, server, &wg)
			}
			mux := http.NewServeMux()
			mux.Handle("/someBlip", server)

			go func() {
				// error will happen when listener closes
				_ = http.Serve(listener, mux)
			}()

			// Client
			client, err := NewContext(ContextOptions{ProtocolIds: []string{protocol}})
			require.NoError(t, err)

			port := listener.Addr().(*net.TCPAddr).Port
			destUrl := fmt.Sprintf("ws://localhost:%d/someBlip", port)
			config := DialOptions{
				URL: destUrl,
			}
			if test.requestOrigin != nil {
				config.HTTPHeader = make(http.Header)
				config.HTTPHeader.Add("Origin", *test.requestOrigin)
			}
			sender, err := client.DialConfig(&config)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				sender.Close()
			}
			require.NoError(t, WaitWithTimeout(&wg, time.Second*5))

			assertHandlerNoError(t, server, &wg)

			// run without origin header
			config = DialOptions{
				URL: destUrl,
			}
			sender, err = client.DialConfig(&config)
			require.NoError(t, err)
			require.NoError(t, WaitWithTimeout(&wg, time.Second*5))
			sender.Close()
		})
	}
}

// TestServerContextClose tests closing server using cancellable context, ensure that clients are disconnected
//
// Test:
//   - Start two blip contexts: an echo server and an echo client
//   - The echo server is configured to respond to incoming echo requests and return responses
//   - The echo client sends echo requests on a loop
//   - Expected: the echo client should receive some sort of error when the server closes the connection, and should not block
func TestServerContextClose(t *testing.T) {

	serverCancelCtx, cancelFunc := context.WithCancel(context.Background())
	contextOptionsWithCancel := ContextOptions{
		ProtocolIds: []string{BlipTestAppProtocolId},
		CancelCtx:   serverCancelCtx,
	}
	blipContextEchoServer, err := NewContext(contextOptionsWithCancel)
	if err != nil {
		t.Fatal(err)
	}

	receivedRequests := sync.WaitGroup{}

	// ----------------- Setup Echo Server that will be closed via cancellation context -------------------------

	// Create a blip profile handler to respond to echo requests
	dispatchEcho := func(request *Message) {
		defer receivedRequests.Done()
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
	server := blipContextEchoServer.WebSocketServer()

	// HTTP Handler wrapping websocket server
	http.Handle("/TestServerContextClose", server)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		err := http.Serve(listener, nil)
		log.Printf("server goroutine closed with error: %v", err)
	}()

	// ----------------- Setup Echo Client ----------------------------------------
	blipContextEchoClient, err := NewContext(defaultContextOptions)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestServerContextClose", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket: %v", err)
	}

	var closeWg, delayWg sync.WaitGroup

	// Start a goroutine to send echo request every 100 ms, time out after 30s (if test fails)
	delayWg.Add(1) // wait for connection and messages to be sent before cancelling server context
	closeWg.Add(1) // wait for client to disconnect before exiting test
	go func() {
		defer closeWg.Done()
		timeout := time.After(time.Second * 30)
		ticker := time.NewTicker(time.Millisecond * 50)
		echoCount := 0
		for {
			select {
			case <-timeout:
				t.Error("Echo client connection wasn't closed before timeout expired")
				return
			case <-ticker.C:
				{
					echoCount++
					// After sending 10 echoes, close delayWg to trigger server-side cancellation
					log.Printf("Sending echo %v", echoCount)
					if echoCount == 10 {
						delayWg.Done()
					}
					// Create echo request
					echoResponseBody := []byte("hello")
					echoRequest := NewRequest()
					echoRequest.SetProfile("BLIPTest/EchoData")
					echoRequest.Properties["Content-Type"] = "application/octet-stream"
					echoRequest.SetBody(echoResponseBody)
					receivedRequests.Add(1)
					sent := sender.Send(echoRequest)
					assert.True(t, sent)

					// Read the echo response.  Closed connection will result in empty response, as EOF message
					// isn't currently returned by blip client
					response := echoRequest.Response()
					responseBody, err := response.Body()
					assert.True(t, err == nil)
					if len(responseBody) == 0 {
						log.Printf("empty response, connection closed")
						return
					}

					assert.Equal(t, echoResponseBody, responseBody)
				}
			}
		}
	}()

	// Wait for client to start sending echo messages before stopping server
	delayWg.Wait()

	// Cancel context on server
	cancelFunc()

	// Wait for client echo loop to exit due to closed connection before exiting test
	closeWg.Wait()

}

// assert that the server handshake callback is called with an error.
func assertHandlerError(t *testing.T, server *BlipWebsocketServer, wg *sync.WaitGroup) {
	wg.Add(1)
	server.PostHandshakeCallback = func(err error) {
		defer wg.Done()
		require.Error(t, err)
	}
}

// assert that the server handshake callback is called without an error.
func assertHandlerNoError(t *testing.T, server *BlipWebsocketServer, wg *sync.WaitGroup) {
	wg.Add(1)
	server.PostHandshakeCallback = func(err error) {
		defer wg.Done()
		require.NoError(t, err)
	}
}

// TestWebSocketServerStopHandler tests stopping the handler with a specific error code.
func TestWebSocketServerStopHandler(t *testing.T) {

	opts := ContextOptions{
		ProtocolIds: []string{BlipTestAppProtocolId},
	}
	blipContextEchoServer, err := NewContext(opts)
	require.NoError(t, err)

	receivedRequests := sync.WaitGroup{}

	// ----------------- Setup Echo Server that will be closed via cancellation context -------------------------

	// Create a blip profile handler to respond to echo requests
	dispatchEcho := func(request *Message) {
		defer receivedRequests.Done()
		body, err := request.Body()
		require.NoError(t, err)
		require.Equal(t, "application/octet-stream", request.Properties["Content-Type"])
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
	server := blipContextEchoServer.WebSocketServer()

	// HTTP Handler wrapping websocket server
	http.Handle("/TestServerContextClose", server)
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		_ = http.Serve(listener, nil)
	}()

	// ----------------- Setup Echo Client ----------------------------------------
	blipContextEchoClient, err := NewContext(defaultContextOptions)
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/TestServerContextClose", port)
	sender, err := blipContextEchoClient.Dial(destUrl)
	require.NoError(t, err)

	// Create echo request
	echoResponseBody := []byte("hello")
	echoRequest := NewRequest()
	echoRequest.SetProfile("BLIPTest/EchoData")
	echoRequest.Properties["Content-Type"] = "application/octet-stream"
	echoRequest.SetBody(echoResponseBody)
	receivedRequests.Add(1)
	require.True(t, sender.Send(echoRequest))

	// Read the echo response.  Closed connection will result in empty response, as EOF message
	// isn't currently returned by blip client
	response := echoRequest.Response()
	responseBody, err := response.Body()
	require.NoError(t, err)
	require.Equal(t, echoResponseBody, responseBody)

	fmt.Printf("Closing connection\n")
	server.StopHandler(websocket.StatusAbnormalClosure)
	//fmt.Printf("sender=%+v\n", sender.conn)
	require.True(t, false)
}

// Wait for the WaitGroup, or return an error if the wg.Wait() doesn't return within timeout
// TODO: this code is duplicated with code in Sync Gateway utilities_testing.go.  Should be refactored to common repo.
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {

	// Create a channel so that a goroutine waiting on the waitgroup can send it's result (if any)
	wgFinished := make(chan bool)

	go func() {
		wg.Wait()
		wgFinished <- true
	}()

	select {
	case <-wgFinished:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("Timed out waiting after %v", timeout)
	}

}

// StringPtr returns a pointer to the string value passed in
func StringPtr(s string) *string {
	return &s
}
