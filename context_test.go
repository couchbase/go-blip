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
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"nhooyr.io/websocket"
)

// The application protocol id of the BLIP websocket subprotocol used in go-blip unit tests
const BlipTestAppProtocolId = "GoBlipUnitTests"

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

	blipContextEchoServer, err := NewContext(BlipTestAppProtocolId)
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
	listener := startTestListener(t, blipContextEchoServer)
	defer listener.Close()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient, err := NewContext(BlipTestAppProtocolId)
	if err != nil {
		t.Fatal(err)
	}
	sender := startTestClient(t, blipContextEchoClient, listener)
	defer sender.Close()

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
	assertBLIPError(t, response, BLIPErrorDomain, DisconnectedCode)
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

	blipContextEchoServer, err := NewContext(BlipTestAppProtocolId)
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
		assertBLIPError(t, echoAmplifyResponse, BLIPErrorDomain, DisconnectedCode)
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
	listener := startTestListener(t, blipContextEchoServer)
	defer listener.Close()

	// ----------------- Setup Echo Client ----------------------------------------

	blipContextEchoClient, err := NewContext(BlipTestAppProtocolId)
	if err != nil {
		t.Fatal(err)
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

	sender := startTestClient(t, blipContextEchoClient, listener)
	defer sender.Close()

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
	assert.NoError(t, err)
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
			serverCtx, err := NewContext(testCase.ServerProtocols...)
			if err != nil {
				t.Fatal(err)
			}
			serverCtx.LogMessages = true
			serverCtx.LogFrames = true

			listener := startTestListener(t, serverCtx)
			defer listener.Close()

			// Client
			client, err := NewContext(testCase.ClientProtocol...)
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			destUrl := fmt.Sprintf("ws://localhost:%d/blip", port)

			s, err := client.Dial(destUrl)
			if testCase.ExpectError {
				assert.True(t, err != nil)
			} else {
				assert.Equal(t, nil, err)
			}
			if s != nil {
				s.Close()
			}

			if testCase.ActiveServerProtocol != "" {
				assert.Equal(t, testCase.ActiveServerProtocol, serverCtx.ActiveSubprotocol())
				assert.Equal(t, serverCtx.ActiveSubprotocol(), client.ActiveSubprotocol())
			}
		})
	}
}
