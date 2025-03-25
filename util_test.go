// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package blip

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Starts a WebSocket server on the Context, returning its net.Listener.
// The server runs on a background goroutine.
// Close the Listener when the test finishes.
func startTestListener(t *testing.T, serverContext *Context) net.Listener {
	mux := http.NewServeMux()
	mux.Handle("/blip", serverContext.WebSocketServer())
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Error opening WebSocket listener: %v", err)
	}
	go func() {
		_ = http.Serve(listener, mux)
	}()
	return listener
}

// Connects the Context to a Listener created by `startTestListener`.
// Close the Sender when the test finishes.
func startTestClient(t *testing.T, clientContext *Context, listener net.Listener) *Sender {
	port := listener.Addr().(*net.TCPAddr).Port
	destUrl := fmt.Sprintf("ws://localhost:%d/blip", port)
	sender, err := clientContext.Dial(destUrl)
	if err != nil {
		t.Fatalf("Error opening WebSocket client: %v", err)
	}
	return sender
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
		return fmt.Errorf("timed out waiting after %v", timeout)
	}
}

// Returns the serialized form (properties + body) of a Message.
func serializeMessage(t *testing.T, m *Message) []byte {
	var writer bytes.Buffer
	err := m.WriteEncodedTo(&writer)
	assert.NoError(t, err)
	return writer.Bytes()
}

// Asserts that two Messages are identical
func assertEqualMessages(t *testing.T, m, m2 *Message) bool {
	if !assert.Equal(t, m.flags.Load(), m2.flags.Load()) || !assert.Equal(t, m.Properties, m2.Properties) {
		return false
	}
	mb, err := m.Body()
	m2b, err2 := m2.Body()
	return assert.NoError(t, err) && assert.NoError(t, err2) && assert.Equal(t, mb, m2b)

}

// Asserts that `response` contains a BLIP error with the given domain and code.
func assertBLIPError(t *testing.T, response *Message, domain string, code int) bool {
	responseError := response.Error()
	if assert.NotNil(t, responseError, "Expected a BLIP error") {
		if responseError.Domain == domain && responseError.Code == code {
			return true
		}
		assert.Fail(t, "Unexpected BLIP error", "Got %v: expected %v",
			responseError, &ErrorResponse{domain, code, ""})
	}
	return false
}
