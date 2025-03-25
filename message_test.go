/*
Copyright 2013-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package blip

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {
	SortProperties = true
}

func makeTestRequest() *Message {
	m := NewRequest()
	m.Properties["Content-Type"] = "ham/rye"
	m.Properties["X-Weather"] = "rainy"
	m.SetProfile("test")
	m.SetBody([]byte("The white knight is sliding down the poker. He balances very badly."))
	return m
}

func TestMessageEncoding(t *testing.T) {
	m := makeTestRequest()
	assert.Equal(t, "test", m.Profile())
	serialized := serializeMessage(t, m)
	assert.Equal(t, "\x32Content-Type\x00ham/rye\x00Profile\x00test\x00X-Weather\x00rainy\x00The white knight is sliding down the poker. He balances very badly.", string(serialized))
}

func TestMessageStreaming(t *testing.T) {
	m := makeTestRequest()
	serialized := serializeMessage(t, m)
	body, _ := m.Body()

	for breakAt := 1; breakAt <= len(serialized); breakAt++ {
		t.Run(fmt.Sprintf("Frame size %d", breakAt), func(t *testing.T) {
			// "Receive" message m2 in two pieces, separated at offset `breakAt`.
			frame1 := serialized[0:breakAt]
			frame2 := serialized[breakAt:]
			complete := len(frame2) == 0
			expectedState := dispatchState{
				atStart: len(frame1) >= len(serialized)-len(body),
				atEnd:   complete,
			}

			m2 := newIncomingMessage(nil, 1, *m.flags.Load())
			state, err := m2.addIncomingBytes(frame1, complete)
			assert.NoError(t, err)
			assert.Equal(t, state, expectedState)
			if state.atStart {
				// Frame 1 completes the properties, so the reader should be available:
				assert.Equal(t, m.Properties, m2.Properties)
				reader, err := m2.BodyReader()
				assert.NoError(t, err)
				buf := make([]byte, 500)
				n, err := reader.TryRead(buf)
				assert.NoError(t, err)
				assert.Equal(t, n, breakAt-51)
				assert.Equal(t, body[0:n], buf[0:n])

				if !expectedState.atEnd {
					state, err = m2.addIncomingBytes(frame2, true)
					assert.NoError(t, err)
					assert.Equal(t, state, dispatchState{false, true})
					n2, err := reader.TryRead(buf[n:])
					assert.NoError(t, err)
					assert.Equal(t, len(body)-n, n2)
					assert.Equal(t, body, buf[0:(n+n2)])
				}

				n, err = reader.TryRead(buf)
				assert.ErrorIs(t, err, io.EOF)
				assert.Equal(t, n, 0)
			}
		})
	}
}

func TestMessageEncodingCompressed(t *testing.T) {
	m := makeTestRequest()
	m.SetCompressed(true)
	serialized := serializeMessage(t, m)

	// Commented due to test failure:
	// http://drone.couchbase.io/couchbase/go-blip/4 (test logs: https://gist.github.com/tleyden/ae2aa71978cd11ca5d9d1f6878593cdb)
	// goassert.Equals(t, string(serialized), "\x1a\x04\x00ham/rye\x00X-Weather\x00rainy\x00\x1f\x8b\b\x00\x00\tn\x88\x00\xff\f\xca\xd1\t\xc5 \f\x05\xd0U\xee\x04\xce\xf1\x06x\v\xd8z\xd1`\x88ń\x8a\xdb\xd7\xcf\x03\xe7߈\xd5$\x88nR[@\x1c\xaeR\xc4*\xcaX\x868\xe1\x19\x9d3\xe1G\\Y\xb3\xddt\xbc\x9c\xfb\xa8\xe8N_\x00\x00\x00\xff\xffs*\xa1\xa6C\x00\x00\x00")
	// log.Printf("Encoded compressed as %d bytes", len(serialized))

	m2 := newIncomingMessage(nil, 1, *m.flags.Load())
	state, err := m2.addIncomingBytes(serialized, true)
	assert.NoError(t, err)
	assert.Equal(t, state, dispatchState{true, true})
	assertEqualMessages(t, m, m2.Message)
}

func BenchmarkMessageEncoding(b *testing.B) {
	msg := NewRequest()
	msg.SetProfile("BLIPTest/EchoData")
	msg.Properties["Content-Type"] = "application/octet-stream"
	body := make([]byte, 10000)
	for i := 0; i < len(body); i++ {
		body[i] = byte(i % 256)
	}
	msg.SetBody(body)
	msg.number = 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.inProgress = false
		sender := &msgSender{Message: msg}
		for {
			_, flags := sender.nextFrameToSend(4090)
			if flags&kMoreComing == 0 {
				break
			}
		}
	}
}
