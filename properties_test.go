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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {
	SortProperties = true
}

func TestReadWriteProperties(t *testing.T) {
	p := Properties{"Content-Type": "application/octet-stream", "Foo": "Bar"}
	var writer bytes.Buffer
	err := p.WriteTo(&writer)
	assert.Equal(t, nil, err)
	writer.Write([]byte("FOOBAR"))
	serialized := writer.Bytes()
	propertiesLength := len(serialized) - len("FOOBAR")
	assert.Equal(t, "\x2EContent-Type\x00application/octet-stream\x00Foo\x00Bar\x00FOOBAR", string(serialized))

	for dataLen := 0; dataLen <= len(serialized); dataLen++ {
		p2, bytesRead, err := ReadProperties(serialized[0:dataLen])
		assert.NoError(t, err)
		if dataLen < propertiesLength {
			assert.Nil(t, p2)
		} else {
			assert.Equal(t, p, p2)
			assert.Equal(t, propertiesLength, bytesRead)
		}
	}
}

func TestReadWriteEmptyProperties(t *testing.T) {
	var p Properties
	var writer bytes.Buffer
	err := p.WriteTo(&writer)
	assert.Equal(t, nil, err)
	serialized := writer.Bytes()
	assert.Equal(t, "\x00", string(serialized))

	p2, bytesRead, err := ReadProperties(serialized)
	assert.Equal(t, nil, err)
	assert.Equal(t, Properties{}, p2)
	assert.Equal(t, len(serialized), bytesRead)
}

func TestReadBadProperties(t *testing.T) {
	bad := [][2]string{
		{"", "EOF"},
		{"\x00", ""},
		{"\x0C", "EOF"},
		{"\x0CX\x00Y\x00Foo\x00Ba", "EOF"},
		{"\x0CX\x00Y\x00Foo\x00Bar\x00", ""},
		{"\x14X\x00Y\x00Foo\x00Bar\x00Foo\x00Zog\x00", "duplicate property name \"Foo\""},

		{"\x02hi", "invalid properties (not NUL-terminated)"},
		{"\x02h\x00", "odd number of strings in properties"},
	}
	for i, pair := range bad {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			serialized := []byte(pair[0])
			p2, bytesRead, err := ReadProperties(serialized)
			var errStr string
			if err == nil {
				if bytesRead == 0 {
					errStr = "EOF"
				} else if bytesRead != len(serialized) {
					t.Errorf("Error decoding #%d %q: No error, but left %d bytes unread", i, pair[0], len(serialized)-bytesRead)
				} else {
					assert.NotNil(t, p2)
				}
			} else {
				errStr = err.Error()
			}
			if errStr != pair[1] {
				t.Errorf("Error decoding #%d %q: %q (expected %q)", i, pair[0], errStr, pair[1])
			}
		})
	}
}
