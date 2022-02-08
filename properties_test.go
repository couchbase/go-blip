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
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func init() {
	SortProperties = true
}

func TestReadWriteProperties(t *testing.T) {
	p := Properties{"Content-Type": "application/octet-stream", "Foo": "Bar"}
	var writer bytes.Buffer
	err := p.WriteTo(&writer)
	assert.Equals(t, err, nil)
	serialized := writer.Bytes()
	assert.Equals(t, string(serialized), "\x2EContent-Type\x00application/octet-stream\x00Foo\x00Bar\x00")

	var p2 Properties
	reader := bytes.NewReader(serialized)
	err = p2.ReadFrom(reader)
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, p2, p)
}

func TestReadWriteEmptyProperties(t *testing.T) {
	var p Properties
	var writer bytes.Buffer
	err := p.WriteTo(&writer)
	assert.Equals(t, err, nil)
	serialized := writer.Bytes()
	assert.Equals(t, string(serialized), "\x00")

	var p2 Properties
	reader := bytes.NewReader(serialized)
	err = p2.ReadFrom(reader)
	assert.Equals(t, err, nil)
	assert.DeepEquals(t, p2, p)
}

func TestReadBadProperties(t *testing.T) {
	bad := [][2]string{
		{"", "EOF"},
		{"\x00", ""},
		{"\x0C", "EOF"},
		{"\x0CX\x00Y\x00Foo\x00Ba", "unexpected EOF"},
		{"\x0CX\x00Y\x00Foo\x00Bar\x00", ""},
		{"\x14X\x00Y\x00Foo\x00Bar\x00Foo\x00Zog\x00", "Duplicate property name \"Foo\""},

		{"\x02hi", "Invalid properties (not NUL-terminated)"},
		{"\x02h\x00", "Odd number of strings in properties"},
	}
	var p2 Properties
	for i, pair := range bad {
		reader := bytes.NewReader([]byte(pair[0]))
		err := p2.ReadFrom(reader)
		var errStr string
		if err == nil {
			if reader.Len() != 0 {
				t.Errorf("Error decoding #%d %q: No error, but left %d bytes unread", i, pair[0], reader.Len())
			}
		} else {
			errStr = err.Error()
		}
		if errStr != pair[1] {
			t.Errorf("Error decoding #%d %q: %q (expected %q)", i, pair[0], errStr, pair[1])
		}
	}
}
