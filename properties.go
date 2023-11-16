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
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// A Message's metadata properties. These are somewhat like HTTP headers.
type Properties map[string]string

// For testing purposes, clients can set this to true to write properties sorted by key
var SortProperties = false

// Implementation-imposed max encoded size of message properties (not part of protocol)
const maxPropertiesLength = 100 * 1024

// Reads encoded Properties from a byte array.
// On success, returns the Properties map and the number of bytes read.
// If the array doesn't contain the complete properties, returns (nil, 0, nil).
// On failure, returns nil Properties and an error.
func ReadProperties(body []byte) (properties Properties, bytesRead int, err error) {
	length, bytesRead := binary.Uvarint(body)
	if bytesRead == 0 {
		// Not enough bytes to read the varint
		return
	} else if bytesRead < 0 || length > maxPropertiesLength {
		err = fmt.Errorf("invalid properties length in BLIP message")
		return
	} else if bytesRead+int(length) > len(body) {
		// Incomplete
		return nil, 0, nil
	}

	if length == 0 {
		// Empty properties
		return Properties{}, bytesRead, nil
	}

	body = body[bytesRead:]
	bytesRead += int(length)

	if body[length-1] != 0 {
		err = fmt.Errorf("invalid properties (not NUL-terminated)")
		return
	}
	eachProp := bytes.Split(body[0:length-1], []byte{0})
	nProps := len(eachProp) / 2
	if nProps*2 != len(eachProp) {
		err = fmt.Errorf("odd number of strings in properties")
		return
	}
	properties = Properties{}
	for i := 0; i < len(eachProp); i += 2 {
		key := string(eachProp[i])
		value := string(eachProp[i+1])
		if _, exists := (properties)[key]; exists {
			return nil, bytesRead, fmt.Errorf("duplicate property name %q", key)
		}
		properties[key] = value
	}
	return
}

// Writes Properties to a stream.
func (properties Properties) WriteEncodedTo(writer io.Writer) error {
	// First convert the property strings into byte arrays, and add up their sizes:
	var strings propertyList = make(propertyList, 2*len(properties))
	i := 0
	size := 0
	for key, value := range properties {
		strings[i] = []byte(key)
		strings[i+1] = []byte(value)
		size += len(strings[i]) + len(strings[i+1]) + 2
		i += 2
	}

	if SortProperties {
		sort.Sort(strings) // Not needed for protocol, but helps unit tests
	}

	// Now write the size prefix and the null-terminated strings:
	var lengthBuf [10]byte
	lengthLength := binary.PutUvarint(lengthBuf[:], uint64(size))
	if _, err := writer.Write(lengthBuf[:lengthLength]); err != nil {
		return err
	}
	for _, str := range strings {
		if _, err := writer.Write(str); err != nil {
			return err
		}
		if _, err := writer.Write([]byte{0}); err != nil {
			return err
		}
	}
	return nil
}

// Writes Properties to a byte array.
func (properties Properties) Encode() []byte {
	var out bytes.Buffer
	_ = properties.WriteEncodedTo(&out)
	return out.Bytes()
}

// Properties stored as alternating keys / values
type propertyList [][]byte

// Implementation of sort.Interface to make propertyList sortable:
func (pl propertyList) Len() int           { return len(pl) / 2 }
func (pl propertyList) Less(i, j int) bool { return bytes.Compare(pl[2*i], pl[2*j]) < 0 }
func (pl propertyList) Swap(i, j int) {
	i *= 2
	j *= 2
	t := pl[i]
	pl[i] = pl[j]
	pl[j] = t
	t = pl[i+1]
	pl[i+1] = pl[j+1]
	pl[j+1] = t
}
