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

// Adapter for io.Reader to io.ByteReader
type byteReader struct {
	reader io.Reader
}

func (br byteReader) ReadByte() (byte, error) {
	var p [1]byte
	_, err := br.reader.Read(p[:])
	return p[0], err
}

// Reads encoded Properties from a stream.
func (properties *Properties) ReadFrom(reader io.Reader) error {
	length, err := binary.ReadUvarint(byteReader{reader})
	if err != nil {
		return err
	}
	if length == 0 {
		return nil
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return err
	}

	if body[length-1] != 0 {
		return fmt.Errorf("Invalid properties (not NUL-terminated)")
	}
	eachProp := bytes.Split(body[0:length-1], []byte{0})
	nProps := len(eachProp) / 2
	if nProps*2 != len(eachProp) {
		return fmt.Errorf("Odd number of strings in properties")
	}
	*properties = Properties{}
	for i := 0; i < len(eachProp); i += 2 {
		key := string(eachProp[i])
		value := string(eachProp[i+1])
		if _, exists := (*properties)[key]; exists {
			return fmt.Errorf("Duplicate property name %q", key)
		}
		(*properties)[key] = value
	}
	return nil
}

// Writes Properties to a stream.
func (properties Properties) WriteTo(writer io.Writer) error {
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
