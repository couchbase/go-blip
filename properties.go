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

// Property names that are encoded as single bytes (first is Ctrl-A, etc.)
// CHANGING THIS ARRAY WILL BREAK PROTOCOL COMPATIBILITY!!
var kSpecialProperties = []string{
	"Profile",
	"Error-Code",
	"Error-Domain",

	"Content-Type",
	"application/json",
	"application/octet-stream",
	"text/plain; charset=UTF-8",
	"text/xml",

	"Accept",
	"Cache-Control",
	"must-revalidate",
	"If-Match",
	"If-None-Match",
	"Location",
}

var kSpecialPropertyEncoder map[string][]byte

func init() {
	kSpecialPropertyEncoder = make(map[string][]byte, len(kSpecialProperties))
	for i, prop := range kSpecialProperties {
		kSpecialPropertyEncoder[prop] = []byte{byte(i + 1)}
	}
}

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
		key := decodeProperty(eachProp[i])
		value := decodeProperty(eachProp[i+1])
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
		strings[i] = encodeProperty(key)
		strings[i+1] = encodeProperty(value)
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

func encodeProperty(prop string) []byte {
	if encoded, found := kSpecialPropertyEncoder[prop]; found {
		return encoded
	} else {
		return []byte(prop)
	}
}

func decodeProperty(encoded []byte) string {
	if len(encoded) == 1 {
		index := int(encoded[0]) - 1
		if index < len(kSpecialProperties) {
			return kSpecialProperties[index]
		}
	}
	return string(encoded)
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

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
