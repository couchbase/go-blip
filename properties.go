package blip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Properties map[string]string

// Property names that are encoded as single bytes (first is Ctrl-A, etc.)
// CHANGING THIS ARRAY WILL BREAK PROTOCOL COMPATIBILITY!!
var kSpecialProperties = []string{
	"Content-Type",
	"Profile",
	"application/octet-stream",
	"text/plain; charset=UTF-8",
	"text/xml",
	"text/yaml",
	"Channel",
	"Error-Code",
	"Error-Domain",
}

var kSpecialPropertyEncoder map[string][]byte

func init() {
	kSpecialPropertyEncoder = make(map[string][]byte, len(kSpecialProperties))
	for i, prop := range kSpecialProperties {
		kSpecialPropertyEncoder[prop] = []byte{byte(i + 1)}
	}
}

// Reads encoded Properties from a stream.
func (properties *Properties) ReadFrom(reader io.Reader) error {
	var length uint16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
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
	strings := make([][]byte, 2*len(properties))
	i := 0
	size := 0
	for key, value := range properties {
		strings[i] = encodeProperty(key)
		strings[i+1] = encodeProperty(value)
		size += len(strings[i]) + len(strings[i+1]) + 2
		i += 2
	}
	if size > 65535 {
		return fmt.Errorf("Message properties too large")
	}

	// Now write the size prefix and the null-terminated strings:
	if err := binary.Write(writer, binary.BigEndian, uint16(size)); err != nil {
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

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
