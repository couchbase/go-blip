package blip

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
)

// Property names that are encoded as single bytes (first is Ctrl-A, etc.)
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

// A BLIP message. It could be a request or response or error, and it could be from me or the peer.
type Message struct {
	Outgoing    bool              // Is this a message created locally?
	Properties  map[string]string // The message's metadata, similar to HTTP headers.
	Body        []byte            // The message body. MIME type is defined by "Content-Type" property
	number      uint32            // The sequence number of the message in the connection.
	flags       frameFlags        // Message flags as seen on the first frame.
	complete    bool              // Has this message been completely received?
	bytesToSend []byte            // Data still waiting to be sent in outgoing frames
	response    *Message          // Response to this message, if it's a request
}

func (message *Message) String() string {
	var msgType, urgent string
	switch typeBits := message.Type(); typeBits {
	case RequestType:
		msgType = "MSG"
	case ResponseType:
		msgType = "RPY"
	case ErrorType:
		msgType = "ERR"
	default:
		msgType = fmt.Sprintf("?%d?", typeBits)
	}
	if message.flags&kUrgent != 0 {
		urgent = "!"
	}
	return fmt.Sprintf("%s%s#%d", msgType, urgent, message.number)
}

// Creates a new outgoing request.
func NewRequest() *Message {
	return &Message{
		flags:      frameFlags(RequestType),
		Outgoing:   true,
		Properties: map[string]string{},
	}
}

// The type of message: request, response or error
func (message *Message) Type() MessageType { return MessageType(message.flags & kTypeMask) }

// True if the message has Urgent priority.
func (message *Message) Urgent() bool { return message.flags&kUrgent != 0 }

// True if the message doesn't want a reply.
func (message *Message) NoReply() bool { return message.flags&kNoReply != 0 }

// True if the message's body was GZIP-compressed in transit.
// (This is for informative purposes only; you don't need to unzip it yourself!)
func (message *Message) Compressed() bool { return message.flags&kCompressed != 0 }

// Marks an outgoing message as having high priority. Urgent messages get a higher amount of
// bandwidth. This is useful for streaming media.
func (message *Message) SetUrgent(urgent bool) {
	message.setFlag(kUrgent, urgent)
}

// Requests GZIP compression of an outgoing message's body.
func (message *Message) SetCompressed(compressed bool) {
	message.setFlag(kCompressed, compressed)
}

// Marks an outgoing message as being one-way: no reply will be sent.
func (request *Message) SetNoReply(noReply bool) {
	if request.Type() != RequestType {
		panic("Can't call SetNoReply on a response")
	}
	request.setFlag(kNoReply, noReply)
}

func (message *Message) setFlag(flag frameFlags, value bool) {
	message.assertMutable()
	if value {
		message.flags |= flag
	} else {
		message.flags &^= flag
	}
}

func (message *Message) assertMutable() {
	if !message.Outgoing || message.bytesToSend != nil {
		panic("Message can't be modified")
	}
}

// Returns the response message to this request. Its properties and body are initially empty.
// Multiple calls return the same object.
// If called on a NoReply request, this returns nil.
func (request *Message) Response() *Message {
	response := request.response
	if response == nil {
		if request.Type() != RequestType {
			panic("Can't respond to this message")
		}
		if request.flags&kNoReply != 0 {
			return nil
		}
		response = &Message{
			flags:      frameFlags(ResponseType) | (request.flags & kUrgent),
			number:     request.number,
			Outgoing:   !request.Outgoing,
			Properties: map[string]string{},
		}
		request.response = response
	}
	return response
}

// Changes a pending response into an error.
// It is safe (and a no-op) to call this on a nil Message.
func (response *Message) SetError(errDomain string, errCode int) {
	if response != nil {
		response.assertMutable()
		if response.Type() == RequestType {
			panic("Can't call SetError on a request")
		}
		response.flags = (response.flags &^ kTypeMask) | frameFlags(ErrorType)
		response.Properties = map[string]string{
			"Error-Domain": errDomain,
			"Error-Code":   fmt.Sprintf("%d", errCode),
		}
		response.Body = nil
	}
}

//////// INTERNALS:

func (m *Message) nextFrameToSend(maxSize int) ([]byte, frameFlags) {
	if m.number == 0 || !m.Outgoing {
		panic("Can't send this message")
	}

	if m.bytesToSend == nil {
		log.Printf("Now sending #%d", m.number)
		m.bytesToSend = m.encode()
	}

	var frame []byte
	flags := m.flags
	lengthToWrite := len(m.bytesToSend)
	if lengthToWrite > maxSize {
		frame = m.bytesToSend[0:maxSize]
		m.bytesToSend = m.bytesToSend[maxSize:]
		flags |= kMoreComing
	} else {
		frame = m.bytesToSend
		m.bytesToSend = []byte{}
		flags &^= kMoreComing
	}
	return frame, flags
}

func (m *Message) receivedFrame(frame []byte, flags frameFlags) error {
	frameType := MessageType(flags & kTypeMask)
	curType := MessageType(m.flags & kTypeMask)
	if frameType != curType {
		if curType == ResponseType && frameType == ErrorType {
			m.flags = (m.flags &^ kTypeMask) | frameFlags(frameType)
		} else {
			return fmt.Errorf("Frame has wrong type (%x) for message (%x)", frameType, curType)
		}
	}

	m.Body = append(m.Body, frame...)

	if m.Properties == nil {
		// Try to extract the properties:
		var err error
		m.Properties, m.Body, err = decodeProperties(m.Body)
		if err != nil {
			return err
		}
	}

	if (flags & kMoreComing) == 0 {
		// After last frame, decode the data:
		m.flags &^= kMoreComing
		if m.Properties == nil {
			return fmt.Errorf("Missing or invalid properties")
		}
		if m.flags&kCompressed != 0 && len(m.Body) > 0 {
			unzipper, err := gzip.NewReader(bytes.NewBuffer(m.Body))
			if err != nil {
				return err
			}
			m.Body, err = ioutil.ReadAll(unzipper)
			unzipper.Close()
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
		}
		m.complete = true
	}
	return nil
}

// Encodes the properties and body of a message.
func (m *Message) encode() []byte {
	var encoder bytes.Buffer
	encodeProperties(m.Properties, &encoder)
	if m.flags&kCompressed != 0 && len(m.Body) > 0 {
		zipper := gzip.NewWriter(&encoder)
		zipper.Write(m.Body)
		zipper.Close()
	} else {
		encoder.Write(m.Body)
	}
	return encoder.Bytes()
}

func encodeProperties(properties map[string]string, encoder io.Writer) {
	var buffer bytes.Buffer
	for key, value := range properties {
		buffer.WriteString(key)
		buffer.WriteByte(0)
		buffer.WriteString(value)
		buffer.WriteByte(0)
	}
	size := len(buffer.Bytes())
	if size > 65535 {
		panic("Message properties > 64kbytes")
	}
	binary.Write(encoder, binary.BigEndian, uint16(size))
	encoder.Write(buffer.Bytes())
}

func decodeProperties(body []byte) (properties map[string]string, remainder []byte, err error) {
	remainder = body
	if len(body) < 2 {
		return // incomplete
	}
	reader := bytes.NewBuffer(body)
	var length uint16
	if err = binary.Read(reader, binary.BigEndian, &length); err != nil {
		return
	}
	if int(length)+2 > len(body) {
		return // incomplete
	}
	remainder = body[length+2:]
	if length == 0 {
		properties = map[string]string{}
		return
	}

	if body[length+1] != 0 {
		err = fmt.Errorf("Invalid properties (not NUL-terminated)")
		return
	}
	eachProp := bytes.Split(body[2:length+1], []byte{0})
	nProps := len(eachProp) / 2
	if nProps*2 != len(eachProp) {
		err = fmt.Errorf("Odd number of strings in properties")
		return
	}
	properties = make(map[string]string, nProps)
	for i := 0; i < len(eachProp); i += 2 {
		var key string
		if len(eachProp[i]) == 1 {
			index := int(eachProp[i][0]) - 1
			if index >= len(kSpecialProperties) {
				continue
			}
			key = kSpecialProperties[index]
		} else {
			key = string(eachProp[i])
		}
		value := string(eachProp[i+1])
		if _, exists := properties[key]; exists {
			err = fmt.Errorf("Duplicate property name")
			return
		}
		properties[key] = value
	}
	return
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
