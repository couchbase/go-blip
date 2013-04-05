package blip

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"sync"
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

var kSpecialPropertyEncoder map[string][]byte

func init() {
	kSpecialPropertyEncoder = make(map[string][]byte, len(kSpecialProperties))
	for i, prop := range kSpecialProperties {
		kSpecialPropertyEncoder[prop] = []byte{byte(i + 1)}
	}
}

type MessageNumber uint32

// A BLIP message. It could be a request or response or error, and it could be from me or the peer.
type Message struct {
	Outgoing     bool              // Is this a message created locally?
	Properties   map[string]string // The message's metadata, similar to HTTP headers.
	Body         []byte            // The message body. MIME type is defined by "Content-Type" property
	number       MessageNumber     // The sequence number of the message in the connection.
	flags        frameFlags        // Message flags as seen on the first frame.
	complete     bool              // Has this message been completely received?
	encoded      []byte            // Encoded props+body yet to be sent (or partially received)
	response     *Message          // Response to this message, if it's a request
	inResponseTo *Message          // Message this is a response to
	cond         *sync.Cond
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
		cond:       sync.NewCond(&sync.Mutex{}),
	}
}

func (message *Message) SerialNumber() MessageNumber {
	if message.number == 0 {
		panic("Unsent message has no serial number yet")
	}
	return message.number
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
	if !message.Outgoing || message.encoded != nil {
		panic("Message can't be modified")
	}
}

func (request *Message) Profile() string {
	return request.Properties["Profile"]
}

func (request *Message) SetProfile(profile string) {
	request.Properties["Profile"] = profile
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
		if request.number == 0 {
			panic("Can't get response before message has been sent")
		}
		if request.Outgoing {
			request.cond.L.Lock()
			defer request.cond.L.Unlock()
			for request.response == nil {
				request.cond.Wait()
			}
			response = request.response
		} else {
			response = request.createResponse()
			response.flags |= request.flags & kUrgent
			response.Properties = map[string]string{}
			request.response = response
		}
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

func newIncomingMessage(number MessageNumber, flags frameFlags) *Message {
	return &Message{
		flags:  flags | kMoreComing,
		number: number,
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

func (request *Message) createResponse() *Message {
	return &Message{
		flags:        frameFlags(ResponseType) | (request.flags & kUrgent) | kMoreComing,
		number:       request.number,
		Outgoing:     !request.Outgoing,
		inResponseTo: request,
		cond:         sync.NewCond(&sync.Mutex{}),
	}
}

func (request *Message) responseComplete(response *Message) {
	request.cond.L.Lock()
	defer request.cond.L.Unlock()
	if request.response != nil {
		panic(fmt.Sprintf("Multiple responses to %s", request))
	}
	request.response = response
	request.cond.Broadcast()
}

func (m *Message) nextFrameToSend(maxSize int) ([]byte, frameFlags) {
	if m.number == 0 || !m.Outgoing {
		panic("Can't send this message")
	}

	if m.encoded == nil {
		m.encoded = m.encode()
	}

	var frame []byte
	flags := m.flags
	lengthToWrite := len(m.encoded)
	if lengthToWrite > maxSize {
		frame = m.encoded[0:maxSize]
		m.encoded = m.encoded[maxSize:]
		flags |= kMoreComing
	} else {
		frame = m.encoded
		m.encoded = []byte{}
		flags &^= kMoreComing
	}
	return frame, flags
}

func (m *Message) receivedFrame(frame []byte, flags frameFlags) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.flags&kMoreComing == 0 {
		panic("Message was already complete")
	}

	// Frame types must match, except that a response may turn out to be an error:
	frameType := MessageType(flags & kTypeMask)
	curType := MessageType(m.flags & kTypeMask)
	if frameType != curType {
		if curType == ResponseType && frameType == ErrorType {
			m.flags = (m.flags &^ kTypeMask) | frameFlags(frameType)
		} else {
			return fmt.Errorf("Frame has wrong type (%x) for message (%x)", frameType, curType)
		}
	}

	if m.encoded == nil {
		m.encoded = frame
	} else {
		m.encoded = append(m.encoded, frame...)
	}

	if m.Properties == nil {
		// Try to extract the properties:
		var err error
		m.Properties, m.encoded, err = decodeProperties(m.encoded)
		if err != nil {
			return err
		}
	}

	if (flags & kMoreComing) == 0 {
		// After last frame, decode the body:
		m.flags &^= kMoreComing
		if m.Properties == nil {
			return fmt.Errorf("Missing or invalid properties")
		}
		if m.flags&kCompressed != 0 && len(m.encoded) > 0 {
			// Uncompress the body:
			unzipper, err := gzip.NewReader(bytes.NewBuffer(m.encoded))
			if err != nil {
				return err
			}
			m.Body, err = ioutil.ReadAll(unzipper)
			unzipper.Close()
			if err != nil {
				return err
			}
		} else {
			m.Body = m.encoded
		}
		m.encoded = nil
		m.complete = true
	}
	return nil
}

// Encodes the properties and body of a message.
func (m *Message) encode() []byte {
	// First convert the property strings into byte arrays, and add up their sizes:
	strings := make([][]byte, 2*len(m.Properties))
	i := 0
	size := 0
	for key, value := range m.Properties {
		strings[i] = encodeProperty(key)
		strings[i+1] = encodeProperty(value)
		size += len(strings[i]) + len(strings[i+1]) + 2
		i += 2
	}
	if size > 65535 {
		panic("Message properties too large")
	}

	encoder := bytes.NewBuffer(make([]byte, 0, 2+size+len(m.Body)))

	// Now write the size prefix and the null-terminated strings:
	binary.Write(encoder, binary.BigEndian, uint16(size))
	for _, str := range strings {
		encoder.Write(str)
		encoder.Write([]byte{0})
	}

	// Finally write the body:
	if m.flags&kCompressed != 0 && len(m.Body) > 0 {
		zipper := gzip.NewWriter(encoder)
		zipper.Write(m.Body)
		zipper.Close()
	} else {
		encoder.Write(m.Body)
	}
	return encoder.Bytes()
}

func encodeProperty(prop string) []byte {
	if encoded, found := kSpecialPropertyEncoder[prop]; found {
		return encoded
	} else {
		return []byte(prop)
	}
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
		key := decodeProperty(eachProp[i])
		value := decodeProperty(eachProp[i+1])
		if _, exists := properties[key]; exists {
			err = fmt.Errorf("Duplicate property name")
			return
		}
		properties[key] = value
	}
	return
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
