package blip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"sync"
)

type MessageNumber uint32

// A BLIP message. It could be a request or response or error, and it could be from me or the peer.
type Message struct {
	Outgoing   bool          // Is this a message created locally?
	Sender     *Sender       // The connection that sent this message.
	Properties Properties    // The message's metadata, similar to HTTP headers.
	body       []byte        // The message body. MIME type is defined by "Content-Type" property
	number     MessageNumber // The sequence number of the message in the connection.
	flags      frameFlags    // Message flags as seen on the first frame.
	bytesSent  uint64
	bytesAcked uint64

	reader       io.Reader  // Stream that an incoming message is being read from
	encoder      io.Reader  // Stream that an outgoing message is being written to
	readingBody  bool       // True if reader stream has been accessed by client already
	complete     bool       // Has this message been completely received?
	response     *Message   // Response to this message, if it's a request
	inResponseTo *Message   // Message this is a response to
	cond         *sync.Cond // Used to make Response() method block until response arrives
}

// Returns a string describing the message for debugging purposes
func (message *Message) String() string {
	return frameString(message.number, message.flags)
}

func frameString(number MessageNumber, flags frameFlags) string {
	str := fmt.Sprintf("%s#%d", flags.messageType().name(), number)
	if flags&kUrgent != 0 {
		str += "!"
	}
	if flags&kCompressed != 0 {
		str += "~"
	}
	return str
}

// Creates a new outgoing request.
func NewRequest() *Message {
	return &Message{
		flags:      frameFlags(RequestType),
		Outgoing:   true,
		Properties: Properties{},
		cond:       sync.NewCond(&sync.Mutex{}),
	}
}

// The order in which a request message was sent.
// A response has the same serial number as its request even though it goes the other direction.
func (message *Message) SerialNumber() MessageNumber {
	if message.number == 0 {
		panic("Unsent message has no serial number yet")
	}
	return message.number
}

// The type of message: request, response or error
func (message *Message) Type() MessageType { return MessageType(message.flags.messageType()) }

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

// Enables GZIP compression of an outgoing message's body.
func (message *Message) SetCompressed(compressed bool) {
	if CompressionLevel != 0 {
		message.setFlag(kCompressed, compressed)
	}
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
	if !message.Outgoing || message.encoder != nil {
		panic("Message can't be modified")
	}
}

// Reads an incoming message's properties from the reader if necessary
func (m *Message) readProperties() error {
	if m.Properties != nil {
		return nil
	} else if m.reader == nil {
		panic("Message has no reader")
	}
	m.Properties = Properties{}
	return m.Properties.ReadFrom(m.reader)
}

// The value of the "Profile" property which is used to identify a request's purpose.
func (request *Message) Profile() string {
	return request.Properties["Profile"]
}

// Sets the value of the "Profile" property which is used to identify a request's purpose.
func (request *Message) SetProfile(profile string) {
	request.Properties["Profile"] = profile
}

// Returns a Reader object from which the message body can be read.
// If this is an incoming message the body will be streamed as the message arrives over
// the network (and multiple calls to BodyReader() won't work.)
func (m *Message) BodyReader() (io.Reader, error) {
	if m.Outgoing || m.body != nil {
		return bytes.NewReader(m.body), nil
	}
	if err := m.readProperties(); err != nil {
		return nil, err
	}
	m.readingBody = true
	return m.reader, nil
}

// Returns the entire message body as a byte array.
// If the message is incoming, blocks until the entire body is received.
func (m *Message) Body() ([]byte, error) {
	if m.body == nil && !m.Outgoing {
		if m.readingBody {
			panic("Already reading body as a stream")
		}
		body, err := ioutil.ReadAll(m.reader)
		if err != nil {
			return nil, err
		}
		m.body = body
	}
	return m.body, nil
}

// Sets the entire body of an outgoing message.
func (m *Message) SetBody(body []byte) {
	m.assertMutable()
	m.body = body
}

// Returns the message body parsed as JSON.
func (m *Message) ReadJSONBody(value interface{}) error {
	if bodyReader, err := m.BodyReader(); err != nil {
		return err
	} else {
		return json.NewDecoder(bodyReader).Decode(value)
	}
}

// Sets the message body to JSON generated from the given JSON-encodable value.
// As a convenience this also sets the "Content-Type" property to "application/json".
func (m *Message) SetJSONBody(value interface{}) error {
	body, err := json.Marshal(value)
	if err == nil {
		m.SetBody(body)
		m.Properties["Content-Type"] = "application/json"
		m.SetCompressed(true)
	}
	return err
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
			response.Properties = Properties{}
			request.response = response
		}
	}
	return response
}

// Changes a pending response into an error.
// It is safe (and a no-op) to call this on a nil Message.
func (response *Message) SetError(errDomain string, errCode int, message string) {
	if response != nil {
		response.assertMutable()
		if response.Type() == RequestType {
			panic("Can't call SetError on a request")
		}
		response.flags = (response.flags &^ kTypeMask) | frameFlags(ErrorType)
		response.Properties = Properties{
			"Error-Domain": errDomain,
			"Error-Code":   fmt.Sprintf("%d", errCode),
		}
		if message != "" {
			response.body = []byte(message)
		}
	}
}

//////// INTERNALS:

func newIncomingMessage(sender *Sender, number MessageNumber, flags frameFlags, reader io.Reader) *Message {
	return &Message{
		Sender: sender,
		flags:  flags | kMoreComing,
		number: number,
		reader: reader,
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

// Creates an incoming message given properties and body; exposed only for testing.
func NewParsedIncomingMessage(sender *Sender, msgType MessageType, properties Properties, body []byte) *Message {
	if properties == nil {
		properties = Properties{}
	}
	if body == nil {
		body = []byte{}
	}
	msg := newIncomingMessage(sender, 1, frameFlags(msgType), nil)
	msg.Properties = properties
	msg.body = body
	return msg
}

func (request *Message) createResponse() *Message {
	response := &Message{
		flags:        frameFlags(ResponseType) | (request.flags & kUrgent),
		number:       request.number,
		Outgoing:     !request.Outgoing,
		inResponseTo: request,
		cond:         sync.NewCond(&sync.Mutex{}),
	}
	if !response.Outgoing {
		response.flags |= kMoreComing
	}
	return response
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

//////// I/O:

func (m *Message) WriteTo(writer io.Writer) error {
	if err := m.Properties.WriteTo(writer); err != nil {
		return err
	}
	var err error
	if len(m.body) > 0 {
		_, err = writer.Write(m.body)
	}
	return err
}

func (m *Message) ReadFrom(reader io.Reader) error {
	if err := m.Properties.ReadFrom(reader); err != nil {
		return err
	}
	var err error
	m.body, err = ioutil.ReadAll(reader)
	return err
}

// Returns a write stream to write the incoming message's content into. When the stream is closed,
// the message will deliver itself.
func (m *Message) asyncRead(onComplete func(error)) io.WriteCloser {
	reader, writer := io.Pipe()
	m.reader = reader
	go func() {
		defer func() {
			if p := recover(); p != nil {
				log.Printf("PANIC in BLIP asyncRead: %v\n%s", p, debug.Stack())
			}
		}()

		err := m.ReadFrom(reader)
		onComplete(err)
	}()
	return writer
}

func (m *Message) nextFrameToSend(maxSize int) ([]byte, frameFlags) {
	if m.number == 0 || !m.Outgoing {
		panic("Can't send this message")
	}

	if m.encoder == nil {
		// Start the encoder goroutine:
		var writer io.WriteCloser
		m.encoder, writer = io.Pipe()
		go func() {
			defer func() {
				if p := recover(); p != nil {
					log.Printf("PANIC in BLIP nextFrameToSend: %v\n%s", p, debug.Stack())
				}
			}()

			_ = m.WriteTo(writer)
			_ = writer.Close()
		}()
	}

	frame := make([]byte, maxSize)
	flags := m.flags
	size, err := io.ReadFull(m.encoder, frame)
	if err == nil {
		flags |= kMoreComing
	} else {
		frame = frame[0:size]
		if err == io.ErrUnexpectedEOF {
			err = nil
		}
	}
	return frame, flags
}

//  Copyright (c) 2013 Jens Alfke. Copyright (c) 2015-2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
