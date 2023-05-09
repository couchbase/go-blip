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
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// A Message's serial number. Outgoing requests are numbered consecutively by the sending peer.
// A response has the same number as its request (i.e. the number was created by the other peer.)
type MessageNumber uint32

// A BLIP message. It could be a request or response or error, and it could be from me or the peer.
type Message struct {
	Outgoing     bool           // Is this a message created locally?
	Sender       *Sender        // The connection that sent this message.
	Properties   Properties     // The message's metadata, similar to HTTP headers.
	body         []byte         // The message body. MIME type is defined by "Content-Type" property
	bodyReader   *PipeReader    // Non-nil if incoming body is being streamed.
	bodyWriter   *PipeWriter    // Non-nil if incoming body is being streamed.
	bodyMutex    sync.Mutex     // Synchronizes access to body properties
	response     *Message       // The response to this outgoing request [must lock 'cond']
	inResponseTo *Message       // Outgoing request that this is a response to
	cond         *sync.Cond     // Used to make Response() method block until response arrives
	onResponse   func(*Message) // Application's callback to receive response
	onSent       func()         // Application's callback when message has been sent
	number       MessageNumber  // The sequence number of the message in the connection.
	flags        frameFlags     // Message flags as seen on the first frame.
	inProgress   bool           // True while message is being sent or received
}

// The error info from a BLIP response, as a Go Error value.
type ErrorResponse struct {
	Domain  string
	Code    int
	Message string
}

// A callback function that takes a message and returns nothing
type MessageCallback func(*Message)

var ErrConnectionClosed = fmt.Errorf("BLIP connection closed")

// Returns a string describing the message for debugging purposes.
// It contains the message number and type. A "!" indicates urgent, and "~" indicates compressed.
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
	message.assertSent()
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
func (message *Message) SetNoReply(noReply bool) {
	message.assertIsRequest()
	message.setFlag(kNoReply, noReply)
}

func (message *Message) setFlag(flag frameFlags, value bool) {
	message.assertMutable()
	if value {
		message.flags |= flag
	} else {
		message.flags &^= flag
	}
}

// Registers a callback that will be called when this message has been written to the socket.
// (This does not mean it's been delivered! If you need delivery confirmation, wait for a reply.)
func (message *Message) OnSent(callback func()) {
	message.assertOutgoing()
	message.onSent = callback
}

// The value of the "Profile" property which is used to identify a request's purpose.
func (message *Message) Profile() string {
	return message.Properties[ProfileProperty]
}

// Sets the value of the "Profile" property which is used to identify a request's purpose.
func (message *Message) SetProfile(profile string) {
	message.Properties[ProfileProperty] = profile
}

// True if a message is in its completed form: an incoming message whose entire body has
// arrived, or an outgoing message that's been queued for delivery.
func (message *Message) Complete() bool {
	return !message.inProgress
}

// Writes the encoded form of a Message to a stream.
func (message *Message) WriteEncodedTo(writer io.Writer) error {
	if err := message.Properties.WriteEncodedTo(writer); err != nil {
		return err
	}
	var err error
	if len(message.body) > 0 {
		_, err = writer.Write(message.body)
	}
	return err
}

//////// ERRORS:

// The error information in a response whose Type is ErrorType.
// (The return type `*ErrorResponse` implements the `error` interface.)
// If the message is not an error, returns nil.
func (response *Message) Error() *ErrorResponse {
	if response.Type() != ErrorType {
		return nil
	}
	domain := response.Properties[ErrorDomainProperty]
	if domain == "" {
		domain = BLIPErrorDomain
	}
	code, _ := strconv.ParseInt(response.Properties[ErrorCodeProperty], 10, 0)
	message, _ := response.Body()
	return &ErrorResponse{
		Domain:  domain,
		Code:    int(code),
		Message: string(message),
	}
}

// ErrorResponse implements the `error` interface.
func (err *ErrorResponse) Error() string {
	if err.Message == "" {
		return fmt.Sprintf("%s/%d", err.Domain, err.Code)
	} else {
		return fmt.Sprintf("%s (%s/%d)", err.Message, err.Domain, err.Code)
	}
}

// Changes a pending response into an error.
// It is safe (and a no-op) to call this on a nil Message.
func (response *Message) SetError(errDomain string, errCode int, message string) {
	if response != nil {
		response.assertMutable()
		response.setError(errDomain, errCode, message)
	}
}

func (response *Message) setError(errDomain string, errCode int, message string) {
	response.assertIsResponse()
	response.flags = (response.flags &^ kTypeMask) | frameFlags(ErrorType)
	response.Properties = Properties{
		ErrorDomainProperty: errDomain,
		ErrorCodeProperty:   fmt.Sprintf("%d", errCode),
	}
	response.body = []byte(message)
}

//////// BODY:

// Returns a Reader object from which the message body can be read.
// If this is an incoming message the body will be streamed as the message arrives over
// the network.
func (m *Message) BodyReader() (*PipeReader, error) {
	if m.Outgoing || m.body != nil {
		r, w := NewPipe()
		_, _ = w.Write(m.body)
		w.Close()
		return r, nil
	} else {
		return m.bodyReader, nil
	}
}

// Returns the entire message body as a byte array.
// If the message is incoming, blocks until the entire body is received.
func (m *Message) Body() ([]byte, error) {
	m.bodyMutex.Lock()
	defer m.bodyMutex.Unlock()
	if m.body == nil && !m.Outgoing {
		body, err := io.ReadAll(m.bodyReader)
		if err != nil {
			return nil, err
		}
		m.body = body
	}
	return m.body, nil
}

// Returns the message body parsed as JSON.
func (m *Message) ReadJSONBody(value interface{}) error {
	if bodyReader, err := m.BodyReader(); err != nil {
		return err
	} else {
		decoder := json.NewDecoder(bodyReader)
		decoder.UseNumber()
		return decoder.Decode(value)
	}
}

// Sets the entire body of an outgoing message.
func (m *Message) SetBody(body []byte) {
	m.assertMutable()
	m.body = body
}

// Sets the message body to JSON generated from the given JSON-encodable value.
// As a convenience this also sets the "Content-Type" property to "application/json".
func (m *Message) SetJSONBody(value interface{}) error {
	body, err := json.Marshal(value)
	if err == nil {
		m.SetJSONBodyAsBytes(body)
	}
	return err
}

// Sets the message body to JSON given by the byte slice.
// As a convenience this also sets the "Content-Type" property to "application/json".
func (m *Message) SetJSONBodyAsBytes(jsonBytes []byte) {
	m.SetBody(jsonBytes)
	m.Properties["Content-Type"] = "application/json"
	m.SetCompressed(true)
}

func (m *Message) bodySize() int {
	if m.body != nil {
		return len(m.body)
	} else {
		return m.bodyWriter.bytesPending()
	}
}

// Makes a copy of a Message. Only for tests.
func (message *Message) Clone() *Message {
	// Make sure the body is available. This may block.
	_, _ = message.Body()

	message.bodyMutex.Lock()
	defer message.bodyMutex.Unlock()
	return &Message{
		Outgoing:   message.Outgoing,
		Properties: message.Properties,
		body:       message.body,
		number:     message.number,
		flags:      message.flags,
	}
}

//////// RESPONSE HANDLING:

// Returns the response message to this request. Multiple calls return the same object.
// If called on a NoReply request, this returns nil.
//   - If this is an incoming request, the response is immediately available. Its properties and
//     body are initially empty and ready to be filled in.
//   - If this is an outgoing request, the function blocks until the response arrives over the
//     network.
func (request *Message) Response() *Message {
	request.assertIsRequest()
	request.assertSent()
	if request.flags&kNoReply != 0 {
		return nil
	} else if request.Outgoing {
		// Outgoing request: block until a response has been set by responseComplete
		request.cond.L.Lock()
		for request.response == nil {
			request.cond.Wait()
		}
		response := request.response
		request.cond.L.Unlock()
		return response
	} else {
		// Incoming request: create a response for the caller to fill in
		request.cond.L.Lock()
		defer request.cond.L.Unlock()
		// if we already have a response, return it
		if request.response != nil {
			return request.response
		}
		response := request.createResponse()
		response.flags |= request.flags & kUrgent
		response.Properties = Properties{}
		request.response = response
		return response
	}
}

// Registers a function to be called when a response to this outgoing Message arrives.
// This Message must be an outgoing request.
// Only one function can be registered; registering another causes a panic.
func (request *Message) OnResponse(callback func(*Message)) {
	request.assertIsRequest()
	request.assertOutgoing()
	precondition(request.flags&kNoReply == 0, "OnResponse: Message %s was sent NoReply", request)

	request.cond.L.Lock()
	response := request.response
	if response == nil {
		if request.onResponse != nil {
			request.cond.L.Unlock()
			panic("Message already has an OnResponse callback")
		}
		request.onResponse = callback
	}
	request.cond.L.Unlock()

	if response != nil {
		// Response has already arrived:
		callback(response)
	}
}

// Creates a response Message for this Message.
func (request *Message) createResponse() *Message {
	response := &Message{
		flags:        frameFlags(ResponseType) | (request.flags & kUrgent),
		number:       request.number,
		Outgoing:     !request.Outgoing,
		inResponseTo: request,
		inProgress:   request.Outgoing,
		cond:         sync.NewCond(&sync.Mutex{}),
	}
	if !response.Outgoing {
		response.flags |= kMoreComing
		response.bodyReader, response.bodyWriter = NewPipe()
	}
	return response
}

// Notifies an outgoing Message that its response is available.
// - This unblocks any waiting `Response` methods, which will now return the response.
// - It calls any registered "OnResponse" callback.
// - It sets the `response` field so subsequent calls to `Response` will return it.
func (request *Message) responseAvailable(response *Message) {
	var callback func(*Message)

	request.cond.L.Lock()
	if existing := request.response; existing != nil {
		request.cond.L.Unlock()
		if existing != response {
			panic(fmt.Sprintf("Multiple responses to %s", request))
		}
		return
	}
	request.response = response
	callback = request.onResponse
	request.onResponse = nil
	request.cond.Broadcast()
	request.cond.L.Unlock()

	if callback != nil {
		callback(response) // Calling on receiver thread; callback should exit quickly
	}
}

//////// SENDING MESSAGES:

// A wrapper around an outgoing Message that generates frame bodies to send.
type msgSender struct {
	*Message
	bytesSent           uint64
	bytesAcked          uint64
	remainingProperties []byte
	remainingBody       []byte
}

func (m *msgSender) nextFrameToSend(maxSize int) ([]byte, frameFlags) {
	m.assertOutgoing()
	m.assertSent()

	if m.remainingProperties == nil {
		// On the first call, encode the properties:
		m.inProgress = true
		m.remainingProperties = m.Properties.Encode()
		m.remainingBody = m.body
	}

	var frame []byte
	if plen := min(maxSize, len(m.remainingProperties)); plen > 0 {
		// Send properties first:
		frame = m.remainingProperties[0:plen]
		m.remainingProperties = m.remainingProperties[plen:]
		maxSize -= plen
	}

	if blen := min(maxSize, len(m.remainingBody)); blen > 0 {
		// Then the body:
		frame = append(frame, m.remainingBody[0:blen]...)
		m.remainingBody = m.remainingBody[blen:]
	}

	flags := m.flags
	if len(m.remainingProperties) > 0 || len(m.remainingBody) > 0 {
		flags |= kMoreComing
	}
	return frame, flags
}

func (m *msgSender) addBytesSent(bytesSent uint64) {
	m.bytesSent += bytesSent
}

func (m *msgSender) needsAck() bool {
	return m.bytesSent > m.bytesAcked+kMaxUnackedBytes
}

func (m *msgSender) receivedAck(bytesAcked uint64) {
	if bytesAcked > m.bytesAcked {
		m.bytesAcked = bytesAcked
	}
}

func (m *msgSender) sent() {
	if callback := m.onSent; callback != nil {
		m.onSent = nil
		callback()
	}
}

// Informs an outgoing message that the connection has closed
func (m *msgSender) cancelOutgoing() {
}

//////// RECEIVING MESSAGES:

// A wrapper around an incoming Message while it's being received.
type msgReceiver struct {
	*Message
	bytesWritten     uint64
	propertiesBuffer []byte
}

func newIncomingMessage(sender *Sender, number MessageNumber, flags frameFlags) *msgReceiver {
	m := &msgReceiver{
		Message: &Message{
			Sender:     sender,
			flags:      flags | kMoreComing,
			number:     number,
			inProgress: true,
			cond:       sync.NewCond(&sync.Mutex{}),
		},
	}
	m.bodyReader, m.bodyWriter = NewPipe()
	return m
}

type dispatchState struct {
	atStart bool // The beginning of the message has arrived
	atEnd   bool // The end of the message has arrived
}

// Appends a frame's data to an incoming message.
func (m *msgReceiver) addIncomingBytes(bytes []byte, complete bool) (dispatchState, error) {
	state := dispatchState{atEnd: complete}
	if m.Properties == nil {
		// First read the message properties:
		m.propertiesBuffer = append(m.propertiesBuffer, bytes...)
		props, bytesRead, err := ReadProperties(m.propertiesBuffer)
		if err != nil {
			return state, err
		} else if props == nil {
			if complete {
				return state, fmt.Errorf("incomplete properties in BLIP message")
			}
			return state, nil // incomplete properties; wait for more
		}
		// Got the complete properties:
		m.Properties = props
		bytes = m.propertiesBuffer[bytesRead:]
		m.propertiesBuffer = nil
		state.atStart = true
	}

	// Now add to the body:
	if complete {
		m.inProgress = false
		m.flags = m.flags &^ kMoreComing
	}
	if m.body != nil {
		m.body = append(m.body, bytes...)
	} else {
		_, _ = m.bodyWriter.Write(bytes)
		if complete {
			m.bodyWriter.Close()
		}
	}
	return state, nil
}

// Add `frameSize` to my `bytesWritten` and send an ACK message every `kAckInterval` bytes
func (m *msgReceiver) maybeSendAck(frameSize int) {
	oldWritten := m.bytesWritten
	m.bytesWritten += uint64(frameSize)
	if oldWritten > 0 && (oldWritten/kAckInterval) < (m.bytesWritten/kAckInterval) {
		m.Sender.sendAck(m.number, m.Type(), m.bytesWritten)
	}
}

// Informs an incoming message that the connection has closed
func (m *msgReceiver) cancelIncoming() {
	if m.inResponseTo != nil {
		if m.bodyWriter != nil {
			_ = m.bodyWriter.CloseWithError(ErrConnectionClosed)
		}
		m.setError(BLIPErrorDomain, DisconnectedCode, "Connection closed")
		m.inResponseTo.responseAvailable(m.Message)
	}
}

//////// UTILITIES

func (message *Message) assertMutable() {
	precondition(message.Outgoing && !message.inProgress, "Message %s is not modifiable", message)
}
func (message *Message) assertOutgoing() {
	precondition(message.Outgoing, "Message %s is not outgoing", message)
}
func (message *Message) assertIsRequest() {
	precondition(message.Type() == RequestType, "Message %s is not a request", message)
}
func (message *Message) assertIsResponse() {
	precondition(message.Type() != RequestType, "Message %s is not a response", message)
}
func (message *Message) assertSent() {
	precondition(message.number != 0, "Message %s has not been sent", message)
}
