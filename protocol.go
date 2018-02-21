package blip

import "fmt"

// WebSocket [sub]protocol prefix for BLIP, used during WebSocket handshake.
// Client request must indicate that it supports this protocol, else the handshake will fail.
// The full websocket subprotocol will also have an identifier for the application layer protocol:
// <WebSocketSubProtocolPrefix>+<AppProtocolId>, eg BLIP_3+CBMobile_2.
// Every sub-protocol used by a caller should begin with this string.
const WebSocketSubProtocolPrefix = "BLIP_3"

// Domain used in errors returned by BLIP itself.
const BLIPErrorDomain = "BLIP"

//////// MESSAGE TYPE:

// Enumeration of the different types of messages in the BLIP protocol.
type MessageType uint16

const (
	RequestType     = MessageType(0) // A message initiated by a peer
	ResponseType    = MessageType(1) // A response to a Request
	ErrorType       = MessageType(2) // A response indicating failure
	AckRequestType  = MessageType(4) // Acknowledgement of data received from a Request (internal)
	AckResponseType = MessageType(5) // Acknowledgement of data received from a Response (internal)
)

var kMessageTypeName = [8]string{"MSG", "RPY", "ERR", "?4?", "ACK_MSG", "ACK_RPY", "?6?", "?7?"}

func (t MessageType) name() string {
	return kMessageTypeName[t]
}

// Returns true if a type is an Ack
func (t MessageType) isAck() bool {
	return t == AckRequestType || t == AckResponseType
}

// Maps a message type to the type of Ack to use
func (t MessageType) ackType() MessageType {
	switch t {
	case RequestType:
		return AckRequestType
	case ResponseType, ErrorType:
		return AckResponseType
	default:
		panic("Ack has no ackType")
	}
}

// Maps an Ack type to the message type it refers to
func (t MessageType) ackSourceType() MessageType {
	return t - 4
}

//////// FRAME FLAGS:

type frameFlags uint8

const (
	kTypeMask   = frameFlags(0x07)
	kCompressed = frameFlags(0x08)
	kUrgent     = frameFlags(0x10)
	kNoReply    = frameFlags(0x20)
	kMoreComing = frameFlags(0x40)
)

func (f frameFlags) messageType() MessageType {
	return MessageType(f & kTypeMask)
}

///////// HELPER UTILS:

// Create a new Websocket subprotocol using the given application protocol identifier.
func NewWebSocketSubProtocol(AppProtocolId string) string {
	return fmt.Sprintf("%s+%s", WebSocketSubProtocolPrefix, AppProtocolId)
}

//  Copyright (c) 2013 Jens Alfke. Copyright (c) 2015-2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
