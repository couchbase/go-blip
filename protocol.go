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
	"fmt"
	"strings"
)

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

func FormatWebSocketSubProtocols(AppProtocolIds ...string) []string {
	formattedProtocols := make([]string, len(AppProtocolIds))
	for i, protocol := range AppProtocolIds {
		formattedProtocols[i] = NewWebSocketSubProtocol(protocol)
	}
	return formattedProtocols
}

// Create a new Websocket subprotocol using the given application protocol identifier.
func NewWebSocketSubProtocol(AppProtocolId string) string {
	return fmt.Sprintf("%s+%s", WebSocketSubProtocolPrefix, AppProtocolId)
}

// Extracts subprotocol from the above format
func ExtractAppProtocolId(protocol string) string {
	splitString := strings.SplitN(protocol, "+", 2)
	if len(splitString) == 2 {
		return splitString[1]
	}
	return ""
}
