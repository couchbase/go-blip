package blip

type MessageType uint16

const (
	RequestType     = MessageType(0)
	ResponseType    = MessageType(1)
	ErrorType       = MessageType(2)
	AckRequestType  = MessageType(4)
	AckResponseType = MessageType(5)
)

const BLIPErrorDomain = "BLIP"

type frameFlags uint8

const (
	kTypeMask   = frameFlags(0x07)
	kCompressed = frameFlags(0x08)
	kUrgent     = frameFlags(0x10)
	kNoReply    = frameFlags(0x20)
	kMoreComing = frameFlags(0x40)
	kMeta       = frameFlags(0x80)
)

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
