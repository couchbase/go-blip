package main

import (
	"math/rand"
	"time"

	"github.com/snej/go-blip"
)

// This program acts as a sender equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

func main() {
	context := blip.NewContext()
	sender, err := context.Dial("ws://localhost:12345/test", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	for {
		request := blip.NewRequest()
		request.SetProfile("BLIPTest/EchoData")
		body := make([]byte, rand.Intn(100000))
		for i := 0; i < len(body); i++ {
			body[i] = byte(i % 256)
		}
		request.Body = body
		sender.Send(request)

		time.Sleep(100 * time.Millisecond)
	}

	sender.Close()
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
