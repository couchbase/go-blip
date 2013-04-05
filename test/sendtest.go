package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/snej/go-blip"
)

const kMessageInterval float64 = 0.000
const kNumToSend = 10000
const kMaxPending = 10000
const kMaxBodySize = 100000

const verbosity = 0
const profiling = false

// This program acts as a sender equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

var pendingResponses map[blip.MessageNumber]bool
var pendingCount, sentCount int
var mutex sync.Mutex

func addPending(request *blip.Message) int {
	mutex.Lock()
	defer mutex.Unlock()
	pendingResponses[request.SerialNumber()] = true
	pendingCount++
	sentCount++
	if verbosity > 0 {
		log.Printf(">>> Sending request: %s %#v +%dbytes (%d pending)",
			request, request.Properties, len(request.Body), pendingCount)
	}
	return pendingCount
}

func removePending(response *blip.Message) int {
	mutex.Lock()
	defer mutex.Unlock()
	if !pendingResponses[response.SerialNumber()] {
		panic("Wasn't expecting this response (response not pending)")
	}
	delete(pendingResponses, response.SerialNumber())
	pendingCount--
	if verbosity > 0 {
		log.Printf("<<< Response arrived: %s %#v +%dbytes (%d pending)",
			response, response.Properties, len(response.Body), pendingCount)
	}
	return pendingCount
}

func main() {
	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)
	log.Printf("Set GOMAXPROCS to %d", maxProcs)

	context := blip.NewContext()
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2
	sender, err := context.Dial("ws://localhost:12345/test", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	pendingResponses = map[blip.MessageNumber]bool{}

	log.Printf("Sending %d messages...", kNumToSend)

	if profiling {
		f, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.WriteHeapProfile(f)
	}

	for sentCount < kNumToSend {
		if sentCount%1000 == 0 {
			//log.Printf("Sent %d messages (backlog = %d)", sentCount, pendingCount)
		}
		request := blip.NewRequest()
		request.SetProfile("BLIPTest/EchoData")
		body := make([]byte, rand.Intn(kMaxBodySize))
		for i := 0; i < len(body); i++ {
			body[i] = byte(i % 256)
		}
		request.Body = body
		sender.Send(request)
		pending := addPending(request)

		go awaitResponse(request)

		//incomingRequests, incomingResponses, outgoingRequests, outgoingResponses := sender.Backlog()
		//log.Printf("Pending: %d ... %d / %d incoming ... %d / %d outgoing",
		//	pendingCount, incomingRequests, incomingResponses, outgoingRequests, outgoingResponses)

		if pending > kMaxPending {
			log.Printf("$$$$$$ Too much backlog! Hang on a sec $$$$$$\n\n")
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(time.Duration(kMessageInterval * float64(time.Second)))
		}
	}

	log.Printf("\n\nNow waiting for the rest of the responses...\n\n")

	for pendingCount > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	sender.Close()
}

func awaitResponse(request *blip.Message) {
	response := request.Response()
	removePending(response)

	if response.SerialNumber() != request.SerialNumber() {
		panic("Mismatched serial numbers")
	}
	if len(response.Body) != len(request.Body) {
		panic(fmt.Sprintf("Mismatched length in response body of %v (got %d, expected %d)",
			response, len(response.Body), len(request.Body)))
	}
	for i := len(response.Body) - 1; i >= 0; i-- {
		if response.Body[i] != request.Body[i] {
			panic("Mismatched data in response body")
		}
	}
}

//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
