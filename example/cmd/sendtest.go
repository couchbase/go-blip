package cmd

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/spf13/cobra"
)

// This program acts as a sender equivalent to the Objective-C one in MYNetwork's
// BLIPWebSocketTest.m.

const kMessageInterval float64 = 0.000 // Seconds to wait after sending each message
const kNumToSend = 10000               // Number of messages to send
const kMaxBodySize = 100000            // Max body size of each message
const kPercentCompressed = 0           // percentage of messages that are sent compressed
const kMaxSendQueueCount = 100
const kMaxPending = 10000

const profilingHeap = false
const profilingCPU = false

var pendingResponses map[blip.MessageNumber]bool
var pendingCount, sentCount int
var mutex sync.Mutex


func init() {
	RootCmd.AddCommand(sendCmd)
}

var sendCmd = &cobra.Command{
	Use:   "sender",
	Short: "Send blip requests",
	Long:  `Send blip requests`,
	Run: func(cmd *cobra.Command, args []string) {
		sender()
	},
}

func sender() {
	maxProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(maxProcs)
	log.Printf("Set GOMAXPROCS to %d", maxProcs)

	context := blip.NewContext(BlipExampleAppProtocolId)
	context.MaxSendQueueCount = kMaxSendQueueCount
	context.LogMessages = verbosity > 1
	context.LogFrames = verbosity > 2
	sender, err := context.Dial("ws://localhost:12345/test", "http://localhost")
	if err != nil {
		panic("Error opening WebSocket: " + err.Error())
	}

	pendingResponses = map[blip.MessageNumber]bool{}
	var totalBytesSent uint64 = 0
	var startTime = time.Now()

	log.Printf("Sending %d messages...", kNumToSend)

	if profilingHeap {
		log.Printf("Writing profile to file heap.pprof")
		f, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.WriteHeapProfile(f)
	} else if profilingCPU {
		log.Printf("Writing profile to file cpu.pprof")
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	for sentCount < kNumToSend {
		request := blip.NewRequest()
		request.SetProfile("BLIPTest/EchoData")
		request.Properties["Content-Type"] = "application/octet-stream"
		request.SetCompressed(rand.Intn(100) < kPercentCompressed)
		body := make([]byte, rand.Intn(kMaxBodySize))
		for i := 0; i < len(body); i++ {
			body[i] = byte(i % 256)
		}
		request.SetBody(body)
		sender.Send(request)
		totalBytesSent += uint64(len(body))
		pending := addPending(request)

		go awaitResponse(request)

		if sentCount%1000 == 0 {
			log.Printf("Sent %d messages (backlog = %d)", sentCount, getPendingCount())
		}

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

	log.Printf("\n\nNow waiting for the remaining %d responses...\n\n", getPendingCount())

	for getPendingCount() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Closing...")
	sender.Close()

	elapsed := float64(time.Now().Sub(startTime)) / float64(time.Second)
	log.Printf("Sent & received %d Mbytes in %.3f sec (%f megabytes/sec)",
		totalBytesSent/1000000, elapsed, float64(totalBytesSent)/elapsed/1.0e6)
}

func awaitResponse(request *blip.Message) {
	response := request.Response()
	removePending(response)

	if response.SerialNumber() != request.SerialNumber() {
		panic("Mismatched serial numbers")
	}
	body, err := response.Body()
	if err != nil {
		log.Printf("ERROR reading body of %s: %s", response, err)
		return
	}
	requestBody, _ := request.Body()
	if len(body) != len(requestBody) {
		panic(fmt.Sprintf("Mismatched length in response body of %v (got %d, expected %d)",
			response, len(body), len(requestBody)))
	}
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] != requestBody[i] {
			panic("Mismatched data in response body")
		}
	}
}

func addPending(request *blip.Message) int {
	mutex.Lock()
	defer mutex.Unlock()
	pendingResponses[request.SerialNumber()] = true
	pendingCount++
	sentCount++
	if verbosity > 0 {
		body, _ := request.Body()
		log.Printf(">>> Sending request: %s %#v +%dbytes (%d pending)",
			request, request.Properties, len(body), pendingCount)
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
		body, _ := response.Body()
		log.Printf("<<< Response arrived: %s %#v +%dbytes (%d pending)",
			response, response.Properties, len(body), pendingCount)
	}
	return pendingCount
}

func getPendingCount() int {
	mutex.Lock()
	defer mutex.Unlock()
	return pendingCount
}


//  Copyright (c) 2013 Jens Alfke.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
